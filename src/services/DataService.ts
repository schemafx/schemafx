import { LRUCache } from 'lru-cache';
import {
    AppActionType,
    type TableQueryOptions,
    type AppTable,
    type AppTableRow,
    type Connector,
    type AppSchema,
    AppSchemaSchema,
    QueryFilterOperator
} from '../types.js';
import type { z } from 'zod';
import { type AppTableFromZodOptions, tableFromZod, zodFromTable } from '../utils/zodUtils.js';
import {
    buildSQLQuery,
    createDuckDBInstance,
    ingestDataToDuckDB,
    ingestStreamToDuckDB
} from '../utils/duckdb.js';
import type { DuckDBValue } from '@duckdb/node-api';
import knex from 'knex';
import { extractKeys } from '../utils/schemaUtils.js';
import { decodeRow, encodeRow } from '../utils/dataUtils.js';

const qb = knex({ client: 'pg' });

export type DataServiceOptions = {
    schemaConnector: Omit<AppTableFromZodOptions, 'id' | 'name' | 'primaryKey'>;
    connectors: Connector[];
    encryptionKey?: string;
    maxRecursiveDepth?: number;
    validatorCacheOpts?: {
        max?: number;
        ttl?: number;
    };
    schemaCacheOpts?: {
        max?: number;
        ttl?: number;
    };
};

export default class DataService {
    schemaCache: LRUCache<string, AppSchema>;
    validatorCache: LRUCache<string, z.ZodType>;

    schemaTable: AppTable;

    connectors: Record<string, Connector> = {};
    encryptionKey?: string;
    maxRecursiveDepth: number;

    constructor({
        schemaCacheOpts,
        validatorCacheOpts,

        schemaConnector,

        connectors,
        maxRecursiveDepth,
        encryptionKey
    }: DataServiceOptions) {
        this.schemaCache = new LRUCache<string, AppSchema>({
            max: schemaCacheOpts?.max ?? 100,
            ttl: schemaCacheOpts?.ttl ?? 1000 * 60 * 5 // 5 minutes TTL
        });

        this.validatorCache = new LRUCache<string, z.ZodType>({
            max: validatorCacheOpts?.max ?? 500,
            ttl: validatorCacheOpts?.ttl ?? 1000 * 60 * 60
        });

        for (const connector of connectors) {
            if (this.connectors[connector.id]) {
                throw new Error(`Duplicated connector "${connector.id}".`);
            }

            this.connectors[connector.id] = connector;
        }

        this.schemaTable = tableFromZod(AppSchemaSchema, {
            id: '',
            name: '',
            primaryKey: 'id',
            ...schemaConnector,
            actions: [
                {
                    id: 'add',
                    name: '',
                    type: AppActionType.Add
                },
                {
                    id: 'update',
                    name: '',
                    type: AppActionType.Update
                },
                {
                    id: 'delete',
                    name: '',
                    type: AppActionType.Delete
                }
            ]
        });

        this.encryptionKey = encryptionKey;
        this.maxRecursiveDepth = maxRecursiveDepth ?? 100;
    }

    async getSchema(appId: string) {
        if (this.schemaCache.has(appId)) return this.schemaCache.get(appId)!;

        const schemas = await this.getData(this.schemaTable, {
            filters: [
                {
                    field: this.schemaTable.fields.find(f => f.isKey)!.id,
                    operator: QueryFilterOperator.Equals,
                    value: appId
                }
            ],
            limit: 1
        });

        const schema = schemas[0] as AppSchema | undefined;
        this.schemaCache.set(appId, schema);
        return schema;
    }

    async setSchema(schema: AppSchema) {
        this.schemaCache.set(schema.id, schema);

        this.executeAction({
            appId: schema.id,
            table: this.schemaTable,
            actId: 'update',
            rows: [schema]
        });

        return schema;
    }

    async deleteSchema(appId: string) {
        const schema = await this.getSchema(appId);
        if (!schema) return this.schemaCache.delete(appId);

        this.executeAction({
            appId,
            table: this.schemaTable,
            actId: 'delete',
            rows: [schema]
        });

        this.schemaCache.delete(appId);
    }

    async executeAction(opts: {
        appId: string;
        table: AppTable;
        actId: string;
        rows: AppTableRow[];
        depth?: number;
    }) {
        const { appId, table, actId, rows, depth } = opts;

        if ((depth ?? 0) > this.maxRecursiveDepth) {
            throw new Error('Max recursion depth exceeded.');
        }

        const action = table.actions?.find(a => a.id === actId);
        if (!action) throw new Error(`Action ${actId} not found.`);
        const keyFields = table.fields.filter(f => f.isKey).map(f => f.id);

        switch (action.type) {
            case AppActionType.Add: {
                const validator = zodFromTable(table, appId, this.validatorCache);
                for (const row of rows) {
                    await this.connectors[table.connector]?.addRow?.(
                        table,
                        encodeRow(
                            (await validator.parseAsync(row)) as Record<string, unknown>,
                            table,
                            this.encryptionKey
                        )
                    );
                }

                return;
            }

            case AppActionType.Update: {
                const validator = zodFromTable(table, appId, this.validatorCache);
                for (const row of rows) {
                    const key = extractKeys(row, keyFields);
                    if (Object.keys(key).length === 0) continue;

                    await this.connectors[table.connector]?.updateRow?.(
                        table,
                        key,
                        encodeRow(
                            (await validator.parseAsync(row)) as Record<string, unknown>,
                            table,
                            this.encryptionKey
                        )
                    );
                }

                return;
            }

            case AppActionType.Delete: {
                for (const row of rows) {
                    const key = extractKeys(row, keyFields);
                    if (Object.keys(key).length === 0) continue;

                    await this.connectors[table.connector]?.deleteRow?.(table, key);
                }

                return;
            }

            case AppActionType.Process: {
                const subActions = (action.config?.actions as string[]) || [];

                for (const subActionId of subActions) {
                    await this.executeAction({
                        ...opts,
                        actId: subActionId,
                        depth: (depth ?? 0) + 1
                    });
                }

                return;
            }
        }
    }

    async getData(table: AppTable, query?: TableQueryOptions) {
        const connector = this.connectors[table.connector];
        if (!connector?.getData && !connector?.getDataStream) return [];

        const tempTableName = `t_${Math.random().toString(36).substring(7)}`;
        const dbInstance = await createDuckDBInstance();
        const connection = await dbInstance.connect();

        if (connector.getDataStream) {
            await ingestStreamToDuckDB(
                connection,
                await connector.getDataStream(table),
                table,
                tempTableName
            );
        } else {
            await ingestDataToDuckDB(
                connection,
                await connector.getData!(table),
                table,
                tempTableName
            );
        }

        const { sql, params } = buildSQLQuery(tempTableName, query || {});
        const reader = await connection.run(sql, params as unknown[] as DuckDBValue[]);
        const rows = await reader.getRows();

        const result = rows.map(row => {
            const obj: Record<string, unknown> = {};
            table.fields.forEach((field, index) => {
                obj[field.id] = row[index];
            });

            return decodeRow(obj, table, this.encryptionKey);
        });

        await connection.run(qb.schema.dropTableIfExists(tempTableName).toString());
        connection.closeSync();

        return result;
    }
}
