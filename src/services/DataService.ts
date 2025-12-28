import { LRUCache } from 'lru-cache';
import {
    AppActionType,
    type TableQueryOptions,
    type AppTable,
    type AppTableRow,
    type Connector,
    type AppSchema,
    AppSchemaSchema,
    QueryFilterOperator,
    AppConnectionSchema,
    AppConnection
} from '../types.js';
import type { z } from 'zod';
import { type AppTableFromZodOptions, tableFromZod, zodFromTable } from '../utils/zodUtils.js';
import {
    buildSQLQuery,
    convertDuckDBRowsToAppRows,
    createDuckDBInstance,
    ingestDataToDuckDB,
    ingestStreamToDuckDB
} from '../utils/duckdb.js';
import type { DuckDBValue } from '@duckdb/node-api';
import knex from 'knex';
import { extractKeys } from '../utils/schemaUtils.js';
import { decodeRow, encodeRow } from '../utils/dataUtils.js';
import { randomUUID } from 'crypto';

const qb = knex({ client: 'pg' });

type executeActionOptions = {
    table: AppTable;
    actId: string;
    rows: AppTableRow[];
    depth?: number;
};

export type DataServiceOptions = {
    schemaConnector: Omit<AppTableFromZodOptions, 'id' | 'name' | 'primaryKey'>;
    connectionsConnection?: string;
    connectionsConnector: Omit<
        AppTableFromZodOptions,
        'id' | 'name' | 'primaryKey' | 'connectionId'
    >;
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
    connectionsCacheOpts?: {
        max?: number;
        ttl?: number;
    };
};

export default class DataService {
    schemaCache: LRUCache<string, AppSchema>;
    connectionsCache: LRUCache<string, AppConnection>;
    validatorCache: LRUCache<string, z.ZodType>;

    schemaTable: AppTable;
    connectionsConnection?: string;
    connectionsTable: AppTable;

    connectors: Record<string, Connector> = {};
    encryptionKey?: string;
    maxRecursiveDepth: number;

    constructor({
        schemaCacheOpts,
        connectionsCacheOpts,
        validatorCacheOpts,

        schemaConnector,
        connectionsConnection,
        connectionsConnector,

        connectors,
        maxRecursiveDepth,
        encryptionKey
    }: DataServiceOptions) {
        this.schemaCache = new LRUCache<string, AppSchema>({
            max: schemaCacheOpts?.max ?? 100,
            ttl: schemaCacheOpts?.ttl ?? 1000 * 60 * 5 // 5 minutes TTL
        });

        this.connectionsCache = new LRUCache<string, AppConnection>({
            max: connectionsCacheOpts?.max ?? 100,
            ttl: connectionsCacheOpts?.ttl ?? 1000 * 60 * 5
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
            id: randomUUID(),
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

        this.connectionsConnection = connectionsConnection;
        this.connectionsTable = tableFromZod(AppConnectionSchema, {
            id: randomUUID(),
            name: '',
            primaryKey: 'id',
            ...connectionsConnector,
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

        this.connectionsTable.fields[
            this.connectionsTable.fields.findIndex(f => f.id === 'content')
        ].encrypted = true;

        this.encryptionKey = encryptionKey;
        this.maxRecursiveDepth = maxRecursiveDepth ?? 100;
    }

    async getConnection(connectionId?: string) {
        if (!connectionId) return;
        if (this.connectionsCache.has(connectionId)) {
            return this.connectionsCache.get(connectionId);
        }

        const connections = await this._getData(this.connectionsTable, this.connectionsConnection, {
            filters: [
                {
                    field: this.connectionsTable.fields.find(f => f.isKey)!.id,
                    operator: QueryFilterOperator.Equals,
                    value: connectionId
                }
            ],
            limit: 1
        });

        const connection = connections[0] as AppConnection | undefined;
        this.connectionsCache.set(connectionId, connection);
        return connection;
    }

    async getConnections() {
        return (await this._getData(
            this.connectionsTable,
            this.connectionsConnection
        )) as AppConnection[];
    }

    async setConnection(connection: AppConnection) {
        const existingConnection = await this.getConnection(connection.id);
        this.connectionsCache.set(connection.id, connection);

        this._executeAction({
            table: this.connectionsTable,
            auth: this.connectionsConnection,
            actId: existingConnection ? 'update' : 'add',
            rows: [connection]
        });

        return connection;
    }

    async deleteConnection(connectionId: string) {
        const connection = await this.getConnection(connectionId);
        if (!connection) return this.connectionsCache.delete(connectionId);

        this._executeAction({
            table: this.connectionsTable,
            auth: this.connectionsConnection,
            actId: 'delete',
            rows: [connection]
        });

        this.connectionsCache.delete(connectionId);
    }

    async getSchema(appId: string) {
        if (this.schemaCache.has(appId)) return this.schemaCache.get(appId);

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
        const hasSchema = await this.getSchema(schema.id);
        this.schemaCache.set(schema.id, schema);

        this.executeAction({
            table: this.schemaTable,
            actId: hasSchema ? 'update' : 'add',
            rows: [schema]
        });

        return schema;
    }

    async deleteSchema(appId: string) {
        const schema = await this.getSchema(appId);
        if (!schema) return this.schemaCache.delete(appId);

        this.executeAction({
            table: this.schemaTable,
            actId: 'delete',
            rows: [schema]
        });

        this.schemaCache.delete(appId);
    }

    private async _executeAction(
        opts: executeActionOptions & {
            auth?: string;
        }
    ) {
        const { table, auth, actId, rows, depth } = opts;

        if ((depth ?? 0) > this.maxRecursiveDepth) {
            throw new Error('Max recursion depth exceeded.');
        }

        const action = table.actions?.find(a => a.id === actId);
        if (!action) throw new Error(`Action ${actId} not found.`);
        const keyFields = table.fields.filter(f => f.isKey).map(f => f.id);

        switch (action.type) {
            case AppActionType.Add: {
                const validator = zodFromTable(table, this.validatorCache);
                for (const row of rows) {
                    await this.connectors[table.connector]?.addRow?.(
                        table,
                        auth,
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
                const validator = zodFromTable(table, this.validatorCache);
                for (const row of rows) {
                    const key = extractKeys(row, keyFields);
                    if (Object.keys(key).length === 0) continue;

                    await this.connectors[table.connector]?.updateRow?.(
                        table,
                        auth,
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

                    await this.connectors[table.connector]?.deleteRow?.(table, auth, key);
                }

                return;
            }

            case AppActionType.Process: {
                const subActions = (action.config?.actions as string[]) || [];

                for (const subActionId of subActions) {
                    await this._executeAction({
                        ...opts,
                        actId: subActionId,
                        depth: (depth ?? 0) + 1
                    });
                }

                return;
            }
        }
    }

    async executeAction(opts: executeActionOptions) {
        const connection = await this.getConnection(opts.table.connectionId);
        return this._executeAction({ ...opts, auth: connection?.content });
    }

    private async _getData(table: AppTable, auth?: string, query?: TableQueryOptions) {
        const connector = this.connectors[table.connector];
        if (!connector?.getData && !connector?.getDataStream) return [];

        const tempTableName = `t_${Math.random().toString(36).substring(7)}`;
        const dbInstance = await createDuckDBInstance();
        const connection = await dbInstance.connect();

        if (connector.getDataStream) {
            await ingestStreamToDuckDB(
                connection,
                await connector.getDataStream(table, auth),
                table,
                tempTableName
            );
        } else {
            await ingestDataToDuckDB(
                connection,
                await connector.getData!(table, auth),
                table,
                tempTableName
            );
        }

        const { sql, params } = buildSQLQuery(tempTableName, query || {});
        const reader = await connection.run(sql, params as unknown[] as DuckDBValue[]);
        const rows = await reader.getRows();

        const result = convertDuckDBRowsToAppRows(
            rows.map(row => {
                const obj: Record<string, unknown> = {};
                table.fields.forEach((field, index) => {
                    obj[field.id] = row[index];
                });

                return decodeRow(obj, table, this.encryptionKey);
            }),
            table
        );

        await connection.run(qb.schema.dropTableIfExists(tempTableName).toString());
        connection.closeSync();

        return result;
    }

    async getData(table: AppTable, query?: TableQueryOptions) {
        const connection = await this.getConnection(table.connectionId);
        return this._getData(table, connection?.content, query);
    }
}
