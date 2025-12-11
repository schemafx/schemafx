import type { FastifyPluginAsyncZod } from 'fastify-type-provider-zod';
import { z } from 'zod';
import {
    type AppSchema,
    type AppTableRow,
    AppTableRowSchema,
    TableQueryOptionsSchema,
    AppActionType,
    type Connector,
    type AppTable
} from '../types.js';
import { zodFromTable, extractKeys, tableQuerySchema } from '../utils/schemaUtils.js';
import {
    createDuckDBInstance,
    ingestStreamToDuckDB,
    buildSQLQuery,
    ingestDataToDuckDB
} from '../utils/duckdb.js';
import { LRUCache } from 'lru-cache';
import type { FastifyReply } from 'fastify';
import type { DuckDBValue } from '@duckdb/node-api';
import knex from 'knex';

const qb = knex({ client: 'pg' });

export type DataPluginOptions = {
    connectors: Record<string, Connector>;
    getSchema: (appId: string) => Promise<AppSchema>;
    validatorCache: LRUCache<string, z.ZodType>;
    maxRecursiveDepth?: number;
};

const plugin: FastifyPluginAsyncZod<DataPluginOptions> = async (
    fastify,
    { connectors, getSchema, validatorCache, maxRecursiveDepth }
) => {
    const MAX_DEPTH = maxRecursiveDepth ?? 100;

    async function handleTable(appId: string, tableId: string, reply: FastifyReply) {
        const schema = await getSchema(appId);
        const table = schema.tables.find(table => table.id === tableId);

        if (!table) {
            return {
                schema,
                table,
                response: reply.code(400).send({
                    error: 'Data Error',
                    message: 'Invalid table.'
                })
            };
        }

        const connectorName = table.connector;
        const connector = connectors[connectorName];

        if (!connector) {
            return {
                schema,
                table,
                connectorName,
                connector,
                response: reply.code(500).send({
                    error: 'Data Error',
                    message: 'Invalid connector.'
                })
            };
        }

        return { schema, table, connectorName, connector, success: true };
    }

    async function handleDataRetriever(table: AppTable, connector: Connector, queryStr?: string) {
        let query;
        if (queryStr) {
            query = JSON.parse(queryStr);
            const parsed = await TableQueryOptionsSchema.parseAsync(query);
            query = parsed;
        }

        if (!connector.getData && !connector.getDataStream) return [];

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

            return obj;
        });

        await connection.run(qb.schema.dropTableIfExists(tempTableName).toString());
        connection.closeSync();

        return result;
    }

    fastify.get(
        '/apps/:appId/data/:tableId',
        {
            onRequest: [fastify.authenticate],
            schema: tableQuerySchema
        },
        async (request, reply) => {
            const { response, success, connector, table } = await handleTable(
                request.params.appId,
                request.params.tableId,
                reply
            );

            if (!success || !table) return response;

            return handleDataRetriever(table, connector, request.query.query);
        }
    );

    fastify.post(
        '/apps/:appId/data/:tableId',
        {
            onRequest: [fastify.authenticate],
            schema: {
                body: z
                    .object({
                        actionId: z.string().min(1).meta({ description: 'Action ID' }),
                        rows: z
                            .array(AppTableRowSchema)
                            .optional()
                            .default([])
                            .meta({ description: 'Rows to perform action on' }),
                        payload: z.any().optional().meta({ description: 'Action payload' })
                    })
                    .meta({ description: 'Action execution request' }),
                ...tableQuerySchema
            }
        },
        async (request, reply) => {
            const { appId, tableId } = request.params;
            const { actionId, rows } = request.body;

            const { response, success, connector, table } = await handleTable(
                appId,
                tableId,
                reply
            );

            if (!success || !table || !connector) return response;

            const keyFields = table.fields.filter(f => f.isKey).map(f => f.id);

            async function executeAction(actId: string, currentRows: AppTableRow[], depth: number) {
                if (depth > MAX_DEPTH) {
                    throw new Error('Max recursion depth exceeded.');
                }

                const action = table!.actions?.find(a => a.id === actId);
                if (!action) throw new Error(`Action ${actId} not found.`);

                switch (action.type) {
                    case AppActionType.Add: {
                        const validator = zodFromTable(table!, appId, validatorCache);
                        for (const row of currentRows) {
                            const rowResult = await validator.parseAsync(row);
                            await connector?.addRow?.(table, rowResult as Record<string, unknown>);
                        }

                        return;
                    }
                    case AppActionType.Update: {
                        const validator = zodFromTable(table!, appId, validatorCache);
                        for (const row of currentRows) {
                            const rowData = await validator.parseAsync(row);
                            const key = extractKeys(row, keyFields);
                            if (Object.keys(key).length === 0) continue;

                            await connector?.updateRow?.(
                                table,
                                key,
                                rowData as Record<string, unknown>
                            );
                        }

                        return;
                    }
                    case AppActionType.Delete: {
                        for (const row of currentRows) {
                            const key = extractKeys(row, keyFields);
                            if (Object.keys(key).length === 0) continue;

                            await connector?.deleteRow?.(table, key);
                        }

                        return;
                    }
                    case AppActionType.Process: {
                        const subActions = (action.config?.actions as string[]) || [];

                        for (const subActionId of subActions) {
                            await executeAction(subActionId, currentRows, depth + 1);
                        }

                        return;
                    }
                }
            }

            await executeAction(actionId, rows, 0);
            return handleDataRetriever(table, connector);
        }
    );
};

export default plugin;
