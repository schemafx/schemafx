import type { FastifyPluginAsyncZod } from 'fastify-type-provider-zod';
import { z } from 'zod';
import {
    type AppSchema,
    type AppTableRow,
    AppTableRowSchema,
    TableQueryOptionsSchema,
    type TableQueryOptions,
    QueryFilterOperator,
    AppActionType,
    Connector
} from '../types.js';
import { zodFromTable, extractKeys, tableQuerySchema } from '../utils/schemaUtils.js';
import { LRUCache } from 'lru-cache';
import type { FastifyReply } from 'fastify';

export type DataPluginOptions = {
    connectors: Record<string, Connector>;
    getSchema: (appId: string) => Promise<AppSchema>;
    validatorCache: LRUCache<string, z.ZodTypeAny>;
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

    fastify.get(
        '/apps/:appId/data/:tableId',
        {
            onRequest: [fastify.authenticate],
            schema: tableQuerySchema
        },
        async (request, reply) => {
            const { appId, tableId } = request.params;
            const { query: queryStr } = request.query;

            const { response, success, connector, table } = await handleTable(
                appId,
                tableId,
                reply
            );
            if (!success || !table) return response;

            let query;
            if (queryStr) {
                try {
                    query = JSON.parse(queryStr);
                    const parsed = TableQueryOptionsSchema.safeParse(query);
                    if (!parsed.success) {
                        return reply.code(400).send({
                            error: 'Validation Error',
                            message: 'Invalid query parameters.',
                            details: parsed.error.issues.map(issue => ({
                                field: issue.path.join('.'),
                                message: issue.message,
                                code: issue.code
                            }))
                        });
                    }

                    query = parsed.data;
                } catch (err: unknown) {
                    reply.log.error(err);
                    return reply.code(400).send({
                        error: 'Validation Error',
                        message: 'Invalid JSON in query parameter.'
                    });
                }
            }

            let finalQuery: TableQueryOptions | undefined;
            let qMissingCaps: TableQueryOptions | undefined;

            if (query) {
                const capabilities = connector.getCapabilities
                    ? await connector.getCapabilities(table)
                    : {};

                if (query.filters?.length) {
                    let filterValid = true;
                    for (const filter of query.filters) {
                        if (capabilities.filter?.[filter.operator]) continue;

                        filterValid = false;
                        break;
                    }

                    if (filterValid) {
                        if (!finalQuery) finalQuery = {};
                        finalQuery.filters = query.filters;
                    } else {
                        if (!qMissingCaps) qMissingCaps = {};
                        qMissingCaps.filters = query.filters;
                    }
                }

                if (typeof query.limit === 'number') {
                    if (
                        // Cannot have limit from query if missing filters.
                        // Would otherwise make inaccurate limit.
                        !qMissingCaps?.filters &&
                        typeof capabilities.limit?.min === 'number' &&
                        typeof capabilities.limit?.max === 'number' &&
                        query.limit >= capabilities.limit.min &&
                        query.limit <= capabilities.limit.max
                    ) {
                        if (!finalQuery) finalQuery = {};
                        finalQuery.limit = query.limit;
                    } else {
                        if (!qMissingCaps) qMissingCaps = {};
                        qMissingCaps.limit = query.limit;
                    }
                }

                if (typeof query.offset === 'number') {
                    if (
                        // Cannot have offset from query if missing filters.
                        // Would otherwise make inaccurate offset.
                        !qMissingCaps?.filters &&
                        typeof capabilities.offset?.min === 'number' &&
                        typeof capabilities.offset?.max === 'number' &&
                        query.offset >= capabilities.offset.min &&
                        query.offset <= capabilities.offset.max
                    ) {
                        if (!finalQuery) finalQuery = {};
                        finalQuery.offset = query.offset;
                    } else {
                        if (!qMissingCaps) qMissingCaps = {};
                        qMissingCaps.offset = query.offset;
                    }
                }
            }

            let data = await connector.getData!(table, finalQuery);
            if (!qMissingCaps) return data;

            if (qMissingCaps.filters) {
                data = data.filter(row =>
                    qMissingCaps.filters!.every(filter => {
                        const rowValue = row[filter.field] as unknown;
                        switch (filter.operator) {
                            case QueryFilterOperator.Equals:
                                return rowValue === filter.value;
                            case QueryFilterOperator.NotEqual:
                                return rowValue !== filter.value;
                            case QueryFilterOperator.GreaterThan:
                                return (rowValue as number) > filter.value;
                            case QueryFilterOperator.GreaterThanOrEqualTo:
                                return (rowValue as number) >= filter.value;
                            case QueryFilterOperator.LowerThan:
                                return (rowValue as number) < filter.value;
                            case QueryFilterOperator.LowerThanOrEqualTo:
                                return (rowValue as number) <= filter.value;
                            case QueryFilterOperator.Contains:
                                return String(rowValue).includes(String(filter.value));
                            default:
                                return true;
                        }
                    })
                );
            }

            if (typeof qMissingCaps.offset === 'number') data = data.slice(qMissingCaps.offset);
            if (typeof qMissingCaps.limit === 'number') data = data.slice(0, qMissingCaps.limit);
            return data;
        }
    );

    fastify.post(
        '/apps/:appId/data/:tableId',
        {
            onRequest: [fastify.authenticate],
            schema: {
                body: z.object({
                    actionId: z.string().min(1),
                    rows: z.array(AppTableRowSchema).optional().default([]),
                    payload: z.any().optional()
                }),
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

            async function executeAction(
                actId: string,
                currentRows: AppTableRow[],
                depth: number
            ): Promise<unknown> {
                if (depth > MAX_DEPTH) {
                    throw new Error('Max recursion depth exceeded.');
                }

                const action = table!.actions?.find(a => a.id === actId);
                if (!action) throw new Error(`Action ${actId} not found.`);

                switch (action.type) {
                    case AppActionType.Add: {
                        const validator = zodFromTable(table!, appId, validatorCache);
                        for (const row of currentRows) {
                            const rowResult = validator.safeParse(row);
                            if (!rowResult.success) {
                                throw new Error(
                                    `Validation Error: ${JSON.stringify(rowResult.error.issues)}`
                                );
                            }

                            await connector?.addRow?.(
                                table,
                                rowResult.data as Record<string, unknown>
                            );
                        }

                        return connector?.getData?.(table) || [];
                    }
                    case AppActionType.Update: {
                        const validator = zodFromTable(table!, appId, validatorCache);
                        for (const row of currentRows) {
                            const rowResult = validator.safeParse(row);
                            if (!rowResult.success) {
                                throw new Error(
                                    `Validation Error: ${JSON.stringify(rowResult.error.issues)}`
                                );
                            }

                            const key = extractKeys(row, keyFields);
                            if (Object.keys(key).length === 0) continue;

                            await connector?.updateRow?.(
                                table,
                                key,
                                rowResult.data as Record<string, unknown>
                            );
                        }

                        return connector?.getData?.(table) || [];
                    }
                    case AppActionType.Delete: {
                        for (const row of currentRows) {
                            const key = extractKeys(row, keyFields);
                            if (Object.keys(key).length === 0) continue;

                            await connector?.deleteRow?.(table, key);
                        }

                        return connector?.getData?.(table) || [];
                    }
                    case AppActionType.Process: {
                        const subActions = (action.config?.actions as string[]) || [];
                        let lastResult;

                        for (const subActionId of subActions) {
                            lastResult = await executeAction(subActionId, currentRows, depth + 1);
                        }

                        return lastResult;
                    }
                    default:
                        return null;
                }
            }

            try {
                return executeAction(actionId, rows, 0);
            } catch (error: unknown) {
                return reply.code(400).send({
                    error: 'Action Error',
                    message: (error as Error).message
                });
            }
        }
    );
};

export default plugin;
