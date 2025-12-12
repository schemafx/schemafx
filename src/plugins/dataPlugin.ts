import type { FastifyPluginAsyncZod } from 'fastify-type-provider-zod';
import { z } from 'zod';
import {
    type AppSchema,
    AppTableRowSchema,
    type Connector,
    TableQueryOptionsSchema
} from '../types.js';
import { tableQuerySchema } from '../utils/fastifyUtils.js';
import type { LRUCache } from 'lru-cache';
import type { FastifyReply } from 'fastify';
import { executeAction, handleDataRetriever } from '../utils/dataUtils.js';

export type DataPluginOptions = {
    connectors: Record<string, Connector>;
    getSchema: (appId: string) => Promise<AppSchema>;
    validatorCache: LRUCache<string, z.ZodType>;
    maxRecursiveDepth?: number;
    encryptionKey?: string;
};

const plugin: FastifyPluginAsyncZod<DataPluginOptions> = async (
    fastify,
    { connectors, getSchema, validatorCache, maxRecursiveDepth, encryptionKey }
) => {
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
            const { response, success, connector, table } = await handleTable(
                request.params.appId,
                request.params.tableId,
                reply
            );

            if (!success || !table) return response;

            let query;
            if (request.query.query) {
                query = JSON.parse(request.query.query);
                const parsed = await TableQueryOptionsSchema.parseAsync(query);
                query = parsed;
            }

            return handleDataRetriever(table, connector, query, encryptionKey);
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

            await executeAction({
                connector,
                table,
                actId: actionId,
                currentRows: rows,
                depth: 0,
                maxDepth: maxRecursiveDepth ?? 100,
                appId,
                validatorCache,
                encryptionKey
            });

            return handleDataRetriever(table, connector, undefined, encryptionKey);
        }
    );
};

export default plugin;
