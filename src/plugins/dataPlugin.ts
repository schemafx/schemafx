import type { FastifyPluginAsyncZod } from 'fastify-type-provider-zod';
import { z } from 'zod';
import { AppTableRowSchema, TableQueryOptionsSchema } from '../types.js';
import { tableQuerySchema } from '../utils/fastifyUtils.js';
import type { FastifyReply } from 'fastify';
import type DataService from '../services/DataService.js';

const plugin: FastifyPluginAsyncZod<{
    dataService: DataService;
}> = async (fastify, { dataService }) => {
    async function handleTable(appId: string, tableId: string, reply: FastifyReply) {
        const schema = await dataService.getSchema(appId);

        if (!schema) {
            return {
                schema,
                response: reply.code(404).send({
                    error: 'Not Found',
                    message: 'Application not found.'
                })
            };
        }

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
        const connector = dataService.connectors[connectorName];

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
            const { response, success, table } = await handleTable(
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

            return dataService.getData(table, query);
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

            await dataService.executeAction({
                table,
                actId: actionId,
                rows,
                appId
            });

            return dataService.getData(table);
        }
    );
};

export default plugin;
