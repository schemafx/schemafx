import type { FastifyPluginAsyncZod } from 'fastify-type-provider-zod';
import { z } from 'zod';
import { AppSchemaSchema, ConnectorTableSchema } from '../types.js';
import { validateTableKeys } from '../utils/schemaUtils.js';
import { ErrorResponseSchema } from '../utils/fastifyUtils.js';
import type { AppSchema } from '../types.js';
import { randomUUID } from 'node:crypto';
import type DataService from '../services/DataService.js';

const plugin: FastifyPluginAsyncZod<{
    dataService: DataService;
}> = async (fastify, { dataService }) => {
    const _connectors = Object.values(dataService.connectors).map(connector => ({
        id: connector.id,
        name: connector.name
    }));

    fastify.get(
        '/connectors',
        {
            schema: {
                response: {
                    200: z
                        .array(
                            z.object({
                                id: z.string().meta({ description: 'Connector ID' }),
                                name: z.string().meta({ description: 'Connector Name' })
                            })
                        )
                        .meta({ description: 'List of available connectors' })
                }
            }
        },
        async () => _connectors
    );

    fastify.post(
        '/connectors/:connectorName/query',
        {
            onRequest: [fastify.authenticate],
            schema: {
                params: z.object({
                    connectorName: z.string().min(1).meta({ description: 'Name of the connector' })
                }),
                body: z.object({
                    path: z.array(z.string()).meta({ description: 'Path to query tables from' })
                }),
                response: {
                    200: z
                        .array(ConnectorTableSchema)
                        .meta({ description: 'List of tables found' }),
                    404: ErrorResponseSchema,
                    400: ErrorResponseSchema
                }
            }
        },
        async (request, reply) => {
            const connector = dataService.connectors[request.params.connectorName];

            if (!connector) {
                return reply.code(404).send({
                    error: 'Not Found',
                    message: 'Connector not found.'
                });
            }

            return connector.listTables(request.body.path);
        }
    );

    fastify.post(
        '/connectors/:connectorName/table',
        {
            onRequest: [fastify.authenticate],
            schema: {
                params: z.object({
                    connectorName: z.string().min(1).meta({ description: 'Name of the connector' })
                }),
                body: z.object({
                    path: z.array(z.string()).meta({ description: 'Path to the table' }),
                    appId: z
                        .string()
                        .min(1)
                        .optional()
                        .meta({ description: 'Application ID to add the table to' })
                }),
                response: {
                    200: AppSchemaSchema,
                    404: ErrorResponseSchema,
                    400: ErrorResponseSchema
                }
            }
        },
        async (request, reply) => {
            const connector = dataService.connectors[request.params.connectorName];

            if (!connector) {
                return reply.code(404).send({
                    error: 'Not Found',
                    message: 'Connector not found.'
                });
            }

            const { path, appId } = request.body;
            const table = await connector.getTable(path);
            validateTableKeys(table);

            let schema: AppSchema;
            if (appId) {
                schema = await dataService.getSchema(appId);

                if (!schema) {
                    return reply.code(404).send({
                        error: 'Not Found',
                        message: 'Application not found.'
                    });
                }

                schema.tables.push(table);
            } else {
                schema = {
                    id: randomUUID(),
                    name: 'New App',
                    tables: [table],
                    views: []
                };
            }

            return dataService.setSchema(schema.id, schema);
        }
    );
};

export default plugin;
