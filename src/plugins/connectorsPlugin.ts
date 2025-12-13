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
    fastify.get(
        '/connectors',
        {
            schema: {
                response: {
                    200: z
                        .array(
                            z.object({
                                id: z.string().meta({ description: 'Connector ID' }),
                                name: z.string().meta({ description: 'Connector Name' }),
                                connection: z
                                    .object({
                                        id: z
                                            .string()
                                            .optional()
                                            .meta({ description: 'Connection Id' }),
                                        name: z
                                            .string()
                                            .optional()
                                            .meta({ description: 'Connection Name' })
                                    })
                                    .optional()
                                    .meta({ description: 'Connection details.' }),
                                requiresConnection: z.boolean().default(false).meta({
                                    description: 'Whether the connector must be reconnected.'
                                })
                            })
                        )
                        .meta({ description: 'List of available connectors' })
                }
            }
        },
        async () => {
            const connections = await dataService.getConnections();

            return Object.values(dataService.connectors)
                .map(connector => {
                    const base = {
                        id: connector.id,
                        name: connector.name,
                        requiresConnection: !!connector.authorize
                    };

                    return [
                        base,
                        ...connections
                            .filter(c => c.connector === connector.id)
                            .map(c => ({
                                ...base,
                                connection: {
                                    id: c.id,
                                    name: c.name
                                },
                                requiresConnection: false
                            }))
                    ];
                })
                .flat();
        }
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

    fastify.get(
        '/connectors/:connectorName/auth',
        {
            schema: {
                params: z.object({
                    connectorName: z.string().min(1).meta({ description: 'Name of the connector' })
                }),
                response: {
                    404: ErrorResponseSchema
                }
            }
        },
        async (request, reply) => {
            const connector = dataService.connectors[request.params.connectorName];

            if (!connector?.getAuthUrl) {
                return reply.code(404).send({
                    error: 'Not Found',
                    message: 'Connector not found.'
                });
            }

            return reply.redirect(await connector.getAuthUrl(), 302);
        }
    );

    fastify.get(
        '/connectors/:connectorName/auth/callback',
        {
            schema: {
                params: z.looseObject({
                    connectorName: z.string().min(1).meta({ description: 'Name of the connector' })
                }),
                response: {
                    200: z.object({ connectionId: z.string() }),
                    404: ErrorResponseSchema
                }
            }
        },
        async (request, reply) => {
            const connector = dataService.connectors[request.params.connectorName];

            if (!connector?.authorize) {
                return reply.code(404).send({
                    error: 'Not Found',
                    message: 'Connector not found.'
                });
            }

            const connection = await dataService.setConnection({
                id: randomUUID(),
                connector: connector.id,
                ...(await connector.authorize({ ...request.params }))
            });

            return reply.code(200).send({ connectionId: connection.id });
        }
    );

    fastify.post(
        '/connectors/:connectorName/auth',
        {
            schema: {
                params: z.object({
                    connectorName: z.string().min(1).meta({ description: 'Name of the connector' })
                }),
                body: z.looseObject({}),
                response: {
                    200: z.object({ connectionId: z.string() }),
                    404: ErrorResponseSchema
                }
            }
        },
        async (request, reply) => {
            const connector = dataService.connectors[request.params.connectorName];

            if (!connector?.authorize) {
                return reply.code(404).send({
                    error: 'Not Found',
                    message: 'Connector not found.'
                });
            }

            const connection = await dataService.setConnection({
                id: randomUUID(),
                connector: connector.id,
                ...(await connector.authorize({ ...request.body }))
            });

            return reply.code(200).send({ connectionId: connection.id });
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
                    connection: z.string().optional(),
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

            const { path, appId, connection } = request.body;
            const auth = await dataService.getConnection(connection);
            const table = await connector.getTable(path, auth?.content);
            table.connectionId = connection;
            validateTableKeys(table);

            let schema: AppSchema | undefined;
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

            return dataService.setSchema(schema);
        }
    );
};

export default plugin;
