import type { FastifyPluginAsyncZod } from 'fastify-type-provider-zod';
import { z } from 'zod';
import { AppSchemaSchema, ConnectorTableSchema } from '../types.js';
import { validateTableKeys } from '../utils/schemaUtils.js';
import { ErrorResponseSchema } from '../utils/fastifyUtils.js';
import type { AppSchema } from '../types.js';
import { randomUUID, randomBytes } from 'node:crypto';
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
                    path: z.array(z.string()).meta({ description: 'Path to query tables from' }),
                    connectionId: z.string().optional()
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

            return connector.listTables(
                request.body.path,
                (await dataService.getConnection(request.body.connectionId))?.content
            );
        }
    );

    fastify.get(
        '/connectors/:connectorName/auth',
        {
            schema: {
                params: z.object({
                    connectorName: z.string().min(1).meta({ description: 'Name of the connector' })
                }),
                querystring: z.object({
                    redirectUri: z
                        .string()
                        .min(1)
                        .optional()
                        .meta({ description: 'Redirect Uri after login.' })
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

            const url = new URL(await connector.getAuthUrl());
            if (Object.keys(request.query).length > 0) {
                url.searchParams.set(
                    'state',
                    Buffer.from(JSON.stringify({ ...request.query })).toString('base64url')
                );
            }

            return reply
                .header('Cross-Origin-Opener-Policy', 'same-origin-allow-popups')
                .redirect(url.href, 302);
        }
    );

    fastify.get(
        '/connectors/:connectorName/auth/callback',
        {
            schema: {
                params: z.object({
                    connectorName: z.string().min(1).meta({ description: 'Name of the connector' })
                }),
                querystring: z.looseObject({}),
                response: {
                    200: z.any(),
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

            const { state, ...query } = request.query;
            const authResult = await connector.authorize({ ...query });
            const connection = await dataService.setConnection({
                id: randomUUID(),
                connector: connector.id,
                name: authResult.name,
                content: authResult.content
            });

            const response: { connectionId: string; token?: string } = {
                connectionId: connection.id
            };

            if (authResult.email) {
                response.token = fastify.jwt.sign({ email: authResult.email }, { expiresIn: '8h' });
            }

            try {
                if (state) {
                    const stateData = JSON.parse(
                        Buffer.from(state as string, 'base64url').toString()
                    );

                    if (stateData.redirectUri) {
                        const redirect = new URL(stateData.redirectUri as string);
                        for (const [k, v] of Object.entries(response)) {
                            redirect.searchParams.set(k, v);
                        }

                        return reply.redirect(redirect.href, 302);
                    }
                }
            } catch {}

            const nonce = randomBytes(16).toString('base64');
            return reply
                .header('Content-Security-Policy', `script-src 'self' 'nonce-${nonce}'`)
                .header('Cross-Origin-Opener-Policy', 'same-origin-allow-popups')
                .type('text/html').send(`<!doctype html>
                    <html>
                        <head>
                            <title>Auth Complete</title>
                            <script nonce="${nonce}">
                                if (window.opener) window.opener.postMessage(JSON.stringify(${JSON.stringify(response)}), '*');
                                else new BroadcastChannel('auth_channel').postMessage(JSON.stringify(${JSON.stringify(response)}));
                                window.close();
                            </script>
                        </head>
                        <body><p>Authentication successful!</p></body>
                    </html>
                `);
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
                    200: z.object({
                        connectionId: z.string(),
                        token: z
                            .string()
                            .optional()
                            .meta({ description: 'JWT token if email is provided by connector' })
                    }),
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

            const authResult = await connector.authorize({ ...request.body });
            const connection = await dataService.setConnection({
                id: randomUUID(),
                connector: connector.id,
                name: authResult.name,
                content: authResult.content
            });

            const response: { connectionId: string; token?: string } = {
                connectionId: connection.id
            };

            if (authResult.email) {
                response.token = fastify.jwt.sign({ email: authResult.email }, { expiresIn: '8h' });
            }

            return reply.code(200).send(response);
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
                    connectionId: z.string().optional(),
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

            const { path, appId, connectionId } = request.body;
            const auth = await dataService.getConnection(connectionId);
            const table = await connector.getTable(path, auth?.content);

            if (!table) {
                return reply.code(404).send({
                    error: 'Not Found',
                    message: 'Application not found.'
                });
            }

            table.connectionId = connectionId;
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
