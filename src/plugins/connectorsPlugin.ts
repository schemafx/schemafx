import type { FastifyPluginAsyncZod } from 'fastify-type-provider-zod';
import { z } from 'zod';
import { AppSchemaSchema, AppViewType, ConnectorTableSchema } from '../types.js';
import { validateTableKeys } from '../utils/schemaUtils.js';
import { ErrorResponseSchema } from '../utils/fastifyUtils.js';
import type { AppSchema } from '../types.js';
import { randomUUID, randomBytes } from 'node:crypto';
import type DataService from '../services/DataService.js';
import { LRUCache } from 'lru-cache';

const tokenCodeCache = new LRUCache<string, string>({
    max: 1000,
    ttl: 2 * 60 * 1000 // 2 minutes
});

function storeTokenCode(token: string): string {
    const code = randomBytes(32).toString('base64url');
    tokenCodeCache.set(code, token);

    return code;
}

function consumeTokenCode(code: string): string | undefined {
    const token = tokenCodeCache.get(code);
    if (token) tokenCodeCache.delete(code);

    return token;
}

const plugin: FastifyPluginAsyncZod<{
    dataService: DataService;
}> = async (fastify, { dataService }) => {
    fastify.get(
        '/token/:code',
        {
            schema: {
                params: z.object({
                    code: z.string().min(1).meta({ description: 'Token exchange code' })
                }),
                response: {
                    200: z.object({
                        token: z.string().meta({ description: 'JWT token' })
                    }),
                    404: ErrorResponseSchema
                }
            }
        },
        async (request, reply) => {
            const token = consumeTokenCode(request.params.code);
            if (token) return { token };

            return reply.code(404).send({
                error: 'Not Found',
                message: 'Token code not found or expired.'
            });
        }
    );

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
                                }),
                                supportsData: z.boolean().default(false).meta({
                                    description: 'Whether the connector supports getting data.'
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
                        requiresConnection: !!connector.authorize,
                        supportsData: !!connector.getData
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
            const connection = await dataService.setConnection(
                {
                    id: randomUUID(),
                    connector: connector.id,
                    name: authResult.name,
                    content: authResult.content
                },
                authResult.email
            );

            const response: { connectionId: string; code?: string } = {
                connectionId: connection.id
            };

            if (authResult.email) {
                const token = fastify.jwt.sign({ email: authResult.email }, { expiresIn: '8h' });
                response.code = storeTokenCode(token);
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

            return response;
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
                        code: z
                            .string()
                            .optional()
                            .meta({ description: 'Code to exchange for token' })
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
            const connection = await dataService.setConnection(
                {
                    id: randomUUID(),
                    connector: connector.id,
                    name: authResult.name,
                    content: authResult.content
                },
                authResult.email
            );

            const response: { connectionId: string; code?: string } = {
                connectionId: connection.id
            };

            if (authResult.email) {
                const token = fastify.jwt.sign({ email: authResult.email }, { expiresIn: '8h' });
                response.code = storeTokenCode(token);
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

                // Prevent adding the same table (same connector + path) twice
                const exists = schema.tables.some(
                    t =>
                        t.connector === table.connector &&
                        Array.isArray(t.path) &&
                        Array.isArray(table.path) &&
                        t.path.length === table.path.length &&
                        t.path.every((p, i) => p === table.path[i])
                );

                if (exists) {
                    return reply.code(400).send({
                        error: 'Bad Request',
                        message: 'Table already exists in application.'
                    });
                }

                schema.tables.push(table);
            } else {
                schema = {
                    id: randomUUID(),
                    name: 'New App',
                    tables: [table],
                    views: [
                        {
                            id: randomUUID(),
                            name: table.name,
                            tableId: table.id,
                            type: AppViewType.Table,
                            config: {
                                fields: table.fields.map(f => f.id)
                            }
                        }
                    ]
                };
            }

            return dataService.setSchema(schema, request.user?.email);
        }
    );
};

export default plugin;
