import type { FastifyPluginAsyncZod } from 'fastify-type-provider-zod';
import { z } from 'zod';
import { PermissionLevel, PermissionTargetType } from '../types.js';
import { ErrorResponseSchema } from '../utils/fastifyUtils.js';
import type DataService from '../services/DataService.js';
import { randomUUID } from 'node:crypto';

const plugin: FastifyPluginAsyncZod<{
    dataService: DataService;
}> = async (fastify, { dataService }) => {
    fastify.post(
        '/login/:connectorName',
        {
            schema: {
                params: z.object({
                    connectorName: z.string().min(1).meta({ description: 'Name of the connector' })
                }),
                body: z.looseObject({}),
                response: {
                    200: z.object({
                        token: z.string().meta({ description: 'JWT Token' }),
                        connectionId: z.string().meta({ description: 'Connection ID' })
                    }),
                    401: ErrorResponseSchema,
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

            if (!authResult.email) {
                return reply.code(401).send({
                    error: 'Unauthorized',
                    message: 'Connector did not provide user identity.'
                });
            }

            const connection = await dataService.setConnection({
                id: randomUUID(),
                connector: connector.id,
                name: authResult.name,
                content: authResult.content
            });

            // Grant admin permission on the connection to the creator
            await dataService.setPermission({
                id: randomUUID(),
                targetType: PermissionTargetType.Connection,
                targetId: connection.id,
                email: authResult.email,
                level: PermissionLevel.Admin
            });

            return {
                token: fastify.jwt.sign({ email: authResult.email }, { expiresIn: '8h' }),
                connectionId: connection.id
            };
        }
    );
};

export default plugin;
