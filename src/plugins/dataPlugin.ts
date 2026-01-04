import type { FastifyPluginAsyncZod } from 'fastify-type-provider-zod';
import { z } from 'zod';
import {
    AppTableRowSchema,
    TableQueryOptionsSchema,
    PermissionTargetType,
    PermissionLevel,
    type AppSchema,
    type AppTable,
    type Connector
} from '../types.js';
import { tableQuerySchema, ErrorResponseSchema } from '../utils/fastifyUtils.js';
import type { FastifyReply, FastifyRequest } from 'fastify';
import type DataService from '../services/DataService.js';

const plugin: FastifyPluginAsyncZod<{
    dataService: DataService;
}> = async (fastify, { dataService }) => {
    /**
     * Check if the user has the required permission level for an app.
     * Returns an error response if not authorized, undefined if authorized.
     */
    async function checkAppPermission(
        request: FastifyRequest,
        reply: FastifyReply,
        appId: string,
        requiredLevel: PermissionLevel
    ): Promise<{ error: true; response: unknown } | undefined> {
        const email = request.user?.email;

        if (!email) {
            return {
                error: true,
                response: reply.code(401).send({
                    error: 'Unauthorized',
                    message: 'Authentication required.'
                })
            };
        }

        const hasAccess = await dataService.hasPermission(
            { targetType: PermissionTargetType.App, targetId: appId },
            email,
            requiredLevel
        );

        if (!hasAccess) {
            return {
                error: true,
                response: reply.code(403).send({
                    error: 'Forbidden',
                    message: `You do not have ${requiredLevel} permission for this application.`
                })
            };
        }

        return undefined;
    }

    async function handleTable(
        appId: string,
        tableId: string,
        reply: FastifyReply
    ): Promise<
        | { success: false; response: unknown }
        | {
              success: true;
              schema: AppSchema;
              table: AppTable;
              connectorName: string;
              connector: Connector;
          }
    > {
        const schema = await dataService.getSchema(appId);

        if (!schema) {
            return {
                success: false,
                response: reply.code(404).send({
                    error: 'Not Found',
                    message: 'Application not found.'
                })
            };
        }

        const table = schema.tables.find(table => table.id === tableId);

        if (!table) {
            return {
                success: false,
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
                success: false,
                response: reply.code(500).send({
                    error: 'Data Error',
                    message: 'Invalid connector.'
                })
            };
        }

        return { success: true, schema, table, connectorName, connector };
    }

    fastify.get(
        '/apps/:appId/data/:tableId',
        {
            onRequest: [fastify.authenticate],
            schema: {
                ...tableQuerySchema,
                response: {
                    ...tableQuerySchema.response,
                    401: ErrorResponseSchema,
                    403: ErrorResponseSchema
                }
            }
        },
        async (request, reply) => {
            const { appId, tableId } = request.params;

            // Check read permission
            const permError = await checkAppPermission(request, reply, appId, PermissionLevel.Read);
            if (permError) return permError.response;

            const result = await handleTable(appId, tableId, reply);

            if (!result.success) return result.response;

            let query;
            if (request.query.query) {
                query = JSON.parse(request.query.query);
                const parsed = await TableQueryOptionsSchema.parseAsync(query);
                query = parsed;
            }

            return dataService.getData(result.table, query);
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
                ...tableQuerySchema,
                response: {
                    ...tableQuerySchema.response,
                    401: ErrorResponseSchema,
                    403: ErrorResponseSchema
                }
            }
        },
        async (request, reply) => {
            const { appId, tableId } = request.params;
            const { actionId, rows } = request.body;

            // Check write permission for data actions
            const permError = await checkAppPermission(
                request,
                reply,
                appId,
                PermissionLevel.Write
            );
            if (permError) return permError.response;

            const result = await handleTable(appId, tableId, reply);

            if (!result.success) return result.response;

            await dataService.executeAction({
                table: result.table,
                actId: actionId,
                rows
            });

            return dataService.getData(result.table);
        }
    );
};

export default plugin;
