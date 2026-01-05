import type { FastifyPluginAsyncZod } from 'fastify-type-provider-zod';
import { z } from 'zod';
import { PermissionLevel, PermissionTargetType } from '../types.js';
import { ErrorResponseSchema } from '../utils/fastifyUtils.js';
import type DataService from '../services/DataService.js';
import { randomUUID } from 'node:crypto';

const PermissionResponseSchema = z.object({
    id: z.string(),
    targetType: z.enum(Object.values(PermissionTargetType)),
    targetId: z.string(),
    email: z.string().email(),
    level: z.enum(Object.values(PermissionLevel))
});

const plugin: FastifyPluginAsyncZod<{
    dataService: DataService;
}> = async (fastify, { dataService }) => {
    // ========================================================================
    // Permission Management Endpoints
    // These are simplified CRUD endpoints. Permission checks for accessing
    // resources (apps, connections, data) are done at those respective endpoints.
    // ========================================================================

    // List all permissions for a target
    fastify.get(
        '/permissions/:targetType/:targetId',
        {
            onRequest: [fastify.authenticate],
            schema: {
                params: z.object({
                    targetType: z
                        .enum(Object.values(PermissionTargetType))
                        .meta({ description: 'Target type (app, connection, etc.)' }),
                    targetId: z.string().min(1).meta({ description: 'Target ID' })
                }),
                response: {
                    200: z
                        .array(PermissionResponseSchema)
                        .meta({ description: 'List of permissions' }),
                    401: ErrorResponseSchema
                }
            }
        },
        async (request, reply) => {
            const { targetType, targetId } = request.params;

            if (!request.user?.email) {
                return reply.code(401).send({
                    error: 'Unauthorized',
                    message: 'Authentication required.'
                });
            }

            return dataService.getPermissions({ targetType, targetId });
        }
    );

    // Get a specific permission by ID
    fastify.get(
        '/permissions/:permissionId',
        {
            onRequest: [fastify.authenticate],
            schema: {
                params: z.object({
                    permissionId: z.string().min(1).meta({ description: 'Permission ID' })
                }),
                response: {
                    200: PermissionResponseSchema,
                    401: ErrorResponseSchema,
                    404: ErrorResponseSchema
                }
            }
        },
        async (request, reply) => {
            const { permissionId } = request.params;

            if (!request.user?.email) {
                return reply.code(401).send({
                    error: 'Unauthorized',
                    message: 'Authentication required.'
                });
            }

            const permission = await dataService.getPermission(permissionId);
            if (!permission) {
                return reply.code(404).send({
                    error: 'Not Found',
                    message: 'Permission not found.'
                });
            }

            return permission;
        }
    );

    // Create a new permission
    fastify.post(
        '/permissions',
        {
            onRequest: [fastify.authenticate],
            schema: {
                body: z.object({
                    targetType: z
                        .enum(Object.values(PermissionTargetType))
                        .meta({ description: 'Target type (app, connection, etc.)' }),
                    targetId: z.string().min(1).meta({ description: 'Target ID' }),
                    email: z.string().email().meta({ description: 'User email address' }),
                    level: z
                        .enum(Object.values(PermissionLevel))
                        .meta({ description: 'Permission level' })
                }),
                response: {
                    201: PermissionResponseSchema,
                    401: ErrorResponseSchema,
                    409: ErrorResponseSchema
                }
            }
        },
        async (request, reply) => {
            const { targetType, targetId, email: targetEmail, level } = request.body;

            if (!request.user?.email) {
                return reply.code(401).send({
                    error: 'Unauthorized',
                    message: 'Authentication required.'
                });
            }

            // Check if user already has a permission for this target
            const existingPermission = await dataService.getUserPermission(
                { targetType, targetId },
                targetEmail
            );

            if (existingPermission) {
                return reply.code(409).send({
                    error: 'Conflict',
                    message: 'User already has a permission for this resource.'
                });
            }

            const permission = await dataService.setPermission({
                id: randomUUID(),
                targetType,
                targetId,
                email: targetEmail.toLowerCase(),
                level
            });

            return reply.code(201).send(permission);
        }
    );

    // Update a permission
    fastify.put(
        '/permissions/:permissionId',
        {
            onRequest: [fastify.authenticate],
            schema: {
                params: z.object({
                    permissionId: z.string().min(1).meta({ description: 'Permission ID' })
                }),
                body: z.object({
                    level: z
                        .enum(Object.values(PermissionLevel))
                        .meta({ description: 'New permission level' })
                }),
                response: {
                    200: PermissionResponseSchema,
                    401: ErrorResponseSchema,
                    404: ErrorResponseSchema
                }
            }
        },
        async (request, reply) => {
            const { permissionId } = request.params;
            const { level } = request.body;

            if (!request.user?.email) {
                return reply.code(401).send({
                    error: 'Unauthorized',
                    message: 'Authentication required.'
                });
            }

            const permission = await dataService.getPermission(permissionId);
            if (!permission) {
                return reply.code(404).send({
                    error: 'Not Found',
                    message: 'Permission not found.'
                });
            }

            const updatedPermission = await dataService.setPermission({
                ...permission,
                level
            });

            return updatedPermission;
        }
    );

    // Delete a permission
    fastify.delete(
        '/permissions/:permissionId',
        {
            onRequest: [fastify.authenticate],
            schema: {
                params: z.object({
                    permissionId: z.string().min(1).meta({ description: 'Permission ID' })
                }),
                response: {
                    200: z.object({ success: z.boolean() }),
                    401: ErrorResponseSchema,
                    404: ErrorResponseSchema
                }
            }
        },
        async (request, reply) => {
            const { permissionId } = request.params;

            if (!request.user?.email) {
                return reply.code(401).send({
                    error: 'Unauthorized',
                    message: 'Authentication required.'
                });
            }

            const deleted = await dataService.deletePermission(permissionId);
            if (!deleted) {
                return reply.code(404).send({
                    error: 'Not Found',
                    message: 'Permission not found.'
                });
            }

            return { success: true };
        }
    );

    // Get current user's permission for a target
    fastify.get(
        '/permissions/:targetType/:targetId/me',
        {
            onRequest: [fastify.authenticate],
            schema: {
                params: z.object({
                    targetType: z
                        .enum(Object.values(PermissionTargetType))
                        .meta({ description: 'Target type (app, connection, etc.)' }),
                    targetId: z.string().min(1).meta({ description: 'Target ID' })
                }),
                response: {
                    200: PermissionResponseSchema.nullable(),
                    401: ErrorResponseSchema
                }
            }
        },
        async (request, reply) => {
            const { targetType, targetId } = request.params;
            const email = request.user?.email;

            if (!email) {
                return reply.code(401).send({
                    error: 'Unauthorized',
                    message: 'Authentication required.'
                });
            }

            const permission = await dataService.getUserPermission({ targetType, targetId }, email);
            return permission ?? null;
        }
    );
};

export default plugin;
