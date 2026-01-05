import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createTestApp } from '../testUtils.js';
import type SchemaFX from '../../src/index.js';
import type { FastifyInstance } from 'fastify';
import { MemoryConnector, PermissionLevel, PermissionTargetType } from '../../src/index.js';
import { randomUUID } from 'node:crypto';

class MockAuthConnector extends MemoryConnector {
    private userEmail: string;

    constructor(email: string = 'admin@example.com') {
        super({ name: 'AuthConnector', id: 'auth' });
        this.userEmail = email;
    }

    override async authorize() {
        return {
            name: 'Mock Connection',
            content: JSON.stringify({ token: 'mock-token' }),
            email: this.userEmail
        };
    }
}

describe('Permissions Management API', () => {
    let app: SchemaFX;
    let server: FastifyInstance;
    let adminToken: string;

    beforeEach(async () => {
        const testApp = await createTestApp();
        app = testApp.app;
        server = app.fastifyInstance;

        const adminConnector = new MockAuthConnector('admin@example.com');
        app.dataService.connectors['auth-admin'] = adminConnector;

        await server.ready();

        const adminResponse = await server.inject({
            method: 'POST',
            url: '/api/login/auth-admin',
            payload: {}
        });
        adminToken = JSON.parse(adminResponse.payload).token;
    });

    afterEach(async () => {
        await server.close();
    });

    describe('GET /permissions/:targetType/:targetId', () => {
        it('should list permissions for a target', async () => {
            // Note: createTestApp() already creates a permission for TEST_USER_EMAIL on app1
            // Create an additional permission
            await app.dataService.setPermission({
                id: randomUUID(),
                targetType: PermissionTargetType.App,
                targetId: 'app1',
                email: 'user@example.com',
                level: PermissionLevel.Read
            });

            const response = await server.inject({
                method: 'GET',
                url: '/api/permissions/app/app1',
                headers: { Authorization: `Bearer ${adminToken}` }
            });

            expect(response.statusCode).toBe(200);
            const body = JSON.parse(response.payload);
            expect(Array.isArray(body)).toBe(true);
            expect(body.length).toBe(2); // TEST_USER_EMAIL + user@example.com
            expect(body.some((p: { email: string }) => p.email === 'user@example.com')).toBe(true);
        });

        it('should return 401 without authentication', async () => {
            const response = await server.inject({
                method: 'GET',
                url: '/api/permissions/app/app1'
            });

            expect(response.statusCode).toBe(401);
        });
    });

    describe('GET /permissions/:permissionId', () => {
        it('should get a specific permission', async () => {
            const permissionId = randomUUID();

            await app.dataService.setPermission({
                id: permissionId,
                targetType: PermissionTargetType.App,
                targetId: 'app1',
                email: 'user@example.com',
                level: PermissionLevel.Write
            });

            const response = await server.inject({
                method: 'GET',
                url: `api/permissions/${permissionId}`,
                headers: { Authorization: `Bearer ${adminToken}` }
            });

            expect(response.statusCode).toBe(200);
            const body = JSON.parse(response.payload);
            expect(body.id).toBe(permissionId);
            expect(body.level).toBe(PermissionLevel.Write);
        });

        it('should return 404 for non-existent permission', async () => {
            const response = await server.inject({
                method: 'GET',
                url: `/api/permissions/${randomUUID()}`,
                headers: { Authorization: `Bearer ${adminToken}` }
            });

            expect(response.statusCode).toBe(404);
        });
    });

    describe('POST /permissions', () => {
        it('should create a new permission', async () => {
            const response = await server.inject({
                method: 'POST',
                url: '/api/permissions',
                headers: { Authorization: `Bearer ${adminToken}` },
                payload: {
                    targetType: PermissionTargetType.App,
                    targetId: 'app1',
                    email: 'newuser@example.com',
                    level: PermissionLevel.Read
                }
            });

            expect(response.statusCode).toBe(201);
            const body = JSON.parse(response.payload);
            expect(body.email).toBe('newuser@example.com');
            expect(body.level).toBe(PermissionLevel.Read);
            expect(body.targetType).toBe(PermissionTargetType.App);
            expect(body.targetId).toBe('app1');
        });

        it('should normalize email to lowercase', async () => {
            const response = await server.inject({
                method: 'POST',
                url: '/api/permissions',
                headers: { Authorization: `Bearer ${adminToken}` },
                payload: {
                    targetType: PermissionTargetType.App,
                    targetId: 'app1',
                    email: 'UPPERCASE@EXAMPLE.COM',
                    level: PermissionLevel.Write
                }
            });

            expect(response.statusCode).toBe(201);
            const body = JSON.parse(response.payload);
            expect(body.email).toBe('uppercase@example.com');
        });

        it('should return 409 if user already has permission', async () => {
            await app.dataService.setPermission({
                id: randomUUID(),
                targetType: PermissionTargetType.App,
                targetId: 'app1',
                email: 'existing@example.com',
                level: PermissionLevel.Read
            });

            const response = await server.inject({
                method: 'POST',
                url: '/api/permissions',
                headers: { Authorization: `Bearer ${adminToken}` },
                payload: {
                    targetType: PermissionTargetType.App,
                    targetId: 'app1',
                    email: 'existing@example.com',
                    level: PermissionLevel.Write
                }
            });

            expect(response.statusCode).toBe(409);
        });
    });

    describe('PUT /permissions/:permissionId', () => {
        it('should update permission level', async () => {
            const permissionId = randomUUID();

            await app.dataService.setPermission({
                id: permissionId,
                targetType: PermissionTargetType.App,
                targetId: 'app1',
                email: 'update@example.com',
                level: PermissionLevel.Read
            });

            const response = await server.inject({
                method: 'PUT',
                url: `/api/permissions/${permissionId}`,
                headers: { Authorization: `Bearer ${adminToken}` },
                payload: {
                    level: PermissionLevel.Admin
                }
            });

            expect(response.statusCode).toBe(200);
            const body = JSON.parse(response.payload);
            expect(body.level).toBe(PermissionLevel.Admin);
        });

        it('should return 404 for non-existent permission', async () => {
            const response = await server.inject({
                method: 'PUT',
                url: `/api/permissions/${randomUUID()}`,
                headers: { Authorization: `Bearer ${adminToken}` },
                payload: {
                    level: PermissionLevel.Write
                }
            });

            expect(response.statusCode).toBe(404);
        });
    });

    describe('DELETE /permissions/:permissionId', () => {
        it('should delete a permission', async () => {
            const permissionId = randomUUID();

            await app.dataService.setPermission({
                id: permissionId,
                targetType: PermissionTargetType.App,
                targetId: 'app1',
                email: 'delete@example.com',
                level: PermissionLevel.Read
            });

            const response = await server.inject({
                method: 'DELETE',
                url: `/api/permissions/${permissionId}`,
                headers: { Authorization: `Bearer ${adminToken}` }
            });

            expect(response.statusCode).toBe(200);
            const body = JSON.parse(response.payload);
            expect(body.success).toBe(true);

            // Verify it's deleted
            const permission = await app.dataService.getPermission(permissionId);
            expect(permission).toBeUndefined();
        });

        it('should return 404 for non-existent permission', async () => {
            const response = await server.inject({
                method: 'DELETE',
                url: `/api/permissions/${randomUUID()}`,
                headers: { Authorization: `Bearer ${adminToken}` }
            });

            expect(response.statusCode).toBe(404);
        });
    });

    describe('GET /permissions/:targetType/:targetId/me', () => {
        it('should return current user permission', async () => {
            await app.dataService.setPermission({
                id: randomUUID(),
                targetType: PermissionTargetType.App,
                targetId: 'app1',
                email: 'admin@example.com',
                level: PermissionLevel.Admin
            });

            const response = await server.inject({
                method: 'GET',
                url: '/api/permissions/app/app1/me',
                headers: { Authorization: `Bearer ${adminToken}` }
            });

            expect(response.statusCode).toBe(200);
            const body = JSON.parse(response.payload);
            expect(body.email).toBe('admin@example.com');
            expect(body.level).toBe(PermissionLevel.Admin);
        });

        it('should return null when user has no permission', async () => {
            const response = await server.inject({
                method: 'GET',
                url: '/api/permissions/app/app1/me',
                headers: { Authorization: `Bearer ${adminToken}` }
            });

            expect(response.statusCode).toBe(200);
            const body = JSON.parse(response.payload);
            expect(body).toBeNull();
        });
    });
});

describe('Data Endpoints with Permissions', () => {
    let app: SchemaFX;
    let server: FastifyInstance;
    let userToken: string;

    beforeEach(async () => {
        const testApp = await createTestApp();
        app = testApp.app;
        server = app.fastifyInstance;

        const adminConnector = new MockAuthConnector('admin@example.com');
        const userConnector = new MockAuthConnector('user@example.com');

        app.dataService.connectors['auth-admin'] = adminConnector;
        app.dataService.connectors['auth-user'] = userConnector;

        await server.ready();

        // Get user token
        const userResponse = await server.inject({
            method: 'POST',
            url: '/api/login/auth-user',
            payload: {}
        });

        userToken = JSON.parse(userResponse.payload).token;
    });

    afterEach(async () => {
        await server.close();
    });

    describe('GET /apps/:appId/data/:tableId', () => {
        it('should return 403 when user has no permission', async () => {
            const response = await server.inject({
                method: 'GET',
                url: '/api/apps/app1/data/users',
                headers: { Authorization: `Bearer ${userToken}` }
            });

            expect(response.statusCode).toBe(403);
        });

        it('should allow access with read permission', async () => {
            await app.dataService.setPermission({
                id: randomUUID(),
                targetType: PermissionTargetType.App,
                targetId: 'app1',
                email: 'user@example.com',
                level: PermissionLevel.Read
            });

            const response = await server.inject({
                method: 'GET',
                url: '/api/apps/app1/data/users',
                headers: { Authorization: `Bearer ${userToken}` }
            });

            expect(response.statusCode).toBe(200);
        });

        it('should allow access with write permission (higher level)', async () => {
            await app.dataService.setPermission({
                id: randomUUID(),
                targetType: PermissionTargetType.App,
                targetId: 'app1',
                email: 'user@example.com',
                level: PermissionLevel.Write
            });

            const response = await server.inject({
                method: 'GET',
                url: '/api/apps/app1/data/users',
                headers: { Authorization: `Bearer ${userToken}` }
            });

            expect(response.statusCode).toBe(200);
        });
    });

    describe('POST /apps/:appId/data/:tableId', () => {
        it('should return 403 when user has no permission', async () => {
            const response = await server.inject({
                method: 'POST',
                url: '/api/apps/app1/data/users',
                headers: { Authorization: `Bearer ${userToken}` },
                payload: {
                    actionId: 'add',
                    rows: [{ id: 100, name: 'Test' }]
                }
            });

            expect(response.statusCode).toBe(403);
        });

        it('should return 403 when user only has read permission', async () => {
            await app.dataService.setPermission({
                id: randomUUID(),
                targetType: PermissionTargetType.App,
                targetId: 'app1',
                email: 'user@example.com',
                level: PermissionLevel.Read
            });

            const response = await server.inject({
                method: 'POST',
                url: '/api/apps/app1/data/users',
                headers: { Authorization: `Bearer ${userToken}` },
                payload: {
                    actionId: 'add',
                    rows: [{ id: 101, name: 'Test' }]
                }
            });

            expect(response.statusCode).toBe(403);
        });

        it('should allow access with write permission', async () => {
            await app.dataService.setPermission({
                id: randomUUID(),
                targetType: PermissionTargetType.App,
                targetId: 'app1',
                email: 'user@example.com',
                level: PermissionLevel.Write
            });

            const response = await server.inject({
                method: 'POST',
                url: '/api/apps/app1/data/users',
                headers: { Authorization: `Bearer ${userToken}` },
                payload: {
                    actionId: 'add',
                    rows: [{ id: 102, name: 'Test' }]
                }
            });

            expect(response.statusCode).toBe(200);
        });

        it('should allow access with admin permission', async () => {
            await app.dataService.setPermission({
                id: randomUUID(),
                targetType: PermissionTargetType.App,
                targetId: 'app1',
                email: 'user@example.com',
                level: PermissionLevel.Admin
            });

            const response = await server.inject({
                method: 'POST',
                url: '/api/apps/app1/data/users',
                headers: { Authorization: `Bearer ${userToken}` },
                payload: {
                    actionId: 'add',
                    rows: [{ id: 103, name: 'Test 2' }]
                }
            });

            expect(response.statusCode).toBe(200);
        });
    });
});

describe('DataService Permission Methods', () => {
    let app: SchemaFX;

    beforeEach(async () => {
        const testApp = await createTestApp();
        app = testApp.app;
    });

    afterEach(async () => {
        await app.fastifyInstance.close();
    });

    it('should check permission levels correctly', async () => {
        await app.dataService.setPermission({
            id: randomUUID(),
            targetType: PermissionTargetType.App,
            targetId: 'app1',
            email: 'reader@example.com',
            level: PermissionLevel.Read
        });

        expect(
            await app.dataService.hasPermission(
                { targetType: PermissionTargetType.App, targetId: 'app1' },
                'reader@example.com',
                PermissionLevel.Read
            )
        ).toBe(true);
        expect(
            await app.dataService.hasPermission(
                { targetType: PermissionTargetType.App, targetId: 'app1' },
                'reader@example.com',
                PermissionLevel.Write
            )
        ).toBe(false);
        expect(
            await app.dataService.hasPermission(
                { targetType: PermissionTargetType.App, targetId: 'app1' },
                'reader@example.com',
                PermissionLevel.Admin
            )
        ).toBe(false);
    });

    it('should handle case-insensitive email lookup', async () => {
        await app.dataService.setPermission({
            id: randomUUID(),
            targetType: PermissionTargetType.App,
            targetId: 'app1',
            email: 'User@Example.COM',
            level: PermissionLevel.Read
        });

        const permission = await app.dataService.getUserPermission(
            { targetType: PermissionTargetType.App, targetId: 'app1' },
            'USER@EXAMPLE.COM'
        );
        expect(permission).toBeDefined();
        expect(permission?.email).toBe('user@example.com');
    });

    it('should delete all permissions for a target', async () => {
        // Note: createTestApp() already creates a permission for TEST_USER_EMAIL on app1
        await app.dataService.setPermission({
            id: randomUUID(),
            targetType: PermissionTargetType.App,
            targetId: 'app1',
            email: 'user1@example.com',
            level: PermissionLevel.Read
        });
        await app.dataService.setPermission({
            id: randomUUID(),
            targetType: PermissionTargetType.App,
            targetId: 'app1',
            email: 'user2@example.com',
            level: PermissionLevel.Write
        });

        let permissions = await app.dataService.getPermissions({
            targetType: PermissionTargetType.App,
            targetId: 'app1'
        });
        expect(permissions.length).toBe(3); // TEST_USER_EMAIL + user1 + user2

        await app.dataService.deletePermissions({
            targetType: PermissionTargetType.App,
            targetId: 'app1'
        });

        permissions = await app.dataService.getPermissions({
            targetType: PermissionTargetType.App,
            targetId: 'app1'
        });
        expect(permissions.length).toBe(0);
    });

    it('should keep different targets separate', async () => {
        const connection = await app.dataService.setConnection({
            id: randomUUID(),
            name: 'Test Connection',
            connector: 'mem',
            content: 'test'
        });

        await app.dataService.setPermission({
            id: randomUUID(),
            targetType: PermissionTargetType.App,
            targetId: 'app1',
            email: 'user@example.com',
            level: PermissionLevel.Read
        });

        await app.dataService.setPermission({
            id: randomUUID(),
            targetType: PermissionTargetType.Connection,
            targetId: connection.id,
            email: 'user@example.com',
            level: PermissionLevel.Admin
        });

        const appPermission = await app.dataService.getUserPermission(
            { targetType: PermissionTargetType.App, targetId: 'app1' },
            'user@example.com'
        );
        const connPermission = await app.dataService.getUserPermission(
            { targetType: PermissionTargetType.Connection, targetId: connection.id },
            'user@example.com'
        );

        expect(appPermission?.level).toBe(PermissionLevel.Read);
        expect(connPermission?.level).toBe(PermissionLevel.Admin);
    });

    it('should get all permissions for a user across all target types', async () => {
        const connection = await app.dataService.setConnection({
            id: randomUUID(),
            name: 'Test Connection',
            connector: 'mem',
            content: 'test'
        });

        await app.dataService.setPermission({
            id: randomUUID(),
            targetType: PermissionTargetType.App,
            targetId: 'app1',
            email: 'allperms@example.com',
            level: PermissionLevel.Read
        });

        await app.dataService.setPermission({
            id: randomUUID(),
            targetType: PermissionTargetType.Connection,
            targetId: connection.id,
            email: 'allperms@example.com',
            level: PermissionLevel.Admin
        });

        // Get all permissions without filtering by target type
        const allPermissions = await app.dataService.getPermissionsByUser('allperms@example.com');
        expect(allPermissions.length).toBe(2);
        expect(allPermissions.some(p => p.targetType === PermissionTargetType.App)).toBe(true);
        expect(allPermissions.some(p => p.targetType === PermissionTargetType.Connection)).toBe(
            true
        );
    });
});

describe('Permissions API - Unauthenticated Access', () => {
    let app: SchemaFX;
    let server: FastifyInstance;

    beforeEach(async () => {
        const testApp = await createTestApp();
        app = testApp.app;
        server = app.fastifyInstance;
        await server.ready();
    });

    afterEach(async () => {
        await server.close();
    });

    it('should return 401 for GET /permissions/:permissionId without authentication', async () => {
        const permissionId = randomUUID();

        // Create a permission to test against
        await app.dataService.setPermission({
            id: permissionId,
            targetType: PermissionTargetType.App,
            targetId: 'app1',
            email: 'user@example.com',
            level: PermissionLevel.Read
        });

        const response = await server.inject({
            method: 'GET',
            url: `/api/permissions/${permissionId}`
        });

        expect(response.statusCode).toBe(401);
        const body = JSON.parse(response.payload);
        expect(body.error).toBe('Unauthorized');
    });

    it('should return 401 for POST /permissions without authentication', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/permissions',
            payload: {
                targetType: PermissionTargetType.App,
                targetId: 'app1',
                email: 'newuser@example.com',
                level: PermissionLevel.Read
            }
        });

        expect(response.statusCode).toBe(401);
        const body = JSON.parse(response.payload);
        expect(body.error).toBe('Unauthorized');
    });

    it('should return 401 for PUT /permissions/:permissionId without authentication', async () => {
        const permissionId = randomUUID();
        // Create a permission to test against
        await app.dataService.setPermission({
            id: permissionId,
            targetType: PermissionTargetType.App,
            targetId: 'app1',
            email: 'user@example.com',
            level: PermissionLevel.Read
        });

        const response = await server.inject({
            method: 'PUT',
            url: `/api/permissions/${permissionId}`,
            payload: {
                level: PermissionLevel.Write
            }
        });

        expect(response.statusCode).toBe(401);
        const body = JSON.parse(response.payload);
        expect(body.error).toBe('Unauthorized');
    });

    it('should return 401 for DELETE /permissions/:permissionId without authentication', async () => {
        const permissionId = randomUUID();

        // Create a permission to test against
        await app.dataService.setPermission({
            id: permissionId,
            targetType: PermissionTargetType.App,
            targetId: 'app1',
            email: 'user@example.com',
            level: PermissionLevel.Read
        });

        const response = await server.inject({
            method: 'DELETE',
            url: `/api/permissions/${permissionId}`
        });

        expect(response.statusCode).toBe(401);
        const body = JSON.parse(response.payload);
        expect(body.error).toBe('Unauthorized');
    });

    it('should return 401 for GET /permissions/:targetType/:targetId/me without authentication', async () => {
        const response = await server.inject({
            method: 'GET',
            url: '/api/permissions/app/app1/me'
        });

        expect(response.statusCode).toBe(401);
        const body = JSON.parse(response.payload);
        expect(body.error).toBe('Unauthorized');
    });
});
