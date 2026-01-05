import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createTestApp, TEST_USER_EMAIL } from '../testUtils.js';
import type SchemaFX from '../../src/index.js';
import type { FastifyInstance } from 'fastify';
import { MemoryConnector, PermissionLevel, PermissionTargetType } from '../../src/index.js';

class MockAuthConnector extends MemoryConnector {
    constructor() {
        super({ name: 'AuthConnector', id: 'auth-conn' });
    }

    override async getAuthUrl() {
        return 'http://example.com/auth';
    }

    override async authorize(params: Record<string, unknown>) {
        return {
            name: 'Mock Connection',
            content: JSON.stringify({ token: 'mock-token', ...params })
        };
    }
}

class MockAuthConnectorWithEmail extends MemoryConnector {
    constructor() {
        super({ name: 'AuthConnectorWithEmail', id: 'auth-conn-email' });
    }

    override async getAuthUrl() {
        return 'http://example.com/auth/email';
    }

    override async authorize(params: Record<string, unknown>) {
        return {
            name: 'Mock Connection With Email',
            content: JSON.stringify({ token: 'mock-token', ...params }),
            email: 'user@example.com'
        };
    }
}

describe('Connectors API', () => {
    let app: SchemaFX;
    let server: FastifyInstance;
    let token: string;

    beforeEach(async () => {
        const testApp = await createTestApp(true);
        app = testApp.app;
        server = app.fastifyInstance;
        token = testApp.token!;

        // Add mock auth connector
        app.dataService.connectors['auth-conn'] = new MockAuthConnector();
        app.dataService.connectors['auth-conn-email'] = new MockAuthConnectorWithEmail();

        // Create a connection for the memory connector
        await app.dataService.setConnection({
            id: 'mem-conn-1',
            name: 'Memory Connection 1',
            connector: 'mem',
            content: 'some-content'
        });
    });

    afterEach(async () => {
        await server.close();
    });

    it('should list connectors', async () => {
        const response = await server.inject({
            method: 'GET',
            url: '/api/connectors'
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as {
            id: string;
            name: string;
            connection?: {
                id: string;
                name: string;
            };
            requiresConnection: boolean;
            supportsData: boolean;
        }[];

        // Find entries for 'mem' connector
        const memEntries = body.filter(c => c.id === 'mem');
        expect(memEntries.length).toBeGreaterThanOrEqual(2);

        // One entry should be the base connector
        // And one entry for the connection we created
        const connectionEntry = memEntries.find(c => c.connection?.id === 'mem-conn-1');
        expect(connectionEntry).toBeDefined();
        expect(connectionEntry?.connection?.name).toBe('Memory Connection 1');
        expect(connectionEntry?.requiresConnection).toBe(false);
    });

    it('should list tables in connector', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/connectors/mem/query',
            headers: { Authorization: `Bearer ${token}` },
            payload: { path: [] }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as Record<string, unknown>[];

        // Expect 'users' table which was seeded
        expect(body.find(t => t.name === 'users')).toBeDefined();
    });

    it('should 404 for unknown connector query', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/connectors/unknown/query',
            headers: { Authorization: `Bearer ${token}` },
            payload: { path: [] }
        });

        expect(response.statusCode).toBe(404);
    });

    it('should import table from connector to new app', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/connectors/mem/table',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                path: ['users']
                // No appId -> new app
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload);
        expect(body.name).toBe('New App');
        expect(body.tables).toHaveLength(1);
        expect(body.tables[0].name).toBe('users');
    });

    it('should create admin permission for creator when creating new app', async () => {
        // Create a new app via connector import (no appId = new app)
        const response = await server.inject({
            method: 'POST',
            url: '/api/connectors/mem/table',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                path: ['users']
            }
        });

        expect(response.statusCode).toBe(200);
        const newApp = JSON.parse(response.payload);
        expect(newApp.id).toBeDefined();

        // Verify that a permission was created for the creator
        const permissions = await app.dataService.getPermissions({
            targetType: PermissionTargetType.App,
            targetId: newApp.id
        });

        expect(permissions.length).toBe(1);
        expect(permissions[0]?.email).toBe(TEST_USER_EMAIL);
        expect(permissions[0]?.level).toBe(PermissionLevel.Admin);
        expect(permissions[0]?.targetType).toBe(PermissionTargetType.App);
        expect(permissions[0]?.targetId).toBe(newApp.id);
    });

    it('should import table from connector to existing app', async () => {
        // Mock new table.
        (app.dataService.connectors.mem as MemoryConnector).tables.set('table1', [{ id: '' }]);

        const payload = {
            path: ['table1'],
            appId: 'app1'
        };

        const response = await server.inject({
            method: 'POST',
            url: '/api/connectors/mem/table',
            headers: { Authorization: `Bearer ${token}` },
            payload
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload);
        expect(body.id).toBe('app1');
        expect(body.tables).toHaveLength(2);

        const duplicateResponse = await server.inject({
            method: 'POST',
            url: '/api/connectors/mem/table',
            headers: { Authorization: `Bearer ${token}` },
            payload
        });

        // Importing the same connector + path into an existing app should fail
        expect(duplicateResponse.statusCode).toBe(400);
        const duplicateBody = JSON.parse(duplicateResponse.payload);
        expect(duplicateBody.message).toBe('Table already exists in application.');
    });

    it('should 404 for unknown connector table', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/connectors/unknown/table',
            headers: { Authorization: `Bearer ${token}` },
            payload: { path: [] }
        });

        expect(response.statusCode).toBe(404);
    });

    it('should 404 for unknown table table', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/connectors/mem/table',
            headers: { Authorization: `Bearer ${token}` },
            payload: { path: [] }
        });

        expect(response.statusCode).toBe(404);
    });

    it('should return 404 for connector without getAuthUrl', async () => {
        const response = await server.inject({
            method: 'GET',
            url: '/api/connectors/mem/auth'
        });

        expect(response.statusCode).toBe(404);
        const body = JSON.parse(response.payload);
        expect(body.message).toBe('Connector not found.');
    });

    it('should redirect for connector with getAuthUrl', async () => {
        const response = await server.inject({
            method: 'GET',
            url: '/api/connectors/auth-conn/auth'
        });

        expect(response.statusCode).toBe(302);
        expect(response.headers.location?.startsWith('http://example.com/auth')).toBe(true);
    });

    it('should handle full OAuth flow with redirectUri in query and redirect back', async () => {
        const authResponse = await server.inject({
            method: 'GET',
            url: '/api/connectors/auth-conn/auth',
            query: { redirectUri: 'http://localhost:3000/callback' }
        });

        expect(authResponse.statusCode).toBe(302);
        const authLocation = new URL(authResponse.headers.location!);
        const state = authLocation.searchParams.get('state');
        expect(state).toBeDefined();

        // Verify state contains the redirectUri
        const stateData = JSON.parse(Buffer.from(state!, 'base64url').toString());
        expect(stateData.redirectUri).toBe('http://localhost:3000/callback');

        const callbackResponse = await server.inject({
            method: 'GET',
            url: '/api/connectors/auth-conn/auth/callback',
            query: { code: '123', state: state! }
        });

        expect(callbackResponse.statusCode).toBe(302);
        const callbackLocation = new URL(callbackResponse.headers.location!);
        expect(callbackLocation.href.startsWith('http://localhost:3000/callback')).toBe(true);
        expect(callbackLocation.searchParams.get('connectionId')).toBeDefined();
    });

    it('should return 404 for connector without authorize (callback)', async () => {
        const response = await server.inject({
            method: 'GET',
            url: '/api/connectors/mem/auth/callback'
        });

        expect(response.statusCode).toBe(404);
        const body = JSON.parse(response.payload);
        expect(body.message).toBe('Connector not found.');
    });

    it('should handle authorize callback with state redirectUri', async () => {
        const stateData = { redirectUri: 'http://localhost:3000/callback' };
        const state = Buffer.from(JSON.stringify(stateData)).toString('base64url');

        const response = await server.inject({
            method: 'GET',
            url: '/api/connectors/auth-conn/auth/callback',
            query: { code: '123', state }
        });

        expect(response.statusCode).toBe(302);
        const location = response.headers.location as string;
        expect(location).toContain('http://localhost:3000/callback');
        expect(location).toContain('connectionId=');
    });

    it('should handle authorize callback with empty state', async () => {
        const stateData = {};
        const state = Buffer.from(JSON.stringify(stateData)).toString('base64url');

        const response = await server.inject({
            method: 'GET',
            url: '/api/connectors/auth-conn/auth/callback',
            query: { code: '123', state }
        });

        expect(response.statusCode).toBe(200);
    });

    it('should handle authorize callback', async () => {
        const response = await server.inject({
            method: 'GET',
            url: '/api/connectors/auth-conn/auth/callback',
            query: { code: '123' }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.body);
        expect(body.connectionId).toBeDefined();

        const connection = await app.dataService.getConnection(body.connectionId);
        expect(connection).toBeDefined();
        expect(connection?.connector).toBe('auth-conn');
        expect(connection?.name).toBe('Mock Connection');
    });

    it('should handle authorize callback with email and return code', async () => {
        const response = await server.inject({
            method: 'GET',
            url: '/api/connectors/auth-conn-email/auth/callback',
            query: { code: '123' }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.body);
        expect(body.connectionId).toBeDefined();
        expect(body.code).toBeDefined();

        const connection = await app.dataService.getConnection(body.connectionId);
        expect(connection).toBeDefined();
        expect(connection?.connector).toBe('auth-conn-email');
        expect(connection?.name).toBe('Mock Connection With Email');

        const tokenResponse = await server.inject({
            method: 'GET',
            url: `/api/token/${body.code}`
        });

        expect(response.statusCode).toBe(200);
        const tokenBody = JSON.parse(tokenResponse.body);
        expect(tokenBody.token).toBeDefined();

        const secondTokenResponse = await server.inject({
            method: 'GET',
            url: `/api/token/${body.code}`
        });

        expect(secondTokenResponse.statusCode).toBe(404);
    });

    it('should return 404 for connector without authorize (POST)', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/connectors/mem/auth',
            payload: {}
        });

        expect(response.statusCode).toBe(404);
        const body = JSON.parse(response.payload);
        expect(body.message).toBe('Connector not found.');
    });

    it('should handle authorize POST', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/connectors/auth-conn/auth',
            payload: { apiKey: 'secret' }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload);
        expect(body.connectionId).toBeDefined();

        const connection = await app.dataService.getConnection(body.connectionId);
        expect(connection).toBeDefined();
        expect(connection?.connector).toBe('auth-conn');
        expect(connection?.name).toBe('Mock Connection');
    });

    it('should handle authorize POST with email and return code', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/connectors/auth-conn-email/auth',
            payload: { apiKey: 'secret' }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload);
        expect(body.connectionId).toBeDefined();
        expect(body.code).toBeDefined();

        const connection = await app.dataService.getConnection(body.connectionId);
        expect(connection).toBeDefined();
        expect(connection?.connector).toBe('auth-conn-email');
        expect(connection?.name).toBe('Mock Connection With Email');

        // Exchange code for token
        const tokenResponse = await server.inject({
            method: 'GET',
            url: `/api/token/${body.code}`
        });
        expect(tokenResponse.statusCode).toBe(200);
        const tokenBody = JSON.parse(tokenResponse.body);
        expect(tokenBody.token).toBeDefined();
    });

    it('should return 404 if appId does not exist when adding table', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/connectors/mem/table',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                path: ['users'],
                appId: 'non-existent-app'
            }
        });

        expect(response.statusCode).toBe(404);
        const body = JSON.parse(response.payload);
        expect(body.message).toBe('Application not found.');
    });
});
