import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createTestApp } from '../testUtils.js';
import type SchemaFX from '../../src/index.js';
import type { FastifyInstance } from 'fastify';
import { MemoryConnector } from '../../src/index.js';

class MockAuthConnector extends MemoryConnector {
    constructor() {
        super('AuthConnector', 'auth-conn');
    }

    async getAuthUrl() {
        return 'http://example.com/auth';
    }

    async authorize(params: any) {
        return {
            name: 'Mock Connection',
            content: JSON.stringify({ token: 'mock-token', ...params })
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
        token = testApp.token;

        // Add mock auth connector
        app.dataService.connectors['auth-conn'] = new MockAuthConnector();

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
        const body = JSON.parse(response.payload);

        // Find entries for 'mem' connector
        const memEntries = body.filter((c: any) => c.id === 'mem');
        expect(memEntries.length).toBeGreaterThanOrEqual(2);

        // One entry should be the base connector
        // And one entry for the connection we created
        const connectionEntry = memEntries.find((c: any) => c.connection?.id === 'mem-conn-1');
        expect(connectionEntry).toBeDefined();
        expect(connectionEntry.connection.name).toBe('Memory Connection 1');
        expect(connectionEntry.requiresConnection).toBe(false);
    });

    it('should list tables in connector', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/connectors/mem/query',
            headers: { Authorization: `Bearer ${token}` },
            payload: { path: [] }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload);

        // Expect 'users' table which was seeded
        expect(body.find((t: any) => t.name === 'users')).toBeDefined();
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

    it('should import table from connector to existing app', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/connectors/mem/table',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                path: ['users'],
                appId: 'app1'
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload);
        expect(body.id).toBe('app1');
        expect(body.tables).toHaveLength(2);
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
        expect(response.headers.location).toBe('http://example.com/auth');
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

    it('should handle authorize callback', async () => {
        const response = await server.inject({
            method: 'GET',
            url: '/api/connectors/auth-conn/auth/callback',
            query: { code: '123' }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload);
        expect(body.connectionId).toBeDefined();

        const connection = await app.dataService.getConnection(body.connectionId);
        expect(connection).toBeDefined();
        expect(connection?.connector).toBe('auth-conn');
        expect(connection?.name).toBe('Mock Connection');
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
