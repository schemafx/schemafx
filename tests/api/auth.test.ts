import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createTestApp } from '../testUtils.js';
import type SchemaFX from '../../src/index.js';
import type { FastifyInstance } from 'fastify';
import { MemoryConnector } from '../../src/index.js';

class MockAuthConnectorWithEmail extends MemoryConnector {
    constructor() {
        super({ name: 'AuthConnectorWithEmail', id: 'auth-login' });
    }

    override async authorize(params: any) {
        return {
            name: 'Mock Login Connection',
            content: JSON.stringify({ token: 'mock-token', ...params }),
            email: 'user@schemafx.com'
        };
    }
}

class MockAuthConnectorWithoutEmail extends MemoryConnector {
    constructor() {
        super({ name: 'AuthConnectorWithoutEmail', id: 'auth-no-email' });
    }

    override async authorize(params: any) {
        return {
            name: 'Mock Connection Without Email',
            content: JSON.stringify({ token: 'mock-token', ...params })
        };
    }
}

describe('Auth API', () => {
    let app: SchemaFX;
    let server: FastifyInstance;

    beforeEach(async () => {
        const testApp = await createTestApp();
        app = testApp.app;
        server = app.fastifyInstance;

        // Add mock auth connectors
        app.dataService.connectors['auth-login'] = new MockAuthConnectorWithEmail();
        app.dataService.connectors['auth-no-email'] = new MockAuthConnectorWithoutEmail();

        await server.ready();
    });

    afterEach(async () => {
        await server.close();
    });

    it('should login successfully via connector', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/login/auth-login',
            payload: {
                apiKey: 'secret'
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload);
        expect(body.token).toBeDefined();
        expect(body.connectionId).toBeDefined();

        const connection = await app.dataService.getConnection(body.connectionId);
        expect(connection).toBeDefined();
        expect(connection?.connector).toBe('auth-login');
    });

    it('should fail with 404 for unknown connector', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/login/unknown-connector',
            payload: {}
        });

        expect(response.statusCode).toBe(404);
        const body = JSON.parse(response.payload);
        expect(body.message).toBe('Connector not found.');
    });

    it('should fail with 404 for connector without authorize', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/login/mem',
            payload: {}
        });

        expect(response.statusCode).toBe(404);
        const body = JSON.parse(response.payload);
        expect(body.message).toBe('Connector not found.');
    });

    it('should fail with 401 when connector does not provide email', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/login/auth-no-email',
            payload: {}
        });

        expect(response.statusCode).toBe(401);
        const body = JSON.parse(response.payload);
        expect(body.message).toBe('Connector did not provide user identity.');
    });
});
