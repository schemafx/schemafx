import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createTestApp } from '../testUtils.js';
import type SchemaFX from '../../src/index.js';
import type { FastifyInstance } from 'fastify';

describe('Connectors API', () => {
    let app: SchemaFX;
    let server: FastifyInstance;
    let token: string;

    beforeEach(async () => {
        const testApp = await createTestApp(true);
        app = testApp.app;
        server = app.fastifyInstance;
        token = testApp.token;
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
        expect(body).toHaveLength(1);
        expect(body[0].id).toBe('mem');
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
});
