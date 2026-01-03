import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createTestApp } from '../testUtils.js';
import type SchemaFX from '../../src/index.js';
import type { FastifyInstance } from 'fastify';

describe('Apps endpoints', () => {
    let app: SchemaFX;
    let server: FastifyInstance;
    let token: string;

    beforeEach(async () => {
        const testApp = await createTestApp(true);
        app = testApp.app;
        server = app.fastifyInstance;
        token = testApp.token as string;
    });

    afterEach(async () => {
        await server.close();
    });

    it('lists available applications with id and name', async () => {
        const res = await server.inject({ method: 'GET', url: '/api/apps' });
        expect(res.statusCode).toBe(200);

        const body = JSON.parse(res.payload);
        expect(Array.isArray(body)).toBe(true);
        const appEntry = body.find((a: any) => a.id === 'app1');
        expect(appEntry).toBeDefined();
        expect(appEntry.name).toBe('App 1');
    });

    it('deletes an application and subsequent fetch returns 404', async () => {
        // Delete existing app
        const delRes = await server.inject({
            method: 'DELETE',
            url: '/api/apps/app1',
            headers: { Authorization: `Bearer ${token}` }
        });

        expect(delRes.statusCode).toBe(200);
        const delBody = JSON.parse(delRes.payload);
        expect(delBody.success).toBe(true);

        // Get schema should return 404
        const getRes = await server.inject({ method: 'GET', url: '/api/apps/app1/schema' });
        expect(getRes.statusCode).toBe(404);

        // Listing apps should not include the deleted app
        const listRes = await server.inject({ method: 'GET', url: '/api/apps' });
        expect(listRes.statusCode).toBe(200);
        const listBody = JSON.parse(listRes.payload);
        expect(listBody.find((a: any) => a.id === 'app1')).toBeUndefined();
    });

    it('returns 404 when deleting a non-existent application', async () => {
        const delRes = await server.inject({
            method: 'DELETE',
            url: '/api/apps/non-existent-app',
            headers: { Authorization: `Bearer ${token}` }
        });

        expect(delRes.statusCode).toBe(404);
        const body = JSON.parse(delRes.payload);
        expect(body.error).toBe('Not Found');
        expect(body.message).toBe('Application not found.');
    });
});
