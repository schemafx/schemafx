import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createTestApp, TEST_USER_EMAIL } from '../testUtils.js';
import type SchemaFX from '../../src/index.js';
import type { FastifyInstance } from 'fastify';
import { type AppSchema } from '../../src/index.js';

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

    it('returns 401 when listing apps without authentication', async () => {
        const res = await server.inject({ method: 'GET', url: '/api/apps' });
        expect(res.statusCode).toBe(401);
    });

    it('lists only applications the user has permission to access', async () => {
        const res = await server.inject({
            method: 'GET',
            url: '/api/apps',
            headers: { Authorization: `Bearer ${token}` }
        });
        expect(res.statusCode).toBe(200);

        const body = JSON.parse(res.payload);
        expect(Array.isArray(body)).toBe(true);
        const appEntry = body.find((a: AppSchema) => a.id === 'app1');
        expect(appEntry).toBeDefined();
        expect(appEntry.name).toBe('App 1');
    });

    it('does not list applications the user has no permission for', async () => {
        // Create a second app without granting permission to the test user
        await app.dataService.setSchema({
            id: 'app2',
            name: 'App 2',
            tables: [],
            views: []
        });

        const res = await server.inject({
            method: 'GET',
            url: '/api/apps',
            headers: { Authorization: `Bearer ${token}` }
        });
        expect(res.statusCode).toBe(200);

        const body = JSON.parse(res.payload);
        expect(body.length).toBe(1);
        expect(body.find((a: AppSchema) => a.id === 'app1')).toBeDefined();
        expect(body.find((a: AppSchema) => a.id === 'app2')).toBeUndefined();
    });

    it('lists additional apps when user is granted permission', async () => {
        // Create a second app
        await app.dataService.setSchema(
            {
                id: 'app2',
                name: 'App 2',
                tables: [],
                views: []
            },
            TEST_USER_EMAIL
        );

        const res = await server.inject({
            method: 'GET',
            url: '/api/apps',
            headers: { Authorization: `Bearer ${token}` }
        });
        expect(res.statusCode).toBe(200);

        const body = JSON.parse(res.payload);
        expect(body.length).toBe(2);
        expect(body.find((a: AppSchema) => a.id === 'app1')).toBeDefined();
        expect(body.find((a: AppSchema) => a.id === 'app2')).toBeDefined();
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
        const getRes = await server.inject({
            method: 'GET',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` }
        });
        expect(getRes.statusCode).toBe(404);

        // Listing apps should not include the deleted app
        const listRes = await server.inject({
            method: 'GET',
            url: '/api/apps',
            headers: { Authorization: `Bearer ${token}` }
        });
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
