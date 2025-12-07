import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createTestApp } from '../testUtils.js';
import type SchemaFX from '../../src/index.js';
import type { Connector } from '../../src/index.js';
import type { FastifyInstance } from 'fastify';

describe('Data API', () => {
    let app: SchemaFX;
    let server: FastifyInstance;
    let token: string;
    let connector: Connector;

    beforeEach(async () => {
        const testApp = await createTestApp(true);
        app = testApp.app;
        connector = testApp.connector;
        server = app.fastifyInstance;
        token = testApp.token;
    });

    afterEach(async () => {
        await server.close();
    });

    it('should list data', async () => {
        const response = await server.inject({
            method: 'GET',
            url: '/api/apps/app1/data/users',
            headers: { Authorization: `Bearer ${token}` }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload);
        expect(body).toHaveLength(1);
        expect(body[0].id).toBe(1);
    });

    it('should filter data', async () => {
        await server.inject({
            method: 'POST',
            url: '/api/apps/app1/data/users',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                actionId: 'add',
                rows: [{ id: 2, name: 'User 2' }]
            }
        });

        const response = await server.inject({
            method: 'GET',
            url:
                '/api/apps/app1/data/users?query=' +
                encodeURIComponent(
                    JSON.stringify({
                        filters: [{ field: 'id', operator: 'eq', value: 2 }]
                    })
                ),
            headers: { Authorization: `Bearer ${token}` }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload);
        expect(body).toHaveLength(1);
        expect(body[0].id).toBe(2);
    });

    it('should create data', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/data/users',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                actionId: 'add',
                rows: [{ id: 2, name: 'User 2' }]
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload);
        expect(body).toHaveLength(2); // Returns full list
        expect(body.find((u: any) => u.id === 2)).toBeDefined();
    });

    it('should validate data on create', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/data/users',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                actionId: 'add',
                rows: [{ id: 'should be number', name: 'User 2' }]
            }
        });

        expect(response.statusCode).toBe(400);
    });

    it('should update data', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/data/users',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                actionId: 'update',
                rows: [{ id: 1, name: 'Updated' }]
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload);
        expect(body.find((u: any) => u.id === 1).name).toBe('Updated');
    });

    it('should delete data', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/data/users',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                actionId: 'delete',
                rows: [{ id: 1 }]
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload);
        expect(body).toHaveLength(0);
    });

    it('should handle recursive relationship max depth', async () => {
        const recursiveSchema = {
            id: 'tree',
            name: 'Tree',
            tables: [
                {
                    id: 'nodes',
                    name: 'Nodes',
                    connector: 'mem',
                    path: ['nodes'],
                    fields: [
                        { id: 'id', name: 'ID', type: 'text', isRequired: true, isKey: true },
                        { id: 'parent', name: 'Parent', type: 'reference', referenceTo: 'nodes' }
                    ],
                    actions: [{ id: 'add', name: 'Add', type: 'add' }]
                }
            ],
            views: []
        };

        await connector.saveSchema!('tree', recursiveSchema as any);

        await server.inject({
            method: 'POST',
            url: '/api/apps/tree/data/nodes',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                actionId: 'add',
                rows: [{ id: 'root' }]
            }
        });

        const response = await server.inject({
            method: 'GET',
            url: '/api/apps/tree/data/nodes',
            headers: { Authorization: `Bearer ${token}` }
        });

        expect(response.statusCode).toBe(200);
    });
});
