import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createTestApp } from '../testUtils.js';
import type SchemaFX from '../../src/index.js';
import { AppFieldType, type Connector } from '../../src/index.js';
import type { FastifyInstance } from 'fastify';

describe('Schema API', () => {
    let app: SchemaFX;
    let server: FastifyInstance;
    let token: string;
    let connector: Connector;

    beforeEach(async () => {
        const testApp = await createTestApp(true);
        app = testApp.app;
        server = app.fastifyInstance;
        connector = testApp.connector;
        token = testApp.token;
    });

    afterEach(async () => {
        await server.close();
    });

    it('should get schema', async () => {
        const response = await server.inject({
            method: 'GET',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload);
        expect(body.id).toBe('app1');
    });

    it('should return mock schema for non-existent schema', async () => {
        const response = await server.inject({
            method: 'GET',
            url: '/api/apps/unknown/schema',
            headers: { Authorization: `Bearer ${token}` }
        });

        expect(response.statusCode).toBe(200);
    });

    it('should save schema (using update action)', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'add',
                element: {
                    partOf: 'tables',
                    element: {
                        id: 'table2',
                        name: 'Table 2',
                        connector: connector.id,
                        path: [],
                        fields: [
                            {
                                id: 'f1',
                                name: 'F1',
                                type: AppFieldType.Text,
                                isRequired: true,
                                isKey: true
                            }
                        ],
                        actions: []
                    }
                }
            }
        });

        expect(response.statusCode).toBe(200);

        const getRes = await server.inject({
            method: 'GET',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` }
        });

        expect(getRes.statusCode).toBe(200);
        expect(JSON.parse(getRes.payload).tables).toHaveLength(2);
    });

    it('should validate schema on add', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'add',
                element: {
                    partOf: 'tables',
                    element: {
                        id: 'table3',
                        // Missing name
                        connector: connector.id,
                        path: [],
                        fields: [],
                        actions: []
                    }
                }
            }
        });

        expect(response.statusCode).toBe(400);
    });
});
