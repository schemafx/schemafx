import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createTestApp } from '../testUtils.js';
import type SchemaFX from '../../src/index.js';
import {
    AppActionType,
    AppFieldType,
    type AppSchema,
    AppViewType,
    type Connector
} from '../../src/index.js';
import type { FastifyInstance } from 'fastify';

describe('Schema Operations', () => {
    let app: SchemaFX;
    let server: FastifyInstance;
    let connector: Connector;
    let token: string;

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

    it('should add a view', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'add',
                element: {
                    partOf: 'views',
                    element: {
                        id: 'view1',
                        name: 'View 1',
                        tableId: 'users',
                        type: AppViewType.Table,
                        config: {}
                    }
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload);
        expect(body.views).toHaveLength(1);
        expect(body.views[0].id).toBe('view1');
    });

    it('should add a field', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'add',
                element: {
                    partOf: 'fields',
                    parentId: 'users',
                    element: {
                        id: 'email',
                        name: 'Email',
                        type: AppFieldType.Email
                    }
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload);
        const table = body.tables.find((t: any) => t.id === 'users');
        expect(table.fields).toHaveLength(3);
        expect(table.fields.find((f: any) => f.id === 'email')).toBeDefined();
    });

    it('should add an action', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'add',
                element: {
                    partOf: 'actions',
                    parentId: 'users',
                    element: {
                        id: 'export',
                        name: 'Export',
                        type: AppActionType.Process,
                        config: {}
                    }
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as AppSchema;
        const table = body.tables.find((t: any) => t.id === 'users');

        // seeded actions (3) + new one
        expect(table?.actions).toHaveLength(4);
        expect(table?.actions.find(a => a.id === 'export')).toBeDefined();
    });

    it('should update a table', async () => {
        const table = {
            id: 'users',
            name: 'Users Updated',
            connector: connector.id,
            path: ['users'],
            fields: [{ id: 'id', name: 'ID', type: AppFieldType.Number, isKey: true }],
            actions: []
        };

        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'update',
                element: {
                    partOf: 'tables',
                    element: table
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as AppSchema;
        const updatedTable = body.tables.find(t => t.id === 'users');
        expect(updatedTable?.name).toBe('Users Updated');
    });

    it('should delete a field', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'delete',
                element: {
                    partOf: 'fields',
                    parentId: 'users',
                    elementId: 'name'
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as AppSchema;
        const table = body.tables.find(t => t.id === 'users');
        expect(table!.fields.find(f => f.id === 'name')).toBeUndefined();
    });

    it('should reorder fields', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'reorder',
                oldIndex: 0,
                newIndex: 1,
                element: {
                    partOf: 'fields',
                    parentId: 'users'
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as AppSchema;
        const table = body.tables.find((t: any) => t.id === 'users');
        expect(table?.fields[0].id).toBe('name');
        expect(table?.fields[1].id).toBe('id');
    });
});
