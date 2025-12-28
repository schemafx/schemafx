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

    it('should return 404 for unknown app schema', async () => {
        const response = await server.inject({
            method: 'GET',
            url: '/api/apps/unknown-app/schema',
            headers: { Authorization: `Bearer ${token}` }
        });

        expect(response.statusCode).toBe(404);
    });

    it('should return 404 for unknown app schema when updating', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/unknown-app/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'add',
                element: {
                    partOf: 'views',
                    element: { id: 'view1', name: 'View 1', tableId: 't1', type: 'table' }
                }
            }
        });

        expect(response.statusCode).toBe(404);
    });

    it('should prevent updating field to remove all keys', async () => {
        // The table 'users' has 'id' as Key.
        // Try to update 'id' to not be a key.
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'update',
                element: {
                    partOf: 'fields',
                    parentId: 'users',
                    element: { id: 'id', name: 'ID', type: 'number', isKey: false }
                }
            }
        });

        expect(response.statusCode).toBe(500);

        // Verify schema was NOT updated
        const schema = await app.dataService.getSchema('app1');
        const table = schema?.tables.find(t => t.id === 'users');
        const idField = table?.fields.find(f => f.id === 'id');
        expect(idField?.isKey).toBe(true);
    });

    it('should prevent deleting key field', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'delete',
                element: {
                    partOf: 'fields',
                    parentId: 'users',
                    elementId: 'id'
                }
            }
        });

        expect(response.statusCode).toBe(500);

        // Verify field was NOT deleted
        const schema = await app.dataService.getSchema('app1');
        const table = schema?.tables.find(t => t.id === 'users');
        const idField = table?.fields.find(f => f.id === 'id');
        expect(idField).toBeDefined();
    });

    it('should delete a table', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'delete',
                element: {
                    partOf: 'tables',
                    elementId: 'users'
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload);
        expect(body.tables).toHaveLength(0);
    });

    it('should delete an action', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'delete',
                element: {
                    partOf: 'actions',
                    parentId: 'users',
                    elementId: 'add'
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload);
        const table = body.tables.find((t: any) => t.id === 'users');
        expect(table.actions.find((a: any) => a.id === 'add')).toBeUndefined();
    });

    it('should reorder actions', async () => {
        // actions: add (0), update (1), delete (2)
        // move delete to 0
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'reorder',
                oldIndex: 2,
                newIndex: 0,
                element: {
                    partOf: 'actions',
                    parentId: 'users'
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload);
        const table = body.tables.find((t: any) => t.id === 'users');
        expect(table.actions[0].id).toBe('delete');
    });

    it('should reorder tables', async () => {
        // Create another table to reorder
        const schema = await app.dataService.getSchema('app1');
        if (!schema) throw new Error('Schema not found');
        schema.tables.push({ ...schema.tables[0], id: 'users2', name: 'Users 2' });
        await app.dataService.setSchema(schema);

        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'reorder',
                oldIndex: 1,
                newIndex: 0,
                element: {
                    partOf: 'tables'
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload);
        expect(body.tables[0].id).toBe('users2');
    });
});
