import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createTestApp } from '../testUtils.js';
import type SchemaFX from '../../src/index.js';
import {
    AppFieldType,
    type Connector,
    AppSchema,
    AppViewType,
    AppActionType
} from '../../src/types.js';
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

    it('should add field and update related views', async () => {
        await connector.saveSchema!('app1', {
            id: 'app1',
            name: 'App 1',
            tables: [
                {
                    id: 't1',
                    name: 'Table 1',
                    connector: connector.id,
                    path: ['t1'],
                    fields: [
                        {
                            id: 'f1',
                            name: 'F1',
                            type: AppFieldType.Text,
                            isKey: true
                        }
                    ],
                    actions: []
                }
            ],
            views: [
                {
                    id: 'v1',
                    name: 'View 1',
                    tableId: 't1',
                    type: AppViewType.Table,
                    config: { fields: ['f1'] }
                }
            ]
        });

        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'add',
                element: {
                    partOf: 'fields',
                    parentId: 't1',
                    element: {
                        id: 'f2',
                        name: 'F2',
                        type: AppFieldType.Text
                    }
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as AppSchema;

        const table = body.tables.find(t => t.id === 't1');
        expect(table?.fields).toHaveLength(2);

        const view = body.views.find(v => v.id === 'v1');
        expect(view?.config.fields).toContain('f2');
        expect(view?.config.fields).toHaveLength(2);
    });

    it('should delete field and update related views', async () => {
        await connector.saveSchema!('app1', {
            id: 'app1',
            name: 'App 1',
            tables: [
                {
                    id: 't1',
                    name: 'Table 1',
                    connector: connector.id,
                    path: ['t1'],
                    fields: [
                        {
                            id: 'f1',
                            name: 'F1',
                            type: AppFieldType.Text,
                            isKey: true
                        },
                        {
                            id: 'f2',
                            name: 'F2',
                            type: AppFieldType.Text
                        }
                    ],
                    actions: []
                }
            ],
            views: [
                {
                    id: 'v1',
                    name: 'View 1',
                    tableId: 't1',
                    type: AppViewType.Table,
                    config: { fields: ['f1', 'f2'] }
                }
            ]
        });

        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'delete',
                element: {
                    partOf: 'fields',
                    parentId: 't1',
                    elementId: 'f2'
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as AppSchema;

        const table = body.tables.find((t: any) => t.id === 't1');
        expect(table?.fields).toHaveLength(1);

        const view = body.views.find((v: any) => v.id === 'v1');
        expect(view?.config.fields).not.toContain('f2');
        expect(view?.config.fields).toHaveLength(1);
    });

    it('should prevent deleting the last key field', async () => {
        await connector.saveSchema!('app1', {
            id: 'app1',
            name: 'App 1',
            tables: [
                {
                    id: 't1',
                    name: 'Table 1',
                    connector: connector.id,
                    path: ['t1'],
                    fields: [
                        {
                            id: 'f1',
                            name: 'F1',
                            type: AppFieldType.Text,
                            isKey: true
                        }
                    ],
                    actions: []
                }
            ],
            views: []
        });

        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'delete',
                element: {
                    partOf: 'fields',
                    parentId: 't1',
                    elementId: 'f1'
                }
            }
        });

        expect(response.statusCode).toBe(500);
        const body = JSON.parse(response.payload);
        expect(body.error).toBe('Internal Server Error');
    });

    it('should prevent updating field to remove key property if no other key exists', async () => {
        await connector.saveSchema!('app1', {
            id: 'app1',
            name: 'App 1',
            tables: [
                {
                    id: 't1',
                    name: 'Table 1',
                    connector: connector.id,
                    path: ['t1'],
                    fields: [
                        {
                            id: 'f1',
                            name: 'F1',
                            type: AppFieldType.Text,
                            isKey: true
                        }
                    ],
                    actions: []
                }
            ],
            views: []
        });

        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'update',
                element: {
                    partOf: 'fields',
                    parentId: 't1',
                    element: {
                        id: 'f1',
                        name: 'F1',
                        type: AppFieldType.Text
                    }
                }
            }
        });

        expect(response.statusCode).toBe(500);
        const body = JSON.parse(response.payload);
        expect(body.error).toBe('Internal Server Error');
    });

    it('should handle actions operations', async () => {
        await connector.saveSchema!('app1', {
            id: 'app1',
            name: 'App 1',
            tables: [
                {
                    id: 't1',
                    name: 'Table 1',
                    connector: connector.id,
                    path: ['t1'],
                    fields: [
                        {
                            id: 'f1',
                            name: 'F1',
                            type: AppFieldType.Text,
                            isKey: true
                        }
                    ],
                    actions: []
                }
            ],
            views: []
        });

        let response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'add',
                element: {
                    partOf: 'actions',
                    parentId: 't1',
                    element: { id: 'a1', name: 'A1', type: AppActionType.Add }
                }
            }
        });

        expect(response.statusCode).toBe(200);
        let body = JSON.parse(response.payload) as AppSchema;
        expect(body.tables[0].actions).toHaveLength(1);

        response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'update',
                element: {
                    partOf: 'actions',
                    parentId: 't1',
                    element: { id: 'a1', name: 'A1 Updated', type: AppActionType.Add }
                }
            }
        });

        expect(response.statusCode).toBe(200);
        body = JSON.parse(response.payload) as AppSchema;
        expect(body.tables[0].actions[0].name).toBe('A1 Updated');

        response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'delete',
                element: {
                    partOf: 'actions',
                    parentId: 't1',
                    elementId: 'a1'
                }
            }
        });
        expect(response.statusCode).toBe(200);
        body = JSON.parse(response.payload) as AppSchema;
        expect(body.tables[0].actions).toHaveLength(0);
    });

    it('should reorder tables', async () => {
        await connector.saveSchema!('app1', {
            id: 'app1',
            name: 'App 1',
            tables: [
                {
                    id: 't1',
                    name: 'T1',
                    connector: connector.id,
                    path: ['t1'],
                    fields: [],
                    actions: []
                },
                {
                    id: 't2',
                    name: 'T2',
                    connector: connector.id,
                    path: ['t2'],
                    fields: [],
                    actions: []
                }
            ],
            views: []
        });

        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'reorder',
                oldIndex: 0,
                newIndex: 1,
                element: { partOf: 'tables' }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as AppSchema;
        expect(body.tables[0].id).toBe('t2');
        expect(body.tables[1].id).toBe('t1');
    });
});
