import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createTestApp } from '../testUtils.js';
import SchemaFX from '../../src/index.js';
import MemoryConnector from '../../src/connectors/memoryConnector.js';
import {
    Connector,
    AppSchema,
    AppTable,
    AppFieldType,
    AppActionType,
    QueryFilterOperator
} from '../../src/types.js';
import type { FastifyInstance } from 'fastify';

class LimitedConnector extends Connector {
    private data: any[] = [];

    constructor() {
        super('Limited', 'limited');
        this.data = [
            { id: 1, name: 'Alice', age: 30 },
            { id: 2, name: 'Bob', age: 25 },
            { id: 3, name: 'Charlie', age: 35 }
        ];
    }

    async listTables() {
        return [];
    }

    async getTable(path: string[]): Promise<AppTable> {
        return {
            id: 't1',
            name: 'T1',
            connector: this.id,
            path: ['t1'],
            fields: [
                {
                    id: 'id',
                    name: 'ID',
                    type: AppFieldType.Text,
                    isKey: true
                }
            ],
            actions: []
        };
    }

    async getCapabilities() {
        // Return NO capabilities (no filter, no limit, no offset)
        return {};
    }

    async getData() {
        return [...this.data];
    }
}

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
        await connector.saveSchema!('tree', {
            id: 'tree',
            name: 'Tree',
            tables: [
                {
                    id: 'nodes',
                    name: 'Nodes',
                    connector: connector.id,
                    path: ['nodes'],
                    fields: [
                        {
                            id: 'id',
                            name: 'ID',
                            type: AppFieldType.Text,
                            isKey: true
                        },
                        {
                            id: 'parent',
                            name: 'Parent',
                            type: AppFieldType.Reference,
                            referenceTo: 'nodes'
                        }
                    ],
                    actions: [{ id: 'add', name: 'Add', type: AppActionType.Add }]
                }
            ],
            views: []
        });

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

    it('should handle invalid table', async () => {
        const response = await server.inject({
            method: 'GET',
            url: '/api/apps/app1/data/nonexistent-table',
            headers: { Authorization: `Bearer ${token}` }
        });

        expect(response.statusCode).toBe(400);
        const body = JSON.parse(response.payload);
        expect(body.error).toBe('Data Error');
        expect(body.message).toBe('Invalid table.');
    });

    it('should handle invalid connector', async () => {
        await connector.saveSchema!('bad-connector-app', {
            id: 'bad-connector-app',
            name: 'Bad App',
            tables: [
                {
                    id: 't1',
                    name: 'T1',
                    connector: 'missing-connector',
                    path: ['t1'],
                    fields: [
                        {
                            id: 'id',
                            name: 'ID',
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
            method: 'GET',
            url: '/api/apps/bad-connector-app/data/t1',
            headers: { Authorization: `Bearer ${token}` }
        });

        expect(response.statusCode).toBe(500);
        const body = JSON.parse(response.payload);
        expect(body.error).toBe('Data Error');
        expect(body.message).toBe('Invalid connector.');
    });

    it('should handle nested actions (Process type)', async () => {
        await connector.saveSchema!('process-app', {
            id: 'process-app',
            name: 'Process App',
            tables: [
                {
                    id: 'users',
                    name: 'Users',
                    connector: connector.id,
                    path: ['users'],
                    fields: [
                        {
                            id: 'id',
                            name: 'ID',
                            type: AppFieldType.Number,
                            isKey: true
                        },
                        {
                            id: 'name',
                            name: 'Name',
                            type: AppFieldType.Text
                        }
                    ],
                    actions: [
                        {
                            id: 'add',
                            name: 'Add',
                            type: AppActionType.Add
                        },
                        {
                            id: 'addWrapper',
                            name: 'Add Wrapper',
                            type: AppActionType.Process,
                            config: { actions: ['add'] }
                        }
                    ]
                }
            ],
            views: []
        });

        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/process-app/data/users',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                actionId: 'addWrapper',
                rows: [{ id: 100, name: 'Processed User' }]
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload);
        expect(body.find((u: any) => u.id === 100)).toBeDefined();
    });

    it('should handle recursion depth limit', async () => {
        await connector.saveSchema!('recursion-app', {
            id: 'recursion-app',
            name: 'Recursion App',
            tables: [
                {
                    id: 'users',
                    name: 'Users',
                    connector: connector.id,
                    path: ['users'],
                    fields: [
                        {
                            id: 'id',
                            name: 'ID',
                            type: AppFieldType.Number,
                            isKey: true
                        }
                    ],
                    actions: [
                        {
                            id: 'infinite',
                            name: 'Infinite',
                            type: AppActionType.Process,
                            config: { actions: ['infinite'] }
                        }
                    ]
                }
            ],
            views: []
        });

        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/recursion-app/data/users',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                actionId: 'infinite',
                rows: [{ id: 1 }]
            }
        });

        expect(response.statusCode).toBe(500);
        const body = JSON.parse(response.payload);
        expect(body.error).toBe('Internal Server Error');
    });
});

describe('Data API Manual Filtering (Limited Connector)', () => {
    it('should perform manual filtering and pagination', async () => {
        const limitedConnector = new LimitedConnector();
        const memConnector = new MemoryConnector('Mem', 'mem');

        const app = new SchemaFX({
            jwtOpts: { secret: 'secret' },
            connectorOpts: {
                schemaConnector: {
                    connector: memConnector.id,
                    path: ['schemas']
                },
                connectors: {
                    mem: memConnector,
                    limited: limitedConnector
                }
            }
        });

        const server = app.fastifyInstance;
        await server.ready();
        const token = app.fastifyInstance.jwt.sign({ sub: 'user' });

        const schema = {
            id: 'limited-app',
            name: 'Limited App',
            tables: [
                {
                    id: 'users',
                    name: 'Users',
                    connector: limitedConnector.id,
                    path: ['users'],
                    fields: [
                        { id: 'id', name: 'ID', type: AppFieldType.Number, isKey: true },
                        { id: 'name', name: 'Name', type: AppFieldType.Text },
                        { id: 'age', name: 'Age', type: AppFieldType.Number }
                    ],
                    actions: []
                }
            ],
            views: []
        };

        await memConnector.saveSchema('limited-app', schema as unknown as AppSchema);

        const response = await server.inject({
            method: 'GET',
            url: `/api/apps/limited-app/data/users?query=${encodeURIComponent(
                JSON.stringify({
                    filters: [
                        { field: 'age', operator: QueryFilterOperator.GreaterThan, value: 28 }
                    ]
                })
            )}`,
            headers: { Authorization: `Bearer ${token}` }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload);
        expect(body).toHaveLength(2);
        expect(body.map((u: any) => u.name).sort()).toEqual(['Alice', 'Charlie']);

        const response2 = await server.inject({
            method: 'GET',
            url: `/api/apps/limited-app/data/users?query=${encodeURIComponent(
                JSON.stringify({
                    filters: [
                        { field: 'age', operator: QueryFilterOperator.GreaterThan, value: 20 }
                    ],
                    offset: 1,
                    limit: 1
                })
            )}`,
            headers: { Authorization: `Bearer ${token}` }
        });

        expect(response2.statusCode).toBe(200);
        const body2 = JSON.parse(response2.payload);
        expect(body2).toHaveLength(1);
        expect(body2[0].name).toBe('Bob');

        await server.close();
    });
});
