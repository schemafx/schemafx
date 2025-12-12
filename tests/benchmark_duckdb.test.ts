import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import SchemaFX from '../src/index.js';
import MemoryConnector from '../src/connectors/memoryConnector.js';
import {
    AppFieldType,
    type AppTable,
    Connector,
    ConnectorTableCapability,
    QueryFilterOperator
} from '../src/types.js';
import { Readable } from 'node:stream';

class BenchmarkConnector extends Connector {
    rowCount: number;

    constructor(name: string, rowCount: number = 100000, id?: string) {
        super(name, id);
        this.rowCount = rowCount;
    }

    async listTables() {
        return [
            {
                name: 'users',
                path: ['users'],
                capabilities: [ConnectorTableCapability.Connect]
            }
        ];
    }

    async getTable() {
        return {
            id: 'users',
            name: 'Users',
            connector: this.id,
            path: ['users'],
            fields: [
                { id: 'id', name: 'ID', type: AppFieldType.Number },
                { id: 'name', name: 'Name', type: AppFieldType.Text },
                { id: 'active', name: 'Active', type: AppFieldType.Boolean }
            ],
            actions: []
        };
    }

    async getDataStream() {
        let current = 0;
        const max = this.rowCount;

        return new Readable({
            objectMode: true,
            read() {
                if (current >= max) {
                    this.push(null);
                    return;
                }

                for (let i = 0; i < 100 && current < max; i++) {
                    this.push({
                        id: current,
                        name: `User ${current}`,
                        active: current % 2 === 0
                    });

                    current++;
                }
            }
        });
    }

    async getData() {
        const rows = [];
        for (let i = 0; i < this.rowCount; i++) {
            rows.push({
                id: i,
                name: `User ${i}`,
                active: i % 2 === 0
            });
        }

        return rows;
    }
}

describe('DuckDB Integration Benchmark', () => {
    let app: SchemaFX;
    const schemaId = 'app1';
    let token: string;

    beforeAll(async () => {
        const memConnector = new MemoryConnector('memory');
        const benchConnector = new BenchmarkConnector('bench', 100_000);

        app = new SchemaFX({
            jwtOpts: { secret: 'supersecret' },
            dataServiceOpts: {
                schemaConnector: {
                    connector: memConnector.id,
                    path: ['schemas']
                },
                connectors: [memConnector, benchConnector]
            }
        });

        await app.fastifyInstance.ready();
        token = app.fastifyInstance.jwt.sign({ sub: 'user1' });

        await app.dataService.setSchema(schemaId, {
            id: schemaId,
            name: 'Benchmark App',
            tables: [
                {
                    id: 'users',
                    name: 'Users',
                    connector: benchConnector.id,
                    path: ['users'],
                    fields: [
                        { id: 'id', name: 'ID', type: AppFieldType.Number },
                        { id: 'name', name: 'Name', type: AppFieldType.Text },
                        { id: 'active', name: 'Active', type: AppFieldType.Boolean }
                    ],
                    actions: []
                }
            ],
            views: []
        });
    });

    afterAll(async () => {
        await app.fastifyInstance.close();
    });

    it('should query data using DuckDB with filters and pagination', async () => {
        const response = await app.fastifyInstance.inject({
            method: 'GET',
            url: `/api/apps/${schemaId}/data/users`,
            query: {
                query: JSON.stringify({
                    filters: [
                        { field: 'id', operator: QueryFilterOperator.GreaterThan, value: 50000 },
                        { field: 'active', operator: QueryFilterOperator.Equals, value: true }
                    ],
                    limit: 5,
                    offset: 0
                })
            },
            headers: {
                Authorization: `Bearer ${token}`
            }
        });

        expect(response.statusCode).toBe(200);
        const data = JSON.parse(response.payload);

        expect(data).toHaveLength(5);
        expect(data[0]).toEqual({ id: 50002, name: 'User 50002', active: true });
    });

    it('should handle limit and offset correctly without filters', async () => {
        const response = await app.fastifyInstance.inject({
            method: 'GET',
            url: `/api/apps/${schemaId}/data/users`,
            query: {
                query: JSON.stringify({
                    limit: 3,
                    offset: 10
                })
            },
            headers: {
                Authorization: `Bearer ${token}`
            }
        });

        expect(response.statusCode).toBe(200);
        const data = JSON.parse(response.payload);

        expect(data).toHaveLength(3);
        expect(data[0].id).toBe(10);
        expect(data[2].id).toBe(12);
    });
});
