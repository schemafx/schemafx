import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import SchemaFX from '../src/index.js';
import MemoryConnector from '../src/connectors/memoryConnector.js';
import {
    AppFieldType,
    Connector,
    ConnectorTableCapability,
    QueryFilterOperator,
    DataSourceType,
    type DataSourceDefinition,
    type ConnectorOptions,
    PermissionTargetType,
    PermissionLevel
} from '../src/types.js';

type BenchmarkConnectorOptions = ConnectorOptions & {
    rowCount: number;
};

class BenchmarkConnector extends Connector {
    rowCount: number;

    constructor(opts: BenchmarkConnectorOptions) {
        super(opts);
        this.rowCount = opts.rowCount ?? 10000;
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

    async getData(): Promise<DataSourceDefinition> {
        const rows = [];
        for (let i = 0; i < this.rowCount; i++) {
            rows.push({
                id: i,
                name: `User ${i}`,
                active: i % 2 === 0
            });
        }

        return {
            type: DataSourceType.Inline,
            data: rows
        };
    }
}

describe('DuckDB Integration Benchmark', () => {
    let app: SchemaFX;
    const schemaId = 'app1';
    let token: string;
    const testEmail = 'dev@schemafx.com';

    beforeAll(async () => {
        const memConnector = new MemoryConnector({ name: 'memory' });
        const benchConnector = new BenchmarkConnector({ name: 'bench', rowCount: 100_000 });

        app = new SchemaFX({
            jwtOpts: { secret: 'supersecret' },
            dataServiceOpts: {
                schemaConnector: {
                    connector: memConnector.id,
                    path: ['schemas']
                },
                connectionsConnector: {
                    connector: memConnector.id,
                    path: ['connections']
                },
                permissionsConnector: {
                    connector: memConnector.id,
                    path: ['permissions']
                },
                connectors: [memConnector, benchConnector]
            }
        });

        await app.fastifyInstance.ready();
        token = app.fastifyInstance.jwt.sign({ email: testEmail });

        await app.dataService.setSchema({
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

        await app.dataService.setPermission({
            id: 'bench-permission',
            targetType: PermissionTargetType.App,
            targetId: schemaId,
            email: testEmail,
            level: PermissionLevel.Read
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
