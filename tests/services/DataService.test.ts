import { describe, it, expect, beforeEach } from 'vitest';
import DataService, { DataServiceOptions } from '../../src/services/DataService.js';
import MemoryConnector from '../../src/connectors/memoryConnector.js';
import { AppActionType, AppFieldType, type AppSchema, Connector } from '../../src/types.js';

describe('DataService', () => {
    let dataService: DataService;
    let connector: MemoryConnector;
    let options: DataServiceOptions;

    beforeEach(() => {
        connector = new MemoryConnector({ name: 'mem' });
        options = {
            schemaConnector: {
                connector: connector.id,
                path: ['schemas']
            },
            connectionsConnector: {
                connector: connector.id,
                path: ['connections']
            },
            connectors: [connector]
        };

        dataService = new DataService(options);
    });

    describe('constructor', () => {
        it('should initialize with valid options', () => {
            expect(dataService).toBeDefined();
            expect(dataService.connectors['mem']).toBeDefined();
        });

        it('should throw error for duplicated connectors', () => {
            const invalidOptions = {
                ...options,
                connectors: [connector, connector]
            };
            expect(() => new DataService(invalidOptions)).toThrow('Duplicated connector "mem".');
        });

        it('should initialize caches with default options', () => {
            expect(dataService.schemaCache).toBeDefined();
            expect(dataService.connectionsCache).toBeDefined();
            expect(dataService.validatorCache).toBeDefined();
        });

        it('should initialize caches with custom options', () => {
            const customOptions: DataServiceOptions = {
                ...options,
                schemaCacheOpts: { max: 10, ttl: 1000 },
                connectionsCacheOpts: { max: 20, ttl: 2000 },
                validatorCacheOpts: { max: 30, ttl: 3000 },
                maxRecursiveDepth: 50,
                encryptionKey: 'test-key'
            };
            const ds = new DataService(customOptions);

            // Access private properties by casting to any for testing purposes
            expect((ds.schemaCache as any).max).toBe(10);
            expect((ds.schemaCache as any).ttl).toBe(1000);
            expect((ds.connectionsCache as any).max).toBe(20);
            expect((ds.connectionsCache as any).ttl).toBe(2000);
            expect((ds.validatorCache as any).max).toBe(30);
            expect((ds.validatorCache as any).ttl).toBe(3000);
            expect(ds.maxRecursiveDepth).toBe(50);
            expect(ds.encryptionKey).toBe('test-key');
        });
    });

    describe('Connections', () => {
        it('should set and get a connection', async () => {
            const connection = {
                id: 'conn1',
                name: 'Connection 1',
                connector: 'mem',
                content: 'some-content'
            };

            await dataService.setConnection(connection);

            const fetched = await dataService.getConnection('conn1');
            expect(fetched).toEqual(connection);

            // Verify it's in cache
            expect(dataService.connectionsCache.has('conn1')).toBe(true);
        });

        it('should return undefined for non-existent connection', async () => {
            const fetched = await dataService.getConnection('non-existent');
            expect(fetched).toBeUndefined();
        });

        it('should return undefined when connectionId is not provided', async () => {
            const fetched = await dataService.getConnection(undefined);
            expect(fetched).toBeUndefined();
        });

        it('should get all connections', async () => {
            const conn1 = { id: 'c1', name: 'C1', connector: 'mem', content: 'c1' };
            const conn2 = { id: 'c2', name: 'C2', connector: 'mem', content: 'c2' };

            await dataService.setConnection(conn1);
            await dataService.setConnection(conn2);

            const connections = await dataService.getConnections();
            expect(connections.length).toBeGreaterThanOrEqual(2);
            expect(connections).toEqual(expect.arrayContaining([conn1, conn2]));
        });

        it('should update an existing connection', async () => {
            const conn = { id: 'update-test', name: 'Original', connector: 'mem', content: 'c1' };
            await dataService.setConnection(conn);

            const updatedConn = { ...conn, name: 'Updated' };
            await dataService.setConnection(updatedConn);

            const fetched = await dataService.getConnection('update-test');
            expect(fetched).toEqual(updatedConn);
        });

        it('should delete a connection', async () => {
            const conn = { id: 'delete-test', name: 'Delete Me', connector: 'mem', content: 'c1' };
            await dataService.setConnection(conn);

            await dataService.deleteConnection('delete-test');
            expect(dataService.connectionsCache.has('delete-test')).toBe(false);

            // Verify connection was deleted from the connector
            const connectorData = connector.tables.get(dataService.connectionsTable.path[0]) ?? [];
            expect(connectorData.find(c => c.id === 'delete-test')).toBeUndefined();

            const fetched = await dataService.getConnection('delete-test');
            expect(fetched).toBeUndefined();
        });

        it('should handle delete of non-existent connection', async () => {
            await expect(dataService.deleteConnection('non-existent')).resolves.not.toThrow();
        });
    });

    describe('Schemas', () => {
        const schema: AppSchema = {
            id: 'app1',
            name: 'App 1',
            tables: [],
            views: []
        };

        it('should set and get a schema', async () => {
            await dataService.setSchema(schema);

            const fetched = await dataService.getSchema('app1');
            expect(fetched).toEqual(schema);
            expect(dataService.schemaCache.has('app1')).toBe(true);
        });

        it('should return undefined for non-existent schema', async () => {
            const fetched = await dataService.getSchema('non-existent');
            expect(fetched).toBeUndefined();
        });

        it('should update an existing schema', async () => {
            await dataService.setSchema(schema);

            const updatedSchema = { ...schema, name: 'App 1 Updated' };
            await dataService.setSchema(updatedSchema);

            const fetched = await dataService.getSchema('app1');
            expect(fetched).toEqual(updatedSchema);
        });

        it('should delete a schema', async () => {
            await dataService.setSchema(schema);

            await dataService.deleteSchema('app1');
            const fetched = await dataService.getSchema('app1');
            expect(fetched).toBeUndefined();
            expect(dataService.schemaCache.has('app1')).toBe(false);
        });

        it('should handle delete of non-existent schema', async () => {
            await expect(dataService.deleteSchema('non-existent')).resolves.not.toThrow();
        });
    });

    describe('Actions & Data', () => {
        const table: any = {
            id: 'users',
            name: 'Users',
            connector: 'mem',
            path: ['users'],
            fields: [
                { id: 'id', name: 'ID', type: AppFieldType.Number, isKey: true },
                { id: 'name', name: 'Name', type: AppFieldType.Text }
            ],
            actions: [
                { id: 'add', name: 'Add', type: AppActionType.Add },
                { id: 'update', name: 'Update', type: AppActionType.Update },
                { id: 'delete', name: 'Delete', type: AppActionType.Delete },
                {
                    id: 'process',
                    name: 'Process',
                    type: AppActionType.Process,
                    config: { actions: ['add'] }
                },
                {
                    id: 'process_empty',
                    name: 'Process Empty',
                    type: AppActionType.Process,
                    config: {}
                },
                {
                    id: 'recursive',
                    name: 'Recursive',
                    type: AppActionType.Process,
                    config: { actions: ['recursive'] }
                }
            ]
        };

        it('should execute add action', async () => {
            await dataService.executeAction({
                table,
                actId: 'add',
                rows: [{ id: 1, name: 'User 1' }]
            });

            const data = await dataService.getData(table);
            expect(data).toHaveLength(1);
            expect(data[0]).toEqual({ id: 1, name: 'User 1' });
        });

        it('should execute update action', async () => {
            await dataService.executeAction({
                table,
                actId: 'add',
                rows: [{ id: 1, name: 'User 1' }]
            });

            await dataService.executeAction({
                table,
                actId: 'update',
                rows: [{ id: 1, name: 'User 1 Updated' }]
            });

            const data = await dataService.getData(table);
            expect(data).toHaveLength(1);
            expect(data[0]).toEqual({ id: 1, name: 'User 1 Updated' });
        });

        it('should execute delete action', async () => {
            await dataService.executeAction({
                table,
                actId: 'add',
                rows: [{ id: 1, name: 'User 1' }]
            });

            await dataService.executeAction({
                table,
                actId: 'delete',
                rows: [{ id: 1 }]
            });

            const data = await dataService.getData(table);
            expect(data).toHaveLength(0);
        });

        it('should execute process action (nested actions)', async () => {
            await dataService.executeAction({
                table,
                actId: 'process',
                rows: [{ id: 2, name: 'User 2' }]
            });

            const data = await dataService.getData(table);
            expect(data).toHaveLength(1); // 'add' was called
            expect(data[0]).toEqual({ id: 2, name: 'User 2' });
        });

        it('should not throw when running action without keys in rows', async () => {
            await dataService.executeAction({
                table,
                actId: 'add',
                rows: [{ name: 'User 1' }]
            });

            await dataService.executeAction({
                table,
                actId: 'update',
                rows: [{ name: 'User 1' }]
            });

            await dataService.executeAction({
                table,
                actId: 'delete',
                rows: [{ name: 'User 1' }]
            });

            // Not thrown
        });

        it('should not throw when running a process action without actions', async () => {
            await dataService.executeAction({
                table,
                actId: 'process_empty',
                rows: [{ id: 2, name: 'User 2' }]
            });

            // Not thrown
        });

        it('should throw error if recursion depth exceeded', async () => {
            await expect(
                dataService.executeAction({
                    table,
                    actId: 'recursive',
                    rows: [{ id: 3 }]
                })
            ).rejects.toThrow('Max recursion depth exceeded.');
        });

        it('should throw error if action not found', async () => {
            await expect(
                dataService.executeAction({
                    table,
                    actId: 'unknown',
                    rows: []
                })
            ).rejects.toThrow('Action unknown not found.');
        });

        it('should filter data', async () => {
            await dataService.executeAction({
                table,
                actId: 'add',
                rows: [
                    { id: 1, name: 'Alice' },
                    { id: 2, name: 'Bob' },
                    { id: 3, name: 'Charlie' }
                ]
            });

            // DuckDB uses strict filtering logic.
            // We need to import QueryFilterOperator in test file
            const data = await dataService.getData(table, {
                filters: [{ field: 'name', operator: 'eq' as any, value: 'Bob' }]
            });

            expect(data).toHaveLength(1);
            expect(data[0].name).toBe('Bob');
        });

        it('should limit data', async () => {
            await dataService.executeAction({
                table,
                actId: 'add',
                rows: [
                    { id: 1, name: 'Alice' },
                    { id: 2, name: 'Bob' }
                ]
            });

            const data = await dataService.getData(table, { limit: 1 });
            expect(data).toHaveLength(1);
        });

        it('should return empty array when connector has no getData', async () => {
            const minimalConnector = new (class MinimalConnector extends Connector {
                async listTables() {
                    return [];
                }

                async getTable() {
                    return undefined;
                }
            })({ name: 'minimal' });

            const minimalOptions: DataServiceOptions = {
                schemaConnector: {
                    connector: minimalConnector.id,
                    path: ['schemas']
                },
                connectionsConnector: {
                    connector: minimalConnector.id,
                    path: ['connections']
                },
                connectors: [minimalConnector]
            };

            const ds = new DataService(minimalOptions);

            const table: any = {
                id: 'test-table',
                name: 'Test Table',
                connector: minimalConnector.id,
                path: ['test'],
                fields: [{ id: 'id', name: 'ID', type: AppFieldType.Number, isKey: true }]
            };

            const data = await ds.getData(table);
            expect(data).toEqual([]);
        });
    });

    describe('Permissions - Cache Invalidation', () => {
        let permissionsConnector: MemoryConnector;
        let dsWithPermissions: DataService;

        beforeEach(() => {
            permissionsConnector = new MemoryConnector({ name: 'perms' });
            const memConnector = new MemoryConnector({ name: 'mem' });

            dsWithPermissions = new DataService({
                schemaConnector: {
                    connector: memConnector.id,
                    path: ['schemas']
                },
                connectionsConnector: {
                    connector: memConnector.id,
                    path: ['connections']
                },
                permissionsConnector: {
                    connector: permissionsConnector.id,
                    path: ['permissions']
                },
                connectors: [memConnector, permissionsConnector]
            });
        });

        it('should invalidate old target cache when permission target changes', async () => {
            // Create initial permission
            await dsWithPermissions.setPermission({
                id: 'perm1',
                targetType: 'app',
                targetId: 'app1',
                email: 'user@example.com',
                level: 'read'
            });

            // Cache the permissions for app1
            const app1Permissions = await dsWithPermissions.getPermissions({
                targetType: 'app',
                targetId: 'app1'
            });
            expect(app1Permissions).toHaveLength(1);

            // Update permission to a different target
            await dsWithPermissions.setPermission({
                id: 'perm1',
                targetType: 'app',
                targetId: 'app2', // Changed from app1 to app2
                email: 'user@example.com',
                level: 'read'
            });

            // Both caches should be invalidated; app1 should now be empty
            const app1PermissionsAfter = await dsWithPermissions.getPermissions({
                targetType: 'app',
                targetId: 'app1'
            });
            expect(app1PermissionsAfter).toHaveLength(0);

            // app2 should have the permission
            const app2Permissions = await dsWithPermissions.getPermissions({
                targetType: 'app',
                targetId: 'app2'
            });
            expect(app2Permissions).toHaveLength(1);
            expect(app2Permissions[0].email).toBe('user@example.com');
        });

        it('should invalidate old target cache when permission targetType changes', async () => {
            // Create initial permission
            await dsWithPermissions.setPermission({
                id: 'perm2',
                targetType: 'app',
                targetId: 'target1',
                email: 'user2@example.com',
                level: 'write'
            });

            // Cache the permissions for app target1
            await dsWithPermissions.getPermissions({
                targetType: 'app',
                targetId: 'target1'
            });

            // Update permission to a different targetType
            await dsWithPermissions.setPermission({
                id: 'perm2',
                targetType: 'connection', // Changed from app to connection
                targetId: 'target1',
                email: 'user2@example.com',
                level: 'write'
            });

            // app/target1 should now be empty
            const appPermissions = await dsWithPermissions.getPermissions({
                targetType: 'app',
                targetId: 'target1'
            });
            expect(appPermissions).toHaveLength(0);

            // connection/target1 should have the permission
            const connPermissions = await dsWithPermissions.getPermissions({
                targetType: 'connection',
                targetId: 'target1'
            });
            expect(connPermissions).toHaveLength(1);
        });
    });
});
