import { describe, it, expect, beforeEach } from 'vitest';
import MemoryConnector from '../../src/connectors/memoryConnector.js';
import { type AppTable, AppFieldType, ConnectorTableCapability } from '../../src/types.js';

describe('MemoryConnector', () => {
    let connector: MemoryConnector;
    const connectorId = 'mem';

    beforeEach(() => {
        connector = new MemoryConnector('Memory', connectorId);
    });

    it('should initialize correctly', () => {
        expect(connector.name).toBe('Memory');
        expect(connector.id).toBe(connectorId);
        expect(connector.schemas).toBeInstanceOf(Map);
        expect(connector.tables).toBeInstanceOf(Map);
    });

    it('should generate id if not provided', () => {
        const conn = new MemoryConnector('Test');
        expect(conn.id).toBeDefined();
        expect(conn.id.length).toBeGreaterThan(0);
    });

    describe('listTables', () => {
        it('should return empty array when path is not empty', async () => {
            const tables = await connector.listTables(['some', 'path']);
            expect(tables).toEqual([]);
        });

        it('should return empty array when no tables exist', async () => {
            const tables = await connector.listTables([]);
            expect(tables).toEqual([]);
        });

        it('should return list of tables with capabilities', async () => {
            connector.tables.set('users', [{ id: 1 }]);
            connector.tables.set('products', [{ id: 1 }]);

            const tables = await connector.listTables([]);
            expect(tables).toHaveLength(2);
            expect(tables).toContainEqual({
                name: 'users',
                path: ['users'],
                capabilities: [ConnectorTableCapability.Connect]
            });
            expect(tables).toContainEqual({
                name: 'products',
                path: ['products'],
                capabilities: [ConnectorTableCapability.Connect]
            });
        });
    });

    describe('getTable', () => {
        it('should return undefined when path is empty', async () => {
            const table = await connector.getTable([]);
            expect(table).toBeUndefined();
        });

        it('should return inferred table when path is valid', async () => {
            connector.tables.set('users', [{ id: 1, name: 'Test' }]);

            const table = await connector.getTable(['users']);
            expect(table).toBeDefined();
            expect(table?.name).toBe('users');
            expect(table?.connector).toBe(connectorId);
        });

        it('should return empty table when table does not exist', async () => {
            const table = await connector.getTable(['nonexistent']);
            expect(table).toBeDefined();
            expect(table?.fields).toEqual([]);
        });
    });

    describe('getCapabilities', () => {
        it('should return empty capabilities object', async () => {
            const capabilities = await connector.getCapabilities();
            expect(capabilities).toEqual({});
        });
    });

    describe('getData', () => {
        const table: AppTable = {
            id: 'users',
            name: 'Users',
            connector: connectorId,
            path: ['users'],
            fields: [],
            actions: []
        };

        it('should return empty array when path is empty', async () => {
            const emptyPathTable = { ...table, path: [] };
            const data = await connector.getData!(emptyPathTable);
            expect(data).toEqual([]);
        });

        it('should return empty array when table does not exist', async () => {
            const data = await connector.getData!(table);
            expect(data).toEqual([]);
        });

        it('should return copy of data when table exists', async () => {
            connector.tables.set('users', [{ id: 1, name: 'Test' }]);

            const data = await connector.getData!(table);
            expect(data).toHaveLength(1);
            expect(data[0]).toEqual({ id: 1, name: 'Test' });

            // Verify it's a copy, not the original reference
            data.push({ id: 2, name: 'New' });
            expect(connector.tables.get('users')).toHaveLength(1);
        });
    });

    describe('addRow', () => {
        const table: AppTable = {
            id: 'users',
            name: 'Users',
            connector: connectorId,
            path: ['users'],
            fields: [
                { id: 'id', name: 'ID', type: AppFieldType.Number, isKey: true },
                { id: 'name', name: 'Name', type: AppFieldType.Text }
            ],
            actions: []
        };

        it('should not add row when path is empty', async () => {
            const emptyPathTable = { ...table, path: [] };
            await connector.addRow!(emptyPathTable, undefined, { id: 1 });
            expect(connector.tables.size).toBe(0);
        });

        it('should not add row when row is undefined', async () => {
            await connector.addRow!(table, undefined, undefined);
            expect(connector.tables.has('users')).toBe(false);
        });

        it('should add row', async () => {
            const row = { id: 1, name: 'User 1' };
            await connector.addRow!(table, undefined, row);

            const data = await connector.getData!(table);
            expect(data).toHaveLength(1);
            expect(data[0]).toEqual(row);
        });

        it('should add row to existing table', async () => {
            connector.tables.set('users', [{ id: 1, name: 'User 1' }]);

            await connector.addRow!(table, undefined, { id: 2, name: 'User 2' });

            const data = await connector.getData!(table);
            expect(data).toHaveLength(2);
        });

        it('should create new table if it does not exist', async () => {
            await connector.addRow!(table, undefined, { id: 1, name: 'User 1' });

            expect(connector.tables.has('users')).toBe(true);
            expect(connector.tables.get('users')).toHaveLength(1);
        });
    });

    describe('updateRow', () => {
        const table: AppTable = {
            id: 'users',
            name: 'Users',
            connector: connectorId,
            path: ['users'],
            fields: [
                { id: 'id', name: 'ID', type: AppFieldType.Number, isKey: true },
                { id: 'name', name: 'Name', type: AppFieldType.Text }
            ],
            actions: []
        };

        it('should not update when path is empty', async () => {
            connector.tables.set('users', [{ id: 1, name: 'User 1' }]);
            const emptyPathTable = { ...table, path: [] };
            await connector.updateRow!(
                emptyPathTable,
                undefined,
                { id: 1 },
                { id: 1, name: 'Updated' }
            );
            expect(connector.tables.get('users')![0].name).toBe('User 1');
        });

        it('should not update when key is undefined', async () => {
            await connector.addRow!(table, undefined, { id: 1, name: 'User 1' });
            await connector.updateRow!(table, undefined, undefined, { id: 1, name: 'Updated' });

            const data = await connector.getData!(table);
            expect(data[0].name).toBe('User 1');
        });

        it('should not update when row is undefined', async () => {
            await connector.addRow!(table, undefined, { id: 1, name: 'User 1' });
            await connector.updateRow!(table, undefined, { id: 1 }, undefined);

            const data = await connector.getData!(table);
            expect(data[0].name).toBe('User 1');
        });

        it('should not update when row not found', async () => {
            await connector.addRow!(table, undefined, { id: 1, name: 'User 1' });
            await connector.updateRow!(table, undefined, { id: 999 }, { id: 999, name: 'Updated' });

            const data = await connector.getData!(table);
            expect(data).toHaveLength(1);
            expect(data[0].name).toBe('User 1');
        });

        it('should update row', async () => {
            await connector.addRow!(table, undefined, { id: 1, name: 'User 1' });
            await connector.updateRow!(table, undefined, { id: 1 }, { id: 1, name: 'Updated' });

            const data = await connector.getData!(table);
            expect(data[0].name).toBe('Updated');
        });

        it('should merge row data on update', async () => {
            await connector.addRow!(table, undefined, { id: 1, name: 'User 1', extra: 'data' });
            await connector.updateRow!(table, undefined, { id: 1 }, { name: 'Updated' });

            const data = await connector.getData!(table);
            expect(data[0]).toEqual({ id: 1, name: 'Updated', extra: 'data' });
        });
    });

    describe('deleteRow', () => {
        const table: AppTable = {
            id: 'users',
            name: 'Users',
            connector: connectorId,
            path: ['users'],
            fields: [
                { id: 'id', name: 'ID', type: AppFieldType.Number, isKey: true },
                { id: 'name', name: 'Name', type: AppFieldType.Text }
            ],
            actions: []
        };

        it('should not delete when path is empty', async () => {
            connector.tables.set('users', [{ id: 1, name: 'User 1' }]);
            const emptyPathTable = { ...table, path: [] };
            await connector.deleteRow!(emptyPathTable, undefined, { id: 1 });
            expect(connector.tables.get('users')).toHaveLength(1);
        });

        it('should not delete when key is undefined', async () => {
            await connector.addRow!(table, undefined, { id: 1, name: 'User 1' });
            await connector.deleteRow!(table, undefined, undefined);

            const data = await connector.getData!(table);
            expect(data).toHaveLength(1);
        });

        it('should not delete when row not found', async () => {
            await connector.addRow!(table, undefined, { id: 1, name: 'User 1' });
            await connector.deleteRow!(table, undefined, { id: 999 });

            const data = await connector.getData!(table);
            expect(data).toHaveLength(1);
        });

        it('should delete row', async () => {
            await connector.addRow!(table, undefined, { id: 1, name: 'User 1' });
            await connector.deleteRow!(table, undefined, { id: 1 });
            const data = await connector.getData!(table);
            expect(data).toHaveLength(0);
        });

        it('should delete correct row when multiple exist', async () => {
            await connector.addRow!(table, undefined, { id: 1, name: 'User 1' });
            await connector.addRow!(table, undefined, { id: 2, name: 'User 2' });
            await connector.deleteRow!(table, undefined, { id: 1 });

            const data = await connector.getData!(table);
            expect(data).toHaveLength(1);
            expect(data[0].id).toBe(2);
        });
    });
});
