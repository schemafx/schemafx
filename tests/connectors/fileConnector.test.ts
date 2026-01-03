import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import FileConnector from '../../src/connectors/fileConnector.js';
import {
    type AppTable,
    AppFieldType,
    ConnectorTableCapability,
    DataSourceType
} from '../../src/types.js';
import { unlink, readFile, writeFile } from 'fs/promises';
import path from 'path';

const TEST_DB_PATH = path.join(process.cwd(), 'tests', 'test_db.json');

describe('FileConnector', () => {
    let connector: FileConnector;
    const connectorId = 'file';

    beforeEach(async () => {
        // Clean up previous test runs if any
        try {
            await unlink(TEST_DB_PATH);
        } catch {}

        connector = new FileConnector({ name: 'File', id: connectorId, filePath: TEST_DB_PATH });
    });

    afterEach(async () => {
        try {
            await unlink(TEST_DB_PATH);
        } catch {}
    });

    it('should initialize correctly', () => {
        expect(connector.name).toBe('File');
        expect(connector.id).toBe(connectorId);
        expect(connector.filePath).toBe(TEST_DB_PATH);
    });

    it('should generate id if not provided', () => {
        const conn = new FileConnector({ name: 'Test', filePath: TEST_DB_PATH });
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
            // Create a file with some tables
            await writeFile(
                TEST_DB_PATH,
                JSON.stringify({
                    schemas: {},
                    tables: {
                        users: [{ id: 1 }],
                        products: [{ id: 1 }]
                    }
                }),
                'utf-8'
            );

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
            await writeFile(
                TEST_DB_PATH,
                JSON.stringify({
                    schemas: {},
                    tables: {
                        users: [{ id: 1, name: 'Test' }]
                    }
                }),
                'utf-8'
            );

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

        it('should return inline data source with table data', async () => {
            const source = await connector.getData!(table);

            expect(source.type).toBe(DataSourceType.Inline);
            if (source.type === DataSourceType.Inline) {
                expect(Array.isArray(source.data)).toBe(true);
            }
        });

        it('should return empty array for non-existent table', async () => {
            const nonExistentTable: AppTable = {
                ...table,
                path: ['non_existent']
            };
            const source = await connector.getData!(nonExistentTable);

            expect(source.type).toBe(DataSourceType.Inline);
            if (source.type === DataSourceType.Inline) {
                expect(source.data).toEqual([]);
            }
        });

        it('should return correct data for specific table', async () => {
            // First add some data
            await connector.addRow!(table, undefined, { id: 1, name: 'Test' });
            const source = await connector.getData!(table);

            expect(source.type).toBe(DataSourceType.Inline);
            if (source.type === DataSourceType.Inline) {
                expect(source.data).toHaveLength(1);
                expect(source.data[0]).toEqual({ id: 1, name: 'Test' });
            }
        });

        it('should return empty array when table path is empty', async () => {
            const emptyPathTable: AppTable = {
                ...table,
                path: []
            };
            const source = await connector.getData!(emptyPathTable);

            expect(source.type).toBe(DataSourceType.Inline);
            if (source.type === DataSourceType.Inline) {
                expect(source.data).toEqual([]);
            }
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
            // File should not be created
            await expect(readFile(TEST_DB_PATH, 'utf-8')).rejects.toThrow();
        });

        it('should not add row when row is undefined', async () => {
            await connector.addRow!(table, undefined, undefined);
            // File should not be created
            await expect(readFile(TEST_DB_PATH, 'utf-8')).rejects.toThrow();
        });

        it('should add row and persist to file', async () => {
            const row = { id: 1, name: 'User 1' };
            await connector.addRow!(table, undefined, row);

            const content = JSON.parse(await readFile(TEST_DB_PATH, 'utf-8'));
            expect(content.tables.users).toHaveLength(1);
            expect(content.tables.users[0]).toEqual(row);
        });

        it('should add row to existing table', async () => {
            await writeFile(
                TEST_DB_PATH,
                JSON.stringify({
                    schemas: {},
                    tables: {
                        users: [{ id: 1, name: 'User 1' }]
                    }
                }),
                'utf-8'
            );

            await connector.addRow!(table, undefined, { id: 2, name: 'User 2' });

            const content = JSON.parse(await readFile(TEST_DB_PATH, 'utf-8'));
            expect(content.tables.users).toHaveLength(2);
        });

        it('should create new table array if it does not exist', async () => {
            await writeFile(
                TEST_DB_PATH,
                JSON.stringify({
                    schemas: {},
                    tables: {}
                }),
                'utf-8'
            );

            await connector.addRow!(table, undefined, { id: 1, name: 'User 1' });

            const content = JSON.parse(await readFile(TEST_DB_PATH, 'utf-8'));
            expect(content.tables.users).toHaveLength(1);
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
            const emptyPathTable = { ...table, path: [] };
            await connector.updateRow!(
                emptyPathTable,
                undefined,
                { id: 1 },
                { id: 1, name: 'Updated' }
            );
            // No error should occur, but nothing should be written
        });

        it('should not update when key is undefined', async () => {
            await connector.addRow!(table, undefined, { id: 1, name: 'User 1' });
            await connector.updateRow!(table, undefined, undefined, { id: 1, name: 'Updated' });

            const content = JSON.parse(await readFile(TEST_DB_PATH, 'utf-8'));
            expect(content.tables.users[0].name).toBe('User 1');
        });

        it('should not update when row is undefined', async () => {
            await connector.addRow!(table, undefined, { id: 1, name: 'User 1' });
            await connector.updateRow!(table, undefined, { id: 1 }, undefined);

            const content = JSON.parse(await readFile(TEST_DB_PATH, 'utf-8'));
            expect(content.tables.users[0].name).toBe('User 1');
        });

        it('should not update when table does not exist', async () => {
            await writeFile(TEST_DB_PATH, JSON.stringify({ schemas: {}, tables: {} }), 'utf-8');
            await connector.updateRow!(table, undefined, { id: 1 }, { id: 1, name: 'Updated' });
            // No error should occur
        });

        it('should not update when row not found', async () => {
            await connector.addRow!(table, undefined, { id: 1, name: 'User 1' });
            await connector.updateRow!(table, undefined, { id: 999 }, { id: 999, name: 'Updated' });

            const content = JSON.parse(await readFile(TEST_DB_PATH, 'utf-8'));
            expect(content.tables.users).toHaveLength(1);
            expect(content.tables.users[0].name).toBe('User 1');
        });

        it('should update row', async () => {
            await connector.addRow!(table, undefined, { id: 1, name: 'User 1' });
            await connector.updateRow!(table, undefined, { id: 1 }, { id: 1, name: 'Updated' });

            const content = JSON.parse(await readFile(TEST_DB_PATH, 'utf-8'));
            expect(content.tables.users[0].name).toBe('Updated');
        });

        it('should merge row data on update', async () => {
            await connector.addRow!(table, undefined, { id: 1, name: 'User 1', extra: 'data' });
            await connector.updateRow!(table, undefined, { id: 1 }, { name: 'Updated' });

            const content = JSON.parse(await readFile(TEST_DB_PATH, 'utf-8'));
            expect(content.tables.users[0]).toEqual({ id: 1, name: 'Updated', extra: 'data' });
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
            const emptyPathTable = { ...table, path: [] };
            await connector.deleteRow!(emptyPathTable, undefined, { id: 1 });
            // No error should occur
        });

        it('should not delete when key is undefined', async () => {
            await connector.addRow!(table, undefined, { id: 1, name: 'User 1' });
            await connector.deleteRow!(table, undefined, undefined);

            const content = JSON.parse(await readFile(TEST_DB_PATH, 'utf-8'));
            expect(content.tables.users).toHaveLength(1);
        });

        it('should not delete when table does not exist', async () => {
            await writeFile(TEST_DB_PATH, JSON.stringify({ schemas: {}, tables: {} }), 'utf-8');
            await connector.deleteRow!(table, undefined, { id: 1 });
            // No error should occur
        });

        it('should not delete when row not found', async () => {
            await connector.addRow!(table, undefined, { id: 1, name: 'User 1' });
            await connector.deleteRow!(table, undefined, { id: 999 });

            const content = JSON.parse(await readFile(TEST_DB_PATH, 'utf-8'));
            expect(content.tables.users).toHaveLength(1);
        });

        it('should delete row', async () => {
            await connector.addRow!(table, undefined, { id: 1, name: 'User 1' });
            await connector.deleteRow!(table, undefined, { id: 1 });

            const content = JSON.parse(await readFile(TEST_DB_PATH, 'utf-8'));
            expect(content.tables.users).toHaveLength(0);
        });

        it('should delete correct row when multiple exist', async () => {
            await connector.addRow!(table, undefined, { id: 1, name: 'User 1' });
            await connector.addRow!(table, undefined, { id: 2, name: 'User 2' });
            await connector.deleteRow!(table, undefined, { id: 1 });

            const content = JSON.parse(await readFile(TEST_DB_PATH, 'utf-8'));
            expect(content.tables.users).toHaveLength(1);
            expect(content.tables.users[0].id).toBe(2);
        });
    });

    describe('_readDB error handling', () => {
        it('should throw non-ENOENT errors', async () => {
            // Write invalid JSON to cause a parse error
            await writeFile(TEST_DB_PATH, 'not valid json{{{', 'utf-8');

            // This should throw because the file contains invalid JSON
            await expect(connector.listTables([])).rejects.toThrow();
        });
    });
});
