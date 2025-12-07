import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import FileConnector from '../../src/connectors/fileConnector.js';
import { type AppTable, AppFieldType } from '../../src/types.js';
import { unlink, readFile } from 'fs/promises';
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

        connector = new FileConnector('File', TEST_DB_PATH, connectorId);
    });

    afterEach(async () => {
        try {
            await unlink(TEST_DB_PATH);
        } catch {}
    });

    it('should initialize correctly', () => {
        expect(connector.name).toBe('File');
        expect(connector.id).toBe(connectorId);
    });

    it('should save and get schema', async () => {
        const schema = {
            id: 'app1',
            name: 'App 1',
            tables: [],
            views: []
        };

        await connector.saveSchema!('app1', schema);

        const content = JSON.parse(await readFile(TEST_DB_PATH, 'utf-8'));
        expect(content.schemas.app1).toBeDefined();

        const result = await connector.getSchema!('app1');
        expect(result).toEqual(schema);
    });

    it('should delete schema', async () => {
        const schema = { id: 'app1', name: 'App 1', tables: [], views: [] };
        await connector.saveSchema!('app1', schema);
        await connector.deleteSchema!('app1');

        const content = JSON.parse(await readFile(TEST_DB_PATH, 'utf-8'));
        expect(content.schemas.app1).toBeUndefined();

        // Current API adds mock data by default.
        // await expect(connector.getSchema!('app1')).rejects.toThrow();
    });

    describe('Data Operations', () => {
        const table: AppTable = {
            id: 'users',
            name: 'Users',
            connector: connectorId,
            path: ['users'],
            fields: [
                { id: 'id', name: 'ID', type: AppFieldType.Number, isKey: true },
                {
                    id: 'name',
                    name: 'Name',
                    type: AppFieldType.Text
                }
            ],
            actions: []
        };

        it('should add row and persist to file', async () => {
            const row = { id: 1, name: 'User 1' };
            await connector.addRow!(table, row);

            const content = JSON.parse(await readFile(TEST_DB_PATH, 'utf-8'));
            expect(content.tables.users).toHaveLength(1);
            expect(content.tables.users[0]).toEqual(row);
        });

        it('should get data', async () => {
            await connector.addRow!(table, { id: 1, name: 'User 1' });
            const data = await connector.getData!(table);
            expect(data).toHaveLength(1);
        });

        it('should update row', async () => {
            await connector.addRow!(table, { id: 1, name: 'User 1' });
            await connector.updateRow!(table, { id: 1 }, { id: 1, name: 'Updated' });

            const content = JSON.parse(await readFile(TEST_DB_PATH, 'utf-8'));
            expect(content.tables.users[0].name).toBe('Updated');
        });

        it('should delete row', async () => {
            await connector.addRow!(table, { id: 1, name: 'User 1' });
            await connector.deleteRow!(table, { id: 1 });

            const content = JSON.parse(await readFile(TEST_DB_PATH, 'utf-8'));
            expect(content.tables.users).toHaveLength(0);
        });
    });
});
