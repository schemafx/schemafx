import { describe, it, expect, beforeEach } from 'vitest';
import MemoryConnector from '../../src/connectors/memoryConnector.js';
import { type AppTable, AppFieldType } from '../../src/types.js';

describe('MemoryConnector', () => {
    let connector: MemoryConnector;
    const connectorId = 'mem';

    beforeEach(() => {
        connector = new MemoryConnector('Memory', connectorId);
    });

    it('should initialize correctly', () => {
        expect(connector.name).toBe('Memory');
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
        const result = await connector.getSchema!('app1');
        expect(result).toEqual(schema);
    });

    // Current API adds mock data by default.
    /*it('should throw when getting non-existent schema', async () => {
        await expect(connector.getSchema!('unknown')).rejects.toThrow();
    });

    it('should delete schema', async () => {
        const schema = { id: 'app1', name: 'App 1', tables: [], views: [] };
        await connector.saveSchema!('app1', schema);
        await connector.deleteSchema!('app1');
        await expect(connector.getSchema!('app1')).rejects.toThrow();
    });*/

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

        it('should add row', async () => {
            const row = { id: 1, name: 'User 1' };
            await connector.addRow!(table, row);

            const data = await connector.getData!(table);
            expect(data).toHaveLength(1);
            expect(data[0]).toEqual(row);
        });

        it('should get data', async () => {
            await connector.addRow!(table, { id: 1, name: 'User 1' });
            await connector.addRow!(table, { id: 2, name: 'User 2' });

            const data = await connector.getData!(table);
            expect(data).toHaveLength(2);
        });

        it('should update row', async () => {
            await connector.addRow!(table, { id: 1, name: 'User 1' });
            await connector.updateRow!(table, { id: 1 }, { id: 1, name: 'Updated' });

            const data = await connector.getData!(table);
            expect(data[0].name).toBe('Updated');
        });

        it('should delete row', async () => {
            await connector.addRow!(table, { id: 1, name: 'User 1' });
            await connector.deleteRow!(table, { id: 1 });
            const data = await connector.getData!(table);
            expect(data).toHaveLength(0);
        });
    });
});
