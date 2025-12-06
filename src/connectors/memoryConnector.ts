import mock_data from './mock_data.json' with { type: 'json' };
import {
    Connector,
    type AppSchema,
    type AppTableRow,
    type AppTable,
    inferTable
} from '../types.js';

export default class MemoryConnector extends Connector {
    schemas: Map<string, AppSchema> = new Map();
    tables: Map<string, AppTableRow[]> = new Map();

    async listTables(path: string[]) {
        if (path.length > 0) return [];
        const tables = new Set<string>();

        for (const [tableId] of this.tables) tables.add(tableId);

        return Array.from(tables).map(tableId => ({
            name: tableId,
            path: [tableId],
            capabilities: ['Connect' as const]
        }));
    }

    async getTable(path: string[]) {
        const tableId = path[0];
        return inferTable(tableId, path, this.tables.get(tableId) || [], this.id);
    }

    async getCapabilities() {
        // In-Memory capabilities only.
        // Default capability handler.
        return {};
    }

    async getSchema(appId: string) {
        let schema = this.schemas.get(appId);

        if (!schema) {
            schema = { ...mock_data } as unknown as AppSchema;
            await this.saveSchema(appId, schema);
        }

        return schema;
    }

    async saveSchema(appId: string, schema: AppSchema) {
        this.schemas.set(appId, schema);
        return schema;
    }

    async deleteSchema(appId: string) {
        this.schemas.delete(appId);
    }

    async getData(table: AppTable) {
        return [...(this.tables.get(table.path[0]) ?? [])];
    }

    async addRow(table: AppTable, row?: AppTableRow) {
        const data = await this.getData(table);
        if (!row) return data;

        data.push(row);
        this.tables.set(table.path[0], data);

        return data;
    }

    async updateRow(table: AppTable, key?: Record<string, unknown>, row?: AppTableRow) {
        const data = await this.getData(table);

        if (!key || !row) return data;

        const rowIndex = data.findIndex(r => Object.entries(key).every(([k, v]) => r[k] === v));

        if (rowIndex === -1) return data;

        data[rowIndex] = { ...data[rowIndex], ...row };
        this.tables.set(table.path[0], data);

        return data;
    }

    async deleteRow(table: AppTable, key?: Record<string, unknown>) {
        const data = await this.getData(table);

        if (!key) return data;

        const rowIndex = data.findIndex(r => Object.entries(key).every(([k, v]) => r[k] === v));

        if (rowIndex === -1) return data;

        data.splice(rowIndex, 1);
        this.tables.set(table.path[0], data);

        return data;
    }
}
