import mock_data from './mock_data.json' with { type: 'json' };
import { Connector, type AppSchema, type AppTableRow } from '../types.js';

export default class MemoryConnector extends Connector {
    schemas: Map<string, AppSchema> = new Map();
    tables: Map<string, Map<string, AppTableRow[]>> = new Map();

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

    async getData(appId: string, tableId: string) {
        if (!this.tables.has(appId)) this.tables.set(appId, new Map());
        return [...(this.tables.get(appId)?.get(tableId) ?? [])];
    }

    async addRow(appId: string, tableId: string, row?: AppTableRow) {
        const data = await this.getData(appId, tableId);
        if (!row) return data;

        data.push(row);

        if (!this.tables.has(appId)) this.tables.set(appId, new Map());
        this.tables.get(appId)?.set(tableId, data);

        return data;
    }

    async updateRow(
        appId: string,
        tableId: string,
        key?: Record<string, unknown>,
        row?: AppTableRow
    ) {
        const data = await this.getData(appId, tableId);

        if (!key || !row) return data;

        const rowIndex = data.findIndex(r => Object.entries(key).every(([k, v]) => r[k] === v));

        if (rowIndex === -1) return data;

        data[rowIndex] = { ...data[rowIndex], ...row };

        if (!this.tables.has(appId)) this.tables.set(appId, new Map());
        this.tables.get(appId)?.set(tableId, data);

        return data;
    }

    async deleteRow(appId: string, tableId: string, key?: Record<string, unknown>) {
        const data = await this.getData(appId, tableId);

        if (!key) return data;

        const rowIndex = data.findIndex(r => Object.entries(key).every(([k, v]) => r[k] === v));

        if (rowIndex === -1) return data;

        data.splice(rowIndex, 1);

        if (!this.tables.has(appId)) this.tables.set(appId, new Map());
        this.tables.get(appId)?.set(tableId, data);

        return data;
    }
}
