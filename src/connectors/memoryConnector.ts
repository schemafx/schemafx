import mock_data from './mock_data.json' with { type: 'json' };
import { Connector, type AppSchema, type AppTableRow } from '../types.js';

export default class MemoryConnector extends Connector {
    schemas: Map<string, AppSchema> = new Map();
    tables: Map<string, Map<string, AppTableRow[]>> = new Map();

    async getSchema(appId: string) {
        let schema = this.schemas.get(appId);

        if (!schema) {
            schema = { ...mock_data } as AppSchema;
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

    async updateRow(appId: string, tableId: string, rowIndex?: number, row?: AppTableRow) {
        const data = await this.getData(appId, tableId);

        if (typeof rowIndex !== 'number' || !data[rowIndex] || !row) return data;

        data[rowIndex] = { ...row };

        if (!this.tables.has(appId)) this.tables.set(appId, new Map());
        this.tables.get(appId)?.set(tableId, data);

        return data;
    }

    async deleteRow(appId: string, tableId: string, rowIndex?: number) {
        const data = await this.getData(appId, tableId);

        if (typeof rowIndex !== 'number' || !data[rowIndex]) return data;

        data.splice(rowIndex, 1);

        if (!this.tables.has(appId)) this.tables.set(appId, new Map());
        this.tables.get(appId)?.set(tableId, data);

        return data;
    }
}
