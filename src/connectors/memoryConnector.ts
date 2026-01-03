import {
    Connector,
    DataSourceType,
    type AppSchema,
    type AppTableRow,
    type AppTable,
    type DataSourceDefinition,
    ConnectorTableCapability
} from '../types.js';
import { inferTable } from '../utils/dataUtils.js';

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
            capabilities: [ConnectorTableCapability.Connect]
        }));
    }

    async getTable(path: string[]) {
        const tableId = path[0];
        if (!tableId) return;

        return inferTable(tableId, path, this.tables.get(tableId) || [], this.id);
    }

    override async getCapabilities() {
        // In-Memory capabilities only.
        // Default capability handler.
        return {};
    }

    override async getData(table: AppTable): Promise<DataSourceDefinition> {
        if (!table.path[0]) {
            return { type: DataSourceType.Inline, data: [] };
        }

        return {
            type: DataSourceType.Inline,
            data: [...(this.tables.get(table.path[0]) ?? [])]
        };
    }

    override async addRow(table: AppTable, _?: string, row?: AppTableRow) {
        if (!table.path[0]) return;
        if (!row) return;

        const data = this.tables.get(table.path[0]) ?? [];
        data.push(row);
        this.tables.set(table.path[0], data);
    }

    override async updateRow(
        table: AppTable,
        _?: string,
        key?: Record<string, unknown>,
        row?: AppTableRow
    ) {
        if (!table.path[0] || !key || !row) return;

        const data = this.tables.get(table.path[0]) ?? [];
        const rowIndex = data.findIndex(r => Object.entries(key).every(([k, v]) => r[k] === v));

        if (rowIndex === -1) return;

        data[rowIndex] = { ...data[rowIndex], ...row };
        this.tables.set(table.path[0], data);
    }

    override async deleteRow(table: AppTable, _?: string, key?: Record<string, unknown>) {
        if (!table.path[0] || !key) return;

        const data = this.tables.get(table.path[0]) ?? [];
        const rowIndex = data.findIndex(r => Object.entries(key).every(([k, v]) => r[k] === v));

        if (rowIndex === -1) return;

        data.splice(rowIndex, 1);
        this.tables.set(table.path[0], data);
    }
}
