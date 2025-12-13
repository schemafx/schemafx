import { readFile, writeFile } from 'node:fs/promises';
import {
    Connector,
    type AppSchema,
    type AppTableRow,
    type AppTable,
    ConnectorTableCapability
} from '../types.js';
import { inferTable } from '../utils/dataUtils.js';

type FileDB = {
    schemas: Record<string, AppSchema>;
    tables: Record<string, AppTableRow[]>;
};

export default class FileConnector extends Connector {
    filePath: string;

    constructor(name: string, filePath: string, id?: string) {
        super(name, id);
        this.filePath = filePath;
    }

    async listTables(path: string[]) {
        if (path.length > 0) return [];
        const db = await this._readDB();
        const tables = new Set<string>();

        for (const tableId in db.tables) tables.add(tableId);

        return Array.from(tables).map(tableId => ({
            name: tableId,
            path: [tableId],
            capabilities: [ConnectorTableCapability.Connect]
        }));
    }

    async getTable(path: string[]) {
        const db = await this._readDB();
        const tableId = path[0];

        return inferTable(tableId, path, db.tables[tableId] || [], this.id);
    }

    async getCapabilities() {
        // In-Memory capabilities only.
        // Default capability handler.
        return {};
    }

    private async _readDB(): Promise<FileDB> {
        try {
            const data = await readFile(this.filePath, 'utf-8');
            return JSON.parse(data);
        } catch (error) {
            if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
                return {
                    schemas: {},
                    tables: {}
                };
            }

            throw error;
        }
    }

    private async _writeDB(db: FileDB) {
        await writeFile(this.filePath, JSON.stringify(db, null, 4), 'utf-8');
    }

    async getData(table: AppTable) {
        const db = await this._readDB();
        return db.tables[table.path[0]] || [];
    }

    async addRow(table: AppTable, auth?: string, row?: AppTableRow) {
        const db = await this._readDB();
        if (!row) return;

        if (!db.tables[table.path[0]]) db.tables[table.path[0]] = [];

        db.tables[table.path[0]].push(row);
        await this._writeDB(db);
    }

    async updateRow(
        table: AppTable,
        auth?: string,
        key?: Record<string, unknown>,
        row?: AppTableRow
    ) {
        const db = await this._readDB();
        if (!key || !row) return;
        if (!db.tables[table.path[0]]) return;

        const data = db.tables[table.path[0]];
        const rowIndex = data.findIndex(r => Object.entries(key).every(([k, v]) => r[k] === v));

        if (rowIndex !== -1) {
            data[rowIndex] = { ...data[rowIndex], ...row };
            await this._writeDB(db);
        }
    }

    async deleteRow(table: AppTable, auth?: string, key?: Record<string, unknown>) {
        const db = await this._readDB();
        if (!key) return;
        if (!db.tables[table.path[0]]) return;

        const data = db.tables[table.path[0]];
        const rowIndex = data.findIndex(r => Object.entries(key).every(([k, v]) => r[k] === v));

        if (rowIndex !== -1) {
            data.splice(rowIndex, 1);
            await this._writeDB(db);
        }
    }
}
