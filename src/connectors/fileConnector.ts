import { readFile, writeFile } from 'node:fs/promises';
import { Connector, type AppSchema, type AppTableRow, type AppTable } from '../types.js';
import mock_data from './mock_data.json' with { type: 'json' };

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

    async getSchema(appId: string) {
        const db = await this._readDB();
        let schema = db.schemas[appId];

        if (!schema) {
            schema = { ...mock_data } as unknown as AppSchema;
            db.schemas[appId] = schema;
            await this._writeDB(db);
        }

        return schema;
    }

    async saveSchema(appId: string, schema: AppSchema) {
        const db = await this._readDB();
        db.schemas[appId] = schema;
        await this._writeDB(db);
        return schema;
    }

    async deleteSchema(appId: string) {
        const db = await this._readDB();
        delete db.schemas[appId];
        await this._writeDB(db);
    }

    async getData(table: AppTable) {
        const db = await this._readDB();
        return db.tables[table.id] || [];
    }

    async addRow(table: AppTable, row?: AppTableRow) {
        const db = await this._readDB();
        if (!row) return db.tables[table.id] || [];

        if (!db.tables[table.id]) db.tables[table.id] = [];

        db.tables[table.id].push(row);
        await this._writeDB(db);

        return db.tables[table.id];
    }

    async updateRow(table: AppTable, key?: Record<string, unknown>, row?: AppTableRow) {
        const db = await this._readDB();
        if (!key || !row) return db.tables[table.id] || [];

        if (!db.tables[table.id]) return [];

        const data = db.tables[table.id];
        const rowIndex = data.findIndex(r => Object.entries(key).every(([k, v]) => r[k] === v));

        if (rowIndex !== -1) {
            data[rowIndex] = { ...data[rowIndex], ...row };
            await this._writeDB(db);
        }

        return data;
    }

    async deleteRow(table: AppTable, key?: Record<string, unknown>) {
        const db = await this._readDB();
        if (!key) return db.tables[table.id] || [];

        if (!db.tables[table.id]) return [];

        const data = db.tables[table.id];
        const rowIndex = data.findIndex(r => Object.entries(key).every(([k, v]) => r[k] === v));

        if (rowIndex !== -1) {
            data.splice(rowIndex, 1);
            await this._writeDB(db);
        }

        return data;
    }
}
