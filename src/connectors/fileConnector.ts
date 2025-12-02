import { readFile, writeFile } from 'node:fs/promises';
import { Connector, type AppSchema, type AppTableRow } from '../types.js';
import mock_data from './mock_data.json' with { type: 'json' };

type FileDB = {
    schemas: Record<string, AppSchema>;
    tables: Record<string, Record<string, AppTableRow[]>>;
};

export default class FileConnector extends Connector {
    filePath: string;

    constructor(filePath: string) {
        super();
        this.filePath = filePath;
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
            schema = { ...mock_data } as AppSchema;
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

    async getData(appId: string, tableId: string) {
        const db = await this._readDB();
        if (!db.tables[appId]) db.tables[appId] = {};
        return db.tables[appId][tableId] || [];
    }

    async addRow(appId: string, tableId: string, row?: AppTableRow) {
        const db = await this._readDB();
        if (!row) return db.tables[appId]?.[tableId] || [];

        if (!db.tables[appId]) db.tables[appId] = {};
        if (!db.tables[appId][tableId]) db.tables[appId][tableId] = [];

        db.tables[appId][tableId].push(row);
        await this._writeDB(db);

        return db.tables[appId][tableId];
    }

    async updateRow(appId: string, tableId: string, rowIndex?: number, row?: AppTableRow) {
        const db = await this._readDB();
        if (typeof rowIndex !== 'number' || !row) return db.tables[appId]?.[tableId] || [];

        if (!db.tables[appId]) db.tables[appId] = {};
        if (!db.tables[appId][tableId]) return [];

        const data = db.tables[appId][tableId];
        if (data[rowIndex]) {
            data[rowIndex] = { ...row };
            await this._writeDB(db);
        }

        return data;
    }

    async deleteRow(appId: string, tableId: string, rowIndex?: number) {
        const db = await this._readDB();
        if (typeof rowIndex !== 'number') return db.tables[appId]?.[tableId] || [];

        if (!db.tables[appId] || !db.tables[appId][tableId]) return [];

        const data = db.tables[appId][tableId];
        if (data[rowIndex]) {
            data.splice(rowIndex, 1);
            await this._writeDB(db);
        }

        return data;
    }
}
