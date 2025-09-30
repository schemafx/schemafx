import { Connector, ConnectorAuthType, type TableDefinition } from '../..';

export default class MockConnector extends Connector {
    /** DB Store. */
    private db: Map<string, Record<string, unknown>[]>;

    /** DB Table Definitions. */
    private dbDef: Map<string, TableDefinition>;

    /**
     * Build With Your Mock.
     * @param name Name of the connector.
     */
    constructor(name?: string) {
        super(name || 'mock');
        this.authType = ConnectorAuthType.None;

        this.db = new Map();
        this.dbDef = new Map();
    }

    async readTables(connectionPath: string[]) {
        return [...this.db.keys()]
            .filter(k => !connectionPath[0] || k === connectionPath[0])
            .map(c => ({
                name: this.dbDef.get(c)?.name ?? c,
                connectionPath: [c],
                final: true
            }));
    }

    async readTable(connectionPath: string[]) {
        return this.dbDef.get(connectionPath[0])!;
    }

    async createTable(table: TableDefinition) {
        this.db.set(table.connectionPath[0], []);
        this.dbDef.set(table.connectionPath[0], table);
        return table;
    }

    async updateTable(oldTable: TableDefinition, newTable: TableDefinition) {
        const oldPath = oldTable.connectionPath[0];
        const newPath = newTable.connectionPath[0];

        if (oldPath !== newPath) {
            this.dbDef.set(newPath, newTable);
            this.dbDef.delete(oldPath);

            this.db.set(newPath, this.db.get(oldPath)!);
            this.db.delete(oldPath);
        } else {
            this.dbDef.set(newPath, newTable);
        }

        return newTable;
    }

    async deleteTable(table: TableDefinition) {
        this.db.delete(table.connectionPath[0]);
        this.dbDef.delete(table.connectionPath[0]);
        return table;
    }

    async readData(tables: TableDefinition[]) {
        return tables.map(table => ({
            table,
            rows: this.db.get(table.connectionPath[0])!
        }));
    }

    async createData(table: TableDefinition, rows: Record<string, unknown>[]) {
        const key = table.connectionPath[0];
        const data = this.db.get(key)!;
        data.push(...rows);

        this.db.set(key, data);
        return rows;
    }

    async updateData(table: TableDefinition, rows: Record<string, unknown>[]) {
        const key = table.connectionPath[0];
        const data = this.db.get(key)!;

        const colIdx = table.columns.find(c => c.key)?.name ?? '';
        const rowIdx: [number, Record<string, unknown>][] = rows.map(r => [
            data.findIndex(d => d[colIdx] === r[colIdx]),
            r
        ]);

        for (const [idx, row] of rowIdx) {
            data[idx] = row;
        }

        this.db.set(key, data);
        return rows;
    }

    async deleteData(table: TableDefinition, rows: Record<string, unknown>[]) {
        const key = table.connectionPath[0];
        const data = this.db.get(key)!;

        const colIdx = table.columns.find(c => c.key)?.name ?? '';
        const rowIdx = rows.map(r => r[colIdx]);

        this.db.set(
            key,
            data.filter(d => !rowIdx.includes(d[colIdx]))
        );

        return rows;
    }
}
