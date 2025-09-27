import type { TableDefinition } from './schemas';

export enum ConnectorAuthType {
    OAuth = 'OAuth',
    Basic = 'Basic',
    None = 'None'
}

export enum ConnectorAuthPropType {
    Text = 'Text',
    Password = 'Password',
    Number = 'Number'
}

export class Connector {
    /** Name of the connector. */
    name: string;

    /** Authentication type. */
    authType: ConnectorAuthType = ConnectorAuthType.None;

    /** Authentication properties for Basic only. */
    authProps?: Record<
        string,
        ConnectorAuthPropType | { type: ConnectorAuthPropType; required: boolean }
    >;

    /**
     * Build With Your Data.
     * @param name Name of the connector.
     */
    constructor(name: string) {
        this.name = name;
    }

    /**
     * Get authentication details.
     * @param params Params from the authentication.
     * @returns Properties to save for future authentications.
     */
    getAuth?(params: Record<string, string>): Promise<Record<string, string>>;

    /**
     * Get the Url to redirect the user to for authentication.
     * @returns The Url to redirect to for authentication.
     */
    getAuthUrl?(): string;

    /**
     * Read available tables.
     * @param connectionPath Connection path to explore.
     * @param connectionPayload Connection payload for auth.
     * @returns Available tables at the requested path.
     */
    readTables?(
        connectionPath: string[],
        connectionPayload: Record<string, string>
    ): Promise<{ name: string; connectionPath: string[]; final: boolean }[]>;

    /**
     * Read a table definition from its path.
     * @param connectionPath Connection path to consider.
     * @param connectionPayload Connection payload for auth.
     * @returns Understood table definition.
     */
    readTable?(
        connectionPath: string[],
        connectionPayload: Record<string, string>
    ): Promise<TableDefinition>;

    /**
     * Create a table.
     * @param table Table to create.
     * @param connectionPayload Connection payload for auth.
     * @returns Created table.
     */
    createTable?(
        table: TableDefinition,
        connectionPayload: Record<string, string>
    ): Promise<TableDefinition>;

    /**
     * Update a table.
     * @param oldTable Table to update.
     * @param newTable Updated Table.
     * @param connectionPayload Connection payload for auth.
     * @returns Updated table.
     */
    updateTable?(
        oldTable: TableDefinition,
        newTable: TableDefinition,
        connectionPayload: Record<string, string>
    ): Promise<TableDefinition>;

    /**
     * Delete a table.
     * @param table Table to delete.
     * @param connectionPayload Connection payload for auth.
     * @returns Deleted table.
     */
    deleteTable?(
        table: TableDefinition,
        connectionPayload: Record<string, string>
    ): Promise<TableDefinition>;

    /**
     * Read data from tables.
     * @param tables Tables to read.
     * @param connectionPayload Connection payload for auth.
     * @returns Rows detail.
     */
    readData?(
        tables: TableDefinition[],
        connectionPayload: Record<string, string>
    ): Promise<{ table: TableDefinition; rows: Record<string, unknown>[] }[]>;

    /**
     * Append data to the table.
     * @param table Table to append data into.
     * @param rows Rows to append.
     * @param connectionPayload Connection payload for auth.
     * @returns Resulting rows.
     */
    createData?(
        table: TableDefinition,
        rows: Record<string, unknown>[],
        connectionPayload: Record<string, string>
    ): Promise<Record<string, unknown>[]>;

    /**
     * Update data in the table.
     * @param table Table to update data into.
     * @param rows Rows to update.
     * @param connectionPayload Connection payload for auth.
     * @returns Resulting rows.
     */
    updateData?(
        table: TableDefinition,
        rows: Record<string, unknown>[],
        connectionPayload: Record<string, string>
    ): Promise<Record<string, unknown>[]>;

    /**
     * Delete data from the table.
     * @param table Table to delete data from.
     * @param rows Rows to delete.
     * @param connectionPayload Connection payload for auth.
     * @returns Deleted rows.
     */
    deleteData?(
        table: TableDefinition,
        rows: Record<string, unknown>[],
        connectionPayload: Record<string, string>
    ): Promise<Record<string, unknown>[]>;
}
