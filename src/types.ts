import { z } from 'zod';

// ============================================================================
// Data Source Definitions
// ============================================================================

/**
 * Types of data sources that connectors can provide.
 */
export enum DataSourceType {
    /** In-memory data (JavaScript objects/arrays) */
    Inline = 'inline',
    /** Local file path (JSON, CSV, Parquet, Excel, etc.) */
    File = 'file',
    /** Remote URL (HTTP/HTTPS endpoints, Google Sheets, cloud storage) */
    Url = 'url',
    /** Readable stream for streaming data sources */
    Stream = 'stream',
    /** Database connection with query (SQL, MongoDB, etc.) */
    Connection = 'connection'
}

/**
 * File format hints for file-based and URL-based sources.
 * Extensible for future format support.
 */
export enum DataSourceFormat {
    Json = 'json',
    Csv = 'csv',
    Parquet = 'parquet',
    Ndjson = 'ndjson',
    Xml = 'xml',
    Auto = 'auto'
}

/**
 * Common options shared across data source definitions.
 */
export type DataSourceOptions = {
    /** Optional format hint for the data source */
    format?: DataSourceFormat;
    /** Optional encoding (e.g., 'utf-8', 'base64') */
    encoding?: string;
    /** Optional compression type (e.g., 'gzip', 'zstd') */
    compression?: string;
    /** Optional headers for HTTP requests */
    headers?: Record<string, string>;
    /** Optional authentication token or credentials */
    auth?: string;
    /** Custom options for specific implementations */
    custom?: Record<string, unknown>;
};

/**
 * Inline data source - data is provided directly as JavaScript objects.
 * Used by MemoryConnector and similar in-memory stores.
 */
export type InlineDataSource = {
    type: DataSourceType.Inline;
    /** The actual data as an array of records */
    data: Record<string, unknown>[];
    options?: DataSourceOptions;
};

/**
 * File data source - data is stored in a local file.
 * DuckDB and similar engines can read directly from file paths.
 */
export type FileDataSource = {
    type: DataSourceType.File;
    /** Absolute or relative file path */
    path: string;
    options?: DataSourceOptions;
};

/**
 * URL data source - data is accessible via HTTP/HTTPS.
 * Supports REST APIs, cloud storage, Google Sheets export URLs, etc.
 * DuckDB fetches directly from the URL.
 */
export type UrlDataSource = {
    type: DataSourceType.Url;
    /** The URL to fetch data from */
    url: string;
    options?: DataSourceOptions;
};

/**
 * Stream data source - data is provided as a readable stream.
 * Useful for large datasets or real-time data ingestion.
 */
export type StreamDataSource = {
    type: DataSourceType.Stream;
    /** The readable stream */
    stream: NodeJS.ReadableStream;
    options?: DataSourceOptions;
};

/**
 * Connection data source - requires a database connection.
 * Uses DuckDB extensions to connect to various database types.
 * Common modules: 'sqlite', 'postgres', 'mysql', 'mongodb'
 * @see https://duckdb.org/docs/extensions/overview.html
 */
export type ConnectionDataSource = {
    type: DataSourceType.Connection;
    /** DuckDB extension module to use (e.g., 'sqlite', 'postgres', 'mysql') */
    module: string;
    /** Connection string or DSN */
    connectionString: string;
    /** Database/collection/table name */
    target: string;
    options?: DataSourceOptions;
};

/**
 * Union type for all data source definitions.
 * Connectors return this to describe how their data should be accessed.
 */
export type DataSourceDefinition =
    | InlineDataSource
    | FileDataSource
    | UrlDataSource
    | StreamDataSource
    | ConnectionDataSource;

// ============================================================================
// Application Field Types
// ============================================================================

export enum AppFieldType {
    Text = 'text',
    Number = 'number',
    Date = 'date',
    Email = 'email',
    Dropdown = 'dropdown',
    Boolean = 'boolean',
    Reference = 'reference',
    JSON = 'json',
    List = 'list'
}

export type AppField = {
    id: string;
    name: string;
    type: AppFieldType;
    isRequired?: boolean | null;
    isKey?: boolean | null;
    encrypted?: boolean | null;
    referenceTo?: string | null;
    minLength?: number | null;
    maxLength?: number | null;
    minValue?: number | null;
    maxValue?: number | null;
    startDate?: Date | null;
    endDate?: Date | null;
    options?: string[] | null;
    fields?: AppField[] | null;
    child?: AppField | null;
};

export const AppFieldSchema: z.ZodType<AppField> = z.lazy(() =>
    z.object({
        id: z.string(),
        name: z.string().min(1),
        type: z.enum(Object.values(AppFieldType)),
        isRequired: z.boolean().default(true).nullable().optional(),
        isKey: z.boolean().default(false).nullable().optional(),
        encrypted: z.boolean().default(false).nullable().optional(),

        // Reference Constraints
        referenceTo: z.string().nullable().optional(),

        // Text Constraints
        minLength: z.number().int().nonnegative().nullable().optional(),
        maxLength: z.number().int().nonnegative().nullable().optional(),

        // Number Constraints
        minValue: z.number().nullable().optional(),
        maxValue: z.number().nullable().optional(),

        // Date Constraints
        startDate: z.date().nullable().optional(),
        endDate: z.date().nullable().optional(),

        // Dropdown Constraints
        options: z.array(z.string()).nullable().optional(),

        // JSON Constraints
        fields: z.array(AppFieldSchema).nullable().optional(),

        // List Constraints
        child: AppFieldSchema.nullable().optional()
    })
);

export enum AppActionType {
    Add = 'add',
    Update = 'update',
    Delete = 'delete',
    Process = 'process'
}

export const AppActionSchema = z.object({
    id: z.string(),
    name: z.string(),
    type: z.enum(Object.values(AppActionType)),
    config: z.looseObject({}).default({}).nullable().optional()
});

export type AppAction = z.infer<typeof AppActionSchema>;

export const AppTableSchema = z.object({
    id: z.string(),
    name: z.string().min(1),
    connector: z.string().min(1),
    connectionId: z.string().nullable().optional(),
    path: z.array(z.string()).default([]),
    fields: z.array(AppFieldSchema),
    actions: z.array(AppActionSchema).default([])
});

export type AppTable = z.infer<typeof AppTableSchema>;

export const AppTableRowSchema = z.looseObject({});

export type AppTableRow = z.infer<typeof AppTableRowSchema>;

export enum AppViewType {
    Table = 'table',
    Form = 'form'
}

export const AppViewSchema = z.object({
    id: z.string(),
    name: z.string().min(1),
    tableId: z.string(),
    type: z.enum(Object.values(AppViewType)),
    config: z.looseObject({}).default({})
});

export type AppView = z.infer<typeof AppViewSchema>;

export const AppSchemaSchema = z.object({
    id: z.string(),
    name: z.string().min(1),
    tables: z.array(AppTableSchema),
    views: z.array(AppViewSchema)
});

export type AppSchema = z.infer<typeof AppSchemaSchema>;

export const AppConnectionSchema = z.object({
    id: z.string(),
    name: z.string().min(1),
    connector: z.string().min(1),
    content: z.string().min(1)
});

export type AppConnection = z.infer<typeof AppConnectionSchema>;

// ============================================================================
// Permission Definitions
// ============================================================================

/**
 * Permission levels for application access.
 */
export enum PermissionLevel {
    /** Read-only access to the application */
    Read = 'read',
    /** Read and write access to the application */
    Write = 'write',
    /** Full administrative access including permission management */
    Admin = 'admin'
}

/**
 * Target type for permissions - extensible for future entity types.
 */
export enum PermissionTargetType {
    /** Permission for an application/schema */
    App = 'app',
    /** Permission for a connection */
    Connection = 'connection'
}

export const AppPermissionSchema = z.object({
    id: z.string(),
    targetType: z.enum(Object.values(PermissionTargetType)),
    targetId: z.string().min(1),
    email: z.string().email(),
    level: z.enum(Object.values(PermissionLevel))
});

export type AppPermission = z.infer<typeof AppPermissionSchema>;

export enum QueryFilterOperator {
    Equals = 'eq',
    NotEqual = 'ne',
    GreaterThan = 'gt',
    GreaterThanOrEqualTo = 'gte',
    LowerThan = 'lt',
    LowerThanOrEqualTo = 'lte',
    Contains = 'contains'
}

export const QueryFilterSchema = z.object({
    field: z.string(),
    operator: z.enum(Object.values(QueryFilterOperator)),
    value: z.any()
});

export type ConnectorCapabilities = {
    filter?: Partial<Record<z.infer<typeof QueryFilterSchema.shape.operator>, boolean>>;
    limit?: { min?: number; max?: number };
    offset?: { min?: number; max?: number };
};

export const TableQueryOptionsSchema = z.object({
    filters: z.array(QueryFilterSchema).optional(),
    orderBy: z
        .object({
            column: z.string(),
            direction: z.enum(['asc', 'desc'])
        })
        .optional(),
    limit: z.number().int().nonnegative().optional(),
    offset: z.number().int().nonnegative().optional()
});

export type TableQueryOptions = z.infer<typeof TableQueryOptionsSchema>;

export enum ConnectorTableCapability {
    Unavailable = 'Unavailable',
    Connect = 'Connect',
    Explore = 'Explore'
}

export const ConnectorTableSchema = z.object({
    name: z.string(),
    path: z.array(z.string()),
    capabilities: z.array(z.enum(Object.values(ConnectorTableCapability)))
});

export type ConnectorTable = z.infer<typeof ConnectorTableSchema>;

export type ConnectorOptions = {
    name: string;
    id?: string;
};

export abstract class Connector {
    name: string;
    id: string;

    constructor(opts: ConnectorOptions) {
        this.name = opts.name;
        this.id = opts.id ?? opts.name;
    }

    /**
     * List available tables.
     * @param path Path to retrieve from.
     * @param auth Auth.
     * @returns List of available tables.
     */
    abstract listTables(path: string[], auth?: string): Promise<ConnectorTable[]>;

    /**
     * Get a Table Schema from a path.
     * @param path Path to the Table.
     * @param auth Auth.
     * @returns Table Schema.
     */
    abstract getTable(path: string[], auth?: string): Promise<AppTable | undefined>;

    /**
     * Performs the authorization exchange using the provided payload.
     * @param body - A key-value object containing the credentials or payload required for authorization.
     * @returns An object containing the connection name, content, and optional email for user authentication.
     */
    authorize?(
        body: Record<string, unknown>
    ): Promise<{ name: string; content: string; email?: string }>;

    /** Generates the URL required to initiate an OAuth authorization flow. */
    getAuthUrl?(): Promise<string>;

    /**
     * Revokes or invalidates the provided authentication credentials.
     * @param auth - The authentication token or string identifier to be revoked.
     */
    revokeAuth?(auth: string): Promise<void>;

    /**
     * Validates that the provided credentials work for a specific table or context.
     * @param table - The `AppTable` context object representing the resource being accessed.
     * @param auth - The authentication token to test.
     */
    testAuth?(table: AppTable, auth: string): Promise<string | undefined>;

    /**
     * Get the capabilities of the connector.
     * @param table Table to get capabilities for.
     * @param auth Auth.
     * @returns Connector Capabilities.
     */
    getCapabilities?(table: AppTable, auth?: string): Promise<ConnectorCapabilities>;

    /**
     * Get data source definition describing how to access the data.
     * This provides metadata for query engines (like DuckDB) to efficiently
     * access the underlying data without loading it into memory first.
     *
     * @param table Table to get data source for.
     * @param auth Auth.
     * @returns Data source definition describing how to access the data.
     *
     * @example
     * // Memory connector returns inline data
     * { type: 'inline', data: [...] }
     *
     * @example
     * // File connector returns file path
     * { type: 'file', path: '/path/to/data.json', options: { format: 'json' } }
     *
     * @example
     * // REST API connector returns URL
     * { type: 'url', url: 'https://api.example.com/data', options: { format: 'json' } }
     *
     * @example
     * // SQL connector returns connection info
     * { type: 'connection', connectionString: 'postgres://...', query: 'SELECT * FROM users' }
     */
    getData?(table: AppTable, auth?: string): Promise<DataSourceDefinition>;

    /**
     * Add a new Row to the Table.
     * @param table Table.
     * @param auth Auth.
     * @param row Table Row.
     */
    addRow?(table: AppTable, auth?: string, row?: AppTableRow): Promise<void>;

    /**
     * Update a Row from the Table.
     * @param table Table.
     * @param auth Auth.
     * @param key Key of the Row.
     * @param row Table Row.
     */
    updateRow?(
        table: AppTable,
        auth?: string,
        key?: Record<string, unknown>,
        row?: AppTableRow
    ): Promise<void>;

    /**
     * Delete a Row from the Table.
     * @param table Table.
     * @param auth Auth.
     * @param key Key of the Row.
     */
    deleteRow?(table: AppTable, auth?: string, key?: Record<string, unknown>): Promise<void>;
}
