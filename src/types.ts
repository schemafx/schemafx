import { z } from 'zod';

export const AppFieldTypeSchema = z.enum([
    'text',
    'number',
    'date',
    'email',
    'dropdown',
    'boolean',
    'reference',
    'json',
    'list'
]);

export type AppFieldType = z.infer<typeof AppFieldTypeSchema>;

export type AppField = {
    id: string;
    name: string;
    type: AppFieldType;
    isRequired: boolean;
    isKey: boolean;
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
        type: AppFieldTypeSchema,
        isRequired: z.boolean().default(false),
        isKey: z.boolean().default(false),

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

export const AppActionSchema = z.object({
    id: z.string(),
    name: z.string(),
    type: z.enum(['add', 'update', 'delete', 'process']),
    config: z.looseObject({}).default({})
});

export type AppAction = z.infer<typeof AppActionSchema>;

export const AppTableSchema = z.object({
    id: z.string(),
    name: z.string().min(1),
    connector: z.string().min(1),
    path: z.array(z.string()).default([]),
    fields: z.array(AppFieldSchema),
    actions: z.array(AppActionSchema).default([])
});

export type AppTable = z.infer<typeof AppTableSchema>;

export const AppTableRowSchema = z.looseObject({});

export type AppTableRow = z.infer<typeof AppTableRowSchema>;

export const AppViewTypeSchema = z.enum(['table', 'form']);

export type AppViewType = z.infer<typeof AppViewTypeSchema>;

export const AppViewSchema = z.object({
    id: z.string(),
    name: z.string().min(1),
    tableId: z.string(),
    type: AppViewTypeSchema,
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

export const QueryFilterSchema = z.object({
    field: z.string(),
    operator: z.enum(['eq', 'ne', 'gt', 'gte', 'lt', 'lte', 'contains']),
    value: z.any()
});

export type ConnectorCapabilities = {
    filter?: Partial<Record<z.infer<typeof QueryFilterSchema.shape.operator>, boolean>>;
    limit?: { min?: number; max?: number };
    offset?: { min?: number; max?: number };
};

export const TableQueryOptionsSchema = z.object({
    filters: z.array(QueryFilterSchema).optional(),
    limit: z.number().int().nonnegative().optional(),
    offset: z.number().int().nonnegative().optional()
});

export type TableQueryOptions = z.infer<typeof TableQueryOptionsSchema>;

export const ConnectorTableSchema = z.object({
    name: z.string(),
    path: z.array(z.string()),
    capabilities: z.array(z.enum(['Unavailable', 'Connect', 'Explore', 'Create']))
});

export type ConnectorTable = z.infer<typeof ConnectorTableSchema>;

export abstract class Connector {
    name: string;
    id: string;

    constructor(name: string, id?: string) {
        this.name = name;
        this.id = id ?? name;
    }

    /**
     * List available tables.
     * @param path Path to retrieve from.
     * @returns List of available tables.
     */
    listTables?(path: string[]): Promise<ConnectorTable[]>;

    /**
     * Get the capabilities of the connector.
     * @param table Table to get capabilities for.
     * @returns Connector Capabilities.
     */
    getCapabilities?(table: AppTable): Promise<ConnectorCapabilities>;

    /**
     * Retrieve a Schema.
     * @param appId Id of the Application.
     * @returns Application Schema.
     */
    getSchema?(appId: string): Promise<AppSchema>;

    /**
     * Save an updated Schema.
     * @param appId Id of the Application.
     * @param schema Updated Schema to save.
     * @returns Application Schema.
     */
    saveSchema?(appId: string, schema: AppSchema): Promise<AppSchema>;

    /**
     * Delete the Schema for an Application.
     * @param appId Id of the Application.
     */
    deleteSchema?(appId: string): Promise<void>;

    /**
     * Get data from a Table.
     * @param table Table.
     * @param query Query Options.
     */
    getData?(table: AppTable, query?: TableQueryOptions): Promise<AppTableRow[]>;

    /**
     * Add a new Row to the Table.
     * @param table Table.
     * @param row Table Row.
     */
    addRow?(table: AppTable, row?: AppTableRow): Promise<AppTableRow[]>;

    /**
     * Update a Row from the Table.
     * @param table Table.
     * @param key Key of the Row.
     * @param row Table Row.
     */
    updateRow?(
        table: AppTable,
        key?: Record<string, unknown>,
        row?: AppTableRow
    ): Promise<AppTableRow[]>;

    /**
     * Delete a Row from the Table.
     * @param table Table.
     * @param key Key of the Row.
     */
    deleteRow?(table: AppTable, key?: Record<string, unknown>): Promise<AppTableRow[]>;

    /**
     * Get a Table Schema from a path.
     * @param path Path to the Table.
     * @returns Table Schema.
     */
    getTable?(path: string[]): Promise<AppTable>;
}

/**
 * Infer a Table Schema from data.
 * @param name Name of the Table.
 * @param path Path to the Table.
 * @param data Data to infer from.
 * @param connectorId Id of the Connector.
 * @returns Inferred Table Schema.
 */
export function inferTable(
    name: string,
    path: string[],
    data: AppTableRow[],
    connectorId: string
): AppTable {
    const keys = new Set<string>();
    for (const row of data) Object.keys(row).forEach(k => keys.add(k));

    const fields: AppField[] = [];
    for (const key of keys) {
        let detectedType: AppFieldType | null = null;

        for (const row of data) {
            const val = row[key];
            if (val === null || val === undefined) continue;

            let type: AppFieldType = 'text';
            if (typeof val === 'number') type = 'number';
            else if (typeof val === 'boolean') type = 'boolean';
            else if (Array.isArray(val)) type = 'list';
            else if (typeof val === 'object') type = 'json';

            if (detectedType && detectedType !== type) {
                detectedType = 'text';
                break;
            }

            if (!detectedType) detectedType = type;
        }

        fields.push({
            id: key,
            name: key,
            type: detectedType || 'text',
            isRequired: false,
            isKey: key === 'id'
        });
    }

    return {
        id: name,
        name,
        connector: connectorId,
        path,
        fields,
        actions: []
    };
}
