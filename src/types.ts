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
    config: z
        .object({
            actions: z.array(z.string()).optional()
        })
        .optional()
});

export type AppAction = z.infer<typeof AppActionSchema>;

export const AppTableSchema = z.object({
    id: z.string(),
    name: z.string().min(1),
    connector: z.string().min(1),
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
    fields: z.array(z.string()),
    showEmpty: z.boolean().optional().default(false)
});

export type AppView = z.infer<typeof AppViewSchema>;

export const AppSchemaSchema = z.object({
    id: z.string(),
    name: z.string().min(1),
    tables: z.array(AppTableSchema),
    views: z.array(AppViewSchema)
});

export type AppSchema = z.infer<typeof AppSchemaSchema>;

export abstract class Connector {
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
     * @param appId Id of the Application.
     * @param tableId Id of the Table.
     */
    getData?(appId: string, tableId: string): Promise<AppTableRow[]>;

    /**
     * Add a new Row to the Table.
     * @param appId Id of the Application.
     * @param tableId Id of the Table.
     * @param row Table Row.
     */
    addRow?(appId: string, tableId: string, row?: AppTableRow): Promise<AppTableRow[]>;

    /**
     * Update a Row from the Table.
     * @param appId Id of the Application.
     * @param tableId Id of the Table.
     * @param key Key of the Row.
     * @param row Table Row.
     */
    updateRow?(
        appId: string,
        tableId: string,
        key?: Record<string, unknown>,
        row?: AppTableRow
    ): Promise<AppTableRow[]>;

    /**
     * Delete a Row from the Table.
     * @param appId Id of the Application.
     * @param tableId Id of the Table.
     * @param key Key of the Row.
     */
    deleteRow?(
        appId: string,
        tableId: string,
        key?: Record<string, unknown>
    ): Promise<AppTableRow[]>;
}
