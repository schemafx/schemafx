import { z } from 'zod';

export const AppFieldTypeSchema = z.enum([
    'text',
    'number',
    'date',
    'email',
    'dropdown',
    'boolean',
    'reference'
]);

export type AppFieldType = z.infer<typeof AppFieldTypeSchema>;

export const AppFieldSchema = z.object({
    id: z.string(),
    name: z.string().min(1),
    type: AppFieldTypeSchema,
    isRequired: z.boolean().default(false),

    // Reference Constraints
    referenceTo: z.string().optional(),

    // Text Constraints
    minLength: z.number().int().nonnegative().optional(),
    maxLength: z.number().int().nonnegative().optional(),

    // Number Constraints
    minValue: z.number().optional(),
    maxValue: z.number().optional(),

    // Date Constraints
    startDate: z.date().optional(),
    endDate: z.date().optional(),

    // Dropdown Constraints
    options: z.array(z.string()).optional()
});

export type AppField = z.infer<typeof AppFieldSchema>;

export const AppTableSchema = z.object({
    id: z.string(),
    name: z.string().min(1),
    connector: z.string().min(1),
    fields: z.array(AppFieldSchema)
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
    fields: z.array(z.string())
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
     * @param rowIndex Index of the Row.
     * @param row Table Row.
     */
    updateRow?(
        appId: string,
        tableId: string,
        rowIndex?: number,
        row?: AppTableRow
    ): Promise<AppTableRow[]>;

    /**
     * Delete a Row from the Table.
     * @param appId Id of the Application.
     * @param tableId Id of the Table.
     * @param rowIndex Index of the Row.
     */
    deleteRow?(appId: string, tableId: string, rowIndex?: number): Promise<AppTableRow[]>;
}
