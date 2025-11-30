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
    getSchema?(appId: string): PromiseLike<AppSchema>;
    saveSchema?(appId: string, schema: AppSchema): PromiseLike<AppSchema>;
    deleteSchema?(appId: string): PromiseLike<void>;

    getData?(appId: string, tableId: string): PromiseLike<AppTableRow[]>;
    addRow?(appId: string, tableId: string, row?: AppTableRow): PromiseLike<AppTableRow[]>;

    updateRow?(
        appId: string,
        tableId: string,
        rowIndex?: number,
        row?: AppTableRow
    ): PromiseLike<AppTableRow[]>;

    deleteRow?(appId: string, tableId: string, rowIndex?: number): PromiseLike<AppTableRow[]>;
}
