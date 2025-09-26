import z from 'zod';

export const TableColumnDefinitionSchema = z.object({
    name: z.string(),
    type: z.enum(['string', 'number', 'date', 'datetime', 'json']),
    key: z.boolean().default(false)
});

export type TableColumnDefinition = z.infer<typeof TableColumnDefinitionSchema>;

export const TableDefinitionSchema = z.object({
    name: z.string(),

    connector: z.string(),
    connection: z.string(),
    connectionPath: z.array(z.string()),

    columns: z.array(TableColumnDefinitionSchema)
});

export type TableDefinition = z.infer<typeof TableDefinitionSchema>;
