import z from 'zod';

export const TableColumnDefinitionSchema = z.object({
    name: z.string(),
    type: z.enum(['string', 'number', 'date', 'datetime']),
    key: z.boolean().default(false)
});

export type TableColumnDefinition = z.infer<typeof TableColumnDefinitionSchema>;

export const TableDefinitionSchema = z.object({
    name: z.string(),

    connector: z.string(),
    connection: z.string(),
    connectionPayload: z.looseObject({}),
    connectionPath: z.array(z.string()),
    connectionTimeZone: z.string(),

    columns: z.array(TableColumnDefinitionSchema)
});

export type TableDefinition = z.infer<typeof TableDefinitionSchema>;
