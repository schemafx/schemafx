import z from 'zod';

export enum TableColumnType {
    String = 'string',
    Number = 'number',
    Boolean = 'boolean',
    Date = 'date',
    DateTime = 'datetime',
    Json = 'json',
    Array = 'array'
}

export const TableColumnDefinitionSchema = z.object({
    name: z.string(),
    type: z.enum(Object.values(TableColumnType)).default(TableColumnType.String),
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
