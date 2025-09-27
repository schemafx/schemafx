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

const TableColumnDefinitionSchemaTypeProps: z.ZodType<unknown> = z.lazy(() =>
    z.object({
        type: z.enum(Object.values(TableColumnType)).default(TableColumnType.String),
        typeProps: TableColumnDefinitionSchemaTypeProps.optional()
    })
);

export const TableColumnDefinitionSchema = z.object({
    name: z.string(),
    type: z.enum(Object.values(TableColumnType)).default(TableColumnType.String),
    typeProps: TableColumnDefinitionSchemaTypeProps.optional(),
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
