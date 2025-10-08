import z from 'zod';

export enum AuthPayloadKeys {
    Email = '_verifiedEmail',
    Name = '_verifiedName'
}

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
    z.strictObject({
        type: z.enum(Object.values(TableColumnType)).default(TableColumnType.String),
        typeProps: TableColumnDefinitionSchemaTypeProps.optional()
    })
);

export const TableColumnDefinitionSchema = z.strictObject({
    name: z.string(),
    type: z.enum(Object.values(TableColumnType)).default(TableColumnType.String),
    typeProps: TableColumnDefinitionSchemaTypeProps.optional(),
    key: z.boolean().default(false)
});

export type TableColumnDefinition = z.infer<typeof TableColumnDefinitionSchema>;

export const TableDefinitionSchema = z.strictObject({
    id: z.string(),
    entity: z.string(), // Ref to Entity.id
    name: z.string(),

    connector: z.string(),
    connection: z.string(), // Ref to Connection.id
    connectionPath: z.array(z.string()),

    columns: z.array(TableColumnDefinitionSchema)
});

export type TableDefinition = z.infer<typeof TableDefinitionSchema>;

export enum EntityType {
    User = 'user',
    Team = 'team',
    Application = 'application'
}

export const EntitySchema = z.strictObject({
    id: z.string(),
    type: z.enum(Object.values(EntityType)),
    name: z.string()
});

export type Entity = z.infer<typeof EntitySchema>;

export const ComponentSchema = z.strictObject({
    id: z.string(),
    entity: z.string(), // Ref to Entity.id
    table: z.optional(z.string()), // Ref to TableDefinition.id
    name: z.string(),
    type: z.string(),
    type_props: z.optional(z.looseObject({}))
});

export type Component = z.infer<typeof ComponentSchema>;

export const ConnectionSchema = z.strictObject({
    id: z.string(),
    connector: z.string(),
    connection_payload: z.string()
});

export type Connection = z.infer<typeof ConnectionSchema>;

export enum RoleGrants {
    Create = 'create',
    Read = 'read',
    Update = 'update',
    Delete = 'delete',
    Owner = 'owner'
}

export enum RoleTargetType {
    Team = 'team',
    Application = 'application',
    Connection = 'connection'
}

export const RoleSchema = z.strictObject({
    id: z.string(),
    entity: z.string(), // Ref to Entity.id
    grants: z.array(z.enum(Object.values(RoleGrants))),
    target_type: z.enum(Object.values(RoleTargetType)),
    target_id: z.string()
});

export type Role = z.infer<typeof RoleSchema>;
