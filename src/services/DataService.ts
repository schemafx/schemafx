import { LRUCache } from 'lru-cache';
import {
    AppActionType,
    type TableQueryOptions,
    type AppTable,
    type AppTableRow,
    type Connector,
    type AppSchema,
    AppSchemaSchema,
    QueryFilterOperator,
    AppConnectionSchema,
    type AppConnection,
    AppPermissionSchema,
    type AppPermission,
    PermissionLevel
} from '../types.js';
import { PermissionTargetType } from '../types.js';
import type { z } from 'zod';
import { type AppTableFromZodOptions, tableFromZod, zodFromTable } from '../utils/zodUtils.js';
import { getData as getDuckDBData } from '../utils/duckdb.js';
import { extractKeys } from '../utils/schemaUtils.js';
import { decodeRow, encodeRow } from '../utils/dataUtils.js';
import { randomUUID } from 'crypto';

/**
 * Permission target for querying permissions.
 */
export type PermissionTarget = {
    targetType: PermissionTargetType;
    targetId: string;
};

type executeActionOptions = {
    table: AppTable;
    actId: string;
    rows: AppTableRow[];
    depth?: number;
};

export type DataServiceOptions = {
    schemaConnector: Omit<AppTableFromZodOptions, 'id' | 'name' | 'primaryKey'>;
    connectionsConnection?: string;
    connectionsConnector: Omit<
        AppTableFromZodOptions,
        'id' | 'name' | 'primaryKey' | 'connectionId'
    >;
    permissionsConnection?: string;
    permissionsConnector: Omit<
        AppTableFromZodOptions,
        'id' | 'name' | 'primaryKey' | 'connectionId'
    >;
    connectors: Connector[];
    encryptionKey?: string;
    maxRecursiveDepth?: number;
    validatorCacheOpts?: {
        max?: number;
        ttl?: number;
    };
    schemaCacheOpts?: {
        max?: number;
        ttl?: number;
    };
    connectionsCacheOpts?: {
        max?: number;
        ttl?: number;
    };
};

export default class DataService {
    schemaCache: LRUCache<string, AppSchema>;
    connectionsCache: LRUCache<string, AppConnection>;
    validatorCache: LRUCache<string, z.ZodType>;

    schemaTable: AppTable;
    connectionsConnection?: string;
    connectionsTable: AppTable;
    permissionsConnection?: string;
    permissionsTable: AppTable;

    connectors: Record<string, Connector> = {};
    encryptionKey?: string;
    maxRecursiveDepth: number;

    constructor({
        schemaCacheOpts,
        connectionsCacheOpts,
        validatorCacheOpts,

        schemaConnector,
        connectionsConnection,
        connectionsConnector,
        permissionsConnection,
        permissionsConnector,

        connectors,
        maxRecursiveDepth,
        encryptionKey
    }: DataServiceOptions) {
        this.schemaCache = new LRUCache<string, AppSchema>({
            max: schemaCacheOpts?.max ?? 100,
            ttl: schemaCacheOpts?.ttl ?? 1000 * 60 * 5 // 5 minutes TTL
        });

        this.connectionsCache = new LRUCache<string, AppConnection>({
            max: connectionsCacheOpts?.max ?? 100,
            ttl: connectionsCacheOpts?.ttl ?? 1000 * 60 * 5
        });

        this.validatorCache = new LRUCache<string, z.ZodType>({
            max: validatorCacheOpts?.max ?? 500,
            ttl: validatorCacheOpts?.ttl ?? 1000 * 60 * 60
        });

        for (const connector of connectors) {
            if (this.connectors[connector.id]) {
                throw new Error(`Duplicated connector "${connector.id}".`);
            }

            this.connectors[connector.id] = connector;
        }

        this.schemaTable = tableFromZod(AppSchemaSchema, {
            id: randomUUID(),
            name: '',
            primaryKey: 'id',
            ...schemaConnector,
            actions: [
                {
                    id: 'add',
                    name: '',
                    type: AppActionType.Add
                },
                {
                    id: 'update',
                    name: '',
                    type: AppActionType.Update
                },
                {
                    id: 'delete',
                    name: '',
                    type: AppActionType.Delete
                }
            ]
        });

        this.connectionsConnection = connectionsConnection;
        this.connectionsTable = tableFromZod(AppConnectionSchema, {
            id: randomUUID(),
            name: '',
            primaryKey: 'id',
            ...connectionsConnector,
            actions: [
                {
                    id: 'add',
                    name: '',
                    type: AppActionType.Add
                },
                {
                    id: 'update',
                    name: '',
                    type: AppActionType.Update
                },
                {
                    id: 'delete',
                    name: '',
                    type: AppActionType.Delete
                }
            ]
        });

        this.connectionsTable.fields[
            this.connectionsTable.fields.findIndex(f => f.id === 'content')
        ]!.encrypted = true;

        this.permissionsConnection = permissionsConnection;
        this.permissionsTable = tableFromZod(AppPermissionSchema, {
            id: randomUUID(),
            name: '',
            primaryKey: 'id',
            ...permissionsConnector,
            actions: [
                {
                    id: 'add',
                    name: '',
                    type: AppActionType.Add
                },
                {
                    id: 'update',
                    name: '',
                    type: AppActionType.Update
                },
                {
                    id: 'delete',
                    name: '',
                    type: AppActionType.Delete
                }
            ]
        });

        this.encryptionKey = encryptionKey;
        this.maxRecursiveDepth = maxRecursiveDepth ?? 100;
    }

    async getConnection(connectionId?: string | null) {
        if (!connectionId) return;
        if (this.connectionsCache.has(connectionId)) {
            return this.connectionsCache.get(connectionId);
        }

        const connections = await this._getData(this.connectionsTable, this.connectionsConnection, {
            filters: [
                {
                    field: this.connectionsTable.fields.find(f => f.isKey)!.id,
                    operator: QueryFilterOperator.Equals,
                    value: connectionId
                }
            ],
            limit: 1
        });

        const connection = connections[0] as AppConnection | undefined;
        this.connectionsCache.set(connectionId, connection);
        return connection;
    }

    async getConnections() {
        return (await this._getData(
            this.connectionsTable,
            this.connectionsConnection
        )) as AppConnection[];
    }

    async setConnection(connection: AppConnection, owner?: string) {
        const existingConnection = await this.getConnection(connection.id);
        this.connectionsCache.set(connection.id, connection);

        await this._executeAction({
            table: this.connectionsTable,
            auth: this.connectionsConnection,
            actId: existingConnection ? 'update' : 'add',
            rows: [connection]
        });

        if (!existingConnection && owner) {
            await this.setPermission({
                id: randomUUID(),
                targetType: PermissionTargetType.Connection,
                targetId: connection.id,
                email: owner.toLowerCase(),
                level: PermissionLevel.Admin
            });
        }

        return connection;
    }

    async deleteConnection(connectionId: string) {
        const connection = await this.getConnection(connectionId);
        if (!connection) return this.connectionsCache.delete(connectionId);

        await this._executeAction({
            table: this.connectionsTable,
            auth: this.connectionsConnection,
            actId: 'delete',
            rows: [connection]
        });

        return this.connectionsCache.delete(connectionId);
    }

    // ========================================================================
    // Permission Methods
    // ========================================================================

    /**
     * Get all permissions for a target (app, connection, etc.).
     * @param target The permission target (targetType and targetId)
     * @returns Array of permissions
     */
    async getPermissions(target: PermissionTarget): Promise<AppPermission[]> {
        return (await this._getData(this.permissionsTable, this.permissionsConnection, {
            filters: [
                {
                    field: 'targetType',
                    operator: QueryFilterOperator.Equals,
                    value: target.targetType
                },
                {
                    field: 'targetId',
                    operator: QueryFilterOperator.Equals,
                    value: target.targetId
                }
            ]
        })) as AppPermission[];
    }

    /**
     * Get a specific permission by ID.
     * @param permissionId Permission ID
     * @returns The permission or undefined
     */
    async getPermission(permissionId: string): Promise<AppPermission | undefined> {
        const permissions = (await this._getData(
            this.permissionsTable,
            this.permissionsConnection,
            {
                filters: [
                    {
                        field: 'id',
                        operator: QueryFilterOperator.Equals,
                        value: permissionId
                    }
                ],
                limit: 1
            }
        )) as AppPermission[];

        return permissions[0];
    }

    /**
     * Get permission for a specific user on a target.
     * @param target The permission target (targetType and targetId)
     * @param email User email
     * @returns The permission or undefined
     */
    async getUserPermission(
        target: PermissionTarget,
        email: string
    ): Promise<AppPermission | undefined> {
        const permissions = await this.getPermissions(target);
        return permissions.find(p => p.email.toLowerCase() === email.toLowerCase());
    }

    /**
     * Get all permissions for a user by email.
     * @param email User email
     * @param targetType Optional filter by target type
     * @returns Array of permissions the user has
     */
    async getPermissionsByUser(
        email: string,
        targetType?: PermissionTargetType
    ): Promise<AppPermission[]> {
        const filters = [
            {
                field: 'email',
                operator: QueryFilterOperator.Equals,
                value: email.toLowerCase()
            }
        ];

        if (targetType) {
            filters.push({
                field: 'targetType',
                operator: QueryFilterOperator.Equals,
                value: targetType
            });
        }

        return (await this._getData(this.permissionsTable, this.permissionsConnection, {
            filters
        })) as AppPermission[];
    }

    /**
     * Check if a user has at least the specified permission level on a target.
     * @param target The permission target (targetType and targetId)
     * @param email User email
     * @param requiredLevel Required permission level
     * @returns True if user has sufficient permissions
     */
    async hasPermission(
        target: PermissionTarget,
        email: string,
        requiredLevel: PermissionLevel
    ): Promise<boolean> {
        const permission = await this.getUserPermission(target, email);
        if (!permission) return false;

        const levelOrder = {
            [PermissionLevel.Read]: 1,
            [PermissionLevel.Write]: 2,
            [PermissionLevel.Admin]: 3
        };

        return levelOrder[permission.level] >= levelOrder[requiredLevel];
    }

    /**
     * Set (create or update) a permission.
     * @param permission The permission to set
     * @returns The saved permission
     */
    async setPermission(permission: AppPermission): Promise<AppPermission> {
        // Normalize email to lowercase
        permission = { ...permission, email: permission.email.toLowerCase() };

        const existingPermission = await this.getPermission(permission.id);

        await this._executeAction({
            table: this.permissionsTable,
            auth: this.permissionsConnection,
            actId: existingPermission ? 'update' : 'add',
            rows: [permission]
        });

        return permission;
    }

    /**
     * Delete a permission by ID.
     * @param permissionId Permission ID
     * @returns True if deleted
     */
    async deletePermission(permissionId: string): Promise<boolean> {
        const permission = await this.getPermission(permissionId);
        if (!permission) return false;

        await this._executeAction({
            table: this.permissionsTable,
            auth: this.permissionsConnection,
            actId: 'delete',
            rows: [permission]
        });

        return true;
    }

    /**
     * Delete all permissions for a target.
     * @param target The permission target (targetType and targetId)
     */
    async deletePermissions(target: PermissionTarget): Promise<void> {
        const permissions = await this.getPermissions(target);

        for (const permission of permissions) {
            await this._executeAction({
                table: this.permissionsTable,
                auth: this.permissionsConnection,
                actId: 'delete',
                rows: [permission]
            });
        }
    }

    async getSchema(appId: string) {
        if (this.schemaCache.has(appId)) return this.schemaCache.get(appId);

        const schemas = await this.getData(this.schemaTable, {
            filters: [
                {
                    field: this.schemaTable.fields.find(f => f.isKey)!.id,
                    operator: QueryFilterOperator.Equals,
                    value: appId
                }
            ],
            limit: 1
        });

        const schema = schemas[0] as AppSchema | undefined;
        this.schemaCache.set(appId, schema);
        return schema;
    }

    async setSchema(schema: AppSchema, owner?: string) {
        const hasSchema = await this.getSchema(schema.id);
        this.schemaCache.set(schema.id, schema);

        await this.executeAction({
            table: this.schemaTable,
            actId: hasSchema ? 'update' : 'add',
            rows: [schema]
        });

        if (!hasSchema && owner) {
            await this.setPermission({
                id: randomUUID(),
                targetType: PermissionTargetType.App,
                targetId: schema.id,
                email: owner.toLowerCase(),
                level: PermissionLevel.Admin
            });
        }

        return schema;
    }

    async deleteSchema(appId: string) {
        const schema = await this.getSchema(appId);
        if (!schema) return this.schemaCache.delete(appId);

        await this.executeAction({
            table: this.schemaTable,
            actId: 'delete',
            rows: [schema]
        });

        return this.schemaCache.delete(appId);
    }

    private async _executeAction(
        opts: executeActionOptions & {
            auth?: string;
        }
    ) {
        const { table, auth, actId, rows, depth } = opts;
        if ((depth ?? 0) > this.maxRecursiveDepth) throw new Error('Max recursion depth exceeded.');

        const action = table.actions?.find(a => a.id === actId);
        if (!action) throw new Error(`Action ${actId} not found.`);
        const keyFields = table.fields.filter(f => f.isKey).map(f => f.id);

        switch (action.type) {
            case AppActionType.Add: {
                const validator = zodFromTable(table, this.validatorCache);
                for (const row of rows) {
                    await this.connectors[table.connector]?.addRow?.(
                        table,
                        auth,
                        encodeRow(
                            (await validator.parseAsync(row)) as Record<string, unknown>,
                            table,
                            this.encryptionKey
                        )
                    );
                }

                return;
            }

            case AppActionType.Update: {
                const validator = zodFromTable(table, this.validatorCache);
                for (const row of rows) {
                    const key = extractKeys(row, keyFields);
                    if (Object.keys(key).length === 0) continue;

                    await this.connectors[table.connector]?.updateRow?.(
                        table,
                        auth,
                        key,
                        encodeRow(
                            (await validator.parseAsync(row)) as Record<string, unknown>,
                            table,
                            this.encryptionKey
                        )
                    );
                }

                return;
            }

            case AppActionType.Delete: {
                for (const row of rows) {
                    const key = extractKeys(row, keyFields);
                    if (Object.keys(key).length === 0) continue;

                    await this.connectors[table.connector]?.deleteRow?.(table, auth, key);
                }

                return;
            }

            case AppActionType.Process: {
                const subActions = (action.config?.actions as string[]) || [];

                for (const subActionId of subActions) {
                    await this._executeAction({
                        ...opts,
                        actId: subActionId,
                        depth: (depth ?? 0) + 1
                    });
                }

                return;
            }
        }
    }

    async executeAction(opts: executeActionOptions) {
        const connection = await this.getConnection(opts.table.connectionId);
        return this._executeAction({ ...opts, auth: connection?.content });
    }

    private async _getData(table: AppTable, auth?: string, query?: TableQueryOptions) {
        const connector = this.connectors[table.connector];
        if (!connector?.getData) return [];

        const encryptionKey = this.encryptionKey;
        const decodeRowFn = encryptionKey
            ? (row: Record<string, unknown>) => decodeRow(row, table, encryptionKey)
            : undefined;

        const source = await connector.getData(table, auth);
        return getDuckDBData({
            source,
            query,
            decodeRow: decodeRowFn
        });
    }

    async getData(table: AppTable, query?: TableQueryOptions) {
        const connection = await this.getConnection(table.connectionId);
        return this._getData(table, connection?.content, query);
    }
}
