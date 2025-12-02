import type { FastifyPluginAsyncZod } from 'fastify-type-provider-zod';
import type { FastifyReply } from 'fastify';
import {
    type AppField,
    AppFieldSchema,
    AppSchemaSchema,
    type AppTable,
    AppTableRowSchema,
    AppTableSchema,
    type AppView,
    AppViewSchema,
    type Connector
} from '../types.js';
import z, { type ZodSafeParseResult } from 'zod';
import { LRUCache } from 'lru-cache';

/**
 * Generate a Zod schema for a single AppField.
 * @param field Field to generate the validator from.
 * @returns Zod schema for the field.
 */
function _zodFromField(field: AppField): z.ZodTypeAny {
    let fld;

    switch (field.type) {
        case 'number':
            fld = z.number();
            if (typeof field.minValue === 'number') fld = fld.min(field.minValue);
            if (typeof field.maxValue === 'number') fld = fld.max(field.maxValue);
            break;
        case 'boolean':
            fld = z.boolean();
            break;
        case 'date':
            fld = z.date();
            if (field.startDate) fld = fld.min(field.startDate);
            if (field.endDate) fld = fld.max(field.endDate);
            break;
        case 'email':
            fld = z.email();
            break;
        case 'dropdown':
            fld = z.enum((field.options as [string, ...string[]]) ?? []);
            break;
        case 'json':
            fld = _zodFromFields(field.fields ?? []);
            break;
        case 'list':
            if (field.child) {
                fld = z.array(_zodFromField(field.child));
            } else {
                fld = z.array(z.any());
            }
            break;
        default:
            fld = z.string();
            if (typeof field.minLength === 'number') fld = fld.min(field.minLength);
            if (typeof field.maxLength === 'number') fld = fld.max(field.maxLength);
            break;
    }

    if (!field.isRequired) fld = fld.optional().nullable();
    return fld;
}

/**
 * Generate a Zod object from a list of AppField definitions.
 * @param fields List of fields to generate the validator from.
 * @returns Zod object validator.
 */
function _zodFromFields(fields: AppField[]) {
    return z.strictObject(
        Object.fromEntries(fields.map(field => [field.id, _zodFromField(field)]))
    );
}

/**
 * Generate a Zod object from an AppTable definition.
 * @param table Table to generate the validator from.
 * @returns Zod object validator from table.
 */
function _zodFromTable(table: AppTable, appId: string, cache: LRUCache<string, z.ZodTypeAny>) {
    const cacheKey = `${appId}:${table.id}`;
    if (cache.has(cacheKey)) {
        return cache.get(cacheKey)!;
    }

    const validator = _zodFromFields(table.fields);
    cache.set(cacheKey, validator);
    return validator;
}

/**
 * Reorders elements within an array.
 * @param oldIndex Previous index.
 * @param newIndex New index.
 * @param array Array containing the data.
 * @returns Reordered array.
 */
function _reorderElement<D>(oldIndex: number, newIndex: number, array: D[]) {
    let arr = [...array];
    const old = arr.splice(oldIndex, 1);
    arr.splice(newIndex, 0, ...old);
    return arr;
}

/** Fastify Schema for table queries. */
const tableQuerySchema = {
    params: z.object({
        appId: z.string().min(1),
        tableId: z.string().min(1)
    }),
    response: {
        200: z.array(AppTableRowSchema),
        500: z.object({
            error: z.string(),
            message: z.string()
        }),
        400: z.object({
            error: z.string(),
            message: z.string(),
            details: z
                .array(
                    z.object({
                        field: z.string(),
                        message: z.string(),
                        code: z.string()
                    })
                )
                .optional()
        })
    }
};

export type SchemaFXConnectorsOptions = {
    schemaConnector: string;
    connectors: Record<string, Connector>;
    validatorCacheOpts?: {
        max?: number;
        ttl?: number;
    };
};

const plugin: FastifyPluginAsyncZod<SchemaFXConnectorsOptions> = async (
    fastify,
    { schemaConnector, connectors, validatorCacheOpts }
) => {
    const validatorCache = new LRUCache<string, z.ZodTypeAny>({
        max: validatorCacheOpts?.max ?? 500,
        ttl: validatorCacheOpts?.ttl ?? 1000 * 60 * 60
    });

    const sConnector = connectors[schemaConnector];

    if (!sConnector) {
        throw new Error(`Unrecognized connector "${schemaConnector}".`);
    } else if (!sConnector.getSchema) {
        throw new Error(`Missing implementation "getSchema" on connector "${schemaConnector}".`);
    } else if (!sConnector.saveSchema) {
        throw new Error(`Missing implementation "saveSchema" on connector "${schemaConnector}".`);
    } else if (!sConnector.deleteSchema) {
        throw new Error(`Missing implementation "deleteSchema" on connector "${schemaConnector}".`);
    }

    fastify.post(
        '/login',
        {
            schema: {
                body: z.object({
                    username: z.string().min(1),
                    password: z.string().min(1)
                }),
                response: {
                    200: z.object({
                        token: z.string()
                    }),
                    401: z.object({
                        error: z.string(),
                        message: z.string()
                    })
                }
            }
        },
        async (request, reply) => {
            const { username, password } = request.body;
            const isValid = username === 'test' && password === 'test';

            if (isValid) {
                return {
                    token: fastify.jwt.sign({ id: username }, { expiresIn: '8h' })
                };
            }

            return reply.code(401).send({
                error: 'Unauthorized',
                message: 'Invalid credentials.'
            });
        }
    );

    fastify.get(
        '/apps/:appId/schema',
        {
            onRequest: [fastify.authenticate],
            schema: {
                params: z.object({ appId: z.string().min(1) }),
                response: { 200: AppSchemaSchema }
            }
        },
        request => sConnector.getSchema!(request.params.appId)
    );

    fastify.post(
        '/apps/:appId/schema',
        {
            onRequest: [fastify.authenticate],
            schema: {
                params: z.object({ appId: z.string().min(1) }),
                body: z.discriminatedUnion('action', [
                    z.object({
                        action: z.enum(['add', 'update']),
                        element: z.discriminatedUnion('partOf', [
                            z.object({
                                partOf: z.literal('tables'),
                                element: AppTableSchema
                            }),
                            z.object({
                                partOf: z.literal('views'),
                                element: AppViewSchema
                            }),
                            z.object({
                                partOf: z.literal('fields'),
                                element: AppFieldSchema,
                                parentId: z.string().min(1)
                            })
                        ])
                    }),
                    z.object({
                        action: z.literal('delete'),
                        element: z.discriminatedUnion('partOf', [
                            z.object({
                                partOf: z.enum(['tables', 'views']),
                                elementId: z.string().min(1)
                            }),
                            z.object({
                                partOf: z.literal('fields'),
                                elementId: z.string().min(1),
                                parentId: z.string().min(1)
                            })
                        ])
                    }),
                    z.object({
                        action: z.literal('reorder'),
                        oldIndex: z.number().nonnegative(),
                        newIndex: z.number().nonnegative(),
                        element: z.discriminatedUnion('partOf', [
                            z.object({ partOf: z.enum(['tables', 'views']) }),
                            z.object({
                                partOf: z.literal('fields'),
                                parentId: z.string().min(1)
                            })
                        ])
                    })
                ]),
                response: { 200: AppSchemaSchema }
            }
        },
        async request => {
            const { appId } = request.params;
            const schema = await sConnector.getSchema!(appId);

            switch (request.body.action) {
                case 'add':
                    const addEl = request.body.element;

                    if (addEl.partOf === 'tables') {
                        schema.tables.push(addEl.element as AppTable);
                    } else if (addEl.partOf === 'views') {
                        schema.views.push(addEl.element as AppView);
                    } else if (addEl.partOf === 'fields' && addEl.parentId) {
                        const oldFieldsLength =
                            schema.tables.find(table => table.id === addEl.parentId)?.fields
                                ?.length ?? 0;

                        schema.views = schema.views.map(view => {
                            if (
                                view.tableId === addEl.parentId &&
                                view.fields.length === oldFieldsLength
                            ) {
                                view.fields.push((addEl.element as AppField).id);
                            }

                            return view;
                        });

                        schema.tables = schema.tables.map(table => {
                            if (table.id === addEl.parentId) {
                                table.fields.push(addEl.element as AppField);
                                validatorCache.delete(`${appId}:${table.id}`);
                            }

                            return table;
                        });
                    }

                    return sConnector.saveSchema!(appId, schema);
                case 'update':
                    const updateEl = request.body.element;

                    if (updateEl.partOf === 'tables') {
                        schema.tables = schema.tables.map(table => {
                            if (table.id === updateEl.element.id) {
                                validatorCache.delete(`${appId}:${table.id}`);
                                return updateEl.element as AppTable;
                            }
                            return table;
                        });
                    } else if (updateEl.partOf === 'views') {
                        schema.views = schema.views.map(view =>
                            view.id === updateEl.element.id ? (updateEl.element as AppView) : view
                        );
                    } else if (updateEl.partOf === 'fields' && updateEl.parentId) {
                        schema.tables = schema.tables.map(table => {
                            if (table.id === updateEl.parentId) {
                                validatorCache.delete(`${appId}:${table.id}`);
                                table.fields = table.fields.map(field =>
                                    field.id === updateEl.element.id
                                        ? (updateEl.element as AppField)
                                        : field
                                );
                            }

                            return table;
                        });
                    }

                    return sConnector.saveSchema!(appId, schema);
                case 'delete':
                    const delEl = request.body.element;

                    if (delEl.partOf === 'tables') {
                        schema.tables = schema.tables.filter(table => {
                            if (table.id === delEl.elementId) {
                                validatorCache.delete(`${appId}:${table.id}`);
                                return false;
                            }
                            return true;
                        });
                    } else if (delEl.partOf === 'views') {
                        schema.views = schema.views.filter(view => view.id !== delEl.elementId);
                    } else if (delEl.partOf === 'fields' && delEl.parentId) {
                        schema.views = schema.views.map(view => {
                            if (view.tableId === delEl.parentId) {
                                view.fields = view.fields.filter(
                                    field => field !== delEl.elementId
                                );
                            }

                            return view;
                        });

                        schema.tables = schema.tables.map(table => {
                            if (table.id === delEl.parentId) {
                                validatorCache.delete(`${appId}:${table.id}`);
                                table.fields = table.fields.filter(
                                    field => field.id !== delEl.elementId
                                );
                            }

                            return table;
                        });
                    }

                    return sConnector.saveSchema!(appId, schema);
                case 'reorder':
                    const reoEl = request.body.element;
                    const { oldIndex, newIndex } = request.body;

                    if (reoEl.partOf === 'tables') {
                        schema.tables = _reorderElement(oldIndex, newIndex, schema.tables);
                    } else if (reoEl.partOf === 'views') {
                        schema.views = _reorderElement(oldIndex, newIndex, schema.views);
                    } else if (reoEl.partOf === 'fields' && reoEl.parentId) {
                        schema.tables = schema.tables.map(table => {
                            if (table.id === reoEl.parentId) {
                                validatorCache.delete(`${appId}:${table.id}`);
                                table.fields = _reorderElement(oldIndex, newIndex, table.fields);
                            }

                            return table;
                        });
                    }

                    return sConnector.saveSchema!(appId, schema);
            }

            return sConnector.getSchema!(request.params.appId);
        }
    );

    async function handleTable(appId: string, tableId: string, reply: FastifyReply) {
        const schema = await sConnector.getSchema!(appId);
        const table = schema.tables.find(table => table.id === tableId);

        let response;
        if (!table) {
            response = reply.code(400).send({
                error: 'Data Error',
                message: 'Invalid table.'
            });

            return { schema, table, response };
        }

        const connectorName = table.connector;
        const connector = connectors[connectorName];

        if (!connector) {
            response = reply.code(500).send({
                error: 'Data Error',
                message: 'Invalid connector.'
            });

            return { schema, table, connectorName, connector, response };
        }

        return { schema, table, connectorName, connector, success: true };
    }

    fastify.get(
        '/apps/:appId/data/:tableId',
        {
            onRequest: [fastify.authenticate],
            schema: tableQuerySchema
        },
        async (request, reply) => {
            const { appId, tableId } = request.params;

            const { response, success, connector } = await handleTable(appId, tableId, reply);
            if (!success) return response;

            return connector.getData!(appId, tableId);
        }
    );

    fastify.post(
        '/apps/:appId/data/:tableId',
        {
            onRequest: [fastify.authenticate],
            schema: {
                body: z.discriminatedUnion('action', [
                    z.object({
                        action: z.literal('add'),
                        row: AppTableRowSchema
                    }),
                    z.object({
                        action: z.literal('update'),
                        rowIndex: z.number().nonnegative(),
                        row: AppTableRowSchema
                    }),
                    z.object({
                        action: z.literal('delete'),
                        rowIndex: z.number().nonnegative()
                    })
                ]),
                ...tableQuerySchema
            }
        },
        async (request, reply) => {
            const { appId, tableId } = request.params;

            const { response, success, connector, table } = await handleTable(
                appId,
                tableId,
                reply
            );

            if (!success) return response;

            let row: Record<string, unknown>;
            let rowResult: ZodSafeParseResult<Record<string, unknown>>;
            switch (request.body.action) {
                case 'add':
                    row = request.body.row;
                    rowResult = _zodFromTable(table, appId, validatorCache).safeParse(
                        row
                    ) as ZodSafeParseResult<Record<string, unknown>>;

                    if (!rowResult.success) {
                        return reply.code(400).send({
                            error: 'Validation Error',
                            message: 'Invalid input data',
                            details: rowResult.error.issues.map(err => ({
                                field: err.path.join('.'),
                                message: err.message,
                                code: err.code
                            }))
                        });
                    }

                    return connector.addRow!(appId, tableId, rowResult.data);
                case 'update':
                    row = request.body.row;
                    rowResult = _zodFromTable(table, appId, validatorCache).safeParse(
                        row
                    ) as ZodSafeParseResult<Record<string, unknown>>;

                    if (!rowResult.success) {
                        return reply.code(400).send({
                            error: 'Validation Error',
                            message: 'Invalid input data',
                            details: rowResult.error.issues.map(err => ({
                                field: err.path.join('.'),
                                message: err.message,
                                code: err.code
                            }))
                        });
                    }

                    return connector.updateRow!(
                        appId,
                        tableId,
                        request.body.rowIndex,
                        rowResult.data
                    );
                case 'delete':
                    return connector.deleteRow!(appId, tableId, request.body.rowIndex);
            }

            return connector.getData!(appId, tableId);
        }
    );
};

export default plugin;
