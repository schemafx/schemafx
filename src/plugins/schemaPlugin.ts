import type { FastifyPluginAsyncZod } from 'fastify-type-provider-zod';
import { z } from 'zod';
import {
    type AppSchema,
    AppSchemaSchema,
    type AppTable,
    type AppView,
    type AppField,
    type AppAction,
    AppTableSchema,
    AppViewSchema,
    AppFieldSchema,
    AppActionSchema
} from '../types.js';
import { reorderElement, validateTableKeys } from '../utils/schemaUtils.js';
import { LRUCache } from 'lru-cache';
import type { Connector } from '../types.js';

export type SchemaPluginOptions = {
    sConnector: Connector;
    schemaCache: LRUCache<string, AppSchema>;
    validatorCache: LRUCache<string, z.ZodType>;
    getSchema: (appId: string) => Promise<AppSchema>;
};

const plugin: FastifyPluginAsyncZod<SchemaPluginOptions> = async (
    fastify,
    { sConnector, schemaCache, validatorCache, getSchema }
) => {
    fastify.get(
        '/apps/:appId/schema',
        {
            onRequest: [fastify.authenticate],
            schema: {
                params: z.object({ appId: z.string().min(1) }),
                response: { 200: AppSchemaSchema }
            }
        },
        request => getSchema(request.params.appId)
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
                            }),
                            z.object({
                                partOf: z.literal('actions'),
                                element: AppActionSchema,
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
                                partOf: z.enum(['fields', 'actions']),
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
                                partOf: z.enum(['fields', 'actions']),
                                parentId: z.string().min(1)
                            })
                        ])
                    })
                ]),
                response: {
                    200: AppSchemaSchema,
                    400: z.object({
                        error: z.string(),
                        message: z.string(),
                        details: z.any().optional()
                    })
                }
            }
        },
        async request => {
            const { appId } = request.params;
            const schema = await getSchema(appId);

            switch (request.body.action) {
                case 'add':
                    const addEl = request.body.element;

                    if (addEl.partOf === 'tables') {
                        validateTableKeys(addEl.element as AppTable);
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
                                view.config.fields &&
                                (view.config.fields as string[]).length === oldFieldsLength
                            ) {
                                (view.config.fields as string[]).push(
                                    (addEl.element as AppField).id
                                );
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
                    } else if (addEl.partOf === 'actions' && addEl.parentId) {
                        schema.tables = schema.tables.map(table => {
                            if (table.id === addEl.parentId) {
                                table.actions.push(addEl.element as AppAction);
                            }

                            return table;
                        });
                    }

                    schemaCache.delete(appId);
                    return sConnector.saveSchema!(appId, schema);
                case 'update':
                    const updateEl = request.body.element;

                    if (updateEl.partOf === 'tables') {
                        validateTableKeys(updateEl.element as AppTable);
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
                                const updatedFields = table.fields.map(field =>
                                    field.id === updateEl.element.id
                                        ? (updateEl.element as AppField)
                                        : field
                                );
                                const hasKey = updatedFields.some(f => f.isKey);
                                if (!hasKey) {
                                    throw new Error(
                                        `Table ${table.name} must have at least one key field.`
                                    );
                                }

                                validatorCache.delete(`${appId}:${table.id}`);
                                table.fields = updatedFields;
                            }

                            return table;
                        });
                    } else if (updateEl.partOf === 'actions' && updateEl.parentId) {
                        schema.tables = schema.tables.map(table => {
                            if (table.id === updateEl.parentId) {
                                table.actions = table.actions.map(action =>
                                    action.id === updateEl.element.id
                                        ? (updateEl.element as AppAction)
                                        : action
                                );
                            }

                            return table;
                        });
                    }

                    schemaCache.delete(appId);
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
                            if (view.tableId === delEl.parentId && view.config.fields) {
                                view.config.fields = (view.config.fields as string[]).filter(
                                    field => field !== delEl.elementId
                                );
                            }

                            return view;
                        });

                        schema.tables = schema.tables.map(table => {
                            if (table.id === delEl.parentId) {
                                // Check if deleting this field leaves the table without a key
                                const remainingFields = table.fields.filter(
                                    field => field.id !== delEl.elementId
                                );
                                const hasKey = remainingFields.some(f => f.isKey);
                                if (!hasKey) {
                                    throw new Error(
                                        `Cannot delete field. Table ${table.name} must have at least one key field.`
                                    );
                                }

                                validatorCache.delete(`${appId}:${table.id}`);
                                table.fields = remainingFields;
                            }

                            return table;
                        });
                    } else if (delEl.partOf === 'actions' && delEl.parentId) {
                        schema.tables = schema.tables.map(table => {
                            if (table.id === delEl.parentId) {
                                table.actions = table.actions.filter(
                                    action => action.id !== delEl.elementId
                                );
                            }

                            return table;
                        });
                    }

                    schemaCache.delete(appId);
                    return sConnector.saveSchema!(appId, schema);
                case 'reorder':
                    const reoEl = request.body.element;
                    const { oldIndex, newIndex } = request.body;

                    if (reoEl.partOf === 'tables') {
                        schema.tables = reorderElement(oldIndex, newIndex, schema.tables);
                    } else if (reoEl.partOf === 'views') {
                        schema.views = reorderElement(oldIndex, newIndex, schema.views);
                    } else if (reoEl.partOf === 'fields' && reoEl.parentId) {
                        schema.tables = schema.tables.map(table => {
                            if (table.id === reoEl.parentId) {
                                table.fields = reorderElement(oldIndex, newIndex, table.fields);
                            }

                            return table;
                        });
                    } else if (reoEl.partOf === 'actions' && reoEl.parentId) {
                        schema.tables = schema.tables.map(table => {
                            if (table.id === reoEl.parentId) {
                                table.actions = reorderElement(oldIndex, newIndex, table.actions);
                            }

                            return table;
                        });
                    }

                    schemaCache.delete(appId);
                    return sConnector.saveSchema!(appId, schema);
            }

            return getSchema(request.params.appId);
        }
    );
};

export default plugin;
