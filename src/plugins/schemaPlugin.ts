import type { FastifyPluginAsyncZod } from 'fastify-type-provider-zod';
import { z } from 'zod';
import {
    AppSchemaSchema,
    type AppTable,
    type AppView,
    type AppField,
    type AppAction,
    AppTableSchema,
    AppViewSchema,
    AppFieldSchema,
    AppActionSchema,
    type AppSchema
} from '../types.js';
import { reorderElement, validateTableKeys } from '../utils/schemaUtils.js';
import type DataService from '../services/DataService.js';
import { ErrorResponseSchema } from '../utils/fastifyUtils.js';

const plugin: FastifyPluginAsyncZod<{
    dataService: DataService;
}> = async (fastify, { dataService }) => {
    fastify.get(
        '/apps/:appId/schema',
        {
            onRequest: [fastify.authenticate],
            schema: {
                params: z.object({ appId: z.string().min(1) }),
                response: { 200: AppSchemaSchema, 404: ErrorResponseSchema }
            }
        },
        async (request, reply) => {
            const schema = await dataService.getSchema(request.params.appId);

            if (!schema) {
                return reply.code(404).send({
                    error: 'Not Found',
                    message: 'Application not found.'
                });
            }

            return schema;
        }
    );

    fastify.get(
        '/apps',
        {
            schema: {
                response: {
                    200: z.array(AppSchemaSchema)
                }
            }
        },
        async () => dataService.getData(dataService.schemaTable) as Promise<AppSchema[]>
    );

    fastify.post(
        '/apps/:appId/schema',
        {
            onRequest: [fastify.authenticate],
            schema: {
                params: z.object({ appId: z.string().min(1) }),
                body: z.discriminatedUnion('action', [
                    z.object({
                        action: z.literal('add'),
                        element: z.discriminatedUnion('partOf', [
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
                        action: z.literal('update'),
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
                    404: ErrorResponseSchema
                }
            }
        },
        async (request, reply) => {
            const { appId } = request.params;
            const schema = await dataService.getSchema(appId);

            if (!schema) {
                return reply.code(404).send({
                    error: 'Not Found',
                    message: 'Application not found.'
                });
            }

            switch (request.body.action) {
                case 'add':
                    const addEl = request.body.element;

                    switch (addEl.partOf) {
                        case 'views':
                            schema.views.push(addEl.element as AppView);
                            break;
                        case 'fields':
                            const oldFieldsLength = schema.tables.find(
                                table => table.id === addEl.parentId
                            )!.fields.length;

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
                                    dataService.validatorCache.delete(`${appId}:${table.id}`);
                                }

                                return table;
                            });

                            break;
                        case 'actions':
                            schema.tables = schema.tables.map(table => {
                                if (table.id === addEl.parentId) {
                                    table.actions.push(addEl.element as AppAction);
                                }

                                return table;
                            });
                    }

                    break;
                case 'update':
                    const updateEl = request.body.element;

                    switch (updateEl.partOf) {
                        case 'tables':
                            validateTableKeys(updateEl.element as AppTable);
                            schema.tables = schema.tables.map(table => {
                                if (table.id === updateEl.element.id) {
                                    dataService.validatorCache.delete(`${appId}:${table.id}`);
                                    return updateEl.element as AppTable;
                                }

                                return table;
                            });

                            break;
                        case 'views':
                            schema.views = schema.views.map(view =>
                                view.id === updateEl.element.id
                                    ? (updateEl.element as AppView)
                                    : view
                            );

                            break;
                        case 'fields':
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

                                    dataService.validatorCache.delete(`${appId}:${table.id}`);
                                    table.fields = updatedFields;
                                }

                                return table;
                            });

                            break;
                        case 'actions':
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

                    break;
                case 'delete':
                    const delEl = request.body.element;

                    switch (delEl.partOf) {
                        case 'tables':
                            schema.tables = schema.tables.filter(table => {
                                if (table.id === delEl.elementId) {
                                    dataService.validatorCache.delete(`${appId}:${table.id}`);
                                    return false;
                                }

                                return true;
                            });

                            break;
                        case 'views':
                            schema.views = schema.views.filter(view => view.id !== delEl.elementId);
                            break;
                        case 'fields':
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

                                    dataService.validatorCache.delete(`${appId}:${table.id}`);
                                    table.fields = remainingFields;
                                }

                                return table;
                            });

                            break;
                        case 'actions':
                            schema.tables = schema.tables.map(table => {
                                if (table.id === delEl.parentId) {
                                    table.actions = table.actions.filter(
                                        action => action.id !== delEl.elementId
                                    );
                                }

                                return table;
                            });
                    }

                    break;
                case 'reorder':
                    const reoEl = request.body.element;
                    const { oldIndex, newIndex } = request.body;

                    switch (reoEl.partOf) {
                        case 'tables':
                            schema.tables = reorderElement(oldIndex, newIndex, schema.tables);
                            break;
                        case 'views':
                            schema.views = reorderElement(oldIndex, newIndex, schema.views);
                            break;
                        case 'fields':
                            schema.tables = schema.tables.map(table => {
                                if (table.id === reoEl.parentId) {
                                    table.fields = reorderElement(oldIndex, newIndex, table.fields);
                                }

                                return table;
                            });

                            break;
                        case 'actions':
                            schema.tables = schema.tables.map(table => {
                                if (table.id === reoEl.parentId) {
                                    table.actions = reorderElement(
                                        oldIndex,
                                        newIndex,
                                        table.actions
                                    );
                                }

                                return table;
                            });

                            break;
                    }

                    break;
            }

            return dataService.setSchema(schema);
        }
    );
};

export default plugin;
