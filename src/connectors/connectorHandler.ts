import type { FastifyPluginAsyncZod } from 'fastify-type-provider-zod';
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
import z from 'zod';

function _reorderElement<D>(oldIndex: number, newIndex: number, array: D[]) {
    let arr = [...array];
    const old = arr.splice(oldIndex, 1);
    arr.splice(newIndex, 0, ...old);
    return arr;
}

export type SchemaFXConnectorsOptions = {
    schemaConnector: string;
    connectors: Record<string, Connector>;
};

const plugin: FastifyPluginAsyncZod<SchemaFXConnectorsOptions> = async (
    fastify,
    { schemaConnector, connectors }
) => {
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

    fastify.get(
        '/apps/:appId/schema',
        {
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
                            }

                            return table;
                        });
                    }

                    return sConnector.saveSchema!(appId, schema);
                case 'update':
                    const updateEl = request.body.element;

                    if (updateEl.partOf === 'tables') {
                        schema.tables = schema.tables.map(table =>
                            table.id === updateEl.element.id
                                ? (updateEl.element as AppTable)
                                : table
                        );
                    } else if (updateEl.partOf === 'views') {
                        schema.views = schema.views.map(view =>
                            view.id === updateEl.element.id ? (updateEl.element as AppView) : view
                        );
                    } else if (updateEl.partOf === 'fields' && updateEl.parentId) {
                        schema.tables = schema.tables.map(table => {
                            if (table.id === updateEl.parentId) {
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
                        schema.tables = schema.tables.filter(table => table.id !== delEl.elementId);
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

    fastify.get(
        '/apps/:appId/data/:tableId',
        {
            schema: {
                params: z.object({
                    appId: z.string().min(1),
                    tableId: z.string().min(1)
                }),
                response: {
                    200: z.array(AppTableRowSchema)
                }
            }
        },
        async request => {
            const { appId, tableId } = request.params;

            const schema = await sConnector.getSchema!(appId);
            const connectorName = schema.tables.find(table => table.id === tableId)?.connector;

            if (!connectorName) return;

            const conn = connectors[connectorName];

            if (!conn) return;

            return conn.getData!(appId, tableId);
        }
    );

    fastify.post(
        '/apps/:appId/data/:tableId',
        {
            schema: {
                params: z.object({
                    appId: z.string().min(1),
                    tableId: z.string().min(1)
                }),
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
                response: {
                    200: z.array(AppTableRowSchema)
                }
            }
        },
        async request => {
            const { appId, tableId } = request.params;

            const schema = await sConnector.getSchema!(appId);
            const connectorName = schema.tables.find(table => table.id === tableId)?.connector;

            if (!connectorName) return;

            const conn = connectors[connectorName];

            if (!conn) return;

            switch (request.body.action) {
                case 'add':
                    return conn.addRow!(appId, tableId, request.body.row);
                case 'update':
                    return conn.updateRow!(appId, tableId, request.body.rowIndex, request.body.row);
                case 'delete':
                    return conn.deleteRow!(appId, tableId, request.body.rowIndex);
            }

            return conn.getData!(appId, tableId);
        }
    );
};

export default plugin;
