import fastify from 'fastify';
import {
    serializerCompiler,
    validatorCompiler,
    type ZodTypeProvider
} from 'fastify-type-provider-zod';
import {
    AppFieldSchema,
    AppSchemaSchema,
    AppTableRowSchema,
    AppTableSchema,
    AppViewSchema
} from './types.js';
import {
    addElement,
    addRow,
    deleteElement,
    deleteRow,
    getData,
    getSchema,
    reorderElement,
    updateElement,
    updateRow
} from './mock_data.js';

import 'dotenv/config';
import z from 'zod';
import fastifyCors from '@fastify/cors';

const app = fastify({
    logger: true
}).withTypeProvider<ZodTypeProvider>();

// Zod Type Provider
app.setValidatorCompiler(validatorCompiler);
app.setSerializerCompiler(serializerCompiler);

app.register(fastifyCors, {
    origin: true,
    methods: ['GET', 'POST', 'OPTIONS'],
    allowedHeaders: ['Content-Type']
});

app.get(
    '/api/apps/:appId/schema',
    {
        schema: {
            params: z.object({ appId: z.string().min(1) }),
            response: { 200: AppSchemaSchema }
        }
    },
    request => getSchema(request.params.appId)
);

app.post(
    '/api/apps/:appId/schema',
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
    request => {
        switch (request.body.action) {
            case 'add':
                return addElement(
                    request.params.appId,
                    request.body.element.element,
                    request.body.element.partOf,
                    'parentId' in request.body.element
                        ? {
                              parentId: request.body.element.parentId
                          }
                        : {}
                );
            case 'update':
                return updateElement(
                    request.params.appId,
                    request.body.element.element,
                    request.body.element.partOf,
                    'parentId' in request.body.element
                        ? {
                              parentId: request.body.element.parentId
                          }
                        : {}
                );
            case 'delete':
                return deleteElement(
                    request.params.appId,
                    request.body.element.elementId,
                    request.body.element.partOf,
                    'parentId' in request.body.element
                        ? {
                              parentId: request.body.element.parentId
                          }
                        : {}
                );
            case 'reorder':
                return reorderElement(
                    request.params.appId,
                    request.body.oldIndex,
                    request.body.newIndex,
                    request.body.element.partOf,
                    'parentId' in request.body.element
                        ? {
                              parentId: request.body.element.parentId
                          }
                        : {}
                );
        }

        return getSchema(request.params.appId);
    }
);

app.get(
    '/api/apps/:appId/data/:tableId',
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
    request => getData(request.params.appId, request.params.tableId)
);

app.post(
    '/api/apps/:appId/data/:tableId',
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
    request => {
        const { tableId } = request.params;
        switch (request.body.action) {
            case 'add':
                return addRow(request.params.appId, tableId, request.body.row);
            case 'update':
                return updateRow(
                    request.params.appId,
                    tableId,
                    request.body.rowIndex,
                    request.body.row
                );
            case 'delete':
                return deleteRow(request.params.appId, tableId, request.body.rowIndex);
        }

        return getData(request.params.appId, tableId);
    }
);

// Start
try {
    const port = process.env.PORT ? parseInt(process.env.PORT) : 3000;

    await app.listen({ port, host: '0.0.0.0' });
    console.log(`\n🚀 Server listening at http://localhost:${port}/`);
} catch (err) {
    app.log.error(err);
    process.exit(1);
}
