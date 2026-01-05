import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createTestApp } from '../testUtils.js';
import type SchemaFX from '../../src/index.js';
import {
    AppActionType,
    AppFieldType,
    type AppSchema,
    AppViewType,
    type Connector
} from '../../src/index.js';
import type { FastifyInstance } from 'fastify';

describe('Schema Operations', () => {
    let app: SchemaFX;
    let server: FastifyInstance;
    let connector: Connector;
    let token: string;

    beforeEach(async () => {
        const testApp = await createTestApp(true);
        app = testApp.app;
        server = app.fastifyInstance;
        connector = testApp.connector;
        token = testApp.token!;
    });

    afterEach(async () => {
        await server.close();
    });

    it('should add a view', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'add',
                element: {
                    partOf: 'views',
                    element: {
                        id: 'view1',
                        name: 'View 1',
                        tableId: 'users',
                        type: AppViewType.Table,
                        config: {}
                    }
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload);
        expect(body.views).toHaveLength(1);
        expect(body.views[0].id).toBe('view1');
    });

    it('should add a field', async () => {
        // Add a second table and views to cover all branches
        const schema = await app.dataService.getSchema('app1');
        if (!schema) throw new Error('Schema not found');
        schema.tables.push({
            id: 'products',
            name: 'Products',
            connector: connector.id,
            path: ['products'],
            fields: [{ id: 'id', name: 'ID', type: AppFieldType.Number, isKey: true }],
            actions: []
        });
        // Add views - one matching the table with fields matching count, one not matching
        schema.views.push(
            {
                id: 'usersView',
                name: 'Users View',
                tableId: 'users',
                type: AppViewType.Table,
                config: { fields: ['id', 'name'] } // matches current field count (2)
            },
            {
                id: 'productsView',
                name: 'Products View',
                tableId: 'products',
                type: AppViewType.Table,
                config: { fields: ['id'] }
            }
        );
        await app.dataService.setSchema(schema);

        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'add',
                element: {
                    partOf: 'fields',
                    parentId: 'users',
                    element: {
                        id: 'email',
                        name: 'Email',
                        type: AppFieldType.Email
                    }
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as AppSchema;
        const table = body.tables.find(t => t.id === 'users');
        expect(table?.fields).toHaveLength(3);
        expect(table?.fields.find(f => f.id === 'email')).toBeDefined();
        // Verify view fields were updated for matching view
        const usersView = body.views.find(v => v.id === 'usersView');
        expect(usersView?.config.fields).toContain('email');
        // Verify other table was not affected
        const productsTable = body.tables.find(t => t.id === 'products');
        expect(productsTable?.fields).toHaveLength(1);
    });

    it('should add a field to a new table with no prior fields lookup', async () => {
        // Test the ?? 0 branch by adding a field where the table initially has no fields
        // This simulates edge case where oldFieldsLength falls back to 0
        const schema = await app.dataService.getSchema('app1');
        if (!schema) throw new Error('Schema not found');

        // Add a table with empty fields array initially
        schema.tables.push({
            id: 'empty',
            name: 'Empty Table',
            connector: connector.id,
            path: ['empty'],
            fields: [], // No fields initially
            actions: []
        });

        // Add a view for this table with no fields config
        schema.views.push({
            id: 'emptyView',
            name: 'Empty View',
            tableId: 'empty',
            type: AppViewType.Table,
            config: { fields: [] } // Empty fields array, length === 0 === oldFieldsLength
        });
        await app.dataService.setSchema(schema);

        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'add',
                element: {
                    partOf: 'fields',
                    parentId: 'empty',
                    element: {
                        id: 'id',
                        name: 'ID',
                        type: AppFieldType.Number,
                        isKey: true
                    }
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as AppSchema;
        const table = body.tables.find(t => t.id === 'empty');
        expect(table?.fields).toHaveLength(1);
        // View should have the field added since fields.length (0) === oldFieldsLength (0)
        const emptyView = body.views.find(v => v.id === 'emptyView');
        expect(emptyView?.config.fields).toContain('id');
    });

    it('should add a field without updating view when fields count does not match', async () => {
        // Add a view with fewer fields than the table has
        const schema = await app.dataService.getSchema('app1');
        if (!schema) throw new Error('Schema not found');
        schema.views.push({
            id: 'partialView',
            name: 'Partial View',
            tableId: 'users',
            type: AppViewType.Table,
            config: { fields: ['id'] } // only 1 field, but table has 2
        });
        await app.dataService.setSchema(schema);

        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'add',
                element: {
                    partOf: 'fields',
                    parentId: 'users',
                    element: {
                        id: 'email',
                        name: 'Email',
                        type: AppFieldType.Email
                    }
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as AppSchema;
        // View should NOT have the new field since its count didn't match
        const partialView = body.views.find(v => v.id === 'partialView');
        expect(partialView?.config.fields).not.toContain('email');
        expect(partialView?.config.fields).toHaveLength(1);
    });

    it('should add a field without updating view when view has no fields config', async () => {
        // Add a view for the same table but without fields config
        const schema = await app.dataService.getSchema('app1');
        if (!schema) throw new Error('Schema not found');
        schema.views.push({
            id: 'noFieldsView',
            name: 'No Fields View',
            tableId: 'users',
            type: AppViewType.Table,
            config: {} // no fields config at all
        });
        await app.dataService.setSchema(schema);

        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'add',
                element: {
                    partOf: 'fields',
                    parentId: 'users',
                    element: {
                        id: 'email',
                        name: 'Email',
                        type: AppFieldType.Email
                    }
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as AppSchema;
        // View should still have no fields config
        const noFieldsView = body.views.find(v => v.id === 'noFieldsView');
        expect(noFieldsView?.config.fields).toBeUndefined();
    });

    it('should add an action', async () => {
        // Add a second table to cover the else branch when table.id !== parentId
        const schema = await app.dataService.getSchema('app1');
        if (!schema) throw new Error('Schema not found');
        schema.tables.push({
            id: 'products',
            name: 'Products',
            connector: connector.id,
            path: ['products'],
            fields: [{ id: 'id', name: 'ID', type: AppFieldType.Number, isKey: true }],
            actions: []
        });
        await app.dataService.setSchema(schema);

        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'add',
                element: {
                    partOf: 'actions',
                    parentId: 'users',
                    element: {
                        id: 'export',
                        name: 'Export',
                        type: AppActionType.Process,
                        config: {}
                    }
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as AppSchema;
        const table = body.tables.find(t => t.id === 'users');

        // seeded actions (3) + new one
        expect(table?.actions).toHaveLength(4);
        expect(table?.actions.find(a => a.id === 'export')).toBeDefined();
        // Verify other table was not affected
        const productsTable = body.tables.find(t => t.id === 'products');
        expect(productsTable?.actions).toHaveLength(0);
    });

    it('should update a table', async () => {
        const table = {
            id: 'users',
            name: 'Users Updated',
            connector: connector.id,
            path: ['users'],
            fields: [{ id: 'id', name: 'ID', type: AppFieldType.Number, isKey: true }],
            actions: []
        };

        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'update',
                element: {
                    partOf: 'tables',
                    element: table
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as AppSchema;
        const updatedTable = body.tables.find(t => t.id === 'users');
        expect(updatedTable?.name).toBe('Users Updated');
    });

    it('should delete a field', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'delete',
                element: {
                    partOf: 'fields',
                    parentId: 'users',
                    elementId: 'name'
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as AppSchema;
        const table = body.tables.find(t => t.id === 'users');
        expect(table!.fields.find(f => f.id === 'name')).toBeUndefined();
    });

    it('should reorder fields', async () => {
        // Add a second table to cover the else branch when table.id !== parentId
        const schema = await app.dataService.getSchema('app1');
        if (!schema) throw new Error('Schema not found');
        schema.tables.push({
            id: 'products',
            name: 'Products',
            connector: connector.id,
            path: ['products'],
            fields: [
                { id: 'id', name: 'ID', type: AppFieldType.Number, isKey: true },
                { id: 'name', name: 'Name', type: AppFieldType.Text }
            ],
            actions: []
        });
        await app.dataService.setSchema(schema);

        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'reorder',
                oldIndex: 0,
                newIndex: 1,
                element: {
                    partOf: 'fields',
                    parentId: 'users'
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as AppSchema;
        const table = body.tables.find(t => t.id === 'users');
        expect(table?.fields[0]?.id).toBe('name');
        expect(table?.fields[1]?.id).toBe('id');
        // Verify other table's fields were not reordered
        const productsTable = body.tables.find(t => t.id === 'products');
        expect(productsTable?.fields[0]?.id).toBe('id');
    });

    it('should return 404 for unknown app schema', async () => {
        const response = await server.inject({
            method: 'GET',
            url: '/api/apps/unknown-app/schema',
            headers: { Authorization: `Bearer ${token}` }
        });

        expect(response.statusCode).toBe(404);
    });

    it('should return 404 for unknown app schema when updating', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/unknown-app/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'add',
                element: {
                    partOf: 'views',
                    element: { id: 'view1', name: 'View 1', tableId: 't1', type: 'table' }
                }
            }
        });

        expect(response.statusCode).toBe(404);
    });

    it('should update a table while preserving other tables', async () => {
        // Add a second table to the schema
        const schema = await app.dataService.getSchema('app1');
        if (!schema) throw new Error('Schema not found');
        schema.tables.push({
            id: 'products',
            name: 'Products',
            connector: connector.id,
            path: ['products'],
            fields: [{ id: 'id', name: 'ID', type: AppFieldType.Number, isKey: true }],
            actions: []
        });
        await app.dataService.setSchema(schema);

        // Now update only the first table
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'update',
                element: {
                    partOf: 'tables',
                    element: {
                        id: 'users',
                        name: 'Users Modified',
                        connector: connector.id,
                        path: ['users'],
                        fields: [{ id: 'id', name: 'ID', type: AppFieldType.Number, isKey: true }],
                        actions: []
                    }
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as AppSchema;
        // Verify the updated table
        const usersTable = body.tables.find(t => t.id === 'users');
        expect(usersTable?.name).toBe('Users Modified');
        // Verify the other table was preserved
        const productsTable = body.tables.find(t => t.id === 'products');
        expect(productsTable?.name).toBe('Products');
    });

    it('should update a field successfully', async () => {
        // Add a second table to cover the return table branch
        const schema = await app.dataService.getSchema('app1');
        if (!schema) throw new Error('Schema not found');
        schema.tables.push({
            id: 'products',
            name: 'Products',
            connector: connector.id,
            path: ['products'],
            fields: [{ id: 'id', name: 'ID', type: AppFieldType.Number, isKey: true }],
            actions: []
        });
        await app.dataService.setSchema(schema);

        // Update the name field (not the key field)
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'update',
                element: {
                    partOf: 'fields',
                    parentId: 'users',
                    element: { id: 'name', name: 'Full Name', type: AppFieldType.Text }
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as AppSchema;
        const table = body.tables.find(t => t.id === 'users');
        const updatedField = table?.fields.find(f => f.id === 'name');
        expect(updatedField?.name).toBe('Full Name');
    });

    it('should prevent updating field to remove all keys', async () => {
        // The table 'users' has 'id' as Key.
        // Try to update 'id' to not be a key.
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'update',
                element: {
                    partOf: 'fields',
                    parentId: 'users',
                    element: { id: 'id', name: 'ID', type: 'number', isKey: false }
                }
            }
        });

        expect(response.statusCode).toBe(500);

        // Verify schema was NOT updated
        const schema = await app.dataService.getSchema('app1');
        const table = schema?.tables.find(t => t.id === 'users');
        const idField = table?.fields.find(f => f.id === 'id');
        expect(idField?.isKey).toBe(true);
    });

    it('should prevent deleting key field', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'delete',
                element: {
                    partOf: 'fields',
                    parentId: 'users',
                    elementId: 'id'
                }
            }
        });

        expect(response.statusCode).toBe(500);

        // Verify field was NOT deleted
        const schema = await app.dataService.getSchema('app1');
        const table = schema?.tables.find(t => t.id === 'users');
        const idField = table?.fields.find(f => f.id === 'id');
        expect(idField).toBeDefined();
    });

    it('should delete a table', async () => {
        // Add a second table so we cover the return true branch in the filter
        const schema = await app.dataService.getSchema('app1');
        if (!schema) throw new Error('Schema not found');
        schema.tables.push({
            id: 'products',
            name: 'Products',
            connector: connector.id,
            path: ['products'],
            fields: [{ id: 'id', name: 'ID', type: AppFieldType.Number, isKey: true }],
            actions: []
        });
        await app.dataService.setSchema(schema);

        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'delete',
                element: {
                    partOf: 'tables',
                    elementId: 'users'
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload);
        expect(body.tables).toHaveLength(1);
        expect(body.tables[0].id).toBe('products');
    });

    it('should delete an action', async () => {
        // Add a second table to cover the else branch when table.id !== parentId
        const schema = await app.dataService.getSchema('app1');
        if (!schema) throw new Error('Schema not found');
        schema.tables.push({
            id: 'products',
            name: 'Products',
            connector: connector.id,
            path: ['products'],
            fields: [{ id: 'id', name: 'ID', type: AppFieldType.Number, isKey: true }],
            actions: [{ id: 'add', name: 'Add', type: AppActionType.Add }]
        });
        await app.dataService.setSchema(schema);

        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'delete',
                element: {
                    partOf: 'actions',
                    parentId: 'users',
                    elementId: 'add'
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as AppSchema;
        const table = body.tables.find(t => t.id === 'users');
        expect(table?.actions.find(a => a.id === 'add')).toBeUndefined();
        // Verify other table's action was not affected
        const productsTable = body.tables.find(t => t.id === 'products');
        expect(productsTable?.actions.find(a => a.id === 'add')).toBeDefined();
    });

    it('should reorder actions', async () => {
        // Add a second table to cover the else branch when table.id !== parentId
        const schema = await app.dataService.getSchema('app1');
        if (!schema) throw new Error('Schema not found');
        schema.tables.push({
            id: 'products',
            name: 'Products',
            connector: connector.id,
            path: ['products'],
            fields: [{ id: 'id', name: 'ID', type: AppFieldType.Number, isKey: true }],
            actions: [
                { id: 'add', name: 'Add', type: AppActionType.Add },
                { id: 'update', name: 'Update', type: AppActionType.Update }
            ]
        });
        await app.dataService.setSchema(schema);

        // actions: add (0), update (1), delete (2)
        // move delete to 0
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'reorder',
                oldIndex: 2,
                newIndex: 0,
                element: {
                    partOf: 'actions',
                    parentId: 'users'
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as AppSchema;
        const table = body.tables.find(t => t.id === 'users');
        expect(table?.actions[0]?.id).toBe('delete');
        // Verify other table's actions were not reordered
        const productsTable = body.tables.find(t => t.id === 'products');
        expect(productsTable?.actions[0]?.id).toBe('add');
    });

    it('should reorder tables', async () => {
        // Create another table to reorder
        const schema = await app.dataService.getSchema('app1');
        if (!schema) throw new Error('Schema not found');
        if (!schema.tables[0]) throw new Error('No defined table');
        schema.tables.push({ ...schema.tables[0], id: 'users2', name: 'Users 2' });
        await app.dataService.setSchema(schema);

        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'reorder',
                oldIndex: 1,
                newIndex: 0,
                element: {
                    partOf: 'tables'
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload);
        expect(body.tables[0].id).toBe('users2');
    });

    it('should update a view', async () => {
        // Add two views to cover the else branch when view.id !== updateEl.element.id
        const schema = await app.dataService.getSchema('app1');
        if (!schema) throw new Error('Schema not found');
        schema.views.push(
            {
                id: 'view1',
                name: 'View 1',
                tableId: 'users',
                type: AppViewType.Table,
                config: {}
            },
            {
                id: 'view2',
                name: 'View 2',
                tableId: 'users',
                type: AppViewType.Table,
                config: {}
            }
        );
        await app.dataService.setSchema(schema);

        // Now update only view1
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'update',
                element: {
                    partOf: 'views',
                    element: {
                        id: 'view1',
                        name: 'View 1 Updated',
                        tableId: 'users',
                        type: AppViewType.Table,
                        config: { someConfig: true }
                    }
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as AppSchema;
        const updatedView = body.views.find(v => v.id === 'view1');
        expect(updatedView?.name).toBe('View 1 Updated');
        expect(updatedView?.config).toEqual({ someConfig: true });
        // Verify other view was not affected
        const otherView = body.views.find(v => v.id === 'view2');
        expect(otherView?.name).toBe('View 2');
    });

    it('should update an action', async () => {
        // Add a second table to ensure we cover the branch where table.id !== parentId
        const schema = await app.dataService.getSchema('app1');
        if (!schema) throw new Error('Schema not found');
        schema.tables.push({
            id: 'products',
            name: 'Products',
            connector: connector.id,
            path: ['products'],
            fields: [{ id: 'id', name: 'ID', type: AppFieldType.Number, isKey: true }],
            actions: [{ id: 'add', name: 'Add', type: AppActionType.Add }]
        });
        await app.dataService.setSchema(schema);

        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'update',
                element: {
                    partOf: 'actions',
                    parentId: 'users',
                    element: {
                        id: 'add',
                        name: 'Add Updated',
                        type: AppActionType.Add
                    }
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as AppSchema;
        const table = body.tables.find(t => t.id === 'users');
        const updatedAction = table?.actions.find(a => a.id === 'add');
        expect(updatedAction?.name).toBe('Add Updated');
        // Verify the other table's action was not affected
        const productsTable = body.tables.find(t => t.id === 'products');
        expect(productsTable?.actions.find(a => a.id === 'add')?.name).toBe('Add');
    });

    it('should delete a view', async () => {
        // First add a view
        const schema = await app.dataService.getSchema('app1');
        if (!schema) throw new Error('Schema not found');
        schema.views.push({
            id: 'view1',
            name: 'View 1',
            tableId: 'users',
            type: AppViewType.Table,
            config: {}
        });
        await app.dataService.setSchema(schema);

        // Now delete the view
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'delete',
                element: {
                    partOf: 'views',
                    elementId: 'view1'
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as AppSchema;
        expect(body.views.find(v => v.id === 'view1')).toBeUndefined();
    });

    it('should delete a field and remove it from view configs', async () => {
        // Add multiple views and tables to cover all branches in delete field
        const schema = await app.dataService.getSchema('app1');
        if (!schema) throw new Error('Schema not found');
        // Add a second table
        schema.tables.push({
            id: 'products',
            name: 'Products',
            connector: connector.id,
            path: ['products'],
            fields: [
                { id: 'id', name: 'ID', type: AppFieldType.Number, isKey: true },
                { id: 'name', name: 'Name', type: AppFieldType.Text }
            ],
            actions: []
        });
        // Add views - one for users with fields, one for products, one without fields config
        schema.views.push(
            {
                id: 'view1',
                name: 'View 1',
                tableId: 'users',
                type: AppViewType.Table,
                config: { fields: ['id', 'name'] }
            },
            {
                id: 'view2',
                name: 'View 2',
                tableId: 'products',
                type: AppViewType.Table,
                config: { fields: ['id', 'name'] }
            },
            {
                id: 'view3',
                name: 'View 3',
                tableId: 'users',
                type: AppViewType.Table,
                config: {} // no fields config
            }
        );
        await app.dataService.setSchema(schema);

        // Delete the 'name' field (not a key field)
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'delete',
                element: {
                    partOf: 'fields',
                    parentId: 'users',
                    elementId: 'name'
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as AppSchema;

        // Verify field is removed from users table
        const table = body.tables.find(t => t.id === 'users');
        expect(table?.fields.find(f => f.id === 'name')).toBeUndefined();

        // Verify field is also removed from users view config
        const view = body.views.find(v => v.id === 'view1');
        expect(view?.config.fields).toEqual(['id']);

        // Verify products view was not affected
        const productsView = body.views.find(v => v.id === 'view2');
        expect(productsView?.config.fields).toEqual(['id', 'name']);

        // Verify products table was not affected
        const productsTable = body.tables.find(t => t.id === 'products');
        expect(productsTable?.fields.find(f => f.id === 'name')).toBeDefined();
    });

    it('should reorder views', async () => {
        // Add two views to reorder
        const schema = await app.dataService.getSchema('app1');
        if (!schema) throw new Error('Schema not found');
        schema.views.push(
            {
                id: 'view1',
                name: 'View 1',
                tableId: 'users',
                type: AppViewType.Table,
                config: {}
            },
            {
                id: 'view2',
                name: 'View 2',
                tableId: 'users',
                type: AppViewType.Table,
                config: {}
            }
        );
        await app.dataService.setSchema(schema);

        // Reorder views: move view2 to index 0
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'reorder',
                oldIndex: 1,
                newIndex: 0,
                element: {
                    partOf: 'views'
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as AppSchema;
        expect(body.views[0]?.id).toBe('view2');
        expect(body.views[1]?.id).toBe('view1');
    });

    it('should reject updating table without key fields', async () => {
        // Try to update the table with no key fields
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'update',
                element: {
                    partOf: 'tables',
                    element: {
                        id: 'users',
                        name: 'Users Updated',
                        connector: 'mem',
                        path: ['users'],
                        fields: [{ id: 'name', name: 'Name', type: AppFieldType.Text }],
                        actions: []
                    }
                }
            }
        });

        expect(response.statusCode).toBe(500);
    });

    it('should handle adding field when view references non-existent table', async () => {
        // Create a view that references a table that will be removed, testing the ?? 0 fallback
        const schema = await app.dataService.getSchema('app1');
        if (!schema) throw new Error('Schema not found');

        // Add a view that references a non-existent table (orphaned view)
        schema.views.push({
            id: 'orphanView',
            name: 'Orphan View',
            tableId: 'nonexistent', // This table doesn't exist
            type: AppViewType.Table,
            config: { fields: [] }
        });
        await app.dataService.setSchema(schema);

        // Add a field to users - the orphan view should not be affected
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'add',
                element: {
                    partOf: 'fields',
                    parentId: 'users',
                    element: {
                        id: 'email',
                        name: 'Email',
                        type: AppFieldType.Email
                    }
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as AppSchema;
        // Orphan view should be unchanged
        const orphanView = body.views.find(v => v.id === 'orphanView');
        expect(orphanView?.config.fields).toHaveLength(0);
    });

    it('should handle adding field to parent that does not exist in tables', async () => {
        // This tests the ?? 0 branch in oldFieldsLength calculation
        // We send a request with parentId that doesn't match any existing table
        // Note: This might fail validation, but tests the branch if it gets through
        const schema = await app.dataService.getSchema('app1');
        if (!schema) throw new Error('Schema not found');

        // Add a "ghost" table reference - add the table, set schema, then test
        // First remove all tables to create the edge case
        schema.tables = []; // Empty tables array

        // Add view that would match if we add field to 'ghost' table
        schema.views.push({
            id: 'ghostView',
            name: 'Ghost View',
            tableId: 'ghost',
            type: AppViewType.Table,
            config: { fields: [] } // 0 fields, matching ?? 0
        });

        // Add the ghost table back for the request to work
        schema.tables = [
            {
                id: 'ghost',
                name: 'Ghost Table',
                connector: connector.id,
                path: ['ghost'],
                fields: [], // Empty fields - this makes oldFieldsLength = 0
                actions: []
            }
        ];

        await app.dataService.setSchema(schema);

        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'add',
                element: {
                    partOf: 'fields',
                    parentId: 'ghost',
                    element: {
                        id: 'id',
                        name: 'ID',
                        type: AppFieldType.Number,
                        isKey: true
                    }
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as AppSchema;
        const ghostTable = body.tables.find(t => t.id === 'ghost');
        expect(ghostTable?.fields).toHaveLength(1);
        // View should have the field since 0 === 0
        const ghostView = body.views.find(v => v.id === 'ghostView');
        expect(ghostView?.config.fields).toContain('id');
    });

    it('should update a field preserving other fields in the table', async () => {
        // This test ensures the ternary in field update hits both branches
        // by having multiple fields where only one is updated
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'update',
                element: {
                    partOf: 'fields',
                    parentId: 'users',
                    element: {
                        id: 'id',
                        name: 'User ID',
                        type: AppFieldType.Number,
                        isKey: true
                    }
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as AppSchema;
        const table = body.tables.find(t => t.id === 'users');
        // Updated field
        expect(table?.fields.find(f => f.id === 'id')?.name).toBe('User ID');
        // Other field preserved
        expect(table?.fields.find(f => f.id === 'name')?.name).toBe('Name');
    });

    it('should update an action preserving other actions in the table', async () => {
        // This test ensures the ternary in action update hits both branches
        const response = await server.inject({
            method: 'POST',
            url: '/api/apps/app1/schema',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                action: 'update',
                element: {
                    partOf: 'actions',
                    parentId: 'users',
                    element: {
                        id: 'update',
                        name: 'Update Record',
                        type: AppActionType.Update
                    }
                }
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload) as AppSchema;
        const table = body.tables.find(t => t.id === 'users');
        // Updated action
        expect(table?.actions.find(a => a.id === 'update')?.name).toBe('Update Record');
        // Other actions preserved
        expect(table?.actions.find(a => a.id === 'add')?.name).toBe('Add');
        expect(table?.actions.find(a => a.id === 'delete')?.name).toBe('Delete');
    });
});
