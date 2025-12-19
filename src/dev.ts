import SchemaFX, {
    MemoryConnector,
    FileConnector,
    AppViewType,
    AppActionType,
    AppFieldType
} from './index.js';
import path from 'path';

const dbPath = path.join(process.cwd(), 'database.json');

const memoryConnector = new MemoryConnector('Memory', 'memory');
const fileConnector = new FileConnector('File System', dbPath, 'file');
const app = new SchemaFX({
    jwtOpts: {
        secret: 'my-very-secret'
    },
    dataServiceOpts: {
        schemaConnector: {
            connector: fileConnector.id,
            path: ['schemas']
        },
        connectionsConnector: {
            connector: fileConnector.id,
            path: ['connections']
        },
        connectors: [memoryConnector, fileConnector],
        encryptionKey:
            process.env.ENCRYPTION_KEY ||
            '1234567890123456789012345678901234567890123456789012345678901234'
    }
});

const devAppId = '123';
if (!(await app.dataService.getSchema(devAppId))) {
    await app.dataService.setSchema({
        id: devAppId,
        name: 'Demo CRM',
        tables: [
            {
                id: 'customers',
                name: 'Customers',
                path: ['customers'],
                connector: 'memory',
                fields: [
                    {
                        id: '_id',
                        name: 'ID',
                        type: AppFieldType.Text,
                        isKey: true
                    },
                    {
                        id: 'name',
                        name: 'Name',
                        type: AppFieldType.Text
                    },
                    {
                        id: 'email',
                        name: 'Email',
                        type: AppFieldType.Email
                    }
                ],
                actions: [
                    { id: 'add', name: 'Add Customer', type: AppActionType.Add },
                    { id: 'update', name: 'Update Customer', type: AppActionType.Update },
                    { id: 'delete', name: 'Delete Customer', type: AppActionType.Delete }
                ]
            },
            {
                id: 'orders',
                name: 'Orders',
                path: ['orders'],
                connector: 'memory',
                fields: [
                    {
                        id: '_id',
                        name: 'ID',
                        type: AppFieldType.Text,
                        isKey: true
                    },
                    {
                        id: 'product',
                        name: 'Product',
                        type: AppFieldType.Text
                    },
                    {
                        id: 'price',
                        name: 'Price',
                        type: AppFieldType.Number
                    },
                    {
                        id: 'customer',
                        name: 'Customer',
                        type: AppFieldType.Reference,
                        referenceTo: 'customers'
                    }
                ],
                actions: [
                    { id: 'add', name: 'Add Order', type: AppActionType.Add },
                    { id: 'update', name: 'Update Order', type: AppActionType.Update },
                    { id: 'delete', name: 'Delete Order', type: AppActionType.Delete }
                ]
            }
        ],
        views: [
            {
                id: 'customers_table',
                name: 'Customers Table',
                tableId: 'customers',
                type: AppViewType.Table,
                config: {
                    fields: ['name', 'email']
                }
            },
            {
                id: 'customers_form',
                name: 'New Customer Form',
                tableId: 'customers',
                type: AppViewType.Form,
                config: {
                    fields: ['name', 'email']
                }
            },
            {
                id: 'orders_table',
                name: 'Orders Table',
                tableId: 'orders',
                type: AppViewType.Table,
                config: {
                    fields: ['product', 'price', 'customer']
                }
            },
            {
                id: 'orders_form',
                name: 'New Order Form',
                tableId: 'orders',
                type: AppViewType.Form,
                config: {
                    fields: ['product', 'price', 'customer']
                }
            }
        ]
    });
}

try {
    const port = 3000;
    await app.listen({ port, host: '0.0.0.0' });
    console.log(`\n🚀 Server listening at http://localhost:${port}/`);
} catch (err) {
    app.log.error(err);
    process.exit(1);
}
