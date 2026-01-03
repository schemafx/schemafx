import SchemaFX, {
    MemoryConnector,
    FileConnector,
    AppViewType,
    AppActionType,
    AppFieldType
} from '../src/index.js';
import path from 'path';
import AuthConnector from './connectors/authConnector.js';

const port = 3000;
const filePath = path.join(process.cwd(), 'dev/database.json');

const memoryConnector = new MemoryConnector({ name: 'Memory', id: 'memory' });
const fileConnector = new FileConnector({ name: 'File System', id: 'file', filePath });
const defaultConnector = fileConnector.id;
const app = new SchemaFX({
    jwtOpts: {
        secret: 'my-very-secret'
    },
    fastifyOpts: {
        logger: { level: 'error' }
    },
    dataServiceOpts: {
        schemaConnector: {
            connector: defaultConnector,
            path: ['schemas']
        },
        connectionsConnector: {
            connector: defaultConnector,
            path: ['connections']
        },
        connectors: [
            memoryConnector,
            fileConnector,
            new AuthConnector({ name: 'Dev', serverUri: `http://localhost:${port}/` })
        ],
        encryptionKey:
            process.env.ENCRYPTION_KEY ||
            '1234567890123456789012345678901234567890123456789012345678901234'
    }
});

// Default dev application.
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
                connector: defaultConnector,
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
                connector: defaultConnector,
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
    await app.listen({ port, host: '0.0.0.0' });
    console.log(`\n🚀 Server listening at http://localhost:${port}/`);
} catch (err) {
    app.log.error(err);
    process.exit(1);
}
