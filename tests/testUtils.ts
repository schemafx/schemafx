import SchemaFX, {
    AppActionType,
    AppFieldType,
    type AppSchema,
    MemoryConnector,
    PermissionTargetType,
    PermissionLevel,
    type AppTable
} from '../src/index.js';

export const TEST_USER_EMAIL = 'test@example.com';

export async function createTestApp(includeToken?: boolean, opts?: { encryptionKey?: string }) {
    const connector = new MemoryConnector({ name: 'Memory', id: 'mem' });
    const schema = {
        id: 'app1',
        name: 'App 1',
        tables: [
            {
                id: 'users',
                name: 'Users',
                connector: connector.id,
                path: ['users'],
                fields: [
                    {
                        id: 'id',
                        name: 'ID',
                        type: AppFieldType.Number,
                        isKey: true
                    },
                    {
                        id: 'name',
                        name: 'Name',
                        type: AppFieldType.Text
                    }
                ],
                actions: [
                    { id: 'add', name: 'Add', type: AppActionType.Add },
                    { id: 'update', name: 'Update', type: AppActionType.Update },
                    { id: 'delete', name: 'Delete', type: AppActionType.Delete }
                ]
            }
        ],
        views: []
    } as AppSchema;

    const app = new SchemaFX({
        jwtOpts: {
            secret: 'test-secret'
        },
        dataServiceOpts: {
            schemaConnector: {
                connector: 'mem',
                path: ['schemas']
            },
            connectionsConnector: {
                connector: 'mem',
                path: ['connections']
            },
            permissionsConnector: {
                connector: 'mem',
                path: ['permissions']
            },
            connectors: [connector],
            encryptionKey: opts?.encryptionKey
        }
    });

    await app.dataService.setSchema(schema);
    await app.dataService.executeAction({
        table: schema.tables[0] as AppTable,
        actId: 'add',
        rows: [{ id: 1, name: 'User 1' }]
    });

    // Grant the test user admin permission on the test app
    await app.dataService.setPermission({
        id: 'test-permission',
        targetType: PermissionTargetType.App,
        targetId: schema.id,
        email: TEST_USER_EMAIL,
        level: PermissionLevel.Admin
    });

    // Create a signed JWT token directly for the test user
    // Only call ready() when we need to sign tokens - otherwise tests can add routes first
    let token: string | undefined;
    if (includeToken) {
        await app.fastifyInstance.ready();
        token = app.fastifyInstance.jwt.sign({ email: TEST_USER_EMAIL }, { expiresIn: '1h' });
    }

    return {
        app,
        connector,
        token
    };
}
