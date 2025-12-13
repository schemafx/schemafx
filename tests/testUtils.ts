import SchemaFX, {
    AppActionType,
    AppFieldType,
    type AppSchema,
    MemoryConnector
} from '../src/index.js';

export async function createTestApp(includeToken?: boolean, opts?: { encryptionKey?: string }) {
    const connector = new MemoryConnector('Memory', 'mem');
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
            connectors: [connector],
            encryptionKey: opts?.encryptionKey
        }
    });

    await app.dataService.setSchema(schema);
    await app.dataService.executeAction({
        appId: schema.id,
        table: schema.tables[0],
        actId: 'add',
        rows: [{ id: 1, name: 'User 1' }]
    });

    return {
        app,
        connector,
        token: includeToken
            ? JSON.parse(
                  (
                      await app.fastifyInstance.inject({
                          method: 'POST',
                          url: '/api/login',
                          payload: {
                              username: 'test',
                              password: 'test'
                          }
                      })
                  ).payload
              ).token
            : undefined
    };
}
