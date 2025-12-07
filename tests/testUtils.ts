import SchemaFX, {
    AppActionType,
    AppFieldType,
    type AppSchema,
    MemoryConnector
} from '../src/index.js';

export async function createTestApp(includeToken?: boolean) {
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
                        isRequired: true,
                        isKey: true
                    },
                    {
                        id: 'name',
                        name: 'Name',
                        type: AppFieldType.Text,
                        isRequired: true,
                        isKey: false
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

    await connector.saveSchema!('app1', schema);
    await connector.addRow!(schema.tables[0], { id: 1, name: 'User 1' });

    const app = new SchemaFX({
        jwtOpts: {
            secret: 'test-secret'
        },
        connectorOpts: {
            schemaConnector: 'mem',
            connectors: {
                [connector.id]: connector
            }
        }
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
