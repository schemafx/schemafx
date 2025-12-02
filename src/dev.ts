import SchemaFX, { MemoryConnector, FileConnector } from './index.js';
import path from 'path';

const dbPath = path.join(process.cwd(), 'database.json');

const app = new SchemaFX({
    jwtOpts: {
        secret: 'my-very-secret'
    },
    connectorOpts: {
        schemaConnector: 'file',
        connectors: {
            memory: new MemoryConnector(),
            file: new FileConnector(dbPath)
        }
    }
});

try {
    const port = 3000;
    await app.listen({ port, host: '0.0.0.0' });
    console.log(`\n🚀 Server listening at http://localhost:${port}/`);
} catch (err) {
    app.log.error(err);
    process.exit(1);
}
