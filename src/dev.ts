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
            memory: new MemoryConnector('Memory', 'memory'),
            file: new FileConnector('File System', dbPath, 'file')
        },
        encryptionKey:
            process.env.ENCRYPTION_KEY ||
            '1234567890123456789012345678901234567890123456789012345678901234'
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
