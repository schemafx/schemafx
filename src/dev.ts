import SchemaFX, { MemoryConnector } from './index.js';

const app = new SchemaFX({
    connectorOpts: {
        schemaConnector: 'memory',
        connectors: { memory: new MemoryConnector() }
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
