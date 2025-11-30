import fastify from 'fastify';
import {
    serializerCompiler,
    validatorCompiler,
    type ZodTypeProvider
} from 'fastify-type-provider-zod';

import 'dotenv/config';

import fastifyCors from '@fastify/cors';
import connectorHandler from './connectors/connectorHandler.js';
import MemoryConnector from './connectors/memoryConnector.js';

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

app.register(connectorHandler, {
    prefix: '/api',
    schemaConnector: 'memory',
    connectors: { memory: new MemoryConnector() }
});

// Start
try {
    const port = process.env.PORT ? parseInt(process.env.PORT) : 3000;

    await app.listen({ port, host: '0.0.0.0' });
    console.log(`\n🚀 Server listening at http://localhost:${port}/`);
} catch (err) {
    app.log.error(err);
    process.exit(1);
}
