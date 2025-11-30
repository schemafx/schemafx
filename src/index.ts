import fastify from 'fastify';
import {
    serializerCompiler,
    validatorCompiler,
    type ZodTypeProvider
} from 'fastify-type-provider-zod';

import 'dotenv/config';

import fastifyCors from '@fastify/cors';
import fastifyRateLimit from '@fastify/rate-limit';
import fastifyHealthcheck from 'fastify-healthcheck';
import fastifyHelmet from '@fastify/helmet';

import connectorHandler from './connectors/connectorHandler.js';
import MemoryConnector from './connectors/memoryConnector.js';

const app = fastify({
    logger: true
}).withTypeProvider<ZodTypeProvider>();

// Zod Type Provider
app.setValidatorCompiler(validatorCompiler);
app.setSerializerCompiler(serializerCompiler);

app.register(fastifyHelmet);

app.register(fastifyCors, {
    origin: true,
    methods: ['GET', 'POST', 'OPTIONS'],
    allowedHeaders: ['Content-Type']
});

app.register(fastifyRateLimit, {
    max: 100,
    timeWindow: '1 minute',
    allowList: ['127.0.0.1'], // Internal bypass
    errorResponseBuilder: (_, context) => ({
        code: 429,
        error: 'Too Many Requests',
        message: `Rate limit exceeded. Retry in ${Math.round(context.ttl / 1000)}s`
    })
});

app.register(fastifyHealthcheck);

app.setErrorHandler((error, _, reply) => {
    reply.log.error(error);
    return reply.status(500).send({
        code: 'Internal Server Error',
        message: 'Unexpected error occurred.'
    });
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
