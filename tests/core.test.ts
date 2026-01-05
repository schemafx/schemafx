import { describe, it, expect } from 'vitest';
import SchemaFX from '../src/index.js';
import MemoryConnector from '../src/connectors/memoryConnector.js';
import { createTestApp } from './testUtils.js';

declare module 'fastify' {
    interface FastifyInstance {
        authenticate: (request: FastifyRequest) => Promise<void>;
    }
}

declare module '@fastify/jwt' {
    interface FastifyJWT {
        payload: { email: string };
        user: { email: string };
    }
}

interface CustomError extends Error {
    code?: string;
    validation?: { field: string; message: string }[];
}

describe('Core SchemaFX', () => {
    it('should initialize with default options', async () => {
        const app = new SchemaFX({
            jwtOpts: { secret: 'secret' },
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
                connectors: [new MemoryConnector({ name: 'Mem', id: 'mem' })]
            }
        });

        expect(app).toBeDefined();
        await app.fastifyInstance.close();
    });

    it('should initialize with custom options', async () => {
        const app = new SchemaFX({
            jwtOpts: { secret: 'secret' },
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
                connectors: [new MemoryConnector({ name: 'Mem', id: 'mem' })]
            },
            corsOpts: { origin: false },
            rateLimitOpts: { max: 50 },
            helmetOpts: { global: false },
            compressOpts: { global: false },
            healthcheckOpts: { healthcheckUrl: '/health' }
        });

        expect(app).toBeDefined();
        await app.fastifyInstance.ready();

        const res = await app.fastifyInstance.inject({
            method: 'GET',
            url: '/health'
        });

        expect(res.statusCode).toBe(200);
        await app.fastifyInstance.close();
    });

    it('should enforce rate limiting', async () => {
        const app = new SchemaFX({
            jwtOpts: { secret: 'secret' },
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
                connectors: [new MemoryConnector({ name: 'Mem', id: 'mem' })]
            },
            rateLimitOpts: { max: 2, timeWindow: '10 minutes' }
        });

        const server = app.fastifyInstance;
        server.after(() => server.get('/rate-limited', () => ({ message: 'ok' })));
        await server.ready();

        // First request - should succeed
        const res1 = await server.inject({
            method: 'GET',
            url: '/rate-limited'
        });
        expect(res1.statusCode).toBe(200);

        // Second request - should succeed
        const res2 = await server.inject({
            method: 'GET',
            url: '/rate-limited'
        });

        expect(res2.statusCode).toBe(200);

        // Third request - should be rate limited
        const res3 = await server.inject({
            method: 'GET',
            url: '/rate-limited'
        });
        expect(res3.statusCode).toBe(429);
        const body = JSON.parse(res3.payload);
        expect(body.error).toBe('Too Many Requests');
        expect(body.message).toMatch(/Rate limit exceeded/);

        await server.close();
    });

    it('should offer OpenAPI documentation', async () => {
        const app = new SchemaFX({
            jwtOpts: { secret: 'secret' },
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
                connectors: [new MemoryConnector({ name: 'Mem', id: 'mem' })]
            },
            corsOpts: { origin: false },
            rateLimitOpts: { max: 50 },
            helmetOpts: { global: false },
            compressOpts: { global: false },
            healthcheckOpts: { healthcheckUrl: '/health' }
        });

        expect(app).toBeDefined();
        await app.fastifyInstance.ready();

        const resJSON = await app.fastifyInstance.inject({
            method: 'GET',
            url: '/api/openapi.json'
        });

        expect(resJSON.statusCode).toBe(200);
        expect(resJSON.headers['content-type']).toBeDefined();
        expect(resJSON.headers['content-type']?.startsWith('application/json')).toBe(true);

        const resYAML = await app.fastifyInstance.inject({
            method: 'GET',
            url: '/api/openapi.yaml'
        });

        expect(resYAML.statusCode).toBe(200);
        expect(resYAML.headers['content-type']).toBeDefined();
        expect(resYAML.headers['content-type']?.startsWith('text/plain')).toBe(true);

        await app.fastifyInstance.close();
    });

    it('should access the logger via the getter', () => {
        const app = new SchemaFX({
            jwtOpts: { secret: 'secret' },
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
                connectors: [new MemoryConnector({ name: 'Mem', id: 'mem' })]
            }
        });

        expect(app.log).toBeDefined();
        expect(app.log).toBe(app.fastifyInstance.log);

        expect(typeof app.log.info).toBe('function');
        expect(typeof app.log.error).toBe('function');
        expect(typeof app.log.debug).toBe('function');
    });

    it('should handle listen', async () => {
        const app = new SchemaFX({
            jwtOpts: { secret: 'secret' },
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
                connectors: [new MemoryConnector({ name: 'Mem', id: 'mem' })]
            }
        });

        const server = await app.listen({ port: 0 });
        expect(server).toBeDefined();
        await app.fastifyInstance.close();
    });

    it('should handle listen with no options', async () => {
        const app = new SchemaFX({
            jwtOpts: { secret: 'secret' },
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
                connectors: [new MemoryConnector({ name: 'Mem', id: 'mem' })]
            }
        });

        const server = await app.listen();
        expect(server).toBeDefined();
        await app.fastifyInstance.close();
    });

    it('should handle listen with callback', async () => {
        const app = new SchemaFX({
            jwtOpts: { secret: 'secret' },
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
                connectors: [new MemoryConnector({ name: 'Mem', id: 'mem' })]
            }
        });

        const address = await app.listen({ port: 0 });
        expect(address).toBeDefined();

        await app.fastifyInstance.close();
    });

    it('should handle SyntaxError (manually thrown)', async () => {
        const app = new SchemaFX({
            jwtOpts: { secret: 'secret' },
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
                connectors: [new MemoryConnector({ name: 'Mem', id: 'mem' })]
            }
        });

        const server = app.fastifyInstance;

        server.get('/syntax-error', async () => {
            throw new SyntaxError('Manual syntax error');
        });

        await server.ready();

        const response = await server.inject({
            method: 'GET',
            url: '/syntax-error'
        });

        expect(response.statusCode).toBe(400);
        const body = JSON.parse(response.payload);
        expect(body.error).toBe('Bad Request');
        expect(body.message).toBe('Manual syntax error');

        await server.close();
    });

    it('should handle unexpected errors', async () => {
        const app = new SchemaFX({
            jwtOpts: { secret: 'secret' },
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
                connectors: [new MemoryConnector({ name: 'Mem', id: 'mem' })]
            }
        });

        const server = app.fastifyInstance;

        server.get('/error', async () => {
            throw new Error('Unexpected Boom');
        });

        await server.ready();

        const response = await server.inject({
            method: 'GET',
            url: '/error'
        });

        expect(response.statusCode).toBe(500);
        const body = JSON.parse(response.payload);
        expect(body.error).toBe('Internal Server Error');
        expect(body.message).toBe('Unexpected error occurred.');

        await server.close();
    });

    it('should catch JWT verification errors', async () => {
        const { app } = await createTestApp(false);
        const server = app.fastifyInstance;

        // Add a route that uses the 'authenticate' decorator
        server.get('/protected', { onRequest: [server.authenticate] }, async () => {
            return { message: 'ok' };
        });

        await server.ready();

        const response = await server.inject({
            method: 'GET',
            url: '/protected',
            headers: {
                Authorization: 'Bearer invalid.token.here'
            }
        });

        expect(response.statusCode).toBe(200);

        await server.close();
    });

    it('should handle Fastify validation error with .validation property', async () => {
        const { app } = await createTestApp(false);
        const server = app.fastifyInstance;

        server.get('/fastify-validation-error', async () => {
            const error: CustomError = new Error('Fastify Validation Error');
            error.validation = [{ field: 'test', message: 'invalid' }];
            throw error;
        });

        await server.ready();

        const response = await server.inject({
            method: 'GET',
            url: '/fastify-validation-error'
        });

        expect(response.statusCode).toBe(400);
        const body = JSON.parse(response.payload);
        expect(body.error).toBe('Bad Request');
        expect(body.details).toBeDefined();

        await server.close();
    });

    it('should handle FST_ERR_VALIDATION code manually', async () => {
        const { app } = await createTestApp(false);
        const server = app.fastifyInstance;

        server.get('/fst-validation-error', async () => {
            const error: CustomError = new Error('Manual Validation Error');
            error.code = 'FST_ERR_VALIDATION';
            throw error;
        });

        await server.ready();

        const response = await server.inject({
            method: 'GET',
            url: '/fst-validation-error'
        });

        expect(response.statusCode).toBe(400);
        const body = JSON.parse(response.payload);
        expect(body.error).toBe('Bad Request');
        expect(body.message).toBe('Manual Validation Error');

        await server.close();
    });

    it('should successfully verify a valid JWT', async () => {
        const app = new SchemaFX({
            jwtOpts: { secret: 'secret' },
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
                connectors: [new MemoryConnector({ name: 'Mem', id: 'mem' })]
            }
        });
        const server = app.fastifyInstance;

        // Add a route that uses the 'authenticate' decorator
        server.get('/protected-valid', { onRequest: [server.authenticate] }, async req => {
            // If jwtVerify passes, req.user should be populated
            return { user: req.user };
        });

        await server.ready();

        const token = app.fastifyInstance.jwt.sign({ email: 'dev@schemafx.com' });

        const response = await server.inject({
            method: 'GET',
            url: '/protected-valid',
            headers: {
                Authorization: `Bearer ${token}`
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload);
        expect(body.user).toBeDefined();

        await server.close();
    });
});
