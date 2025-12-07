import { describe, it, expect } from 'vitest';
import SchemaFX from '../src/index.js';
import MemoryConnector from '../src/connectors/memoryConnector.js';

describe('Core SchemaFX', () => {
    it('should initialize with default options', async () => {
        const app = new SchemaFX({
            jwtOpts: { secret: 'secret' },
            connectorOpts: {
                schemaConnector: 'mem',
                connectors: { mem: new MemoryConnector('Mem', 'mem') }
            }
        });

        expect(app).toBeDefined();
        await app.fastifyInstance.close();
    });

    it('should initialize with custom options', async () => {
        const app = new SchemaFX({
            jwtOpts: { secret: 'secret' },
            connectorOpts: {
                schemaConnector: 'mem',
                connectors: { mem: new MemoryConnector('Mem', 'mem') }
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

    it('should access the logger via the getter', () => {
        const app = new SchemaFX({
            jwtOpts: { secret: 'secret' },
            connectorOpts: {
                schemaConnector: 'mem',
                connectors: { mem: new MemoryConnector('Mem', 'mem') }
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
            connectorOpts: {
                schemaConnector: 'mem',
                connectors: { mem: new MemoryConnector('Mem', 'mem') }
            }
        });

        const server = await app.listen({ port: 0 });
        expect(server).toBeDefined();
        await app.fastifyInstance.close();
    });

    it('should handle listen with callback', async () => {
        const app = new SchemaFX({
            jwtOpts: { secret: 'secret' },
            connectorOpts: {
                schemaConnector: 'mem',
                connectors: { mem: new MemoryConnector('Mem', 'mem') }
            }
        });

        await new Promise<void>((resolve, reject) => {
            app.listen({ port: 0 }, (err, address) => {
                if (err) reject(err);
                expect(address).toBeDefined();
                resolve();
            });
        });

        await app.fastifyInstance.close();
    });
});
