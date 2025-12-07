import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createTestApp } from '../testUtils.js';
import type SchemaFX from '../../src/index.js';
import type { FastifyInstance } from 'fastify';

describe('Auth API', () => {
    let app: SchemaFX;
    let server: FastifyInstance;

    beforeEach(async () => {
        const testApp = await createTestApp();
        app = testApp.app;
        server = app.fastifyInstance;

        await server.ready();
    });

    afterEach(async () => {
        await server.close();
    });

    it('should login successfully', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/login',
            payload: {
                username: 'test',
                password: 'test'
            }
        });

        expect(response.statusCode).toBe(200);
        const body = JSON.parse(response.payload);
        expect(body.token).toBeDefined();
    });

    it('should fail with invalid credentials', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/login',
            payload: {
                username: 'wrong',
                password: 'wrong'
            }
        });

        expect(response.statusCode).toBe(401);
    });

    it('should fail with missing fields', async () => {
        const response = await server.inject({
            method: 'POST',
            url: '/api/login',
            payload: {
                username: 'test'
            }
        });

        expect(response.statusCode).toBe(400);
    });
});
