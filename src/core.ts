import fastify, {
    type FastifyListenOptions,
    type FastifyBaseLogger,
    type FastifyInstance,
    type FastifyServerOptions,
    type RawServerDefault
} from 'fastify';

import {
    serializerCompiler,
    validatorCompiler,
    type ZodTypeProvider
} from 'fastify-type-provider-zod';

import fastifyCors, { type FastifyCorsOptions } from '@fastify/cors';
import fastifyRateLimit, { type FastifyRateLimitOptions } from '@fastify/rate-limit';
import fastifyHealthcheck, { type FastifyHealthcheckOptions } from 'fastify-healthcheck';
import fastifyHelmet, { type FastifyHelmetOptions } from '@fastify/helmet';
import fastifyJwt, { type FastifyJWTOptions } from '@fastify/jwt';
import fastifyCompress, { type FastifyCompressOptions } from '@fastify/compress';
import type { IncomingMessage, ServerResponse } from 'node:http';

import connectorHandler, { type SchemaFXConnectorsOptions } from './plugins/index.js';
import { ZodError } from 'zod';

export type SchemaFXOptions = {
    fastifyOpts?: FastifyServerOptions;
    helmetOpts?: FastifyHelmetOptions;
    corsOpts?: FastifyCorsOptions;
    rateLimitOpts?: FastifyRateLimitOptions;
    healthcheckOpts?: FastifyHealthcheckOptions;
    compressOpts?: FastifyCompressOptions;
    jwtOpts: FastifyJWTOptions;
    connectorOpts: SchemaFXConnectorsOptions;
};

export default class SchemaFX {
    fastifyInstance: FastifyInstance<
        RawServerDefault,
        IncomingMessage,
        ServerResponse<IncomingMessage>,
        FastifyBaseLogger,
        ZodTypeProvider
    >;

    constructor(opts: SchemaFXOptions) {
        this.fastifyInstance = fastify(opts.fastifyOpts).withTypeProvider<ZodTypeProvider>();

        // Zod Type Provider
        this.fastifyInstance.setValidatorCompiler(validatorCompiler);
        this.fastifyInstance.setSerializerCompiler(serializerCompiler);

        this.fastifyInstance.register(fastifyHelmet, opts.helmetOpts ?? {});
        this.fastifyInstance.register(fastifyCompress, opts.compressOpts ?? { global: true });

        this.fastifyInstance.register(fastifyCors, {
            origin: true,
            methods: ['GET', 'POST', 'OPTIONS'],
            allowedHeaders: ['Content-Type', 'Authorization'],
            ...(opts.corsOpts ?? {})
        });

        this.fastifyInstance.register(fastifyRateLimit, {
            max: 100,
            timeWindow: '1 minute',
            allowList: ['127.0.0.1'], // Internal bypass
            errorResponseBuilder: (_, context) => ({
                code: 429,
                error: 'Too Many Requests',
                message: `Rate limit exceeded. Retry in ${Math.round(context.ttl / 1000)}s`
            }),
            ...(opts.rateLimitOpts ?? {})
        });

        this.fastifyInstance.register(fastifyJwt, opts.jwtOpts);
        this.fastifyInstance.decorate('authenticate', async request => {
            await request.jwtVerify().catch(() => undefined);
        });

        this.fastifyInstance.register(fastifyHealthcheck, opts.healthcheckOpts ?? {});

        this.fastifyInstance.setErrorHandler((error, _, reply) => {
            if ((error as FastifyError).validation) {
                return reply.status(400).send({
                    error: 'Bad Request',
                    message: (error as FastifyError).message,
                    details: (error as FastifyError).validation
                });
            }

            if (error instanceof ZodError) {
                return reply.status(400).send({
                    error: 'Bad Request',
                    message: error.message,
                    details: error.issues.map(issue => ({
                        field: issue.path.join('.'),
                        message: issue.message,
                        code: issue.code
                    }))
                });
            }

            if ((error as FastifyError).code === 'FST_ERR_VALIDATION') {
                return reply.status(400).send({
                    error: 'Bad Request',
                    message: (error as FastifyError).message
                });
            }

            if (error instanceof SyntaxError) {
                return reply.status(400).send({
                    error: 'Bad Request',
                    message: error.message
                });
            }

            reply.log.error(error);
            return reply.status(500).send({
                code: 'Internal Server Error',
                message: 'Unexpected error occurred.'
            });
        });

        this.fastifyInstance.register(connectorHandler, {
            prefix: '/api',
            ...opts.connectorOpts
        });
    }

    get log() {
        return this.fastifyInstance.log;
    }

    listen(opts?: FastifyListenOptions, callback?: (err: Error | null, address: string) => void) {
        opts = { host: '0.0.0.0', ...(opts ?? {}) };

        if (callback) return this.fastifyInstance.listen(opts, callback);
        return this.fastifyInstance.listen(opts);
    }
}
