import fastifyCors, { type FastifyCorsOptions } from '@fastify/cors';
import fastifyHelmet, { type FastifyHelmetOptions } from '@fastify/helmet';
import fastifyRateLimit, { type FastifyRateLimitOptions } from '@fastify/rate-limit';
import fastifyJwt, { type FastifyJWTOptions } from '@fastify/jwt';
import fastifyHealthcheck, { type FastifyHealthcheckOptions } from 'fastify-healthcheck';
import fastify, {
    type FastifyInstance,
    type FastifyListenOptions,
    type FastifyServerOptions,
    type FastifyRequest,
    type FastifyReply
} from 'fastify';

export interface SchemaFXOptions {
    /** Optional configuration for Fastify instance. */
    fastifyOptions?: FastifyServerOptions & {
        /** Optional configuration for Fastify helmet. */
        helmetOptions?: FastifyHelmetOptions;

        /** Optional configuration for Fastify CORS. */
        corsOptions?: FastifyCorsOptions;

        /** Optional configuration for Fastify Rate Limit. */
        rateLimitOptions?: FastifyRateLimitOptions;

        /** Optional configuration for Fastify JWT. */
        jwtOptions?: FastifyJWTOptions;

        /** Optional configuration for Fastify Healthcheck. */
        healthcheckOptions?: FastifyHealthcheckOptions;
    };

    /** Security secret. */
    secret: string;
}

export class SchemaFX {
    /** Underlying Fastify instance. */
    fastifyInstance: FastifyInstance;

    /**
     * Create a SchemaFX instance.
     * @param opts Instance configuration.
     */
    constructor(opts: SchemaFXOptions) {
        this.fastifyInstance = fastify(opts?.fastifyOptions);
        this.fastifyInstance.register(fastifyHelmet, opts.fastifyOptions?.helmetOptions ?? {});

        this.fastifyInstance.register(
            fastifyCors,
            opts.fastifyOptions?.corsOptions ?? {
                origin: true,
                methods: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'],
                allowedHeaders: ['Content-Type', 'Authorization'],
                credentials: true
            }
        );

        this.fastifyInstance.register(
            fastifyRateLimit,
            opts.fastifyOptions?.rateLimitOptions ?? {
                max: 100,
                timeWindow: '1 minute',
                allowList: ['127.0.0.1'], // Internal bypass
                errorResponseBuilder: (req, context) => ({
                    code: 429,
                    error: 'Too Many Requests',
                    message: `Rate limit exceeded. Retry in ${Math.round(context.ttl / 1000)}s`
                })
            }
        );

        this.fastifyInstance.register(
            fastifyJwt,
            opts.fastifyOptions?.jwtOptions ?? {
                secret: opts.secret,
                verify: {
                    extractToken: r => r.headers.authorization?.replace('Bearer ', '')
                }
            }
        );

        this.fastifyInstance.decorate(
            'authenticate',
            async (request: FastifyRequest, reply: FastifyReply) => {
                try {
                    await request.jwtVerify();
                } catch (err) {
                    this.fastifyInstance.log.error(err);
                    reply.code(401).send({ error: 'Unauthorized.' });
                }
            }
        );

        this.fastifyInstance.register(
            fastifyHealthcheck,
            opts.fastifyOptions?.healthcheckOptions ?? {}
        );

        this.fastifyInstance.setErrorHandler((error, request, reply) => {
            this.fastifyInstance.log.error(error);

            reply.status(500).send({
                code: 'Internal Server Error',
                message: 'Unexpected error occurred'
            });
        });
    }

    /**
     * Start the Fastify server listening.
     * @param opts Fastify listen options.
     */
    listen(opts: FastifyListenOptions) {
        return this.fastifyInstance.listen(opts);
    }
}
