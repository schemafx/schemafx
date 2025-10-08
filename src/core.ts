import fastifyCors, { type FastifyCorsOptions } from '@fastify/cors';
import fastifyHelmet, { type FastifyHelmetOptions } from '@fastify/helmet';
import fastifyRateLimit, { type FastifyRateLimitOptions } from '@fastify/rate-limit';
import fastifyJwt, { type FastifyJWTOptions } from '@fastify/jwt';
import fastifyHealthcheck, { type FastifyHealthcheckOptions } from 'fastify-healthcheck';
import {
    serializerCompiler,
    validatorCompiler,
    type ZodTypeProvider
} from 'fastify-type-provider-zod';
import fastify, {
    type FastifyInstance,
    type FastifyListenOptions,
    type FastifyServerOptions,
    type FastifyRequest,
    type FastifyReply,
    type RawServerDefault,
    type FastifyBaseLogger
} from 'fastify';
import { Connector } from './connector';
import z from 'zod';
import type { IncomingMessage, ServerResponse } from 'node:http';
import {
    ComponentSchema,
    ConnectionSchema,
    EntitySchema,
    RoleSchema,
    type TableDefinition,
    TableDefinitionSchema
} from './schemas';
import { zodToTableColumns } from './utils/zodToTableColumns';

interface SchemaFXDBOption {
    connector: string;
    connectionPath: string[];
    connectionPayload?: Record<string, unknown>;
}

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

    /** Configuration for system tables. */
    dbOptions: {
        /** Configuration for the tables DB. */
        tables: SchemaFXDBOption;

        /** Configuration for the entities DB. */
        entities: SchemaFXDBOption;

        /** Configuration for the components DB. */
        components: SchemaFXDBOption;

        /** Configuration for the connections DB. */
        connections: SchemaFXDBOption;

        /** Configuration for the roles DB. */
        roles: SchemaFXDBOption;
    };

    /** Connectors to include. */
    connectors: Connector[];

    /** Security secret. */
    secret: string;
}

export class SchemaFX {
    [key: string]: unknown;

    /** Underlying Fastify instance. */
    fastifyInstance: FastifyInstance<
        RawServerDefault,
        IncomingMessage,
        ServerResponse<IncomingMessage>,
        FastifyBaseLogger,
        ZodTypeProvider
    >;

    /** Table definition for tables. */
    private tablesTable!: TableDefinition;

    /** Table definition for entities. */
    private entitiesTable!: TableDefinition;

    /** Table definition for components. */
    private componentsTable!: TableDefinition;

    /** Table definition for connections. */
    private connectionsTable!: TableDefinition;

    /** Table definition for roles. */
    private rolesTable!: TableDefinition;

    /** Connection payload for system tables */
    private tablePayload: Map<string, Record<string, unknown>>;

    /** Connector */
    private connectors: Map<string, Connector>;

    /**
     * Create a SchemaFX instance.
     * @param opts Instance configuration.
     */
    constructor(opts: SchemaFXOptions) {
        this.fastifyInstance = fastify(opts?.fastifyOptions).withTypeProvider<ZodTypeProvider>();
        this.fastifyInstance.setValidatorCompiler(validatorCompiler);
        this.fastifyInstance.setSerializerCompiler(serializerCompiler);
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
                message: 'Unexpected error occurred.'
            });
        });

        this.connectors = new Map();
        this.addConnectors(opts.connectors);

        this.fastifyInstance.get(
            '/auth/:connectorName/callback',
            {
                schema: {
                    params: z.object({
                        connectorName: z
                            .string()
                            .describe('The name of the OAuth connector (e.g., google, github)')
                    }),
                    querystring: z.object({
                        code: z
                            .string()
                            .optional()
                            .describe('Authorization code returned by provider'),
                        state: z.string().optional().describe('State parameter for CSRF protection')
                    }),
                    response: {
                        200: z.object({
                            success: z.boolean(),
                            message: z.string()
                        }),
                        404: z.object({
                            code: z.string(),
                            message: z.string()
                        })
                    }
                }
            },
            async (request, reply) => {
                const connector = this.connectors.get(request.params?.connectorName as string);
                if (typeof connector?.getAuth !== 'function') {
                    return reply.status(404).send({
                        code: 'Not Found',
                        message: 'Connector Not Found.'
                    });
                }

                await connector.getAuth((request.query ?? {}) as Record<string, string>);
                return reply.status(200).send({
                    success: true,
                    message: `Successfully authenticated with ${connector.name}.`
                });
            }
        );

        this.tablePayload = new Map();
        this.parseTable('tables', opts.dbOptions.tables, TableDefinitionSchema);
        this.parseTable('entities', opts.dbOptions.entities, EntitySchema);
        this.parseTable('components', opts.dbOptions.components, ComponentSchema);
        this.parseTable('connections', opts.dbOptions.connections, ConnectionSchema);
        this.parseTable('roles', opts.dbOptions.roles, RoleSchema);
    }

    /**
     * Parse system table.
     * @param table Table used for definition.
     * @param tableProp Table property on SchemaFX.
     * @param opts DB Options.
     * @param schema ZodSchema for Table definition.
     */
    private parseTable(table: string, opts: SchemaFXDBOption, schema: z.ZodObject) {
        if (!opts) {
            throw new Error(`No DB options provided for ${table}.`);
        }

        this.tablePayload.set(table, opts.connectionPayload || {});
        this[`${table}Table`] = {
            id: '',
            name: table,
            entity: '',
            connection: '',
            connector: opts.connector,
            connectionPath: opts.connectionPath,
            columns: zodToTableColumns(schema)
        };
    }

    /**
     * Add data connectors.
     * @param connectors Connectors to add.
     */
    addConnectors(...connectors: (Connector | Connector[])[]) {
        for (const connector of connectors.flat()) {
            const name = connector.name;

            if (this.connectors.has(name) && this.connectors.get(name) !== connector) {
                throw new Error(`A connector has already been registered for "${name}".`);
            }

            this.connectors.set(name, connector);
        }

        return this;
    }

    /**
     * Remove data connectors.
     * @param connectors Connectors to remove.
     */
    removeConnectors(...connectors: (string | string[])[]) {
        for (const connector of connectors.flat()) {
            this.connectors.delete(connector);
        }

        return this;
    }

    /**
     * Start the Fastify server listening.
     * @param opts Fastify listen options.
     */
    listen(opts: FastifyListenOptions) {
        return this.fastifyInstance.listen(opts);
    }
}
