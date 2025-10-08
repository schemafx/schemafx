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
import type { Connector } from './connector';
import z from 'zod';
import type { IncomingMessage, ServerResponse } from 'node:http';
import {
    AuthPayloadKeys,
    ComponentSchema,
    type Connection,
    ConnectionSchema,
    type Entity,
    EntitySchema,
    EntityType,
    type Role,
    RoleGrants,
    RoleSchema,
    RoleTargetType,
    type TableDefinition,
    TableDefinitionSchema
} from './schemas';
import { zodToTableColumns } from './utils/zodToTableColumns';
import authPropToZod from './utils/authPropToZod';
import { ulid } from 'ulid';
import { decrypt, encrypt } from './utils/crypto';
import containsAll from './utils/containsAll';

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

declare module 'fastify' {
    interface FastifyInstance {
        authenticate: (request: FastifyRequest) => Promise<void>;
        authorize: (request: FastifyRequest, reply: FastifyReply) => Promise<void>;
    }
}

declare module '@fastify/jwt' {
    interface FastifyJWT {
        payload: { id: string };
        user: { id: string };
    }
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

    /** Secret. */
    private secret: string;

    /**
     * Create a SchemaFX instance.
     * @param opts Instance configuration.
     */
    constructor(opts: SchemaFXOptions) {
        this.secret = opts.secret;

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

        this.fastifyInstance.decorate('authenticate', async (request: FastifyRequest) => {
            await request.jwtVerify().catch(() => undefined);
        });

        this.fastifyInstance.decorate(
            'authorize',
            async (request: FastifyRequest, reply: FastifyReply) => {
                if (!request.user.id) {
                    return reply.code(401).send({ error: 'Unauthorized.' });
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
                onRequest: [this.fastifyInstance.authenticate],
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
                            message: z.string(),
                            token: z.string().optional()
                        }),
                        403: z.object({
                            code: z.string(),
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

                const authPayload = await connector.getAuth(
                    (request.query ?? {}) as Record<string, string>
                );

                let token: string | undefined;
                if (!request.user) {
                    const userEmail = authPayload[AuthPayloadKeys.Email];
                    if (!userEmail) {
                        return reply.status(403).send({
                            code: 'Forbidden',
                            message: 'Insufficient Permissions.'
                        });
                    }

                    const entitiesConnectorName = this.entitiesTable.connector;
                    const entitiesConnector = this.connectors.get(entitiesConnectorName)!;

                    const entities = await entitiesConnector.readData!(
                        [this.entitiesTable],
                        this.tablePayload.get('entities') as Record<string, string>
                    );

                    let user: Entity | undefined = (entities[0].rows as Entity[]).find(
                        e => e.type === EntityType.User && e.name === userEmail
                    );

                    if (!user) {
                        user = {
                            id: ulid(),
                            name: userEmail,
                            type: EntityType.User
                        };

                        await entitiesConnector.createData!(
                            this.entitiesTable,
                            [user],
                            this.tablePayload.get('entities') as Record<string, string>
                        );
                    }

                    request.user = { id: user.id };
                    token = await reply.jwtSign(request.user);
                }

                await this.saveCredentials(request.user.id, authPayload);

                return reply.status(200).send({
                    success: true,
                    message: `Successfully authenticated with ${connector.name}.`,
                    ...(token ? { token } : {})
                });
            }
        );

        this.fastifyInstance.get(
            '/auth/:connectorName/connect',
            {
                onRequest: [this.fastifyInstance.authenticate],
                schema: {
                    params: z.object({
                        connectorName: z
                            .string()
                            .describe('The name of the OAuth connector (e.g., google, github)')
                    }),
                    response: {
                        302: z.undefined(),
                        404: z.object({
                            code: z.string(),
                            message: z.string()
                        })
                    }
                }
            },
            async (request, reply) => {
                const connector = this.connectors.get(request.params?.connectorName as string);
                if (typeof connector?.getAuthUrl !== 'function') {
                    return reply.status(404).send({
                        code: 'Not Found',
                        message: 'Connector Not Found.'
                    });
                }

                return reply.redirect(connector.getAuthUrl(), 302);
            }
        );

        this.fastifyInstance.post(
            '/auth/:connectorName/connect',
            {
                onRequest: [this.fastifyInstance.authenticate, this.fastifyInstance.authorize],
                schema: {
                    params: z.object({
                        connectorName: z
                            .string()
                            .describe('The name of the OAuth connector (e.g., google, github)')
                    }),
                    body: z.looseObject({}),
                    response: {
                        200: z.object({
                            success: z.boolean(),
                            message: z.string()
                        }),
                        400: z.object({
                            error: z.string(),
                            message: z.string(),
                            details: z
                                .array(
                                    z.object({
                                        field: z.string(),
                                        message: z.string(),
                                        code: z.string()
                                    })
                                )
                                .optional()
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
                if (
                    !connector ||
                    !connector?.authProps ||
                    !Object.keys(connector.authProps).length
                ) {
                    return reply.status(404).send({
                        code: 'Not Found',
                        message: 'Connector Not Found.'
                    });
                }

                const result = z
                    .strictObject(
                        Object.fromEntries(
                            Object.entries(connector.authProps).map(e => [
                                e[0],
                                authPropToZod(e[1])
                            ])
                        )
                    )
                    .safeParse(request.body);

                if (!result.success) {
                    return reply.code(400).send({
                        error: 'Validation Error',
                        message: 'Invalid input data',
                        details: result.error.issues.map(err => ({
                            field: err.path.join('.'),
                            message: err.message,
                            code: err.code
                        }))
                    });
                }

                if (typeof connector.validateAuth === 'function') {
                    if (!(await connector.validateAuth(result.data as Record<string, string>))) {
                        return reply.code(400).send({
                            error: 'Validation Error',
                            message: 'Unable to connect.'
                        });
                    }
                }

                // TODO: Have proper owner id.
                await this.saveCredentials(request.user.id, result.data as Record<string, string>);

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
     * Save credentials.
     * @param owner Id of the owner.
     * @param connectionPayload Connection data to save.
     */
    private async saveCredentials(owner: string, connectionPayload: Record<string, string>) {
        const connectionsConnectorName = this.connectionsTable.connector;
        const connectionsConnector = this.connectors.get(connectionsConnectorName)!;

        const rolesConnectorName = this.rolesTable.connector;
        const rolesConnector = this.connectors.get(rolesConnectorName)!;

        const connection = {
            id: ulid(),
            connector: connectionsConnectorName,
            connection_payload: encrypt(JSON.stringify(connectionPayload), this.secret)
        };

        await connectionsConnector.createData!(
            this.connectionsTable,
            [connection],
            (this.tablePayload.get('connections') ?? {}) as Record<string, string>
        );

        await rolesConnector.createData!(
            this.rolesTable,
            [
                {
                    id: ulid(),
                    entity: owner,
                    grants: [RoleGrants.Owner],
                    target_type: RoleTargetType.Connection,
                    target_id: connection.id
                }
            ],
            (this.tablePayload.get('roles') ?? {}) as Record<string, string>
        );

        return connection.id;
    }

    /**
     * Retrieve a connection's details.
     * @param context Context for permissions.
     * @param connection Connection to retrieve.
     * @returns The connection details, if available.
     */
    private async getConnection(context: string, connection: string) {
        if (!this.hasGrant(RoleGrants.Read, context, connection)) {
            return;
        }

        const connectionsConnectorName = this.connectionsTable.connector;
        const connectionsConnector = this.connectors.get(connectionsConnectorName)!;

        const connections = await connectionsConnector.readData!(
            [this.connectionsTable],
            this.tablePayload.get('connections') as Record<string, string>
        );

        const payload = (connections[0].rows as Connection[]).find(r => r.id === connection);

        if (!payload?.connection_payload) {
            return;
        }

        const decrypted = decrypt(payload.connection_payload, this.secret);
        if (typeof decrypted !== 'string') {
            return;
        }

        return JSON.parse(decrypted) as Record<string, string>;
    }

    /**
     * Whether a context has grants.
     * @param grant Grant(s) to look for.
     * @param context Context to use.
     * @param target Target to find.
     * @returns Whether the context has grants.
     */
    private async hasGrant(grant: RoleGrants | RoleGrants[], context: string, target: string) {
        const grants: RoleGrants[] = Array.isArray(grant) ? grant : [grant];
        console.log(target);

        const rolesConnectorName = this.rolesTable.connector;
        const rolesConnector = this.connectors.get(rolesConnectorName)!;

        const roles = (
            await rolesConnector.readData!(
                [this.rolesTable],
                this.tablePayload.get('roles') as Record<string, string>
            )
        )[0].rows as Role[];

        let possibleRoles: Role[] = [];
        let validGrants: string[] = [];

        for (const role of roles) {
            if (role.target_id === target) {
                if (role.grants.includes(RoleGrants.Owner) || containsAll(grants, role.grants)) {
                    validGrants.push(role.entity);
                }

                continue;
            } else if (role.target_type === RoleTargetType.Connection) {
                // No sub-permission for that type.
                continue;
            }

            possibleRoles.push(role);
        }

        while (validGrants.length) {
            if (validGrants.includes(context)) {
                return true;
            }

            validGrants = possibleRoles
                .filter(r => validGrants.includes(r.target_id))
                .map(r => r.entity);
        }

        return false;
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
