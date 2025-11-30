export * from 'fastify';
declare module 'fastify' {
    interface FastifyInstance {
        authenticate: (request: FastifyRequest) => Promise<void>;
    }
}

export * from '@fastify/jwt';
declare module '@fastify/jwt' {
    interface FastifyJWT {
        payload: { id: string };
        user: { id: string };
    }
}
