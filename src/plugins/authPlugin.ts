import type { FastifyPluginAsyncZod } from 'fastify-type-provider-zod';
import { z } from 'zod';

const plugin: FastifyPluginAsyncZod = async fastify => {
    fastify.post(
        '/login',
        {
            schema: {
                body: z.object({
                    username: z.string().min(1),
                    password: z.string().min(1)
                }),
                response: {
                    200: z.object({
                        token: z.string()
                    }),
                    401: z.object({
                        error: z.string(),
                        message: z.string()
                    })
                }
            }
        },
        async (request, reply) => {
            const { username, password } = request.body;
            const isValid = username === 'test' && password === 'test';

            if (isValid) {
                return {
                    token: fastify.jwt.sign({ id: username }, { expiresIn: '8h' })
                };
            }

            return reply.code(401).send({
                error: 'Unauthorized',
                message: 'Invalid credentials.'
            });
        }
    );
};

export default plugin;
