import type { FastifyPluginAsyncZod } from 'fastify-type-provider-zod';
import { z } from 'zod';
import { ErrorResponseSchema } from '../utils/fastifyUtils.js';

const plugin: FastifyPluginAsyncZod = async fastify => {
    fastify.post(
        '/login',
        {
            schema: {
                body: z.object({
                    username: z.string().min(1).meta({ description: 'Username' }),
                    password: z.string().min(1).meta({ description: 'Password' })
                }),
                response: {
                    200: z.object({
                        token: z.string().meta({ description: 'JWT Token' })
                    }),
                    401: ErrorResponseSchema
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
