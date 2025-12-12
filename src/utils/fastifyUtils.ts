import { z } from 'zod';

/**
 * Standard error response schema.
 */
export const ErrorResponseSchema = z
    .object({
        error: z.string().meta({ description: 'Error type or code' }),
        message: z.string().meta({ description: 'Error message' })
    })
    .meta({ description: 'Default Error' });

/** Fastify Schema for table queries. */
export const tableQuerySchema = {
    params: z.object({
        appId: z.string().min(1).meta({ description: 'Application ID' }),
        tableId: z.string().min(1).meta({ description: 'Table ID' })
    }),
    querystring: z.object({
        query: z.string().optional().meta({ description: 'JSON stringified query options' })
    }),
    response: {
        200: z.any().meta({ description: 'Query results' }),
        500: ErrorResponseSchema,
        400: ErrorResponseSchema.extend({
            details: z
                .array(
                    z.object({
                        field: z.string().meta({ description: 'Field name' }),
                        message: z.string().meta({ description: 'Error message' }),
                        code: z.string().meta({ description: 'Error code' })
                    })
                )
                .optional()
                .meta({ description: 'Validation error details' })
        }).meta({ description: 'Validation error' })
    }
};
