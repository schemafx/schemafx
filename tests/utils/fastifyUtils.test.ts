import { describe, it, expect } from 'vitest';
import { tableQuerySchema } from '../../src/utils/fastifyUtils.js';

describe('fastifyUtils', () => {
    describe('tableQuerySchema', () => {
        it('should define valid schemas', () => {
            expect(tableQuerySchema.params).toBeDefined();
            expect(tableQuerySchema.querystring).toBeDefined();
            expect(tableQuerySchema.response).toBeDefined();
        });
    });
});
