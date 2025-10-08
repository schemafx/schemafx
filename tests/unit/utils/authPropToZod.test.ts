import { describe, it, expect } from 'vitest';
import { z } from 'zod';
import authPropToZod from '../../../src/utils/authPropToZod';
import { ConnectorAuthPropType } from '../../../src/connector';

describe('authPropToZod', () => {
    it('should support numbers', () =>
        expect(authPropToZod(ConnectorAuthPropType.Number).def).toEqual(z.number().def));

    it('should default to string', () =>
        expect(authPropToZod(undefined!).def).toEqual(z.string().def));

    it('should support objects', () =>
        expect(
            authPropToZod({
                type: ConnectorAuthPropType.Number,
                required: undefined!
            }).def
        ).toEqual(z.number().def));

    it('should default support optionals', () =>
        expect(
            authPropToZod({
                type: ConnectorAuthPropType.Text,
                required: false
            }).type
        ).toEqual(z.string().optional().type));
});
