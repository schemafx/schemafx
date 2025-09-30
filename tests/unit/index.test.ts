import { describe, it, expect } from 'vitest';

describe('Exports', () => {
    it('should export all schema types', async () => {
        const { TableColumnType } = await import('../../src/index');

        expect(TableColumnType).toBeDefined();
        expect(typeof TableColumnType).toBe('object');
    });

    it('should export utils object with zodToTableColumns', async () => {
        const { utils } = await import('../../src/index');

        expect(utils).toBeDefined();
        expect(utils.zodToTableColumns).toBeDefined();
        expect(typeof utils.zodToTableColumns).toBe('function');
    });

    it('should export connector types and classes', async () => {
        const { Connector, ConnectorAuthType, ConnectorAuthPropType } = await import(
            '../../src/index'
        );

        expect(Connector).toBeDefined();
        expect(typeof Connector).toBe('function');

        expect(ConnectorAuthType).toBeDefined();
        expect(typeof ConnectorAuthType).toBe('object');

        expect(ConnectorAuthPropType).toBeDefined();
        expect(typeof ConnectorAuthPropType).toBe('object');
    });

    it('should have SchemaFX as default export', async () => {
        const SchemaFX = (await import('../../src/index')).default;

        expect(SchemaFX).toBeDefined();
        expect(typeof SchemaFX).toBe('function');
    });

    it('should verify all exports are accessible in a single import', async () => {
        const indexModule = await import('../../src/index');

        const expectedExports = [
            'TableColumnType',
            'utils',
            'Connector',
            'ConnectorAuthType',
            'ConnectorAuthPropType',
            'default'
        ];

        for (const exportName of expectedExports) {
            expect(indexModule).toHaveProperty(exportName);
        }
    });
});
