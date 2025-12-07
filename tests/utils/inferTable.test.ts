import { describe, it, expect } from 'vitest';
import inferTable from '../../src/utils/inferTable.js';
import { AppFieldType } from '../../src/types.js';

describe('inferTable', () => {
    it('should infer table from array of objects', () => {
        const data = [
            {
                id: 1,
                name: 'Test',
                active: true,
                created: new Date(),
                mixed: 3,
                undefined,
                null: null
            },
            { id: 2, name: 'Test 2', active: false, mixed: false, undefined, null: null }
        ];

        const table = inferTable('TestTable', [], data, 'mem');

        expect(table.name).toBe('TestTable');
        expect(table.connector).toBe('mem');
        expect(table.fields).toHaveLength(7);

        const idField = table.fields.find(f => f.id === 'id');
        expect(idField?.type).toBe(AppFieldType.Number);

        const nameField = table.fields.find(f => f.id === 'name');
        expect(nameField?.type).toBe(AppFieldType.Text);

        const activeField = table.fields.find(f => f.id === 'active');
        expect(activeField?.type).toBe(AppFieldType.Boolean);

        const createdField = table.fields.find(f => f.id === 'created');
        expect(createdField?.type).toBe(AppFieldType.Date);

        const mixedField = table.fields.find(f => f.id === 'mixed');
        expect(mixedField?.type).toBe(AppFieldType.Text);

        const undefinedField = table.fields.find(f => f.id === 'undefined');
        expect(undefinedField?.type).toBe(AppFieldType.Text);

        const nullField = table.fields.find(f => f.id === 'null');
        expect(nullField?.type).toBe(AppFieldType.Text);
    });

    it('should infer JSON object', () => {
        const data = [{ meta: { version: 1, tags: ['a', 'b'] } }];
        const table = inferTable('Table', [], data, 'mem');
        const metaField = table.fields.find(f => f.id === 'meta');
        expect(metaField?.type).toBe(AppFieldType.JSON);
    });

    it('should infer empty table for empty data', () => {
        const table = inferTable('Empty', [], [], 'mem');
        expect(table.fields).toHaveLength(0);
    });

    it('should handle null/undefined values', () => {
        const data = [{ val: null }, { val: 'str' }];
        const table = inferTable('Table', [], data, 'mem');
        const field = table.fields.find(f => f.id === 'val');
        expect(field?.type).toBe(AppFieldType.Text);
    });

    it('should default to Text for unknown types', () => {
        const data = [{ val: Symbol('s') }];
        // JSON.stringify ignores symbols, but let's see how inferTable handles it.
        // inferTable likely iterates keys.
        const table = inferTable('Table', [], data as any, 'mem');
        const field = table.fields.find(f => f.id === 'val');
        expect(field?.type).toBe(AppFieldType.Text);
    });
});
