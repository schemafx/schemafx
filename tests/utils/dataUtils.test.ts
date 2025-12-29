import { describe, it, expect } from 'vitest';
import { inferTable } from '../../src/utils/dataUtils.js';
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

        expect(table.fields.find(f => f.id === 'id')?.type).toBe(AppFieldType.Number);
        expect(table.fields.find(f => f.id === 'name')?.type).toBe(AppFieldType.Text);
        expect(table.fields.find(f => f.id === 'active')?.type).toBe(AppFieldType.Boolean);
        expect(table.fields.find(f => f.id === 'created')?.type).toBe(AppFieldType.Date);
        expect(table.fields.find(f => f.id === 'mixed')?.type).toBe(AppFieldType.Text);
        expect(table.fields.find(f => f.id === 'undefined')?.type).toBe(AppFieldType.Text);
        expect(table.fields.find(f => f.id === 'null')?.type).toBe(AppFieldType.Text);
    });

    it('should infer JSON object', () => {
        const data = [{ meta: { version: 1, tags: ['a', 'b'] } }];
        const table = inferTable('Table', [], data, 'mem');

        expect(table.fields.find(f => f.id === 'meta')?.type).toBe(AppFieldType.JSON);
    });

    it('should infer empty table for empty data', () => {
        const table = inferTable('Empty', [], [], 'mem');
        expect(table.fields).toHaveLength(0);
    });

    it('should handle null/undefined values', () => {
        const data = [{ val: null }, { val: 'str' }];
        const table = inferTable('Table', [], data, 'mem');

        expect(table.fields.find(f => f.id === 'val')?.type).toBe(AppFieldType.Text);
    });

    it('should default to Text for unknown types', () => {
        const data = [{ val: Symbol('s') }];
        const table = inferTable('Table', [], data as any, 'mem');

        expect(table.fields.find(f => f.id === 'val')?.type).toBe(AppFieldType.Text);
    });

    it('should mark first field as key when no id field exists', () => {
        const data = [{ name: 'Test', value: 123 }];
        const table = inferTable('Table', [], data, 'mem');

        expect(table.fields.find(f => f.id === 'name')?.isKey).toBe(true);
    });

    it('should mark id field as key and not the first field', () => {
        const data = [{ name: 'Test', id: 1 }];
        const table = inferTable('Table', [], data, 'mem');

        expect(table.fields.find(f => f.id === 'id')?.isKey).toBe(true);
        expect(table.fields.filter(f => f.isKey)).toHaveLength(1);
    });

    it('should not mark any field as key when data is empty', () => {
        const table = inferTable('Empty', [], [], 'mem');
        expect(table.fields.filter(f => f.isKey)).toHaveLength(0);
    });

    it('should set path correctly', () => {
        const data = [{ id: 1 }];
        const table = inferTable('Table', ['folder', 'subfolder'], data, 'mem');
        expect(table.path).toEqual(['folder', 'subfolder']);
    });

    it('should infer List type for arrays', () => {
        const data = [{ tags: ['a', 'b', 'c'] }];
        const table = inferTable('Table', [], data, 'mem');

        expect(table.fields.find(f => f.id === 'tags')?.type).toBe(AppFieldType.List);
    });
});
