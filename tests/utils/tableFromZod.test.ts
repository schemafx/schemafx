import { describe, it, expect } from 'vitest';
import { z } from 'zod';
import { tableFromZod } from '../../src/utils/schemaUtils.js';
import { AppFieldType, AppTableSchema } from '../../src/types.js';

describe('tableFromZod', () => {
    it('should convert a simple schema to an AppTable', () => {
        const table = tableFromZod(
            z.object({
                id: z.string(),
                name: z.string(),
                age: z.number().int().min(0),
                isActive: z.boolean().default(true)
            }),
            {
                id: 'test_table',
                name: 'Test Table',
                connector: 'memory'
            }
        );

        expect(table.id).toBe('test_table');
        expect(table.name).toBe('Test Table');
        expect(table.connector).toBe('memory');
        expect(table.fields).toHaveLength(4);

        const idField = table.fields.find(f => f.id === 'id');
        expect(idField).toBeDefined();
        expect(idField?.type).toBe(AppFieldType.Text);
        expect(idField?.isRequired).toBe(true);
        expect(idField?.isKey).toBe(true);

        const nameField = table.fields.find(f => f.id === 'name');
        expect(nameField?.type).toBe(AppFieldType.Text);
        expect(nameField?.isRequired).toBe(true);

        const ageField = table.fields.find(f => f.id === 'age');
        expect(ageField?.type).toBe(AppFieldType.Number);
        expect(ageField?.minValue).toBe(0);

        const activeField = table.fields.find(f => f.id === 'isActive');
        expect(activeField?.type).toBe(AppFieldType.Boolean);
        expect(activeField?.isRequired).toBe(false);
    });

    it('should handle complex constraints', () => {
        const table = tableFromZod(
            z.object({
                id: z.string(),
                email: z.string().email(),
                limitedString: z.string().min(5).max(10),
                limitedNumber: z.number().min(1).max(100),
                dateRange: z.date().min(new Date('2020-01-01')).max(new Date('2020-12-31')),
                choice: z.enum(['A', 'B', 'C'])
            }),
            { id: 't1', name: 'T1', connector: 'c1' }
        );

        const email = table.fields.find(f => f.id === 'email');
        expect(email?.type).toBe(AppFieldType.Email);

        const str = table.fields.find(f => f.id === 'limitedString');
        expect(str?.minLength).toBe(5);
        expect(str?.maxLength).toBe(10);

        const num = table.fields.find(f => f.id === 'limitedNumber');
        expect(num?.minValue).toBe(1);
        expect(num?.maxValue).toBe(100);

        const date = table.fields.find(f => f.id === 'dateRange');
        expect(date?.startDate).toEqual(new Date('2020-01-01'));
        expect(date?.endDate).toEqual(new Date('2020-12-31'));

        const choice = table.fields.find(f => f.id === 'choice');
        expect(choice?.type).toBe(AppFieldType.Dropdown);
        expect(choice?.options).toEqual(['A', 'B', 'C']);
    });

    it('should handle recursion (Objects and Arrays)', () => {
        const table = tableFromZod(
            z.object({
                id: z.string(),
                tags: z.array(z.string()),
                meta: z.object({
                    created: z.date(),
                    author: z.object({
                        name: z.string()
                    })
                }),
                list_of_objects: z.array(z.object({ item: z.string() }))
            }),
            { id: 't2', name: 'T2', connector: 'c1' }
        );

        const tags = table.fields.find(f => f.id === 'tags');
        expect(tags?.type).toBe(AppFieldType.List);
        expect(tags?.child?.type).toBe(AppFieldType.Text);

        const meta = table.fields.find(f => f.id === 'meta');
        expect(meta?.type).toBe(AppFieldType.JSON);
        expect(meta?.fields).toBeDefined();
        expect(meta?.fields?.find(f => f.id === 'created')?.type).toBe(AppFieldType.Date);

        const author = meta?.fields?.find(f => f.id === 'author');
        expect(author?.type).toBe(AppFieldType.JSON);
        expect(author?.fields?.find(f => f.id === 'name')?.type).toBe(AppFieldType.Text);

        const listObj = table.fields.find(f => f.id === 'list_of_objects');
        expect(listObj?.type).toBe(AppFieldType.List);
        expect(listObj?.child?.type).toBe(AppFieldType.JSON);
        expect(listObj?.child?.fields?.[0].id).toBe('item');
    });

    it('should throw error if primary key is missing', () => {
        expect(() =>
            tableFromZod(
                z.object({
                    name: z.string()
                }),
                { id: 't3', name: 'T3', connector: 'c1' }
            )
        ).toThrow();
    });

    it('should support custom primary key', () => {
        const table = tableFromZod(
            z.object({
                code: z.string(),
                name: z.string()
            }),
            {
                id: 't4',
                name: 'T4',
                connector: 'c1',
                primaryKey: 'code'
            }
        );

        const code = table.fields.find(f => f.id === 'code');
        expect(code?.isKey).toBe(true);
    });

    it('should handle optional and nullable', () => {
        const table = tableFromZod(
            z.object({
                id: z.string(),
                opt: z.string().optional(),
                nullb: z.string().nullable(),
                both: z.string().optional().nullable()
            }),
            { id: 't5', name: 'T5', connector: 'c1' }
        );

        expect(table.fields.find(f => f.id === 'opt')?.isRequired).toBe(false);
        expect(table.fields.find(f => f.id === 'nullb')?.isRequired).toBe(false);
        expect(table.fields.find(f => f.id === 'both')?.isRequired).toBe(false);
        expect(table.fields.find(f => f.id === 'id')?.isRequired).toBe(true);
    });

    it('should fallback to Text for unknown simple types and JSON for unknown objects', () => {
        const table = tableFromZod(
            z.object({
                id: z.string(),
                record: z.record(z.string(), z.any()),
                tuple: z.tuple([z.string(), z.number()])
            }),
            { id: 't6', name: 'T6', connector: 'c1' }
        );

        expect(table.fields.find(f => f.id === 'record')?.type).toBe(AppFieldType.JSON);
        expect(table.fields.find(f => f.id === 'tuple')?.type).toBe(AppFieldType.JSON);
    });

    it('should convert AppTableSchema itself', () => {
        const table = tableFromZod(AppTableSchema, {
            id: 'app_table_meta',
            name: 'App Table Metadata',
            connector: 'memory'
        });

        expect(table.fields.find(f => f.id === 'id')?.isKey).toBe(true);
        expect(table.fields.find(f => f.id === 'fields')?.type).toBe(AppFieldType.List);
        expect(table.fields.find(f => f.id === 'fields')?.child?.type).toBe(AppFieldType.JSON);
    });
});
