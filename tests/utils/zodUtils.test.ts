import { describe, it, expect } from 'vitest';
import { z } from 'zod';
import { LRUCache } from 'lru-cache';
import {
    tableFromZod,
    zodFromField,
    zodFromFields,
    zodFromTable
} from '../../src/utils/zodUtils.js';
import { AppFieldType, AppTableSchema, type AppTable } from '../../src/types.js';

describe('zodUtils', () => {
    describe('zodFromField', () => {
        it('should generate string validator', () => {
            const schema = zodFromField({
                id: 'test',
                name: 'Test',
                type: AppFieldType.Text
            });

            expect(schema.safeParse('valid').success).toBe(true);
            expect(schema.safeParse(123).success).toBe(false);
        });

        it('should generate string validator with min/max length', () => {
            const schema = zodFromField({
                id: 'test',
                name: 'Test',
                type: AppFieldType.Text,
                minLength: 2,
                maxLength: 5
            });

            expect(schema.safeParse('ab').success).toBe(true);
            expect(schema.safeParse('a').success).toBe(false);
            expect(schema.safeParse('abcdef').success).toBe(false);
        });

        it('should generate number validator with min/max value', () => {
            const schema = zodFromField({
                id: 'num',
                name: 'Num',
                type: AppFieldType.Number,
                minValue: 0,
                maxValue: 10
            });

            expect(schema.safeParse(5).success).toBe(true);
            expect(schema.safeParse(-1).success).toBe(false);
            expect(schema.safeParse(11).success).toBe(false);
        });

        it('should generate boolean validator', () => {
            const schema = zodFromField({
                id: 'bool',
                name: 'Bool',
                type: AppFieldType.Boolean
            });

            expect(schema.safeParse(true).success).toBe(true);
            expect(schema.safeParse('true').success).toBe(false);
        });

        it('should generate date validator with start/end date', () => {
            const schema = zodFromField({
                id: 'date',
                name: 'Date',
                type: AppFieldType.Date,
                startDate: new Date('2023-01-01'),
                endDate: new Date('2023-12-31')
            });

            expect(schema.safeParse(new Date('2023-06-01')).success).toBe(true);
            expect(schema.safeParse(new Date('2022-12-31')).success).toBe(false);
            expect(schema.safeParse(new Date('2024-01-01')).success).toBe(false);
        });

        it('should generate email validator', () => {
            const schema = zodFromField({
                id: 'email',
                name: 'Email',
                type: AppFieldType.Email
            });

            expect(schema.safeParse('test@example.com').success).toBe(true);
            expect(schema.safeParse('invalid').success).toBe(false);
        });

        it('should generate dropdown validator', () => {
            const schema = zodFromField({
                id: 'drop',
                name: 'Drop',
                type: AppFieldType.Dropdown,
                options: ['a', 'b']
            });

            expect(schema.safeParse('a').success).toBe(true);
            expect(schema.safeParse('c').success).toBe(false);
        });

        it('should handle empty dropdown options', () => {
            const schema = zodFromField({
                id: 'drop',
                name: 'Drop',
                type: AppFieldType.Dropdown,
                options: []
            });

            expect(schema.safeParse('a').success).toBe(false);
        });

        it('should generate JSON validator', () => {
            const schema = zodFromField({
                id: 'json',
                name: 'Json',
                type: AppFieldType.JSON,
                fields: [
                    {
                        id: 'sub',
                        name: 'Sub',
                        type: AppFieldType.Text
                    }
                ]
            });

            expect(schema.safeParse({ sub: 'val' }).success).toBe(true);
            expect(schema.safeParse({ sub: 123 }).success).toBe(false);
        });

        it('should generate JSON validator with empty fields', () => {
            const schema = zodFromField({
                id: 'json',
                name: 'Json',
                type: AppFieldType.JSON,
                fields: []
            });

            expect(schema.safeParse({}).success).toBe(true);
        });

        it('should generate List validator with child', () => {
            const schema = zodFromField({
                id: 'list',
                name: 'List',
                type: AppFieldType.List,
                child: {
                    id: 'sub',
                    name: 'Sub',
                    type: AppFieldType.Text
                }
            });

            expect(schema.safeParse(['a', 'b']).success).toBe(true);
            expect(schema.safeParse([1, 2]).success).toBe(false);
        });

        it('should generate List validator without child', () => {
            const schema = zodFromField({
                id: 'list',
                name: 'List',
                type: AppFieldType.List
            });

            expect(schema.safeParse(['a', 1]).success).toBe(true);
        });

        it('should handle optional fields', () => {
            const schema = zodFromField({
                id: 'opt',
                name: 'Opt',
                type: AppFieldType.Text
            });

            expect(schema.safeParse(undefined).success).toBe(true);
            expect(schema.safeParse(null).success).toBe(true);
        });
    });

    describe('zodFromFields', () => {
        it('should create object schema from fields', () => {
            const schema = zodFromFields([
                {
                    id: 'f1',
                    name: 'F1',
                    type: AppFieldType.Text
                }
            ]);

            expect(schema.safeParse({ f1: 'val' }).success).toBe(true);
        });
    });

    describe('zodFromTable', () => {
        it('should cache generated schema', () => {
            const table: AppTable = {
                id: 't1',
                name: 'Table',
                connector: 'mem',
                path: [],
                fields: [
                    {
                        id: 'f1',
                        name: 'F1',
                        type: AppFieldType.Text
                    }
                ],
                actions: []
            };

            const cache = new LRUCache<string, z.ZodType>({ max: 10 });
            const s1 = zodFromTable(table, cache);
            expect(cache.has('t1')).toBe(true);

            const s2 = zodFromTable(table, cache);
            expect(s1).toBe(s2);
        });
    });

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
                    connector: 'memory',
                    path: [],
                    primaryKey: 'id'
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
                    email: z.email(),
                    limitedString: z.string().min(5).max(10),
                    limitedNumber: z.number().min(1).max(100),
                    dateRange: z.date().min(new Date('2020-01-01')).max(new Date('2020-12-31')),
                    choice: z.enum(['A', 'B', 'C'])
                }),
                { id: 't1', name: 'T1', connector: 'c1', path: [], primaryKey: 'id' }
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
                { id: 't2', name: 'T2', connector: 'c1', path: [], primaryKey: 'id' }
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
                    { id: 't3', name: 'T3', connector: 'c1', path: [], primaryKey: 'id' }
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
                    path: [],
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
                { id: 't5', name: 'T5', connector: 'c1', path: [], primaryKey: 'id' }
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
                { id: 't6', name: 'T6', connector: 'c1', path: [], primaryKey: 'id' }
            );

            expect(table.fields.find(f => f.id === 'record')?.type).toBe(AppFieldType.JSON);
            expect(table.fields.find(f => f.id === 'tuple')?.type).toBe(AppFieldType.JSON);
        });

        it('should convert AppTableSchema itself', () => {
            const table = tableFromZod(AppTableSchema, {
                id: 'app_table_meta',
                name: 'App Table Metadata',
                connector: 'memory',
                path: [],
                primaryKey: 'id'
            });

            expect(table.fields.find(f => f.id === 'id')?.isKey).toBe(true);
            expect(table.fields.find(f => f.id === 'fields')?.type).toBe(AppFieldType.List);
            expect(table.fields.find(f => f.id === 'fields')?.child?.type).toBe(AppFieldType.JSON);
        });
    });
});
