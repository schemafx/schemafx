import { describe, it, expect, vi } from 'vitest';
import { z } from 'zod';
import { LRUCache } from 'lru-cache';
import {
    zodFromField,
    zodFromFields,
    zodFromTable,
    reorderElement,
    validateTableKeys,
    extractKeys,
    tableQuerySchema
} from '../../src/utils/schemaUtils.js';
import { AppFieldType, type AppField, type AppTable } from '../../src/types.js';

describe('schemaUtils', () => {
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

            const cache = new LRUCache<string, z.ZodTypeAny>({ max: 10 });
            const s1 = zodFromTable(table, 'app1', cache);
            expect(cache.has('app1:t1')).toBe(true);

            const s2 = zodFromTable(table, 'app1', cache);
            expect(s1).toBe(s2);
        });
    });

    describe('reorderElement', () => {
        it('should reorder elements', () =>
            expect(reorderElement(0, 2, [1, 2, 3])).toEqual([2, 3, 1]));
    });

    describe('validateTableKeys', () => {
        it('should pass if key exists', () =>
            expect(() =>
                validateTableKeys({
                    id: 't1',
                    name: 'Table',
                    connector: 'mem',
                    path: [],
                    fields: [
                        {
                            id: 'f1',
                            name: 'F1',
                            type: AppFieldType.Text,
                            isKey: true
                        }
                    ],
                    actions: []
                })
            ).not.toThrow());

        it('should throw if no key exists', () =>
            expect(() =>
                validateTableKeys({
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
                })
            ).toThrow());
    });

    describe('extractKeys', () => {
        it('should extract keys', () => {
            const row = { id: 1, name: 'test', other: 'val' };
            const keys = extractKeys(row, ['id', 'name']);
            expect(keys).toEqual({ id: 1, name: 'test' });
        });

        it('should ignore missing keys', () => {
            const row = { id: 1 };
            const keys = extractKeys(row, ['id', 'name']);
            expect(keys).toEqual({ id: 1 });
        });
    });

    describe('tableQuerySchema', () => {
        it('should define valid schemas', () => {
            expect(tableQuerySchema.params).toBeDefined();
            expect(tableQuerySchema.querystring).toBeDefined();
            expect(tableQuerySchema.response).toBeDefined();
        });
    });
});
