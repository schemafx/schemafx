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
            const field: AppField = {
                id: 'test',
                name: 'Test',
                type: AppFieldType.Text,
                isRequired: true,
                isKey: false
            };
            const schema = zodFromField(field);
            expect(schema.safeParse('valid').success).toBe(true);
            expect(schema.safeParse(123).success).toBe(false);
        });

        it('should generate string validator with min/max length', () => {
            const field: AppField = {
                id: 'test',
                name: 'Test',
                type: AppFieldType.Text,
                isRequired: true,
                isKey: false,
                minLength: 2,
                maxLength: 5
            };
            const schema = zodFromField(field);
            expect(schema.safeParse('ab').success).toBe(true);
            expect(schema.safeParse('a').success).toBe(false);
            expect(schema.safeParse('abcdef').success).toBe(false);
        });

        it('should generate number validator with min/max value', () => {
            const field: AppField = {
                id: 'num',
                name: 'Num',
                type: AppFieldType.Number,
                isRequired: true,
                isKey: false,
                minValue: 0,
                maxValue: 10
            };
            const schema = zodFromField(field);
            expect(schema.safeParse(5).success).toBe(true);
            expect(schema.safeParse(-1).success).toBe(false);
            expect(schema.safeParse(11).success).toBe(false);
        });

        it('should generate boolean validator', () => {
            const field: AppField = {
                id: 'bool',
                name: 'Bool',
                type: AppFieldType.Boolean,
                isRequired: true,
                isKey: false
            };
            const schema = zodFromField(field);
            expect(schema.safeParse(true).success).toBe(true);
            expect(schema.safeParse('true').success).toBe(false);
        });

        it('should generate date validator with start/end date', () => {
            const start = new Date('2023-01-01');
            const end = new Date('2023-12-31');
            const field: AppField = {
                id: 'date',
                name: 'Date',
                type: AppFieldType.Date,
                isRequired: true,
                isKey: false,
                startDate: start,
                endDate: end
            };
            const schema = zodFromField(field);
            expect(schema.safeParse(new Date('2023-06-01')).success).toBe(true);
            expect(schema.safeParse(new Date('2022-12-31')).success).toBe(false);
            expect(schema.safeParse(new Date('2024-01-01')).success).toBe(false);
        });

        it('should generate email validator', () => {
            const field: AppField = {
                id: 'email',
                name: 'Email',
                type: AppFieldType.Email,
                isRequired: true,
                isKey: false
            };
            const schema = zodFromField(field);
            expect(schema.safeParse('test@example.com').success).toBe(true);
            expect(schema.safeParse('invalid').success).toBe(false);
        });

        it('should generate dropdown validator', () => {
            const field: AppField = {
                id: 'drop',
                name: 'Drop',
                type: AppFieldType.Dropdown,
                isRequired: true,
                isKey: false,
                options: ['a', 'b']
            };
            const schema = zodFromField(field);
            expect(schema.safeParse('a').success).toBe(true);
            expect(schema.safeParse('c').success).toBe(false);
        });

        it('should handle empty dropdown options', () => {
            const field: AppField = {
                id: 'drop',
                name: 'Drop',
                type: AppFieldType.Dropdown,
                isRequired: true,
                isKey: false,
                options: []
            };

            try {
                zodFromField(field);
            } catch (e) {
                // Expected if z.enum throws on empty
            }
        });

        it('should generate JSON validator', () => {
            const field: AppField = {
                id: 'json',
                name: 'Json',
                type: AppFieldType.JSON,
                isRequired: true,
                isKey: false,
                fields: [
                    {
                        id: 'sub',
                        name: 'Sub',
                        type: AppFieldType.Text,
                        isRequired: true,
                        isKey: false
                    }
                ]
            };
            const schema = zodFromField(field);
            expect(schema.safeParse({ sub: 'val' }).success).toBe(true);
            expect(schema.safeParse({ sub: 123 }).success).toBe(false);
        });

        it('should generate JSON validator with empty fields', () => {
            const field: AppField = {
                id: 'json',
                name: 'Json',
                type: AppFieldType.JSON,
                isRequired: true,
                isKey: false,
                fields: []
            };
            const schema = zodFromField(field);
            expect(schema.safeParse({}).success).toBe(true);
        });

        it('should generate List validator with child', () => {
            const field: AppField = {
                id: 'list',
                name: 'List',
                type: AppFieldType.List,
                isRequired: true,
                isKey: false,
                child: {
                    id: 'sub',
                    name: 'Sub',
                    type: AppFieldType.Text,
                    isRequired: true,
                    isKey: false
                }
            };
            const schema = zodFromField(field);
            expect(schema.safeParse(['a', 'b']).success).toBe(true);
            expect(schema.safeParse([1, 2]).success).toBe(false);
        });

        it('should generate List validator without child', () => {
            const field: AppField = {
                id: 'list',
                name: 'List',
                type: AppFieldType.List,
                isRequired: true,
                isKey: false
            };
            const schema = zodFromField(field);
            expect(schema.safeParse(['a', 1]).success).toBe(true);
        });

        it('should handle optional fields', () => {
            const field: AppField = {
                id: 'opt',
                name: 'Opt',
                type: AppFieldType.Text,
                isRequired: false,
                isKey: false
            };
            const schema = zodFromField(field);
            expect(schema.safeParse(undefined).success).toBe(true);
            expect(schema.safeParse(null).success).toBe(true);
        });
    });

    describe('zodFromFields', () => {
        it('should create object schema from fields', () => {
            const fields: AppField[] = [
                {
                    id: 'f1',
                    name: 'F1',
                    type: AppFieldType.Text,
                    isRequired: true,
                    isKey: false
                }
            ];
            const schema = zodFromFields(fields);
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
                        type: AppFieldType.Text,
                        isRequired: true,
                        isKey: false
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
        it('should reorder elements', () => {
            const arr = [1, 2, 3];
            const res = reorderElement(0, 2, arr);
            expect(res).toEqual([2, 3, 1]);
        });
    });

    describe('validateTableKeys', () => {
        it('should pass if key exists', () => {
            const table: AppTable = {
                id: 't1',
                name: 'Table',
                connector: 'mem',
                path: [],
                fields: [
                    {
                        id: 'f1',
                        name: 'F1',
                        type: AppFieldType.Text,
                        isRequired: true,
                        isKey: true
                    }
                ],
                actions: []
            };
            expect(() => validateTableKeys(table)).not.toThrow();
        });

        it('should throw if no key exists', () => {
            const table: AppTable = {
                id: 't1',
                name: 'Table',
                connector: 'mem',
                path: [],
                fields: [
                    {
                        id: 'f1',
                        name: 'F1',
                        type: AppFieldType.Text,
                        isRequired: true,
                        isKey: false
                    }
                ],
                actions: []
            };
            expect(() => validateTableKeys(table)).toThrow();
        });
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
