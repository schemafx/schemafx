import { describe, it, expect } from 'vitest';
import { z } from 'zod';
import { zodToTableColumns } from '../../../src/utils/zodToTableColumns';
import { TableColumnType } from '../../../src/schemas';

describe('zodToTableColumns', () => {
    it('should generate the proper structure', () => {
        const cols = zodToTableColumns(
            z.object({
                key: z.string(),
                second: z.string()
            }),
            'key'
        );

        expect(typeof cols[0].key).toBe('boolean');
        expect(typeof cols[0].name).toBe('string');
        expect(cols[0].name).toBe('key');
        expect(cols[0].key).toBe(true);

        expect(typeof cols[1].key).toBe('boolean');
        expect(typeof cols[1].name).toBe('string');
        expect(cols[1].name).toBe('second');
        expect(cols[1].key).toBe(false);
    });

    it('should convert string schema', () => {
        const cols = zodToTableColumns(
            z.object({
                string: z.string()
            })
        );

        for (const col of cols) {
            expect(col.type).toBe(TableColumnType.String);
            expect(col.typeProps).toBeUndefined();
        }
    });

    it('should convert number schema', () => {
        const cols = zodToTableColumns(
            z.object({
                number: z.number(),
                bigint: z.bigint()
            })
        );

        for (const col of cols) {
            expect(col.type).toBe(TableColumnType.Number);
            expect(col.typeProps).toBeUndefined();
        }
    });

    it('should convert boolean schema', () => {
        const cols = zodToTableColumns(
            z.object({
                boolean: z.boolean()
            })
        );

        for (const col of cols) {
            expect(col.type).toBe(TableColumnType.Boolean);
            expect(col.typeProps).toBeUndefined();
        }
    });

    it('should convert date schema', () => {
        const cols = zodToTableColumns(
            z.object({
                number: z.date()
            })
        );

        for (const col of cols) {
            expect(col.type).toBe(TableColumnType.Date);
            expect(col.typeProps).toBeUndefined();
        }
    });

    it('should convert object schema with nested properties', () => {
        const col = zodToTableColumns(
            z.object({
                json: z.object({
                    number: z.number()
                })
            })
        )[0];

        expect(col.type).toBe(TableColumnType.Json);

        const typeProps = col.typeProps as Record<string, unknown>;
        expect(typeProps).toBeDefined();

        const columns = typeProps.columns as Record<string, unknown>[];
        expect(columns).toBeDefined();
        expect(columns.length).toEqual(1);
        expect(columns[0].name).toEqual('number');
    });

    it('should convert array schema', () => {
        const col = zodToTableColumns(
            z.object({
                number: z.array(z.number())
            })
        )[0];

        expect(col.type).toBe(TableColumnType.Array);
        expect(col.typeProps).toBeDefined();
        expect((col.typeProps! as Record<string, unknown>).type).toBe(TableColumnType.Number);
    });

    it('should convert array of objects', () => {
        const col = zodToTableColumns(
            z.object({
                number: z.array(z.object({}))
            })
        )[0];

        expect(col.type).toBe(TableColumnType.Array);
        expect(col.typeProps).toBeDefined();
        expect((col.typeProps! as Record<string, unknown>).type).toBe(TableColumnType.Json);
    });

    it('should handle optional fields', () => {
        const cols = zodToTableColumns(
            z.object({
                number: z.number().optional()
            })
        );

        for (const col of cols) {
            expect(col.type).toBe(TableColumnType.Number);
            expect(col.typeProps).toBeUndefined();
        }
    });

    it('should handle default values', () => {
        const cols = zodToTableColumns(
            z.object({
                number: z.number().default(3)
            })
        );

        for (const col of cols) {
            expect(col.type).toBe(TableColumnType.Number);
            expect(col.typeProps).toBeUndefined();
        }
    });

    it('should handle lazy schemas', () => {
        const cols = zodToTableColumns(
            z.object({
                json: z.lazy(() => z.object({})),
                string: z.lazy(() => z.string().optional().default('str'))
            })
        );

        expect(cols[0].type).toBe(TableColumnType.Json);
        expect(cols[1].type).toBe(TableColumnType.String);
    });

    it('should handle deeply nested objects', () => {
        const col = zodToTableColumns(
            z.object({
                json: z.object({
                    obj: z.object({})
                })
            })
        )[0];

        expect(col.type).toBe(TableColumnType.Json);

        const typeProps = col.typeProps as Record<string, unknown>;
        expect(typeProps).toBeDefined();

        const columns = typeProps.columns as Record<string, unknown>[];
        expect(columns).toBeDefined();
        expect(columns.length).toEqual(1);
        expect(columns[0].type).toEqual(TableColumnType.Json);
    });

    it('should handle empty schemas', () => {
        expect(zodToTableColumns(z.object())).toEqual([]);
    });

    describe('unhandled Zod types', () => {
        it('should default to string for various unhandled types', () => {
            const schema = z.object({
                any: z.any(),
                unknown: z.unknown(),
                void: z.void(),
                tuple: z.tuple([z.string(), z.number()]),
                union: z.union([z.string(), z.number()]),
                enum: z.enum(['a', 'b', 'c']),
                intersection: z.intersection(z.string(), z.number()),
                map: z.map(z.string(), z.string()),
                function: z.function(),
                promise: z.promise(z.string()),
                null: z.null(),
                undefined: z.undefined()
            });

            const cols = zodToTableColumns(schema);
            expect(cols.length).toBe(12);
            for (const col of cols) {
                expect(col.type).toBe(TableColumnType.String);
            }
        });

        it('should handle lazy evaluation of non-object/non-array types', () => {
            const schema = z.object({
                lazyString: z.lazy(() => z.string())
            });
            const cols = zodToTableColumns(schema);
            expect(cols[0].type).toBe(TableColumnType.String);
        });

        it('should handle lazy schema with no getter', () => {
            const lazyWithNoGetter = z.string() as any;
            lazyWithNoGetter.def.type = 'lazy';
            delete lazyWithNoGetter.def.getter;

            const schema = z.object({
                lazy: lazyWithNoGetter
            });

            const cols = zodToTableColumns(schema);
            expect(cols[0].type).toBe(TableColumnType.String);
        });
    });
});
