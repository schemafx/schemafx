import { describe, it, expect } from 'vitest';
import { reorderElement, validateTableKeys, extractKeys } from '../../src/utils/schemaUtils.js';
import { AppFieldType } from '../../src/types.js';

describe('schemaUtils', () => {
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
});
