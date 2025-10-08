import { describe, it, expect } from 'vitest';
import containsAll from '../../../src/utils/containsAll';

describe('containsAll', () => {
    it('should return true when all elements of subset are in superset', () => {
        const subset = ['read', 'write'];
        const superset = ['read', 'write', 'delete'];
        expect(containsAll(subset, superset)).toBe(true);
    });

    it('should return false when some elements of subset are not in superset', () => {
        const subset = ['read', 'admin'];
        const superset = ['read', 'write', 'delete'];
        expect(containsAll(subset, superset)).toBe(false);
    });

    it('should return true for identical arrays', () => {
        const subset = [1, 2, 3];
        const superset = [1, 2, 3];
        expect(containsAll(subset, superset)).toBe(true);
    });

    it('should return false if the subset is larger than the superset', () => {
        const subset = [1, 2, 3, 4];
        const superset = [1, 2, 3];
        expect(containsAll(subset, superset)).toBe(false);
    });

    it('should return true when the subset is empty', () => {
        const subset: any[] = [];
        const superset = [1, 2, 3];
        expect(containsAll(subset, superset)).toBe(true);
    });

    it('should return false when the superset is empty but the subset is not', () => {
        const subset = [1];
        const superset: any[] = [];
        expect(containsAll(subset, superset)).toBe(false);
    });

    it('should return true when both arrays are empty', () => {
        const subset: any[] = [];
        const superset: any[] = [];
        expect(containsAll(subset, superset)).toBe(true);
    });

    it('should work correctly with duplicate values in arrays', () => {
        const subset = [1, 1, 2];
        const superset = [1, 2, 3, 1];
        expect(containsAll(subset, superset)).toBe(true);

        const subsetWithMissing = [1, 4, 4];
        const supersetWithDuplicates = [1, 2, 3, 1, 2, 3];
        expect(containsAll(subsetWithMissing, supersetWithDuplicates)).toBe(false);
    });

    it('should be case-sensitive when comparing strings', () => {
        const subset = ['a', 'B'];
        const superset = ['a', 'b', 'c'];
        expect(containsAll(subset, superset)).toBe(false);
    });

    it('should handle different primitive data types', () => {
        const subset = [1, 'hello', null];
        const superset = [null, 100, 'world', 'hello', 1];
        expect(containsAll(subset, superset)).toBe(true);
    });

    it('should correctly handle undefined in arrays', () => {
        const subset = [undefined, 1];
        const superset = [1, 2, undefined];
        expect(containsAll(subset, superset)).toBe(true);

        const subsetMissing = [undefined, 1];
        const supersetMissing = [1, 2, 3];
        expect(containsAll(subsetMissing, supersetMissing)).toBe(false);
    });

    it('should perform efficiently on large arrays', () => {
        const largeSuperset = Array.from({ length: 10000 }, (_, i) => i);
        const largeSubset = [50, 500, 5000, 9999];
        const largeSubsetMissing = [50, 500, 10001];

        expect(containsAll(largeSubset, largeSuperset)).toBe(true);
        expect(containsAll(largeSubsetMissing, largeSuperset)).toBe(false);
    });
});
