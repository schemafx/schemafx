import { describe, it, expect } from 'vitest';
import {
    hasControlChars,
    validatePathOrUrl,
    escapeSqlLiteral,
    validateIdentifier
} from '../../src/utils/sqlSecurity.js';

describe('sqlSecurity', () => {
    describe('hasControlChars', () => {
        it('should return false for normal strings', () => {
            expect(hasControlChars('hello world')).toBe(false);
            expect(hasControlChars('path/to/file.txt')).toBe(false);
            expect(hasControlChars('')).toBe(false);
        });

        it('should return true for strings with control characters', () => {
            expect(hasControlChars('hello\x00world')).toBe(true);
            expect(hasControlChars('test\x1Fvalue')).toBe(true);
            expect(hasControlChars('test\x7Fvalue')).toBe(true);
            expect(hasControlChars('\x00')).toBe(true);
            expect(hasControlChars('a\nb')).toBe(true); // newline is a control char
            expect(hasControlChars('a\tb')).toBe(true); // tab is a control char
        });
    });

    describe('validatePathOrUrl', () => {
        it('should return normalized path for valid input', () => {
            expect(validatePathOrUrl('path/to/file')).toBe('path/to/file');
            expect(validatePathOrUrl('C:\\Users\\test')).toBe('C:/Users/test');
            expect(validatePathOrUrl('https://example.com/path')).toBe('https://example.com/path');
        });

        it('should throw for non-string input', () => {
            expect(() => validatePathOrUrl(123 as unknown as string)).toThrow(
                'value must be a string'
            );
            expect(() => validatePathOrUrl(null as unknown as string)).toThrow(
                'value must be a string'
            );
            expect(() => validatePathOrUrl(undefined as unknown as string)).toThrow(
                'value must be a string'
            );
            expect(() => validatePathOrUrl({} as unknown as string)).toThrow(
                'value must be a string'
            );
        });

        it('should throw for non-string input with custom name', () => {
            expect(() => validatePathOrUrl(123 as unknown as string, 'filePath')).toThrow(
                'filePath must be a string'
            );
        });

        it('should throw for strings with control characters', () => {
            expect(() => validatePathOrUrl('path\x00file')).toThrow(
                'value contains control characters'
            );
            expect(() => validatePathOrUrl('path\x1Ffile')).toThrow(
                'value contains control characters'
            );
            expect(() => validatePathOrUrl('test\x7Fpath', 'myPath')).toThrow(
                'myPath contains control characters'
            );
        });

        it('should throw for strings with embedded quotes', () => {
            expect(() => validatePathOrUrl("path'with'quotes")).toThrow(
                'value contains embedded quotes'
            );
            expect(() => validatePathOrUrl('path"with"quotes')).toThrow(
                'value contains embedded quotes'
            );
            expect(() => validatePathOrUrl("test'path", 'url')).toThrow(
                'url contains embedded quotes'
            );
        });

        it('should convert backslashes to forward slashes', () => {
            expect(validatePathOrUrl('a\\b\\c')).toBe('a/b/c');
            expect(validatePathOrUrl('C:\\Program Files\\App')).toBe('C:/Program Files/App');
        });
    });

    describe('escapeSqlLiteral', () => {
        it('should return string unchanged if no single quotes', () => {
            expect(escapeSqlLiteral('hello world')).toBe('hello world');
            expect(escapeSqlLiteral('')).toBe('');
            expect(escapeSqlLiteral('no quotes here')).toBe('no quotes here');
        });

        it('should escape single quotes by doubling them', () => {
            expect(escapeSqlLiteral("it's")).toBe("it''s");
            expect(escapeSqlLiteral("'test'")).toBe("''test''");
            expect(escapeSqlLiteral("a'b'c")).toBe("a''b''c");
            expect(escapeSqlLiteral("'''")).toBe("''''''");
        });
    });

    describe('validateIdentifier', () => {
        it('should return valid identifiers unchanged', () => {
            expect(validateIdentifier('users')).toBe('users');
            expect(validateIdentifier('table_name')).toBe('table_name');
            expect(validateIdentifier('schema.table')).toBe('schema.table');
            expect(validateIdentifier('Column123')).toBe('Column123');
            expect(validateIdentifier('my$var')).toBe('my$var');
        });

        it('should throw for non-string input', () => {
            expect(() => validateIdentifier(123 as unknown as string)).toThrow(
                'identifier must be a string'
            );
            expect(() => validateIdentifier(null as unknown as string)).toThrow(
                'identifier must be a string'
            );
            expect(() => validateIdentifier(undefined as unknown as string)).toThrow(
                'identifier must be a string'
            );
            expect(() => validateIdentifier({} as unknown as string)).toThrow(
                'identifier must be a string'
            );
        });

        it('should throw for strings with control characters', () => {
            expect(() => validateIdentifier('id\x00name')).toThrow(
                'identifier contains control characters'
            );
            expect(() => validateIdentifier('col\x1F')).toThrow(
                'identifier contains control characters'
            );
            expect(() => validateIdentifier('\x7Ftable')).toThrow(
                'identifier contains control characters'
            );
        });

        it('should throw for strings with quotes', () => {
            expect(() => validateIdentifier("user's")).toThrow('identifier contains quotes');
            expect(() => validateIdentifier('table"name')).toThrow('identifier contains quotes');
            expect(() => validateIdentifier("'id'")).toThrow('identifier contains quotes');
            expect(() => validateIdentifier('"col"')).toThrow('identifier contains quotes');
        });

        it('should throw for strings with invalid characters', () => {
            expect(() => validateIdentifier('table name')).toThrow(
                'identifier contains invalid characters'
            );
            expect(() => validateIdentifier('col-name')).toThrow(
                'identifier contains invalid characters'
            );
            expect(() => validateIdentifier('table;drop')).toThrow(
                'identifier contains invalid characters'
            );
            expect(() => validateIdentifier('id@name')).toThrow(
                'identifier contains invalid characters'
            );
            expect(() => validateIdentifier('table#1')).toThrow(
                'identifier contains invalid characters'
            );
            expect(() => validateIdentifier('col*')).toThrow(
                'identifier contains invalid characters'
            );
            expect(() => validateIdentifier('')).toThrow('identifier contains invalid characters');
        });
    });
});
