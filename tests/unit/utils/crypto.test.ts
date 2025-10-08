import { describe, it, expect, vi } from 'vitest';
import { encrypt, decrypt } from '../../../src/utils/crypto';

describe('crypto', () => {
    const secret = 'this-is-a-32-byte-secret-string';

    /**
     * Validate a string.
     * @param str String to test.
     */
    function validate(str: string) {
        const encrypted = encrypt(str, secret);
        expect(encrypted.split(':').length).toEqual(3);
        expect(decrypt(encrypted, secret));
    }

    it('should work with an empty string', () => validate(''));
    it('should work with a long string', () => validate('a'.repeat(1024 * 1024)));
    it('should handle special characters', () => validate('@^~`'));

    it('should return undefined for invalid encrypted text format', () => {
        const consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
        expect(decrypt('invalid-text', secret)).toBeUndefined();
        expect(consoleErrorSpy).toHaveBeenCalledWith(
            'Decryption failed:',
            new Error('Invalid encrypted text format')
        );
        consoleErrorSpy.mockRestore();
    });

    it('should return undefined for wrong secret', () => {
        const consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
        const encrypted = encrypt('some text', secret);
        expect(decrypt(encrypted, 'wrong-secret')).toBeUndefined();
        expect(consoleErrorSpy).toHaveBeenCalled();
        consoleErrorSpy.mockRestore();
    });

    it('should return undefined for tampered authTag', () => {
        const consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
        const encrypted = encrypt('some text', secret);
        const parts = encrypted.split(':');
        parts[1] = '0'.repeat(parts[1].length); // Tamper authTag
        const tamperedEncrypted = parts.join(':');
        expect(decrypt(tamperedEncrypted, secret)).toBeUndefined();
        expect(consoleErrorSpy).toHaveBeenCalled();
        consoleErrorSpy.mockRestore();
    });
});
