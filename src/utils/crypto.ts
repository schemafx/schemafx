import { createCipheriv, createDecipheriv, createHash, randomBytes } from 'node:crypto';

/**
 * Hash the key.
 * @param text Text to hash.
 * @returns Hashed key.
 */
function deriveKey(text: string) {
    return createHash('sha256').update(text).digest();
}

/**
 * Encrypt the text.
 * @param text Text to encrypt.
 * @param secret Secret to use.
 * @returns Encrypted content.
 */
export function encrypt(text: string, secret: string): string {
    const iv = randomBytes(16);
    const cipher = createCipheriv('aes-256-gcm', deriveKey(secret), iv);

    const encrypted = Buffer.concat([cipher.update(text, 'utf8'), cipher.final()]);
    return `${iv.toString('hex')}:${cipher.getAuthTag().toString('hex')}:${encrypted.toString('hex')}`;
}

/**
 * Decrypt the text.
 * @param encryptedText Text to decrypt.
 * @param secret Secret to use.
 * @returns Decrypted content.
 */
export function decrypt(encryptedText: string, secret: string): string | undefined {
    try {
        const parts = encryptedText.split(':');
        if (parts.length !== 3) {
            throw new Error('Invalid encrypted text format');
        }

        const iv = Buffer.from(parts[0], 'hex');
        const authTag = Buffer.from(parts[1], 'hex');
        const encryptedContent = Buffer.from(parts[2], 'hex');
        const decipher = createDecipheriv('aes-256-gcm', deriveKey(secret), iv);
        decipher.setAuthTag(authTag);

        return Buffer.concat([decipher.update(encryptedContent), decipher.final()]).toString(
            'utf8'
        );
    } catch (error) {
        console.error('Decryption failed:', error);
        return;
    }
}
