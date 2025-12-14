import { createCipheriv, createDecipheriv, randomBytes } from 'node:crypto';

const ALGORITHM = 'aes-256-gcm';
const IV_LENGTH = 16;

/**
 * Encrypts a string using AES-256-GCM.
 * @param text Text to encrypt.
 * @param key Encryption key (hex string or raw bytes).
 * @returns Encrypted text in format iv:authTag:encryptedText (hex).
 */
export function encrypt(text: string, key: string): string {
    const iv = randomBytes(IV_LENGTH);
    const cipher = createCipheriv(ALGORITHM, Buffer.from(key, 'hex'), iv);

    let encrypted = cipher.update(text, 'utf8', 'hex');
    encrypted += cipher.final('hex');

    const authTag = cipher.getAuthTag();

    return `${iv.toString('hex')}:${authTag.toString('hex')}:${encrypted}`;
}

/**
 * Decrypts a string using AES-256-GCM.
 * @param text Encrypted text in format iv:authTag:encryptedText (hex).
 * @param key Encryption key (hex string or raw bytes).
 * @returns Decrypted text.
 */
export function decrypt(text: string, key: string): string {
    const [ivHex, authTagHex, encryptedText] = text.split(':');

    if (!ivHex || !authTagHex || !encryptedText) {
        throw new Error('Invalid encrypted text format.');
    }

    const iv = Buffer.from(ivHex, 'hex');
    const authTag = Buffer.from(authTagHex, 'hex');
    const decipher = createDecipheriv(ALGORITHM, Buffer.from(key, 'hex'), iv);

    decipher.setAuthTag(authTag);

    let decrypted = decipher.update(encryptedText, 'hex', 'utf8');
    decrypted += decipher.final('utf8');

    return decrypted;
}
