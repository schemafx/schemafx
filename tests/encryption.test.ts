import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createTestApp } from './testUtils.js';
import { encrypt, decrypt } from '../src/utils/encryption.js';
import MemoryConnector from '../src/connectors/memoryConnector.js';
import { AppFieldType, AppActionType } from '../src/types.js';

describe('Encryption Utils', () => {
    const key = '1234567890123456789012345678901234567890123456789012345678901234'; // 32 bytes in hex

    it('should encrypt and decrypt a string', () => {
        const text = 'Hello, World!';
        const encrypted = encrypt(text, key);

        expect(encrypted).not.toBe(text);
        expect(encrypted).toMatch(/^[0-9a-f]+:[0-9a-f]+:[0-9a-f]+$/);

        const decrypted = decrypt(encrypted, key);
        expect(decrypted).toBe(text);
    });

    it('should throw error on invalid format', () => {
        expect(() => decrypt('invalid', key)).toThrow('Invalid encrypted text format.');
    });

    it('should fail with wrong key', () => {
        const text = 'Hello';
        const encrypted = encrypt(text, key);

        expect(() => decrypt(encrypted, '0'.repeat(64))).toThrow();
    });
});

describe('Encrypted Fields Integration', () => {
    const encryptionKey = '1234567890123456789012345678901234567890123456789012345678901234';

    it('should encrypt data in the connector and decrypt it on retrieval', async () => {
        const { app, connector, token } = await createTestApp(true, {
            encryptionKey
        });

        // Setup schema with encrypted fields
        await app.dataService.setSchema('enc-app', {
            id: 'enc-app',
            name: 'Encrypted App',
            tables: [
                {
                    id: 'secrets',
                    name: 'Secrets',
                    connector: connector.id,
                    path: ['secrets'],
                    fields: [
                        { id: 'id', name: 'ID', type: AppFieldType.Text, isKey: true },
                        { id: 'secret', name: 'Secret', type: AppFieldType.Text, encrypted: true },
                        {
                            id: 'confidential',
                            name: 'Confidential',
                            type: AppFieldType.JSON,
                            encrypted: true,
                            fields: [{ id: 'code', name: 'Code', type: AppFieldType.Number }]
                        },
                        { id: 'public', name: 'Public', type: AppFieldType.Text }
                    ],
                    actions: [{ id: 'add', name: 'Add', type: AppActionType.Add }]
                }
            ],
            views: []
        });

        const server = app.fastifyInstance;

        await server.inject({
            method: 'POST',
            url: '/api/apps/enc-app/data/secrets',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                actionId: 'add',
                rows: [
                    {
                        id: '1',
                        secret: 'MySecretValue',
                        confidential: { code: 1234 },
                        public: 'PublicValue'
                    }
                ]
            }
        });

        const storedData = await connector.getData!({
            id: 'secrets',
            name: 'Secrets',
            connector: connector.id,
            path: ['secrets'],
            fields: [],
            actions: []
        });

        expect(storedData).toHaveLength(1);

        const row = storedData[0];
        expect(row.secret).not.toBe('MySecretValue');
        expect(row.secret).toMatch(/^[0-9a-f]+:[0-9a-f]+:[0-9a-f]+$/);
        expect(row.confidential).not.toEqual({ code: 1234 });
        expect(typeof row.confidential).toBe('string');
        expect(row.public).toBe('PublicValue');

        const response = await server.inject({
            method: 'GET',
            url: '/api/apps/enc-app/data/secrets',
            headers: { Authorization: `Bearer ${token}` }
        });

        expect(response.statusCode).toBe(200);

        const body = JSON.parse(response.payload);
        expect(body).toHaveLength(1);
        expect(body[0].secret).toBe('MySecretValue');
        expect(body[0].confidential).toEqual({ code: 1234 });
        expect(body[0].public).toBe('PublicValue');

        await server.close();
    });

    it('should handle missing encryption key gracefully (store plain?) or throw?', async () => {
        const { app, connector, token } = await createTestApp(true);

        await app.dataService.setSchema('plain-app', {
            id: 'plain-app',
            name: 'Plain App',
            tables: [
                {
                    id: 'secrets',
                    name: 'Secrets',
                    connector: connector.id,
                    path: ['secrets'],
                    fields: [
                        { id: 'id', name: 'ID', type: AppFieldType.Text, isKey: true },
                        { id: 'secret', name: 'Secret', type: AppFieldType.Text, encrypted: true }
                    ],
                    actions: [{ id: 'add', name: 'Add', type: AppActionType.Add }]
                }
            ],
            views: []
        });

        const server = app.fastifyInstance;

        await server.inject({
            method: 'POST',
            url: '/api/apps/plain-app/data/secrets',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                actionId: 'add',
                rows: [{ id: '1', secret: 'MySecretValue' }]
            }
        });

        const storedData = await connector.getData!({
            id: 'secrets',
            name: 'Secrets',
            connector: connector.id,
            path: ['secrets'],
            fields: [],
            actions: []
        });

        expect(storedData[0].secret).toBe('MySecretValue'); // Not encrypted because no key

        await server.close();
    });

    it('should handle falsy values in JSON fields', async () => {
        const { app, connector, token } = await createTestApp(true, {
            encryptionKey
        });

        await app.dataService.setSchema('falsy-app', {
            id: 'falsy-app',
            name: 'Falsy App',
            tables: [
                {
                    id: 'data',
                    name: 'Data',
                    connector: connector.id,
                    path: ['data'],
                    fields: [
                        { id: 'id', name: 'ID', type: AppFieldType.Text, isKey: true },
                        { id: 'flag', name: 'Flag', type: AppFieldType.JSON, encrypted: true },
                        { id: 'zero', name: 'Zero', type: AppFieldType.JSON, encrypted: true }
                    ],
                    actions: [{ id: 'add', name: 'Add', type: AppActionType.Add }]
                }
            ],
            views: []
        });

        const server = app.fastifyInstance;

        const postResponse = await server.inject({
            method: 'POST',
            url: '/api/apps/falsy-app/data/data',
            headers: { Authorization: `Bearer ${token}` },
            payload: {
                actionId: 'add',
                rows: [
                    {
                        id: '1',
                        flag: false,
                        zero: 0
                    }
                ]
            }
        });

        expect(postResponse.statusCode).toBe(200);

        const storedData = await connector.getData!({
            id: 'data',
            name: 'Data',
            connector: connector.id,
            path: ['data'],
            fields: [],
            actions: []
        });

        expect(storedData[0].flag).not.toBe(false);
        expect(storedData[0].flag).toMatch(/^[0-9a-f]+:[0-9a-f]+:[0-9a-f]+$/);
        expect(storedData[0].zero).not.toBe(0);
        expect(storedData[0].zero).toMatch(/^[0-9a-f]+:[0-9a-f]+:[0-9a-f]+$/);

        const response = await server.inject({
            method: 'GET',
            url: '/api/apps/falsy-app/data/data',
            headers: { Authorization: `Bearer ${token}` }
        });

        const body = JSON.parse(response.payload);
        expect(body[0].flag).toBe(false);
        expect(body[0].zero).toBe(0);

        await server.close();
    });

    it('should throw error when decryption fails', async () => {
        const { app, connector, token } = await createTestApp(true, {
            encryptionKey
        });

        await app.dataService.setSchema('error-app', {
            id: 'error-app',
            name: 'Error App',
            tables: [
                {
                    id: 'secrets',
                    name: 'Secrets',
                    connector: connector.id,
                    path: ['secrets'],
                    fields: [
                        { id: 'id', name: 'ID', type: AppFieldType.Text, isKey: true },
                        { id: 'secret', name: 'Secret', type: AppFieldType.Text, encrypted: true }
                    ],
                    actions: [{ id: 'add', name: 'Add', type: AppActionType.Add }]
                }
            ],
            views: []
        });

        await connector.addRow!(
            {
                id: 'secrets',
                name: 'Secrets',
                connector: connector.id,
                path: ['secrets'],
                fields: [],
                actions: []
            },
            { id: '1', secret: 'invalid-encrypted-string' }
        );

        const server = app.fastifyInstance;

        const response = await server.inject({
            method: 'GET',
            url: '/api/apps/error-app/data/secrets',
            headers: { Authorization: `Bearer ${token}` }
        });

        expect(response.statusCode).toBe(500);

        await server.close();
    });
});
