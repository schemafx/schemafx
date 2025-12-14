import { randomUUID } from 'node:crypto';
import { type AppField, AppFieldType, type AppTableRow, type AppTable } from '../types.js';
import { decrypt, encrypt } from './encryption.js';

export function encodeRow(
    row: Record<string, unknown>,
    table: AppTable,
    encryptionKey?: string
): Record<string, unknown> {
    if (!encryptionKey) return row;
    const processedRow = { ...row };

    for (const field of table.fields) {
        if (
            field.encrypted &&
            (field.type === AppFieldType.Text || field.type === AppFieldType.JSON) &&
            processedRow[field.id] !== undefined &&
            processedRow[field.id] !== null
        ) {
            let valueToEncrypt = processedRow[field.id];
            if (field.type === AppFieldType.JSON) valueToEncrypt = JSON.stringify(valueToEncrypt);

            processedRow[field.id] = encrypt(String(valueToEncrypt), encryptionKey);
        }
    }

    return processedRow;
}

export function decodeRow(
    row: Record<string, unknown>,
    table: AppTable,
    encryptionKey?: string
): Record<string, unknown> {
    if (!encryptionKey) return row;
    const processedRow = { ...row };

    for (const field of table.fields) {
        let value = processedRow[field.id];

        if (
            field.encrypted &&
            value !== undefined &&
            value !== null &&
            typeof value === 'string' &&
            (field.type === AppFieldType.Text || field.type === AppFieldType.JSON)
        ) {
            const decrypted = decrypt(value, encryptionKey);
            if (field.type === AppFieldType.JSON) value = JSON.parse(decrypted);
            else value = decrypted;

            processedRow[field.id] = value;
        }
    }

    return processedRow;
}

/**
 * Infer a Table Schema from data.
 * @param name Name of the Table.
 * @param path Path to the Table.
 * @param data Data to infer from.
 * @param connectorId Id of the Connector.
 * @returns Inferred Table Schema.
 */
export function inferTable(
    name: string,
    path: string[],
    data: AppTableRow[],
    connectorId: string
): AppTable {
    const keys = new Set<string>();
    for (const row of data) Object.keys(row).forEach(k => keys.add(k));

    const fields: AppField[] = [];
    let hasKey = false;
    for (const key of keys) {
        let detectedType: AppFieldType | null = null;

        for (const row of data) {
            const val = row[key];
            if (val === null || val === undefined) continue;

            let type = AppFieldType.Text;
            if (typeof val === 'number') type = AppFieldType.Number;
            else if (typeof val === 'boolean') type = AppFieldType.Boolean;
            else if (val instanceof Date) type = AppFieldType.Date;
            else if (Array.isArray(val)) type = AppFieldType.List;
            else if (typeof val === 'object') type = AppFieldType.JSON;

            if (detectedType && detectedType !== type) {
                detectedType = AppFieldType.Text;
                break;
            }

            if (!detectedType) detectedType = type;
        }

        const isKey = key === 'id';
        fields.push({
            id: key,
            name: key,
            type: detectedType || AppFieldType.Text,
            isRequired: false,
            isKey
        });

        if (isKey) hasKey = true;
    }

    if (!hasKey && fields[0]) fields[0].isKey = true;

    return {
        id: randomUUID(),
        name,
        connector: connectorId,
        path,
        fields,
        actions: []
    };
}
