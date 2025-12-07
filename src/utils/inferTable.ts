import { randomUUID } from 'node:crypto';
import { AppFieldType, type AppTableRow, type AppTable, type AppField } from '../types.js';

/**
 * Infer a Table Schema from data.
 * @param name Name of the Table.
 * @param path Path to the Table.
 * @param data Data to infer from.
 * @param connectorId Id of the Connector.
 * @returns Inferred Table Schema.
 */
export default function inferTable(
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
