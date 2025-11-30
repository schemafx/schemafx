import mock_data from './mock_data.json' with { type: 'json' };
import type { AppSchema, AppTableRow } from './types.js';

const schemas: Map<string, AppSchema> = new Map();
const tables: Map<string, Map<string, AppTableRow[]>> = new Map();

export async function getSchema(appId: string) {
    let schema = schemas.get(appId);

    if (!schema) {
        schema = { ...mock_data } as AppSchema;
        await saveSchema(appId, schema);
    }

    return schema;
}

export async function saveSchema(appId: string, schema: AppSchema) {
    schemas.set(appId, schema);
    return schema;
}

export async function deleteSchema(appId: string) {
    schemas.delete(appId);
}

export async function getData(appId: string, tableId: string) {
    if (!tables.has(appId)) tables.set(appId, new Map());
    return [...(tables.get(appId)?.get(tableId) ?? [])];
}

export async function addRow(appId: string, tableId: string, row?: AppTableRow) {
    const data = await getData(appId, tableId);
    if (!row) return data;

    data.push(row);

    if (!tables.has(appId)) tables.set(appId, new Map());
    tables.get(appId)?.set(tableId, data);

    return data;
}

export async function updateRow(
    appId: string,
    tableId: string,
    rowIndex?: number,
    row?: AppTableRow
) {
    const data = await getData(appId, tableId);

    if (typeof rowIndex !== 'number' || !data[rowIndex] || !row) return data;

    data[rowIndex] = { ...row };

    if (!tables.has(appId)) tables.set(appId, new Map());
    tables.get(appId)?.set(tableId, data);

    return data;
}

export async function deleteRow(appId: string, tableId: string, rowIndex?: number) {
    const data = await getData(appId, tableId);

    if (typeof rowIndex !== 'number' || !data[rowIndex]) return data;

    data.splice(rowIndex, 1);

    if (!tables.has(appId)) tables.set(appId, new Map());
    tables.get(appId)?.set(tableId, data);

    return data;
}
