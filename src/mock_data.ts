import mock_data from './mock_data.json' with { type: 'json' };
import type { AppField, AppSchema, AppTable, AppTableRow, AppView } from './types.js';

const schemas: Map<string, AppSchema> = new Map();
const tables: Map<string, AppTableRow[]> = new Map();

type ElementParentType = 'tables' | 'fields' | 'views';

export async function addElement(
    appId: string,
    element: AppTable | AppView | AppField,
    partOf: ElementParentType,
    options?: {
        parentId?: string;
    }
) {
    const schema = await getSchema(appId);

    if (partOf === 'tables') {
        schema.tables.push(element as AppTable);
    } else if (partOf === 'views') {
        schema.views.push(element as AppView);
    } else if (partOf === 'fields' && options?.parentId) {
        const oldFieldsLength =
            schema.tables.find(table => table.id === options.parentId)?.fields?.length ?? 0;

        schema.views = schema.views.map(view => {
            if (view.tableId === options.parentId && view.fields.length === oldFieldsLength) {
                view.fields.push((element as AppField).id);
            }

            return view;
        });

        schema.tables = schema.tables.map(table => {
            if (table.id === options.parentId) table.fields.push(element as AppField);
            return table;
        });
    }

    return saveSchema(appId, schema);
}

export async function updateElement(
    appId: string,
    element: AppTable | AppView | AppField,
    partOf: ElementParentType,
    options?: {
        parentId?: string;
    }
) {
    const schema = await getSchema(appId);

    if (partOf === 'tables') {
        schema.tables = schema.tables.map(table =>
            table.id === element.id ? (element as AppTable) : table
        );
    } else if (partOf === 'views') {
        schema.views = schema.views.map(view =>
            view.id === element.id ? (element as AppView) : view
        );
    } else if (partOf === 'fields' && options?.parentId) {
        schema.tables = schema.tables.map(table => {
            if (table.id === options.parentId) {
                table.fields = table.fields.map(field =>
                    field.id === element.id ? (element as AppField) : field
                );
            }

            return table;
        });
    }

    return saveSchema(appId, schema);
}

export async function deleteElement(
    appId: string,
    elementId: string,
    partOf: ElementParentType,
    options?: {
        parentId?: string;
    }
) {
    const schema = await getSchema(appId);

    if (partOf === 'tables') {
        schema.tables = schema.tables.filter(table => table.id !== elementId);
    } else if (partOf === 'views') {
        schema.views = schema.views.filter(view => view.id !== elementId);
    } else if (partOf === 'fields' && options?.parentId) {
        schema.views = schema.views.map(view => {
            if (view.tableId === options.parentId) {
                view.fields = view.fields.filter(field => field !== elementId);
            }

            return view;
        });

        schema.tables = schema.tables.map(table => {
            if (table.id === options.parentId) {
                table.fields = table.fields.filter(field => field.id !== elementId);
            }

            return table;
        });
    }

    return saveSchema(appId, schema);
}

function _reorderElement<D>(oldIndex: number, newIndex: number, array: D[]) {
    let arr = [...array];
    const old = arr.splice(oldIndex, 1);
    arr.splice(newIndex, 0, ...old);
    return arr;
}

export async function reorderElement(
    appId: string,
    oldIndex: number,
    newIndex: number,
    partOf: ElementParentType,
    options?: {
        parentId?: string;
    }
) {
    const schema = await getSchema(appId);

    if (partOf === 'tables') {
        schema.tables = _reorderElement(oldIndex, newIndex, schema.tables);
    } else if (partOf === 'views') {
        schema.views = _reorderElement(oldIndex, newIndex, schema.views);
    } else if (partOf === 'fields' && options?.parentId) {
        schema.tables = schema.tables.map(table => {
            if (table.id === options.parentId) {
                table.fields = _reorderElement(oldIndex, newIndex, table.fields);
            }

            return table;
        });
    }

    return saveSchema(appId, schema);
}

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
    return [...(tables.get(tableId) ?? [])];
}

export async function addRow(appId: string, tableId: string, row?: AppTableRow) {
    const data = await getData(appId, tableId);
    if (!row) return data;

    data.push(row);
    tables.set(tableId, data);

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
    tables.set(tableId, data);

    return data;
}

export async function deleteRow(appId: string, tableId: string, rowIndex?: number) {
    const data = await getData(appId, tableId);

    if (typeof rowIndex !== 'number' || !data[rowIndex]) return data;

    data.splice(rowIndex, 1);
    tables.set(tableId, data);

    return data;
}
