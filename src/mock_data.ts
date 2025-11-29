import mock_data from './mock_data.json' with { type: 'json' };
import type { AppField, AppSchema, AppTable, AppTableRow, AppView } from './types.js';

const mockSchema = { ...mock_data } as AppSchema;

type ElementParentType = 'tables' | 'fields' | 'views';

export async function addElement(
    element: AppTable | AppView | AppField,
    partOf: ElementParentType,
    options?: {
        parentId?: string;
    }
) {
    if (partOf === 'tables') {
        mockSchema.tables.push(element as AppTable);
    } else if (partOf === 'views') {
        mockSchema.views.push(element as AppView);
    } else if (partOf === 'fields' && options?.parentId) {
        const oldFieldsLength =
            mockSchema.tables.find(table => table.id === options.parentId)?.fields?.length ?? 0;

        mockSchema.views = mockSchema.views.map(view => {
            if (view.tableId === options.parentId && view.fields.length === oldFieldsLength) {
                view.fields.push((element as AppField).id);
            }

            return view;
        });

        mockSchema.tables = mockSchema.tables.map(table => {
            if (table.id === options.parentId) table.fields.push(element as AppField);
            return table;
        });
    }

    return mockSchema;
}

export async function updateElement(
    element: AppTable | AppView | AppField,
    partOf: ElementParentType,
    options?: {
        parentId?: string;
    }
) {
    if (partOf === 'tables') {
        mockSchema.tables = mockSchema.tables.map(table =>
            table.id === element.id ? (element as AppTable) : table
        );
    } else if (partOf === 'views') {
        mockSchema.views = mockSchema.views.map(view =>
            view.id === element.id ? (element as AppView) : view
        );
    } else if (partOf === 'fields' && options?.parentId) {
        mockSchema.tables = mockSchema.tables.map(table => {
            if (table.id === options.parentId) {
                table.fields = table.fields.map(field =>
                    field.id === element.id ? (element as AppField) : field
                );
            }

            return table;
        });
    }

    return mockSchema;
}

export async function deleteElement(
    elementId: string,
    partOf: ElementParentType,
    options?: {
        parentId?: string;
    }
) {
    if (partOf === 'tables') {
        mockSchema.tables = mockSchema.tables.filter(table => table.id !== elementId);
    } else if (partOf === 'views') {
        mockSchema.views = mockSchema.views.filter(view => view.id !== elementId);
    } else if (partOf === 'fields' && options?.parentId) {
        mockSchema.views = mockSchema.views.map(view => {
            if (view.tableId === options.parentId) {
                view.fields = view.fields.filter(field => field !== elementId);
            }

            return view;
        });

        mockSchema.tables = mockSchema.tables.map(table => {
            if (table.id === options.parentId) {
                table.fields = table.fields.filter(field => field.id !== elementId);
            }

            return table;
        });
    }

    return mockSchema;
}

function _reorderElement<D>(oldIndex: number, newIndex: number, array: D[]) {
    let arr = [...array];
    const old = arr.splice(oldIndex, 1);
    arr.splice(newIndex, 0, ...old);
    return arr;
}

export async function reorderElement(
    oldIndex: number,
    newIndex: number,
    partOf: ElementParentType,
    options?: {
        parentId?: string;
    }
) {
    if (partOf === 'tables') {
        mockSchema.tables = _reorderElement(oldIndex, newIndex, mockSchema.tables);
    } else if (partOf === 'views') {
        mockSchema.views = _reorderElement(oldIndex, newIndex, mockSchema.views);
    } else if (partOf === 'fields' && options?.parentId) {
        mockSchema.tables = mockSchema.tables.map(table => {
            if (table.id === options.parentId) {
                table.fields = _reorderElement(oldIndex, newIndex, table.fields);
            }

            return table;
        });
    }

    return mockSchema;
}

export async function getSchema() {
    return mockSchema;
}

const tables: Map<string, AppTableRow[]> = new Map();

export async function getData(tableId: string) {
    return tables.get(tableId) ?? [];
}

export async function addRow(tableId: string, row?: AppTableRow) {
    const data = [...(await getData(tableId))];
    if (!row) return data;

    data.push(row);
    tables.set(tableId, data);

    return data;
}

export async function updateRow(tableId: string, rowIndex?: number, row?: AppTableRow) {
    const data = [...(await getData(tableId))];

    if (typeof rowIndex !== 'number' || !data[rowIndex] || !row) return data;

    data[rowIndex] = { ...row };
    tables.set(tableId, data);

    return data;
}

export async function deleteRow(tableId: string, rowIndex?: number) {
    const data = [...(await getData(tableId))];

    if (typeof rowIndex !== 'number' || !data[rowIndex]) return data;

    data.splice(rowIndex, 1);
    tables.set(tableId, data);

    return data;
}
