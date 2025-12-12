import type { AppTable, AppTableRow } from '../types.js';

/**
 * Reorders elements within an array.
 * @param oldIndex Previous index.
 * @param newIndex New index.
 * @param array Array containing the data.
 * @returns Reordered array.
 */
export function reorderElement<D>(oldIndex: number, newIndex: number, array: D[]) {
    let arr = [...array];
    const old = arr.splice(oldIndex, 1);
    arr.splice(newIndex, 0, ...old);
    return arr;
}

export function validateTableKeys(table: AppTable) {
    const hasKey = table.fields.some(f => f.isKey);
    if (!hasKey) throw new Error(`Table ${table.name} must have at least one key field.`);
}

export function extractKeys(
    row: AppTableRow,
    keyFields: (keyof AppTableRow)[]
): Record<keyof AppTableRow, unknown> {
    const key: Record<keyof AppTableRow, unknown> = {};
    for (const fieldId of keyFields) {
        if (row[fieldId] !== undefined) key[fieldId] = row[fieldId];
    }

    return key;
}
