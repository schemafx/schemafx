import { z } from 'zod';
import { LRUCache } from 'lru-cache';
import { type AppField, AppFieldType, type AppTable, type AppTableRow } from '../types.js';

/**
 * Standard error response schema.
 */
export const ErrorResponseSchema = z
    .object({
        error: z.string().meta({ description: 'Error type or code' }),
        message: z.string().meta({ description: 'Error message' })
    })
    .meta({ description: 'Default Error' });

/**
 * Generate a Zod schema for a single AppField.
 * @param field Field to generate the validator from.
 * @returns Zod schema for the field.
 */
export function zodFromField(field: AppField): z.ZodTypeAny {
    let fld;

    switch (field.type) {
        case AppFieldType.Number:
            fld = z.number();
            if (typeof field.minValue === 'number') fld = fld.min(field.minValue);
            if (typeof field.maxValue === 'number') fld = fld.max(field.maxValue);
            break;
        case AppFieldType.Boolean:
            fld = z.boolean();
            break;
        case AppFieldType.Date:
            fld = z.date();
            if (field.startDate) fld = fld.min(field.startDate);
            if (field.endDate) fld = fld.max(field.endDate);
            break;
        case AppFieldType.Email:
            fld = z.email();
            break;
        case AppFieldType.Dropdown:
            fld = z.enum((field.options as [string, ...string[]]) ?? []);
            break;
        case AppFieldType.JSON:
            fld = zodFromFields(field.fields ?? []);
            break;
        case AppFieldType.List:
            if (field.child) fld = z.array(zodFromField(field.child));
            else fld = z.array(z.any());

            break;
        default:
            fld = z.string();
            if (typeof field.minLength === 'number') fld = fld.min(field.minLength);
            if (typeof field.maxLength === 'number') fld = fld.max(field.maxLength);
            break;
    }

    if (!field.isRequired) fld = fld.optional().nullable();
    return fld;
}

/**
 * Generate a Zod object from a list of AppField definitions.
 * @param fields List of fields to generate the validator from.
 * @returns Zod object validator.
 */
export function zodFromFields(fields: AppField[]) {
    return z.strictObject(Object.fromEntries(fields.map(field => [field.id, zodFromField(field)])));
}

/**
 * Generate a Zod object from an AppTable definition.
 * @param table Table to generate the validator from.
 * @returns Zod object validator from table.
 */
export function zodFromTable(
    table: AppTable,
    appId: string,
    cache: LRUCache<string, z.ZodTypeAny>
) {
    const cacheKey = `${appId}:${table.id}`;
    if (cache.has(cacheKey)) return cache.get(cacheKey)!;

    const validator = zodFromFields(table.fields);
    cache.set(cacheKey, validator);
    return validator;
}

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

/** Fastify Schema for table queries. */
export const tableQuerySchema = {
    params: z.object({
        appId: z.string().min(1).meta({ description: 'Application ID' }),
        tableId: z.string().min(1).meta({ description: 'Table ID' })
    }),
    querystring: z.object({
        query: z.string().optional().meta({ description: 'JSON stringified query options' })
    }),
    response: {
        200: z.any().meta({ description: 'Query results' }),
        500: ErrorResponseSchema,
        400: ErrorResponseSchema.extend({
            details: z
                .array(
                    z.object({
                        field: z.string().meta({ description: 'Field name' }),
                        message: z.string().meta({ description: 'Error message' }),
                        code: z.string().meta({ description: 'Error code' })
                    })
                )
                .optional()
                .meta({ description: 'Validation error details' })
        }).meta({ description: 'Validation error' })
    }
};
