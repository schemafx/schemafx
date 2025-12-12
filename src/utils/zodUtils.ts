import { z } from 'zod';
import type { LRUCache } from 'lru-cache';
import { type AppAction, type AppField, AppFieldType, type AppTable } from '../types.js';

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
export function zodFromField(field: AppField): z.ZodType {
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
            if (field.fields && field.fields.length > 0) {
                fld = zodFromFields(field.fields);
            } else {
                fld = z.any();
            }
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
export function zodFromTable(table: AppTable, appId: string, cache: LRUCache<string, z.ZodType>) {
    const cacheKey = `${appId}:${table.id}`;
    if (cache.has(cacheKey)) return cache.get(cacheKey)!;

    const validator = zodFromFields(table.fields);
    cache.set(cacheKey, validator);
    return validator;
}

/**
 * Generate an AppField definition from a Zod Schema.
 * @param id ID of the field.
 * @param schema Zod Schema.
 * @returns AppField definition.
 */
export function fieldFromZod(id: string, schema: z.ZodType): AppField {
    let type = AppFieldType.Text;
    let isRequired = true;
    let options: string[] | undefined;
    let minValue: number | undefined;
    let maxValue: number | undefined;
    let minLength: number | undefined;
    let maxLength: number | undefined;
    let startDate: Date | undefined;
    let endDate: Date | undefined;
    let fields: AppField[] | undefined;
    let child: AppField | undefined;

    let def: z.core.$ZodTypeDef = schema.def;
    let currentSchema: z.ZodType = schema;

    while (def.type === 'optional' || def.type === 'nullable' || def.type === 'default') {
        if (def.type === 'optional' || def.type === 'nullable' || def.type === 'default') {
            isRequired = false;
        }

        if ('innerType' in def) {
            currentSchema = def.innerType as z.ZodType;
            def = currentSchema.def;
        } else if ('innerType' in currentSchema) {
            currentSchema = currentSchema.innerType as z.ZodType;
            def = currentSchema.def;
        } else {
            break;
        }
    }

    switch (def.type) {
        case 'string':
            type = AppFieldType.Text;
            if (def.checks) {
                for (const check of def.checks) {
                    const checkDef =
                        'def' in check ? (check.def as z.core.$ZodCheckDef) : check._zod?.def;

                    if (checkDef) {
                        if ((checkDef as z.core.$ZodCheckStringFormatDef).format === 'email') {
                            type = AppFieldType.Email;
                        }

                        if (checkDef.check === 'min_length') {
                            minLength = (checkDef as z.core.$ZodCheckMinLengthDef).minimum;
                        }

                        if (checkDef.check === 'max_length') {
                            maxLength = (checkDef as z.core.$ZodCheckMaxLengthDef).maximum;
                        }
                    }
                }
            }
            break;
        case 'number':
            type = AppFieldType.Number;
            if (def.checks) {
                for (const check of def.checks) {
                    const checkDef =
                        'def' in check ? (check.def as z.core.$ZodCheckDef) : check._zod?.def;

                    if (checkDef) {
                        if (checkDef.check === 'greater_than') {
                            minValue = (checkDef as z.core.$ZodCheckGreaterThanDef).value as number;
                        }

                        if (checkDef.check === 'less_than') {
                            maxValue = (checkDef as z.core.$ZodCheckLessThanDef).value as number;
                        }
                    }
                }
            }
            break;
        case 'boolean':
            type = AppFieldType.Boolean;
            break;
        case 'date':
            type = AppFieldType.Date;
            if (def.checks) {
                for (const check of def.checks) {
                    const checkDef =
                        'def' in check ? (check.def as z.core.$ZodCheckDef) : check._zod?.def;

                    if (checkDef) {
                        if (checkDef.check === 'greater_than') {
                            startDate = new Date(
                                (checkDef as z.core.$ZodCheckGreaterThanDef).value as number
                            );
                        }

                        if (checkDef.check === 'less_than') {
                            endDate = new Date(
                                (checkDef as z.core.$ZodCheckLessThanDef).value as number
                            );
                        }
                    }
                }
            }
            break;
        case 'enum':
            type = AppFieldType.Dropdown;
            if ((def as z.core.$ZodEnumDef).entries) {
                options = Object.values((def as z.core.$ZodEnumDef).entries).map(v => v.toString());
            }

            break;
        case 'array':
            type = AppFieldType.List;
            if ((currentSchema as z.ZodArray).element) {
                child = fieldFromZod('child', (currentSchema as z.ZodArray).element as z.ZodType);
            }

            break;
        case 'object':
            type = AppFieldType.JSON;
            if ((currentSchema as z.ZodObject).shape) {
                fields = Object.entries((currentSchema as z.ZodObject).shape).map(([key, val]) =>
                    fieldFromZod(key, val as z.ZodType)
                );
            }

            break;
        case 'record':
        case 'tuple':
        case 'map':
        case 'set':
        case 'intersection':
        case 'union':
            type = AppFieldType.JSON;
            break;
        default:
            if (
                ['any', 'unknown', 'void', 'undefined', 'null', 'symbol', 'nan', 'never'].includes(
                    def.type
                )
            ) {
                type = AppFieldType.JSON;
            } else type = AppFieldType.JSON;

            break;
    }

    return {
        id,
        name: id,
        type,
        isRequired,
        options,
        minValue,
        maxValue,
        minLength,
        maxLength,
        startDate,
        endDate,
        fields,
        child
    };
}

export type AppTableFromZodOptions = {
    id: string;
    name: string;
    connector: string;
    path: string[];
    primaryKey: string;
    actions?: AppAction[];
};

/**
 * Generate an AppTable definition from a Zod Object Schema.
 * @param schema Zod Object Schema.
 * @param options Table Options.
 * @returns AppTable definition.
 */
export function tableFromZod(schema: z.ZodObject, options: AppTableFromZodOptions): AppTable {
    const fields = Object.entries(schema.shape).map(([key, val]) =>
        fieldFromZod(key, val as z.ZodType)
    );

    const primaryKey = options.primaryKey ?? 'id';
    const keyField = fields.find(f => f.id === primaryKey);

    if (!keyField) {
        throw new Error(
            `Table ${options.name} must have a primary key field matching '${primaryKey}'.`
        );
    }

    keyField.isKey = true;

    return {
        id: options.id,
        name: options.name,
        connector: options.connector,
        path: options.path,
        fields,
        actions: options.actions ?? []
    };
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
