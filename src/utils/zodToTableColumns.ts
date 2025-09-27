import type { ZodObject } from 'zod';
import { type TableColumnDefinition, TableColumnType } from '../schemas';

/**
 * Parse a Zod definition.
 * @param def Zod definition.
 * @returns Type and props.
 */
function parseZodDef(def?: Record<string, unknown>): {
    type: TableColumnType;
    typeProps?: unknown;
} {
    switch (def?.type as string) {
        case 'object':
            return {
                type: TableColumnType.Json,
                typeProps: {
                    columns: Object.entries(def?.shape ?? {}).map(e => ({
                        name: e[0],
                        ...parseZodDef(e[1].def)
                    }))
                }
            };
        case 'boolean':
            return { type: TableColumnType.Boolean };
        case 'date':
            return { type: TableColumnType.Date };
        case 'array':
            return {
                type: TableColumnType.Array,
                typeProps: parseZodDef(
                    (def?.element as Record<string, unknown>)?.def as Record<string, unknown>
                )
            };
        case 'number':
        case 'bigint':
            return { type: TableColumnType.Number };
        case 'default':
        case 'optional':
            return parseZodDef(def?.innerType as Record<string, unknown>);
        case 'lazy':
            const getter = (def?.def as Record<string, unknown>)?.getter;
            const obj = typeof getter === 'function' ? getter() : {};

            // Resolve non-object and array to avoid loops.
            return parseZodDef(
                obj?.type === 'object' || obj?.type === 'array' ? { type: obj.type } : obj
            );
    }

    return { type: TableColumnType.String };
}

/**
 * Parse a Zod definition into TableColumnDefinition.
 * @param obj Zod object definition.
 * @returns Column definitions.
 */
export function zodToTableColumns(obj: ZodObject, key?: string): TableColumnDefinition[] {
    return Object.entries(obj.shape).map(e => ({
        name: e[0],
        ...parseZodDef(e[1].def),
        key: e[0] === key
    }));
}
