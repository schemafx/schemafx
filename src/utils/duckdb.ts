import { Readable } from 'node:stream';
import knex from 'knex';

import {
    DuckDBInstance,
    type DuckDBAppender,
    type DuckDBConnection,
    DuckDBTimestampMillisecondsValue,
    type DuckDBValue,
    type DuckDBType,
    VARCHAR,
    DOUBLE,
    BOOLEAN,
    TIMESTAMP,
    LIST,
    STRUCT,
    type DuckDBListType,
    type DuckDBStructType,
    structValue,
    listValue,
    DuckDBStructValue,
    DuckDBListValue,
    DuckDBTimestampValue
} from '@duckdb/node-api';

import {
    AppFieldType,
    type AppTable,
    type AppField,
    QueryFilterOperator,
    type TableQueryOptions
} from '../types.js';

const qb = knex({ client: 'pg' });

export async function createDuckDBInstance() {
    return DuckDBInstance.create(':memory:');
}

function mapFieldToDuckDBTypeString(field: AppField): string {
    switch (field.type) {
        case AppFieldType.Text:
        case AppFieldType.Email:
        case AppFieldType.Dropdown:
        case AppFieldType.Reference:
            return 'VARCHAR';
        case AppFieldType.JSON:
            if (field.encrypted) return 'VARCHAR';
            if (field.fields && field.fields.length > 0) {
                return `STRUCT(${field.fields
                    .map(f => `${qb.ref(f.id).toString()} ${mapFieldToDuckDBTypeString(f)}`)
                    .join(', ')})`;
            }

            return 'VARCHAR';
        case AppFieldType.List:
            if (field.child) return `${mapFieldToDuckDBTypeString(field.child)}[]`;
            return 'VARCHAR';
        case AppFieldType.Number:
            return 'DOUBLE';
        case AppFieldType.Boolean:
            return 'BOOLEAN';
        case AppFieldType.Date:
            return 'TIMESTAMP';
        default:
            return 'VARCHAR';
    }
}

function getDuckDBType(field: AppField): DuckDBType {
    switch (field.type) {
        case AppFieldType.Text:
        case AppFieldType.Email:
        case AppFieldType.Dropdown:
        case AppFieldType.Reference:
            return VARCHAR;
        case AppFieldType.Number:
            return DOUBLE;
        case AppFieldType.Boolean:
            return BOOLEAN;
        case AppFieldType.Date:
            return TIMESTAMP;
        case AppFieldType.List:
            if (field.child) return LIST(getDuckDBType(field.child));
            return VARCHAR;
        case AppFieldType.JSON:
            if (field.encrypted) return VARCHAR;
            if (field.fields && field.fields.length > 0) {
                const entries: Record<string, DuckDBType> = {};
                for (const f of field.fields) entries[f.id] = getDuckDBType(f);

                return STRUCT(entries);
            }

            return VARCHAR;
        default:
            return VARCHAR;
    }
}

function prepareDuckDBValue(field: AppField, value: unknown): DuckDBValue {
    if (value === null || value === undefined) {
        return null;
    }

    switch (field.type) {
        case AppFieldType.Text:
        case AppFieldType.Email:
        case AppFieldType.Dropdown:
        case AppFieldType.Reference:
            return String(value);
        case AppFieldType.Number:
            return Number(value);
        case AppFieldType.Boolean:
            return Boolean(value);
        case AppFieldType.Date:
            return new DuckDBTimestampMillisecondsValue(
                BigInt((value instanceof Date ? value : new Date(String(value))).getTime())
            );
        case AppFieldType.List:
            if (Array.isArray(value) && field.child) {
                const items = value.map(item => prepareDuckDBValue(field.child!, item));
                return listValue(items);
            }

            return String(value);
        case AppFieldType.JSON:
            if (
                typeof value === 'object' &&
                value !== null &&
                field.fields &&
                field.fields.length > 0
            ) {
                const structEntries: Record<string, DuckDBValue> = {};
                for (const subField of field.fields) {
                    structEntries[subField.id] = prepareDuckDBValue(
                        subField,
                        (value as Record<string, unknown>)[subField.id]
                    );
                }

                return structValue(structEntries);
            }

            return JSON.stringify(value);
        default:
            return String(value);
    }
}

function appendValue(appender: DuckDBAppender, field: AppField, value: unknown) {
    if (value === null || value === undefined) {
        appender.appendNull();
        return;
    }

    switch (field.type) {
        case AppFieldType.Text:
        case AppFieldType.Email:
        case AppFieldType.Dropdown:
        case AppFieldType.Reference:
            appender.appendVarchar(String(value));
            break;
        case AppFieldType.Number:
            appender.appendDouble(Number(value));
            break;
        case AppFieldType.Boolean:
            appender.appendBoolean(Boolean(value));
            break;
        case AppFieldType.Date:
            appender.appendTimestampMilliseconds(
                new DuckDBTimestampMillisecondsValue(
                    BigInt((value instanceof Date ? value : new Date(String(value))).getTime())
                )
            );

            break;
        case AppFieldType.List:
            if (field.child) {
                const preparedList = prepareDuckDBValue(field, value);
                if (typeof preparedList === 'string') {
                    appender.appendVarchar(preparedList);
                } else {
                    appender.appendList(
                        preparedList as unknown as DuckDBListValue,
                        getDuckDBType(field) as DuckDBListType
                    );
                }
            } else {
                appender.appendVarchar(JSON.stringify(value));
            }

            break;
        case AppFieldType.JSON:
            if (field.encrypted) appender.appendVarchar(value as string);
            else if (field.fields && field.fields.length > 0) {
                const preparedStruct = prepareDuckDBValue(field, value);
                if (typeof preparedStruct === 'string') {
                    appender.appendVarchar(preparedStruct);
                } else {
                    appender.appendStruct(
                        preparedStruct as unknown as DuckDBStructValue,
                        getDuckDBType(field) as DuckDBStructType
                    );
                }
            } else {
                appender.appendVarchar(JSON.stringify(value));
            }

            break;
        default:
            appender.appendVarchar(String(value));
    }
}

export async function ingestStreamToDuckDB(
    connection: DuckDBConnection,
    stream: Readable,
    table: AppTable,
    tableName: string
) {
    await connection.run(
        `CREATE OR REPLACE TABLE ${qb.ref(tableName).toString()} (${table.fields
            .map(f => `${qb.ref(f.id).toString()} ${mapFieldToDuckDBTypeString(f)}`)
            .join(', ')})`
    );

    const appender = await connection.createAppender(tableName);

    for await (const row of stream) {
        for (const field of table.fields) appendValue(appender, field, row[field.id]);
        appender.endRow();
    }

    appender.closeSync();
}

export async function ingestDataToDuckDB(
    connection: DuckDBConnection,
    data: Record<string, unknown>[],
    table: AppTable,
    tableName: string
) {
    return ingestStreamToDuckDB(connection, Readable.from(data), table, tableName);
}

function convertDuckDBValue(value: unknown, field: AppField): unknown {
    if (value === null || value === undefined) return undefined;

    switch (field.type) {
        case AppFieldType.JSON:
            if (field.fields && field.fields.length > 0) {
                // Expected Struct
                if (value instanceof DuckDBStructValue) {
                    const result: Record<string, unknown> = {};
                    for (const subField of field.fields) {
                        if (!(subField.id in value.entries)) continue;

                        result[subField.id] = convertDuckDBValue(
                            value.entries[subField.id],
                            subField
                        );
                    }

                    return result;
                }

                // If we get here, it might be null or something unexpected
                return value;
            } else {
                // Expected JSON String
                if (typeof value === 'string') {
                    try {
                        return JSON.parse(value);
                    } catch {}
                }

                return value;
            }

        case AppFieldType.List:
            if (field.child) {
                // Expected List of values
                if (value instanceof DuckDBListValue) {
                    return value.items.map(item => convertDuckDBValue(item, field.child!));
                } else if (Array.isArray(value)) {
                    return value.map(item => convertDuckDBValue(item, field.child!));
                }

                return value;
            } else if (typeof value === 'string') {
                // Expected JSON String (List)
                try {
                    return JSON.parse(value);
                } catch {}
            }

            return value;

        case AppFieldType.Date:
            if (value instanceof DuckDBTimestampValue) {
                // micros is BigInt
                return new Date(Number(value.micros) / 1000);
            } else if (value instanceof DuckDBTimestampMillisecondsValue) {
                return new Date(Number(value.millis));
            } else if (typeof value === 'bigint') {
                // Fallback for number/bigint
                return new Date(Number(value) / 1000);
            } else if (typeof value === 'number') {
                return new Date(value);
            }

            return value;

        default:
            // Primitive types
            if (typeof value === 'bigint') {
                if (value <= Number.MAX_SAFE_INTEGER && value >= Number.MIN_SAFE_INTEGER) {
                    return Number(value);
                }

                return String(value);
            }

            return value;
    }
}

export function convertDuckDBRowsToAppRows(
    rows: Record<string, unknown>[],
    table: AppTable
): Record<string, unknown>[] {
    return rows.map(row => {
        const result: Record<string, unknown> = {};

        for (const field of table.fields) {
            if (!(field.id in row)) continue;
            let val = row[field.id];

            // Apply conversion
            val = convertDuckDBValue(val, field);

            // Special handling for JSON fields that were stored as VARCHAR (string)
            if (field.type === AppFieldType.JSON && (!field.fields || field.fields.length === 0)) {
                if (typeof val === 'string') {
                    try {
                        val = JSON.parse(val);
                    } catch {}
                }
            }

            result[field.id] = val;
        }

        return result;
    });
}

export function buildSQLQuery(tableName: string, options: TableQueryOptions) {
    let query = qb(tableName).select('*');

    if (options.filters && options.filters.length > 0) {
        for (const filter of options.filters) {
            switch (filter.operator) {
                case QueryFilterOperator.Equals:
                    query = query.where(filter.field, filter.value);
                    break;
                case QueryFilterOperator.NotEqual:
                    query = query.whereNot(filter.field, filter.value);
                    break;
                case QueryFilterOperator.GreaterThan:
                    query = query.where(filter.field, '>', filter.value);
                    break;
                case QueryFilterOperator.GreaterThanOrEqualTo:
                    query = query.where(filter.field, '>=', filter.value);
                    break;
                case QueryFilterOperator.LowerThan:
                    query = query.where(filter.field, '<', filter.value);
                    break;
                case QueryFilterOperator.LowerThanOrEqualTo:
                    query = query.where(filter.field, '<=', filter.value);
                    break;
                case QueryFilterOperator.Contains:
                    query = query.where(filter.field, 'like', `%${filter.value}%`);
                    break;
            }
        }
    }

    if (typeof options.limit === 'number') query = query.limit(options.limit);
    if (typeof options.offset === 'number') query = query.offset(options.offset);

    const { sql, bindings } = query.toSQL();
    return { sql, params: bindings };
}
