import {
    DuckDBInstance,
    DuckDBAppender,
    DuckDBConnection,
    DuckDBTimestampMillisecondsValue
} from '@duckdb/node-api';
import { AppFieldType, type AppTable, QueryFilterOperator, TableQueryOptions } from '../types.js';
import { Readable } from 'node:stream';
import knex from 'knex';

const qb = knex({ client: 'pg' });

export async function createDuckDBInstance() {
    return DuckDBInstance.create(':memory:');
}

function mapFieldTypeToDuckDB(type: AppFieldType): string {
    switch (type) {
        case AppFieldType.Text:
        case AppFieldType.Email:
        case AppFieldType.Dropdown:
        case AppFieldType.Reference:
        case AppFieldType.JSON:
        case AppFieldType.List:
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

function appendValue(appender: DuckDBAppender, type: AppFieldType, value: unknown) {
    if (value === null || value === undefined) {
        appender.appendNull();
        return;
    }

    switch (type) {
        case AppFieldType.Text:
        case AppFieldType.Email:
        case AppFieldType.Dropdown:
        case AppFieldType.Reference:
        case AppFieldType.JSON:
        case AppFieldType.List:
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
            .map(f => `${qb.ref(f.id).toString()} ${mapFieldTypeToDuckDB(f.type)}`)
            .join(', ')})`
    );

    const appender = await connection.createAppender(tableName);

    for await (const row of stream) {
        for (const field of table.fields) appendValue(appender, field.type, row[field.id]);
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
