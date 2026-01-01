import fs from 'fs';
import os from 'os';
import path from 'path';
import { pipeline } from 'stream/promises';

import knex from 'knex';

import {
    DuckDBInstance,
    DuckDBStructValue,
    DuckDBListValue,
    type DuckDBValue
} from '@duckdb/node-api';

import {
    QueryFilterOperator,
    DataSourceType,
    DataSourceFormat,
    type TableQueryOptions,
    type DataSourceDefinition
} from '../types.js';

export { QueryFilterOperator };

const qb = knex({ client: 'pg' });

/**
 * Recursively convert DuckDB types to plain JavaScript values.
 */
function toPlainValue(value: unknown): unknown {
    if (value === null || value === undefined) return value;

    if (value instanceof DuckDBStructValue) {
        const result: Record<string, unknown> = {};
        for (const [key, val] of Object.entries(value.entries)) result[key] = toPlainValue(val);

        return result;
    }

    if (value instanceof DuckDBListValue) return value.items.map(toPlainValue);

    // DuckDB returns bigint for integer types - convert to number
    if (typeof value === 'bigint') return Number(value);

    // Handle plain objects (e.g., from getRowObjects)
    if (typeof value === 'object' && value !== null) {
        const result: Record<string, unknown> = {};
        for (const [key, val] of Object.entries(value)) result[key] = toPlainValue(val);

        return result;
    }

    return value;
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

    if (options.orderBy) {
        query = query.orderBy(options.orderBy.column, options.orderBy.direction);
    }

    if (typeof options.limit === 'number') query = query.limit(options.limit);
    if (typeof options.offset === 'number') query = query.offset(options.offset);

    const { sql, bindings } = query.toSQL();
    return { sql, params: bindings };
}

/**
 * Options for querying data from a DataSourceDefinition.
 */
export type GetDataOptions = {
    /** Data source definition describing how to access the data */
    source: DataSourceDefinition;
    /** Query options (filters, limit, offset) */
    query?: TableQueryOptions;
    /** Optional row decoder for encrypted data */
    decodeRow?: (row: Record<string, unknown>) => Record<string, unknown>;
};

/**
 * Map DataSourceFormat to DuckDB read function.
 */
function getReadFunction(format?: DataSourceFormat): string {
    switch (format) {
        case DataSourceFormat.Json:
            return 'read_json_auto';
        case DataSourceFormat.Csv:
            return 'read_csv_auto';
        case DataSourceFormat.Parquet:
            return 'read_parquet';
        case DataSourceFormat.Ndjson:
            return 'read_ndjson_auto';
        case DataSourceFormat.Auto:
        default:
            return 'read_json_auto';
    }
}

/**
 * Query data from a DataSourceDefinition using DuckDB.
 * Handles different source types (inline, file, url, etc.) and
 * performs efficient querying without unnecessary data copying.
 */
export async function getData({
    source,
    query,
    decodeRow
}: GetDataOptions): Promise<Record<string, unknown>[]> {
    const tempTableName = `t_${Math.random().toString(36).substring(7)}`;
    const quotedName = qb.ref(tempTableName).toString();
    const dbInstance = await DuckDBInstance.create(':memory:');
    const connection = await dbInstance.connect();

    let tempFilePath: string | undefined;

    try {
        // Create table based on source type
        switch (source.type) {
            case DataSourceType.Inline: {
                // For inline data, we need to write to a temp file
                if (source.data.length === 0) return [];

                tempFilePath = path.join(
                    os.tmpdir(),
                    `duckdb_${Date.now()}_${Math.random().toString(36).substring(7)}.json`
                );

                fs.writeFileSync(tempFilePath, JSON.stringify(source.data));

                await connection.run(
                    `CREATE TABLE ${quotedName} AS SELECT * FROM read_json_auto('${tempFilePath}')`
                );

                break;
            }

            case DataSourceType.File: {
                // For file sources, DuckDB can read directly from the file
                await connection.run(
                    `CREATE TABLE ${quotedName} AS SELECT * FROM ${getReadFunction(source.options?.format)}('${source.path.replace(/\\/g, '/')}')`
                );

                break;
            }

            case DataSourceType.Url: {
                // Create HTTP secret with headers if provided
                const headers = source.options?.headers;
                if (headers && Object.keys(headers).length > 0) {
                    await connection.run(`
                        CREATE SECRET http_auth (
                            TYPE HTTP,
                            EXTRA_HTTP_HEADERS MAP {${Object.entries(headers)
                                .map(([k, v]) => `'${k}': '${v}'`)
                                .join(', ')}}
                        );
                    `);
                }

                // DuckDB can fetch directly from HTTP/HTTPS URLs
                await connection.run(
                    `CREATE TABLE ${quotedName} AS SELECT * FROM ${getReadFunction(source.options?.format)}('${source.url}')`
                );

                break;
            }

            case DataSourceType.Stream: {
                // For streams, pipe directly to a temp file using streams
                tempFilePath = path.join(
                    os.tmpdir(),
                    `duckdb_${Date.now()}_${Math.random().toString(36).substring(7)}.json`
                );

                // Use pipeline to efficiently stream data to file
                await pipeline(source.stream, fs.createWriteStream(tempFilePath));

                await connection.run(
                    `CREATE TABLE ${quotedName} AS SELECT * FROM ${getReadFunction(source.options?.format)}('${tempFilePath}')`
                );

                break;
            }

            case DataSourceType.Connection: {
                // Install and load the specified DuckDB extension dynamically
                const { module, connectionString, target } = source;
                const connStr = connectionString.replace(/\\/g, '/');
                const attachName = `db_${Math.random().toString(36).substring(7)}`;
                const moduleUpper = module.toUpperCase();

                await connection.run(`INSTALL ${module}; LOAD ${module};`);
                await connection.run(
                    `ATTACH '${connStr}' AS ${attachName} (TYPE ${moduleUpper}, READ_ONLY)`
                );

                await connection.run(
                    `CREATE TABLE ${quotedName} AS SELECT * FROM ${attachName}.${target}`
                );

                break;
            }
        }

        // Build and execute query
        const { sql, params } = buildSQLQuery(tempTableName, query || {});
        const reader = await connection.run(sql, params as unknown[] as DuckDBValue[]);

        // Get rows as objects and convert DuckDB types to plain JS values
        const rows = await reader.getRowObjects();

        // Convert DuckDB types (bigint, DuckDBListValue, etc.) to plain JS,
        // and apply optional decryption
        return rows.map(row => {
            const plain = toPlainValue(row) as Record<string, unknown>;
            return decodeRow ? decodeRow(plain) : plain;
        });
    } finally {
        // Cleanup
        connection.closeSync();
        if (tempFilePath) {
            try {
                fs.unlinkSync(tempFilePath);
            } catch {
                // Ignore cleanup errors
            }
        }
    }
}
