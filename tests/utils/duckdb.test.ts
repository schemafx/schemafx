import { describe, it, expect } from 'vitest';
import {
    buildSQLQuery,
    createDuckDBInstance,
    ingestStreamToDuckDB,
    convertDuckDBRowsToAppRows,
    ingestDataToDuckDB
} from '../../src/utils/duckdb.js';
import { Readable } from 'node:stream';
import { AppFieldType, type AppTable, QueryFilterOperator } from '../../src/types.js';

describe('DuckDB Utils', () => {
    describe('buildSQLQuery', () => {
        it('should generate simple select * query', () => {
            const { sql, params } = buildSQLQuery('users', {});
            expect(sql).toBe('select * from "users"');
            expect(params).toEqual([]);
        });

        it('should handle filters with escaping', () => {
            const { sql, params } = buildSQLQuery('users', {
                filters: [
                    { field: 'id', operator: QueryFilterOperator.Equals, value: 1 },
                    { field: 'name', operator: QueryFilterOperator.Equals, value: 'Alice' }
                ]
            });

            expect(sql).toContain('select * from "users"');
            expect(sql).toContain('"id" = ?');
            expect(sql).toContain('"name" = ?');
            expect(params).toEqual([1, 'Alice']);
        });

        it('should handle limit and offset', () => {
            const { sql, params } = buildSQLQuery('users', {
                limit: 10,
                offset: 5
            });

            expect(sql).toContain('limit ?');
            expect(sql).toContain('offset ?');
            expect(params).toEqual([10, 5]);
        });

        it('should handle special operators', () => {
            const { sql, params } = buildSQLQuery('users', {
                filters: [
                    { field: 'age', operator: QueryFilterOperator.GreaterThan, value: 18 },
                    { field: 'bio', operator: QueryFilterOperator.Contains, value: 'hello' }
                ]
            });

            expect(sql).toContain('"age" > ?');
            expect(sql).toContain('"bio" like ?');
            expect(params).toEqual([18, '%hello%']);
        });

        it('should escape malicious table names', () => {
            const { sql } = buildSQLQuery('users"; DROP TABLE users; --', {});
            expect(sql).toBe('select * from "users""; DROP TABLE users; --"');
        });
    });

    describe('ingestStreamToDuckDB', () => {
        it('should ingest data into DuckDB', async () => {
            const instance = await createDuckDBInstance();
            const connection = await instance.connect();
            const table: AppTable = {
                id: 'users',
                name: 'Users',
                connector: 'test',
                path: ['users'],
                fields: [
                    { id: 'id', name: 'ID', type: AppFieldType.Number },
                    { id: 'name', name: 'Name', type: AppFieldType.Text }
                ],
                actions: []
            };

            const stream = new Readable({
                objectMode: true,
                read() {
                    this.push({ id: 1, name: 'Alice' });
                    this.push({ id: 2, name: 'Bob' });
                    this.push(null);
                }
            });

            await ingestStreamToDuckDB(connection, stream, table, 'users');

            const reader = await connection.run('SELECT * FROM users ORDER BY id');
            const rows = await reader.getRows();

            expect(rows).toHaveLength(2);
            expect(rows[0]).toEqual([1, 'Alice']);
            expect(rows[1]).toEqual([2, 'Bob']);
        });

        it('should ingest and convert complex types correctly', async () => {
            const instance = await createDuckDBInstance();
            const connection = await instance.connect();
            const table: AppTable = {
                id: 'complex_data',
                name: 'Complex Data',
                connector: 'test',
                path: [],
                fields: [
                    {
                        id: 'struct_json',
                        name: 'Structured JSON',
                        type: AppFieldType.JSON,
                        fields: [{ id: 'a', name: 'A', type: AppFieldType.Number }]
                    },
                    {
                        id: 'simple_json',
                        name: 'Simple JSON',
                        type: AppFieldType.JSON
                    },
                    {
                        id: 'struct_list',
                        name: 'Structured List',
                        type: AppFieldType.List,
                        child: { id: 'item', name: 'Item', type: AppFieldType.Number }
                    },
                    {
                        id: 'simple_list',
                        name: 'Simple List',
                        type: AppFieldType.List
                    },
                    {
                        id: 'date_field',
                        name: 'Date Field',
                        type: AppFieldType.Date
                    }
                ],
                actions: []
            };

            const now = new Date();
            const data = [
                {
                    struct_json: { a: 100 },
                    simple_json: { foo: 'bar', baz: 123 },
                    struct_list: [1, 2, 3],
                    simple_list: ['a', 'b', 'c'],
                    date_field: now
                }
            ];

            const stream = new Readable({
                objectMode: true,
                read() {
                    this.push(data[0]);
                    this.push(null);
                }
            });

            await ingestStreamToDuckDB(connection, stream, table, 'complex_data');

            const reader = await connection.run('SELECT * FROM complex_data');
            const rows = await reader.getRows();

            // Convert back to app rows
            const convertedRows = convertDuckDBRowsToAppRows(
                rows.map(row => {
                    const obj: Record<string, unknown> = {};
                    table.fields.forEach((field, index) => {
                        obj[field.id] = row[index];
                    });

                    return obj;
                }),
                table
            );

            expect(convertedRows).toHaveLength(1);
            const row = convertedRows[0];

            // Verify structured JSON
            expect(row.struct_json).toEqual({ a: 100 });

            // Verify simple JSON (parsed from string)
            expect(row.simple_json).toEqual({ foo: 'bar', baz: 123 });

            // Verify structured List
            expect(row.struct_list).toEqual([1, 2, 3]);

            // Verify simple List (parsed from string)
            expect(row.simple_list).toEqual(['a', 'b', 'c']);

            // Verify Date
            expect(row.date_field).toEqual(now);
        });
    });

    describe('Extra Coverage', () => {
        it('should handle various field types during ingestion and retrieval', async () => {
            const instance = await createDuckDBInstance();
            const connection = await instance.connect();
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    { id: 'str', name: 'Str', type: AppFieldType.Text },
                    { id: 'num', name: 'Num', type: AppFieldType.Number },
                    { id: 'bool', name: 'Bool', type: AppFieldType.Boolean },
                    { id: 'date', name: 'Date', type: AppFieldType.Date },
                    {
                        id: 'list',
                        name: 'List',
                        type: AppFieldType.List,
                        child: { id: 'c', name: 'C', type: AppFieldType.Number }
                    },
                    { id: 'json', name: 'JSON', type: AppFieldType.JSON }, // Unstructured JSON
                    {
                        id: 'struct',
                        name: 'Struct',
                        type: AppFieldType.JSON,
                        fields: [{ id: 'subStr', name: 'SubStr', type: AppFieldType.Text }]
                    }
                ],
                actions: []
            };

            const now = new Date();
            const data = [
                {
                    str: 'hello',
                    num: 123,
                    bool: true,
                    date: now,
                    list: [1, 2, 3],
                    json: { foo: 'bar' },
                    struct: { subStr: 'world' }
                },
                {
                    str: 'nulls',
                    num: null,
                    bool: null,
                    date: null,
                    list: null,
                    json: null,
                    struct: null
                }
            ];

            await ingestDataToDuckDB(connection, data, table, 'test_table');

            // DuckDB ingestion ordering depends on field order in table
            const reader = await connection.run(
                'SELECT str, num, bool, date, list, json, struct FROM test_table ORDER BY str'
            );
            const rows = await reader.getRows();

            // Simulate DataService mapping
            const mappedRows = rows.map((row: any[]) => {
                const obj: Record<string, unknown> = {};
                // Fields order matches SELECT order above
                obj['str'] = row[0];
                obj['num'] = row[1];
                obj['bool'] = row[2];
                obj['date'] = row[3];
                obj['list'] = row[4];
                obj['json'] = row[5];
                obj['struct'] = row[6];
                return obj;
            });

            // Convert back
            const appRows = convertDuckDBRowsToAppRows(mappedRows, table);

            expect(appRows).toHaveLength(2);

            // Row 1
            expect(appRows[0].str).toBe('hello');
            expect(appRows[0].num).toBe(123);
            expect(appRows[0].bool).toBe(true);
            expect(new Date(appRows[0].date as any).getTime()).toBeCloseTo(now.getTime(), -2);
            expect(appRows[0].list).toEqual([1, 2, 3]);
            expect(appRows[0].json).toEqual({ foo: 'bar' });
            expect(appRows[0].struct).toEqual({ subStr: 'world' });

            // Row 2 (nulls)
            expect(appRows[1].str).toBe('nulls');
            expect(appRows[1].num).toBeUndefined();
        });

        it('should handle unstructured list (no child)', async () => {
            const instance = await createDuckDBInstance();
            const connection = await instance.connect();
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    { id: 'list', name: 'List', type: AppFieldType.List } // No child -> JSON string
                ],
                actions: []
            };

            const data = [{ list: ['a', 'b'] }];
            await ingestDataToDuckDB(connection, data, table, 'test_list');

            const reader = await connection.run('SELECT list FROM test_list');
            const rows = await reader.getRows();

            const mappedRows = rows.map((row: any[]) => {
                return { list: row[0] };
            });

            const appRows = convertDuckDBRowsToAppRows(mappedRows, table);

            expect(appRows[0].list).toEqual(['a', 'b']);
        });

        it('should handle JSON stored as string', async () => {
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [{ id: 'json', name: 'JSON', type: AppFieldType.JSON }],
                actions: []
            };

            // We can simulate this by manually creating rows with strings for JSON
            const mappedRows = [
                { json: '{"a": 1}' }, // valid json string
                { json: 'invalid-json' }, // invalid json string
                { json: 123 } // not a string
            ];

            const appRows = convertDuckDBRowsToAppRows(mappedRows, table);

            expect(appRows[0].json).toEqual({ a: 1 });
            expect(appRows[1].json).toBe('invalid-json'); // returns original value if parse fails
            expect(appRows[2].json).toBe(123);
        });

        it('should handle BigInt conversion', async () => {
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    { id: 'num', name: 'Num', type: AppFieldType.Number },
                    { id: 'str', name: 'Str', type: AppFieldType.Text }
                ],
                actions: []
            };

            const safeInt = BigInt(Number.MAX_SAFE_INTEGER);
            const unSafeInt = BigInt(Number.MAX_SAFE_INTEGER) + 100n;

            const mappedRows = [
                { num: safeInt, str: safeInt },
                { num: unSafeInt, str: unSafeInt }
            ];

            const appRows = convertDuckDBRowsToAppRows(mappedRows, table);

            expect(appRows[0].num).toBe(Number(safeInt));
            expect(appRows[0].str).toBe(Number(safeInt));

            expect(appRows[1].num).toBe(String(unSafeInt)); // Fallback to string for unsafe int
            expect(appRows[1].str).toBe(String(unSafeInt));
        });

        it('should handle BigInt dates', async () => {
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [{ id: 'date', name: 'Date', type: AppFieldType.Date }],
                actions: []
            };

            const ts = 1600000000000n; // millis
            const mappedRows = [
                { date: ts * 1000n } // micros as bigint
            ];

            const appRows = convertDuckDBRowsToAppRows(mappedRows, table);

            expect(appRows[0].date).toBeInstanceOf(Date);
            expect((appRows[0].date as Date).getTime()).toBe(Number(ts));
        });
    });
});
