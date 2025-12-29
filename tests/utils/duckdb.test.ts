import { describe, it, expect } from 'vitest';
import {
    buildSQLQuery,
    createDuckDBInstance,
    ingestStreamToDuckDB,
    convertDuckDBRowsToAppRows,
    ingestDataToDuckDB
} from '../../src/utils/duckdb.js';
import { Readable } from 'node:stream';
import {
    AppFieldType,
    type AppTable,
    type AppField,
    QueryFilterOperator
} from '../../src/types.js';

describe('DuckDB Utils', () => {
    describe('buildSQLQuery', () => {
        it('should generate simple select * query', () => {
            const { sql, params } = buildSQLQuery('users', {});
            expect(sql).toBe('select * from "users"');
            expect(params).toEqual([]);
        });

        it('should handle operators', () => {
            const { sql } = buildSQLQuery('users', {
                filters: [
                    { field: 'eq', operator: QueryFilterOperator.Equals, value: 1 },
                    { field: 'neq', operator: QueryFilterOperator.NotEqual, value: 1 },
                    { field: 'gt', operator: QueryFilterOperator.GreaterThan, value: 1 },
                    { field: 'gte', operator: QueryFilterOperator.GreaterThanOrEqualTo, value: 1 },
                    { field: 'lt', operator: QueryFilterOperator.LowerThan, value: 1 },
                    { field: 'lte', operator: QueryFilterOperator.LowerThanOrEqualTo, value: 1 },
                    { field: 'co', operator: QueryFilterOperator.Contains, value: 1 }
                ]
            });

            expect(sql).toContain('"eq" = ?');
            expect(sql.includes('not "eq" = ?')).toBe(false);
            expect(sql).toContain('not "neq" = ?');
            expect(sql).toContain('"gt" > ?');
            expect(sql).toContain('"gte" >= ?');
            expect(sql).toContain('"lt" < ?');
            expect(sql).toContain('"lte" <= ?');
            expect(sql).toContain('"co" like ?');
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

    describe('convertDuckDBRowsToAppRows', () => {
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
            const fields = ['str', 'num', 'bool', 'date', 'list', 'json', 'struct'];
            const reader = await connection.run(
                `SELECT ${fields.join(', ')} FROM test_table ORDER BY str`
            );
            const rows = await reader.getRows();

            // Simulate DataService mapping
            const mappedRows = rows.map((row: any[]) => {
                const obj: Record<string, unknown> = {};
                for (let i = 0; i < fields.length; i++) obj[fields[i]] = row[i];
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

            const mappedRows = rows.map((row: any[]) => ({ list: row[0] }));
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

        it('should handle Number dates', async () => {
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [{ id: 'date', name: 'Date', type: AppFieldType.Date }],
                actions: []
            };

            const ts = Date.now();
            const appRows = convertDuckDBRowsToAppRows([{ date: ts }], table);

            expect(appRows[0].date).toBeInstanceOf(Date);
            expect((appRows[0].date as Date).getTime()).toBe(ts);
        });

        it('should handle Text dates', async () => {
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [{ id: 'date', name: 'Date', type: AppFieldType.Date }],
                actions: []
            };

            const ts = new Date().toISOString();
            const appRows = convertDuckDBRowsToAppRows([{ date: ts }], table);

            expect(appRows[0].date).toBeInstanceOf(Date);
            expect((appRows[0].date as Date).toISOString()).toBe(ts);
        });

        it('should not throw for unknown date formats', async () => {
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [{ id: 'date', name: 'Date', type: AppFieldType.Date }],
                actions: []
            };

            const appRows = convertDuckDBRowsToAppRows([{ date: false }], table);
            expect(appRows[0].date).toBe(null);
        });
    });

    describe('Unknown/Default field types', () => {
        it('should handle unknown field types in ingestion and retrieval', async () => {
            const instance = await createDuckDBInstance();
            const connection = await instance.connect();

            // Create a field with an unknown type (cast to bypass TypeScript)
            const unknownField: AppField = {
                id: 'unknown',
                name: 'Unknown',
                type: 'SomeUnknownType' as AppFieldType
            };

            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [unknownField],
                actions: []
            };

            const data = [{ unknown: 'test-value' }, { unknown: null }];

            await ingestDataToDuckDB(connection, data, table, 'test_unknown');

            const reader = await connection.run('SELECT unknown FROM test_unknown');
            const rows = await reader.getRows();

            const mappedRows = rows.map((row: any[]) => ({ unknown: row[0] }));
            const appRows = convertDuckDBRowsToAppRows(mappedRows, table);

            expect(appRows[0].unknown).toBe('test-value');
            expect(appRows[1].unknown).toBeUndefined();
        });

        it('should handle encrypted JSON fields', async () => {
            const instance = await createDuckDBInstance();
            const connection = await instance.connect();

            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    {
                        id: 'encrypted_json',
                        name: 'Encrypted JSON',
                        type: AppFieldType.JSON,
                        encrypted: true,
                        fields: [{ id: 'secret', name: 'Secret', type: AppFieldType.Text }]
                    }
                ],
                actions: []
            };

            // Encrypted JSON should be stored as VARCHAR (encrypted string)
            const encryptedValue = 'encrypted-payload-string';
            const data = [{ encrypted_json: encryptedValue }];

            await ingestDataToDuckDB(connection, data, table, 'test_encrypted');

            const reader = await connection.run('SELECT encrypted_json FROM test_encrypted');
            const rows = await reader.getRows();

            expect(rows[0][0]).toBe(encryptedValue);
        });

        it('should handle List without child definition', async () => {
            const instance = await createDuckDBInstance();
            const connection = await instance.connect();

            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    {
                        id: 'list_no_child',
                        name: 'List No Child',
                        type: AppFieldType.List
                        // no child field
                    }
                ],
                actions: []
            };

            const data = [{ list_no_child: [1, 2, 3] }];

            await ingestDataToDuckDB(connection, data, table, 'test_list_no_child');

            const reader = await connection.run('SELECT list_no_child FROM test_list_no_child');
            const rows = await reader.getRows();

            // Should be stored as JSON string
            expect(rows[0][0]).toBe('[1,2,3]');

            // Verify conversion back
            const mappedRows = rows.map((row: any[]) => ({ list_no_child: row[0] }));
            const appRows = convertDuckDBRowsToAppRows(mappedRows, table);

            expect(appRows[0].list_no_child).toEqual([1, 2, 3]);
        });

        it('should handle nested struct with encrypted JSON field', async () => {
            const instance = await createDuckDBInstance();
            const connection = await instance.connect();

            // Nested struct contains an encrypted JSON field - tests getDuckDBType with encrypted field
            // The outer_struct is not encrypted and has fields, so appendStruct will be called
            // which triggers getDuckDBType to build the struct type including the encrypted inner field
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    {
                        id: 'outer_struct',
                        name: 'Outer Struct',
                        type: AppFieldType.JSON,
                        encrypted: false,
                        fields: [
                            {
                                id: 'inner_encrypted',
                                name: 'Inner Encrypted',
                                type: AppFieldType.JSON,
                                encrypted: true
                            },
                            {
                                id: 'normal_field',
                                name: 'Normal Field',
                                type: AppFieldType.Text
                            }
                        ]
                    }
                ],
                actions: []
            };

            // Provide an actual object for the struct value so it goes through appendStruct path
            const data = [
                { outer_struct: { inner_encrypted: 'encrypted-data', normal_field: 'hello' } }
            ];
            await ingestDataToDuckDB(connection, data, table, 'test_nested_encrypted');

            const reader = await connection.run('SELECT outer_struct FROM test_nested_encrypted');
            const rows = await reader.getRows();

            expect(rows).toHaveLength(1);
            // The struct should have been inserted correctly
            expect(rows[0][0]).toBeTruthy();
        });

        it('should handle List of encrypted JSON items', async () => {
            const instance = await createDuckDBInstance();
            const connection = await instance.connect();

            // List with encrypted JSON child
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    {
                        id: 'encrypted_list',
                        name: 'Encrypted List',
                        type: AppFieldType.List,
                        child: {
                            id: 'item',
                            name: 'Item',
                            type: AppFieldType.JSON,
                            encrypted: true
                        }
                    }
                ],
                actions: []
            };

            const data = [{ encrypted_list: ['enc1', 'enc2'] }];
            await ingestDataToDuckDB(connection, data, table, 'test_encrypted_list');

            const reader = await connection.run('SELECT encrypted_list FROM test_encrypted_list');
            const rows = await reader.getRows();

            expect(rows).toHaveLength(1);
        });

        it('should handle List with nested unknown child type', async () => {
            const instance = await createDuckDBInstance();
            const connection = await instance.connect();

            // List with an unknown child type - tests getDuckDBType default for List child
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    {
                        id: 'list_unknown_child',
                        name: 'List Unknown Child',
                        type: AppFieldType.List,
                        child: {
                            id: 'item',
                            name: 'Item',
                            type: 'UnknownChildType' as AppFieldType
                        }
                    }
                ],
                actions: []
            };

            const data = [{ list_unknown_child: ['a', 'b', 'c'] }];
            await ingestDataToDuckDB(connection, data, table, 'test_list_unknown_child');

            const reader = await connection.run(
                'SELECT list_unknown_child FROM test_list_unknown_child'
            );
            const rows = await reader.getRows();

            expect(rows).toHaveLength(1);
        });

        it('should handle struct with nested unknown field type', async () => {
            const instance = await createDuckDBInstance();
            const connection = await instance.connect();

            // Struct with an unknown field type - tests getDuckDBType default in struct iteration
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    {
                        id: 'struct_unknown',
                        name: 'Struct Unknown',
                        type: AppFieldType.JSON,
                        fields: [
                            {
                                id: 'unknown_field',
                                name: 'Unknown Field',
                                type: 'SomeUnknownType' as AppFieldType
                            }
                        ]
                    }
                ],
                actions: []
            };

            const data = [{ struct_unknown: { unknown_field: 'test-value' } }];
            await ingestDataToDuckDB(connection, data, table, 'test_struct_unknown');

            const reader = await connection.run('SELECT struct_unknown FROM test_struct_unknown');
            const rows = await reader.getRows();

            expect(rows).toHaveLength(1);
        });

        it('should handle struct containing a List without child', async () => {
            const instance = await createDuckDBInstance();
            const connection = await instance.connect();

            // Struct contains a List field without child
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    {
                        id: 'struct_with_list',
                        name: 'Struct With List',
                        type: AppFieldType.JSON,
                        fields: [
                            {
                                id: 'list_no_child',
                                name: 'List No Child',
                                type: AppFieldType.List
                                // no child - should map to VARCHAR
                            }
                        ]
                    }
                ],
                actions: []
            };

            const data = [{ struct_with_list: { list_no_child: '[1,2,3]' } }];
            await ingestDataToDuckDB(connection, data, table, 'test_struct_with_list');

            const reader = await connection.run(
                'SELECT struct_with_list FROM test_struct_with_list'
            );
            const rows = await reader.getRows();

            expect(rows).toHaveLength(1);
        });

        it('should handle struct with encrypted JSON inside struct', async () => {
            const instance = await createDuckDBInstance();
            const connection = await instance.connect();

            // Create a deeply nested structure to ensure getDuckDBType is called on encrypted JSON
            // The outer struct must have fields and NOT be encrypted, containing an encrypted JSON field
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    {
                        id: 'wrapper',
                        name: 'Wrapper',
                        type: AppFieldType.JSON,
                        encrypted: false,
                        fields: [
                            {
                                id: 'encrypted_inner',
                                name: 'Encrypted Inner',
                                type: AppFieldType.JSON,
                                encrypted: true
                            }
                        ]
                    }
                ],
                actions: []
            };

            // The object value triggers appendStruct path, which calls getDuckDBType
            // getDuckDBType iterates over fields and hits encrypted_inner
            const data = [{ wrapper: { encrypted_inner: 'secret-encrypted-value' } }];
            await ingestDataToDuckDB(connection, data, table, 'test_encrypted_in_struct');

            const reader = await connection.run('SELECT wrapper FROM test_encrypted_in_struct');
            const rows = await reader.getRows();

            expect(rows).toHaveLength(1);
            expect(rows[0][0]).toBeTruthy();
        });

        it('should handle List containing encrypted JSON child type', async () => {
            const instance = await createDuckDBInstance();
            const connection = await instance.connect();

            // List with encrypted JSON child to trigger getDuckDBType
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    {
                        id: 'list_encrypted_json',
                        name: 'List Encrypted JSON',
                        type: AppFieldType.List,
                        child: {
                            id: 'enc_item',
                            name: 'Encrypted Item',
                            type: AppFieldType.JSON,
                            encrypted: true
                        }
                    }
                ],
                actions: []
            };

            // Pass array values - this calls appendList which calls getDuckDBType(field)
            // getDuckDBType for List calls getDuckDBType(field.child) which is encrypted JSON
            const data = [{ list_encrypted_json: ['encrypted1', 'encrypted2'] }];
            await ingestDataToDuckDB(connection, data, table, 'test_list_encrypted_json');

            const reader = await connection.run(
                'SELECT list_encrypted_json FROM test_list_encrypted_json'
            );
            const rows = await reader.getRows();

            expect(rows).toHaveLength(1);
        });

        it('should handle struct with Boolean field inside', async () => {
            const instance = await createDuckDBInstance();
            const connection = await instance.connect();

            // JSON struct with a Boolean field to exercise getDuckDBType's Boolean case
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    {
                        id: 'struct_with_bool',
                        name: 'Struct With Bool',
                        type: AppFieldType.JSON,
                        fields: [{ id: 'flag', name: 'Flag', type: AppFieldType.Boolean }]
                    }
                ],
                actions: []
            };

            const data = [{ struct_with_bool: { flag: true } }];
            await ingestDataToDuckDB(connection, data, table, 'test_struct_bool');

            const reader = await connection.run('SELECT struct_with_bool FROM test_struct_bool');
            const rows = await reader.getRows();

            expect(rows).toHaveLength(1);
        });

        it('should handle List with Boolean child', async () => {
            const instance = await createDuckDBInstance();
            const connection = await instance.connect();

            // List with Boolean child to exercise getDuckDBType's Boolean case recursively
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    {
                        id: 'bool_list',
                        name: 'Bool List',
                        type: AppFieldType.List,
                        child: { id: 'item', name: 'Item', type: AppFieldType.Boolean }
                    }
                ],
                actions: []
            };

            const data = [{ bool_list: [true, false, true] }];
            await ingestDataToDuckDB(connection, data, table, 'test_bool_list');

            const reader = await connection.run('SELECT bool_list FROM test_bool_list');
            const rows = await reader.getRows();

            expect(rows).toHaveLength(1);
        });

        it('should handle struct containing unstructured JSON field', async () => {
            const instance = await createDuckDBInstance();
            const connection = await instance.connect();

            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    {
                        id: 'outer_struct',
                        name: 'Outer Struct',
                        type: AppFieldType.JSON,
                        fields: [
                            {
                                id: 'inner_json',
                                name: 'Inner JSON',
                                type: AppFieldType.JSON
                                // No fields = unstructured JSON, getDuckDBType returns VARCHAR
                            }
                        ]
                    }
                ],
                actions: []
            };

            const data = [{ outer_struct: { inner_json: { foo: 'bar', num: 123 } } }];
            await ingestDataToDuckDB(connection, data, table, 'test_struct_unstructured_json');

            const reader = await connection.run(
                'SELECT outer_struct FROM test_struct_unstructured_json'
            );
            const rows = await reader.getRows();

            expect(rows).toHaveLength(1);
        });

        it('should handle List with unstructured JSON child', async () => {
            const instance = await createDuckDBInstance();
            const connection = await instance.connect();

            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    {
                        id: 'json_list',
                        name: 'JSON List',
                        type: AppFieldType.List,
                        child: {
                            id: 'item',
                            name: 'Item',
                            type: AppFieldType.JSON
                            // No fields = unstructured JSON
                        }
                    }
                ],
                actions: []
            };

            const data = [{ json_list: [{ a: 1 }, { b: 2 }] }];
            await ingestDataToDuckDB(connection, data, table, 'test_list_unstructured_json');

            const reader = await connection.run(
                'SELECT json_list FROM test_list_unstructured_json'
            );
            const rows = await reader.getRows();

            expect(rows).toHaveLength(1);
        });

        it('should handle null values inside struct fields', async () => {
            const instance = await createDuckDBInstance();
            const connection = await instance.connect();

            // Struct with a field that has null value to hit prepareDuckDBValue null branch
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    {
                        id: 'my_struct',
                        name: 'My Struct',
                        type: AppFieldType.JSON,
                        fields: [
                            { id: 'name', name: 'Name', type: AppFieldType.Text },
                            { id: 'count', name: 'Count', type: AppFieldType.Number }
                        ]
                    }
                ],
                actions: []
            };

            // Pass null for one of the struct fields
            const data = [{ my_struct: { name: 'test', count: null } }];
            await ingestDataToDuckDB(connection, data, table, 'test_struct_null_field');

            const reader = await connection.run('SELECT my_struct FROM test_struct_null_field');
            const rows = await reader.getRows();

            expect(rows).toHaveLength(1);
        });

        it('should handle null values inside list items', async () => {
            const instance = await createDuckDBInstance();
            const connection = await instance.connect();

            // List with null items to hit prepareDuckDBValue null branch for list items
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    {
                        id: 'nullable_list',
                        name: 'Nullable List',
                        type: AppFieldType.List,
                        child: { id: 'item', name: 'Item', type: AppFieldType.Text }
                    }
                ],
                actions: []
            };

            // Pass array with null item
            const data = [{ nullable_list: ['value1', null, 'value2'] }];
            await ingestDataToDuckDB(connection, data, table, 'test_list_null_items');

            const reader = await connection.run('SELECT nullable_list FROM test_list_null_items');
            const rows = await reader.getRows();

            expect(rows).toHaveLength(1);
        });

        it('should handle Date inside struct', async () => {
            const instance = await createDuckDBInstance();
            const connection = await instance.connect();

            // Struct with a Date field to hit prepareDuckDBValue Date case
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    {
                        id: 'event',
                        name: 'Event',
                        type: AppFieldType.JSON,
                        fields: [
                            { id: 'name', name: 'Name', type: AppFieldType.Text },
                            { id: 'timestamp', name: 'Timestamp', type: AppFieldType.Date }
                        ]
                    }
                ],
                actions: []
            };

            const now = new Date();
            const data = [{ event: { name: 'test-event', timestamp: now } }];
            await ingestDataToDuckDB(connection, data, table, 'test_struct_with_date');

            const reader = await connection.run('SELECT event FROM test_struct_with_date');
            const rows = await reader.getRows();

            expect(rows).toHaveLength(1);
        });

        it('should handle Date inside List', async () => {
            const instance = await createDuckDBInstance();
            const connection = await instance.connect();

            // List with Date child to hit prepareDuckDBValue Date case for list items
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    {
                        id: 'date_list',
                        name: 'Date List',
                        type: AppFieldType.List,
                        child: { id: 'item', name: 'Item', type: AppFieldType.Date }
                    }
                ],
                actions: []
            };

            const dates = [new Date('2024-01-01'), new Date('2024-06-15')];
            const data = [{ date_list: dates }];
            await ingestDataToDuckDB(connection, data, table, 'test_list_of_dates');

            const reader = await connection.run('SELECT date_list FROM test_list_of_dates');
            const rows = await reader.getRows();

            expect(rows).toHaveLength(1);
        });

        it('should handle struct with missing subfield in data', async () => {
            const instance = await createDuckDBInstance();
            const connection = await instance.connect();

            // Table schema expects two fields but data only has one
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    {
                        id: 'profile',
                        name: 'Profile',
                        type: AppFieldType.JSON,
                        fields: [
                            { id: 'name', name: 'Name', type: AppFieldType.Text },
                            { id: 'age', name: 'Age', type: AppFieldType.Number }
                        ]
                    }
                ],
                actions: []
            };

            // Only provide 'name', not 'age' - this should trigger the continue branch
            // when convertDuckDBValue iterates and 'age' is not in entries
            const data = [{ profile: { name: 'John' } }];
            await ingestDataToDuckDB(connection, data, table, 'test_missing_subfield');

            const reader = await connection.run('SELECT profile FROM test_missing_subfield');
            const rows = await reader.getRows();

            // Map rows to objects
            const mappedRows = rows.map((row: unknown[]) => ({
                profile: row[0]
            }));

            const appRows = convertDuckDBRowsToAppRows(mappedRows, table);

            expect(appRows).toHaveLength(1);
            expect(appRows[0].profile).toBeTruthy();
            expect((appRows[0].profile as { name: string }).name).toBe('John');
        });

        it('should handle nested struct with null value', async () => {
            const instance = await createDuckDBInstance();
            const connection = await instance.connect();

            // Outer struct with inner struct that can be null
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    {
                        id: 'wrapper',
                        name: 'Wrapper',
                        type: AppFieldType.JSON,
                        fields: [
                            {
                                id: 'inner',
                                name: 'Inner',
                                type: AppFieldType.JSON,
                                fields: [{ id: 'value', name: 'Value', type: AppFieldType.Text }]
                            }
                        ]
                    }
                ],
                actions: []
            };

            // Pass null for the inner struct - when converted, inner is not DuckDBStructValue
            const data = [{ wrapper: { inner: null } }];
            await ingestDataToDuckDB(connection, data, table, 'test_nested_null_struct');

            const reader = await connection.run('SELECT wrapper FROM test_nested_null_struct');
            const rows = await reader.getRows();

            // Map rows to objects
            const mappedRows = rows.map((row: unknown[]) => ({
                wrapper: row[0]
            }));

            const appRows = convertDuckDBRowsToAppRows(mappedRows, table);

            expect(appRows).toHaveLength(1);
        });

        it('should handle structured JSON with plain object value', () => {
            // Pass raw data (not from DuckDB) with a plain object for a structured JSON field
            // This tests the fallback return when value is not a DuckDBStructValue
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    {
                        id: 'profile',
                        name: 'Profile',
                        type: AppFieldType.JSON,
                        fields: [{ id: 'name', name: 'Name', type: AppFieldType.Text }]
                    }
                ],
                actions: []
            };

            // Pass a plain object (not DuckDBStructValue) for the structured field
            const rawData = [{ profile: { name: 'John' } }];
            const appRows = convertDuckDBRowsToAppRows(rawData, table);

            expect(appRows).toHaveLength(1);
            // Since value is not DuckDBStructValue, it should be returned as-is
            expect(appRows[0].profile).toEqual({ name: 'John' });
        });

        it('should handle List with non-array/non-string value', () => {
            // Pass non-array, non-string value for a List field
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    {
                        id: 'items',
                        name: 'Items',
                        type: AppFieldType.List,
                        child: { id: 'item', name: 'Item', type: AppFieldType.Text }
                    }
                ],
                actions: []
            };

            // Pass a number instead of array - should return empty array
            const rawData = [{ items: 12345 }];
            const appRows = convertDuckDBRowsToAppRows(rawData, table);

            expect(appRows).toHaveLength(1);
            expect(appRows[0].items).toEqual([]);
        });

        it('should skip fields missing from row data', () => {
            // Table has more fields than the row data
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    { id: 'name', name: 'Name', type: AppFieldType.Text },
                    { id: 'age', name: 'Age', type: AppFieldType.Number }
                ],
                actions: []
            };

            // Row only has 'name', not 'age' - should skip the missing field
            const rawData = [{ name: 'John' }];
            const appRows = convertDuckDBRowsToAppRows(rawData, table);

            expect(appRows).toHaveLength(1);
            expect(appRows[0].name).toBe('John');
            expect('age' in appRows[0]).toBe(false);
        });

        it('should handle invalid JSON string for unstructured JSON field (catch block)', () => {
            // Invalid JSON string should be returned as-is when parse fails
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    {
                        id: 'data',
                        name: 'Data',
                        type: AppFieldType.JSON
                        // No fields = unstructured JSON
                    }
                ],
                actions: []
            };

            // Invalid JSON string - should return as-is
            const rawData = [{ data: 'not valid json {{{' }];
            const appRows = convertDuckDBRowsToAppRows(rawData, table);

            expect(appRows).toHaveLength(1);
            expect(appRows[0].data).toBe('not valid json {{{');
        });

        it('should handle invalid JSON string for List field (catch block)', () => {
            // Invalid JSON string for List should return empty array
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    {
                        id: 'items',
                        name: 'Items',
                        type: AppFieldType.List,
                        child: { id: 'item', name: 'Item', type: AppFieldType.Text }
                    }
                ],
                actions: []
            };

            // Invalid JSON string - JSON.parse will fail, data stays undefined, return []
            const rawData = [{ items: 'invalid [json' }];
            const appRows = convertDuckDBRowsToAppRows(rawData, table);

            expect(appRows).toHaveLength(1);
            expect(appRows[0].items).toEqual([]);
        });

        it('should handle Date as string inside struct (prepareDuckDBValue non-Date branch)', async () => {
            const instance = await createDuckDBInstance();
            const connection = await instance.connect();

            // Struct with a Date field where value is a string, not a Date object
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    {
                        id: 'event',
                        name: 'Event',
                        type: AppFieldType.JSON,
                        fields: [
                            { id: 'name', name: 'Name', type: AppFieldType.Text },
                            { id: 'timestamp', name: 'Timestamp', type: AppFieldType.Date }
                        ]
                    }
                ],
                actions: []
            };

            // Pass date as ISO string instead of Date object
            const dateString = new Date().toISOString();
            const data = [{ event: { name: 'string-date-event', timestamp: dateString } }];
            await ingestDataToDuckDB(connection, data, table, 'test_struct_with_date_string');

            const reader = await connection.run('SELECT event FROM test_struct_with_date_string');
            const rows = await reader.getRows();

            expect(rows).toHaveLength(1);
            // The struct should contain the date as ISO string
            // DuckDB returns struct with entries property
            const eventStruct = rows[0][0] as { entries: Record<string, unknown> };
            expect(eventStruct.entries.timestamp).toBe(dateString);
        });

        it('should handle non-array value for List with child (prepareDuckDBValue array wrap)', async () => {
            const instance = await createDuckDBInstance();
            const connection = await instance.connect();

            // List field where the value is a single item, not an array
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    {
                        id: 'tags',
                        name: 'Tags',
                        type: AppFieldType.List,
                        child: { id: 'tag', name: 'Tag', type: AppFieldType.Text }
                    }
                ],
                actions: []
            };

            // Pass a single value instead of array - should be wrapped into [value]
            const data = [{ tags: 'single-tag' }];
            await ingestDataToDuckDB(connection, data, table, 'test_list_single_value');

            const reader = await connection.run('SELECT tags FROM test_list_single_value');
            const rows = await reader.getRows();

            expect(rows).toHaveLength(1);
            // The list should contain the single item wrapped in an array
            // DuckDB returns DuckDBListValue with items property
            const tagsList = rows[0][0] as { items: unknown[] };
            expect(tagsList.items).toEqual(['single-tag']);
        });

        it('should handle Date as string for direct append (appendValue non-Date branch)', async () => {
            const instance = await createDuckDBInstance();
            const connection = await instance.connect();

            // Top-level Date field with string value instead of Date object
            const table: AppTable = {
                id: 'test',
                name: 'Test',
                connector: 'mem',
                path: ['test'],
                fields: [
                    { id: 'id', name: 'ID', type: AppFieldType.Number },
                    { id: 'created_at', name: 'Created At', type: AppFieldType.Date }
                ],
                actions: []
            };

            // Pass date as string instead of Date object
            const dateString = new Date().toISOString();
            const data = [{ id: 1, created_at: dateString }];
            await ingestDataToDuckDB(connection, data, table, 'test_date_string_direct');

            const reader = await connection.run(
                'SELECT id, created_at FROM test_date_string_direct'
            );
            const rows = await reader.getRows();

            expect(rows).toHaveLength(1);
            expect(rows[0][0]).toBe(1);
            // Date is stored as ISO string
            expect(rows[0][1]).toBe(dateString);
        });
    });
});
