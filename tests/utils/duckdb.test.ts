import { describe, it, expect } from 'vitest';
import { getData, QueryFilterOperator } from '../../src/utils/duckdb';
import { DataSourceType, DataSourceFormat } from '../../src/types';

describe('DuckDB Utils', () => {
    const testData = [
        { id: 1, name: 'Alice', age: 30 },
        { id: 2, name: 'Bob', age: 25 },
        { id: 3, name: 'Charlie', age: 35 }
    ];

    describe('getData', () => {
        it('should return all data when no query is provided', async () => {
            const result = await getData({
                source: { type: DataSourceType.Inline, data: testData }
            });

            expect(result).toHaveLength(3);
            expect(result).toEqual(testData);
        });

        it('should return empty array for empty data', async () => {
            const result = await getData({
                source: { type: DataSourceType.Inline, data: [] }
            });

            expect(result).toEqual([]);
        });

        it('should apply limit', async () => {
            const result = await getData({
                source: { type: DataSourceType.Inline, data: testData },
                query: { limit: 2 }
            });

            expect(result).toHaveLength(2);
        });

        it('should apply offset', async () => {
            const result = await getData({
                source: { type: DataSourceType.Inline, data: testData },
                query: { offset: 1 }
            });

            expect(result).toHaveLength(2);
            expect(result[0]).toEqual({ id: 2, name: 'Bob', age: 25 });
        });

        it('should apply limit and offset together', async () => {
            const result = await getData({
                source: { type: DataSourceType.Inline, data: testData },
                query: { limit: 1, offset: 1 }
            });

            expect(result).toHaveLength(1);
            expect(result[0]).toEqual({ id: 2, name: 'Bob', age: 25 });
        });

        it('should apply orderBy ascending', async () => {
            const result = await getData({
                source: { type: DataSourceType.Inline, data: testData },
                query: { orderBy: { column: 'age', direction: 'asc' } }
            });

            expect(result[0].name).toBe('Bob');
            expect(result[1].name).toBe('Alice');
            expect(result[2].name).toBe('Charlie');
        });

        it('should apply orderBy descending', async () => {
            const result = await getData({
                source: { type: DataSourceType.Inline, data: testData },
                query: { orderBy: { column: 'age', direction: 'desc' } }
            });

            expect(result[0].name).toBe('Charlie');
            expect(result[1].name).toBe('Alice');
            expect(result[2].name).toBe('Bob');
        });

        it('should apply equals filter', async () => {
            const result = await getData({
                source: { type: DataSourceType.Inline, data: testData },
                query: {
                    filters: [
                        { field: 'name', operator: QueryFilterOperator.Equals, value: 'Alice' }
                    ]
                }
            });

            expect(result).toHaveLength(1);
            expect(result[0].name).toBe('Alice');
        });

        it('should apply notEqual filter', async () => {
            const result = await getData({
                source: { type: DataSourceType.Inline, data: testData },
                query: {
                    filters: [
                        { field: 'name', operator: QueryFilterOperator.NotEqual, value: 'Alice' }
                    ]
                }
            });

            expect(result).toHaveLength(2);
            expect(result.every(r => r.name !== 'Alice')).toBe(true);
        });

        it('should apply greaterThan filter', async () => {
            const result = await getData({
                source: { type: DataSourceType.Inline, data: testData },
                query: {
                    filters: [
                        { field: 'age', operator: QueryFilterOperator.GreaterThan, value: 30 }
                    ]
                }
            });

            expect(result).toHaveLength(1);
            expect(result[0].name).toBe('Charlie');
        });

        it('should apply greaterThanOrEqualTo filter', async () => {
            const result = await getData({
                source: { type: DataSourceType.Inline, data: testData },
                query: {
                    filters: [
                        {
                            field: 'age',
                            operator: QueryFilterOperator.GreaterThanOrEqualTo,
                            value: 30
                        }
                    ]
                }
            });

            expect(result).toHaveLength(2);
        });

        it('should apply lowerThan filter', async () => {
            const result = await getData({
                source: { type: DataSourceType.Inline, data: testData },
                query: {
                    filters: [{ field: 'age', operator: QueryFilterOperator.LowerThan, value: 30 }]
                }
            });

            expect(result).toHaveLength(1);
            expect(result[0].name).toBe('Bob');
        });

        it('should apply lowerThanOrEqualTo filter', async () => {
            const result = await getData({
                source: { type: DataSourceType.Inline, data: testData },
                query: {
                    filters: [
                        {
                            field: 'age',
                            operator: QueryFilterOperator.LowerThanOrEqualTo,
                            value: 30
                        }
                    ]
                }
            });

            expect(result).toHaveLength(2);
        });

        it('should apply contains filter', async () => {
            const result = await getData({
                source: { type: DataSourceType.Inline, data: testData },
                query: {
                    filters: [
                        { field: 'name', operator: QueryFilterOperator.Contains, value: 'li' }
                    ]
                }
            });

            expect(result).toHaveLength(2);
            expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Charlie']);
        });

        it('should apply multiple filters', async () => {
            const result = await getData({
                source: { type: DataSourceType.Inline, data: testData },
                query: {
                    filters: [
                        {
                            field: 'age',
                            operator: QueryFilterOperator.GreaterThanOrEqualTo,
                            value: 25
                        },
                        { field: 'age', operator: QueryFilterOperator.LowerThan, value: 35 }
                    ]
                }
            });

            expect(result).toHaveLength(2);
            expect(result.map(r => r.name).sort()).toEqual(['Alice', 'Bob']);
        });

        it('should apply filters, orderBy, limit and offset together', async () => {
            const result = await getData({
                source: { type: DataSourceType.Inline, data: testData },
                query: {
                    filters: [
                        {
                            field: 'age',
                            operator: QueryFilterOperator.GreaterThanOrEqualTo,
                            value: 25
                        }
                    ],
                    orderBy: { column: 'age', direction: 'desc' },
                    limit: 2,
                    offset: 1
                }
            });

            expect(result).toHaveLength(2);
            expect(result[0].name).toBe('Alice');
            expect(result[1].name).toBe('Bob');
        });

        it('should handle Number dates (returns as number)', async () => {
            const data = [
                { id: 1, created: 1704067200000 },
                { id: 2, created: 1704153600000 }
            ];

            const result = await getData({
                source: { type: DataSourceType.Inline, data }
            });

            expect(result[0].created).toBe(1704067200000);
            expect(result[1].created).toBe(1704153600000);
        });

        it('should handle Text dates (returns as ISO string)', async () => {
            const data = [
                { id: 1, created: '2024-01-01T00:00:00.000Z' },
                { id: 2, created: '2024-01-02T00:00:00.000Z' }
            ];

            const result = await getData({
                source: { type: DataSourceType.Inline, data }
            });

            expect(result[0].created).toBe('2024-01-01T00:00:00.000Z');
            expect(result[1].created).toBe('2024-01-02T00:00:00.000Z');
        });

        it('should handle various field types', async () => {
            const data = [
                {
                    id: 1,
                    name: 'Test',
                    age: 30,
                    active: true,
                    score: 95.5,
                    created: '2024-01-01T00:00:00.000Z'
                }
            ];

            const result = await getData({
                source: { type: DataSourceType.Inline, data }
            });

            expect(result).toHaveLength(1);
            expect(result[0].id).toBe(1);
            expect(result[0].name).toBe('Test');
            expect(result[0].age).toBe(30);
            expect(result[0].active).toBe(true);
            expect(result[0].score).toBe(95.5);
            expect(result[0].created).toBe('2024-01-01T00:00:00.000Z');
        });

        it('should handle decodeRow option', async () => {
            const data = [
                { id: 1, name: 'encoded_Alice' },
                { id: 2, name: 'encoded_Bob' }
            ];

            const result = await getData({
                source: { type: DataSourceType.Inline, data },
                decodeRow: row => ({
                    ...row,
                    name: (row.name as string).replace('encoded_', '')
                })
            });

            expect(result).toHaveLength(2);
            expect(result[0].name).toBe('Alice');
            expect(result[1].name).toBe('Bob');
        });

        it('should handle struct field types', async () => {
            const data = [
                {
                    id: 1,
                    metadata: { key: 'value', nested: { a: 1 } }
                },
                {
                    id: 2,
                    metadata: { key: 'other', nested: { a: 2 } }
                }
            ];

            const result = await getData({
                source: { type: DataSourceType.Inline, data }
            });

            expect(result).toHaveLength(2);
            expect(result[0].metadata).toEqual({ key: 'value', nested: { a: 1 } });
            expect(result[1].metadata).toEqual({ key: 'other', nested: { a: 2 } });
        });

        it('should handle list field types', async () => {
            const data = [
                { id: 1, tags: ['a', 'b', 'c'] },
                { id: 2, tags: ['d', 'e'] }
            ];

            const result = await getData({
                source: { type: DataSourceType.Inline, data }
            });

            expect(result).toHaveLength(2);
            expect(result[0].tags).toEqual(['a', 'b', 'c']);
            expect(result[1].tags).toEqual(['d', 'e']);
        });

        it('should handle Date inside struct (returns as ISO string)', async () => {
            const data = [
                {
                    id: 1,
                    info: { name: 'Test', created: '2024-01-01T00:00:00.000Z' }
                }
            ];

            const result = await getData({
                source: { type: DataSourceType.Inline, data }
            });

            expect(result).toHaveLength(1);
            expect(result[0].info).toEqual({
                name: 'Test',
                created: '2024-01-01T00:00:00.000Z'
            });
        });

        it('should handle Date inside List (returns as ISO strings)', async () => {
            const data = [
                {
                    id: 1,
                    dates: ['2024-01-01T00:00:00.000Z', '2024-01-02T00:00:00.000Z']
                }
            ];

            const result = await getData({
                source: { type: DataSourceType.Inline, data }
            });

            expect(result).toHaveLength(1);
            expect(result[0].dates).toEqual([
                '2024-01-01T00:00:00.000Z',
                '2024-01-02T00:00:00.000Z'
            ]);
        });

        it('should handle bigint outside safe integer range', async () => {
            // Use a value larger than Number.MAX_SAFE_INTEGER (9007199254740991)
            const largeNumber = '9007199254740992123';
            const data = [{ id: 1, bigValue: largeNumber }];

            const result = await getData({
                source: { type: DataSourceType.Inline, data }
            });

            expect(result).toHaveLength(1);
            // DuckDB will parse and return this - the important thing is it doesn't crash
            expect(result[0].bigValue).toBeDefined();
        });

        it('should handle plain arrays in data', async () => {
            const data = [{ id: 1, values: [1, 2, 3] }];

            const result = await getData({
                source: { type: DataSourceType.Inline, data }
            });

            expect(result).toHaveLength(1);
            expect(result[0].values).toEqual([1, 2, 3]);
        });

        it('should handle null values in data', async () => {
            const data = [
                { id: 1, name: 'Alice', optional: null },
                { id: 2, name: 'Bob', optional: 'value' }
            ];

            const result = await getData({
                source: { type: DataSourceType.Inline, data }
            });

            expect(result).toHaveLength(2);
            expect(result[0].optional).toBe(null);
            expect(result[1].optional).toBe('value');
        });

        it('should handle Connection data source with SQLite and target table', async () => {
            // Create a temp SQLite database using DuckDB
            const { DuckDBInstance } = await import('@duckdb/node-api');
            const fs = await import('fs');
            const path = await import('path');
            const os = await import('os');

            const tempPath = path.join(os.tmpdir(), `test_${Date.now()}.sqlite`);

            try {
                // Create a SQLite file with test data using DuckDB's SQLite extension
                const instance = await DuckDBInstance.create(':memory:');
                const conn = await instance.connect();
                await conn.run('INSTALL sqlite; LOAD sqlite;');
                await conn.run(`ATTACH '${tempPath.replace(/\\/g, '/')}' AS testdb (TYPE SQLITE)`);
                await conn.run('CREATE TABLE testdb.users (id INTEGER, name VARCHAR)');
                await conn.run("INSERT INTO testdb.users VALUES (1, 'Alice'), (2, 'Bob')");
                conn.closeSync();

                // Now read it using Connection data source
                const result = await getData({
                    source: {
                        type: DataSourceType.Connection,
                        module: 'sqlite',
                        connectionString: tempPath,
                        target: 'users'
                    }
                });

                expect(result).toHaveLength(2);
                expect(result[0].name).toBe('Alice');
                expect(result[1].name).toBe('Bob');
            } finally {
                try {
                    fs.unlinkSync(tempPath);
                } catch {
                    /* ignore */
                }
            }
        });

        it('should handle Connection data source with query filters', async () => {
            // Create a temp SQLite database using DuckDB
            const { DuckDBInstance } = await import('@duckdb/node-api');
            const fs = await import('fs');
            const path = await import('path');
            const os = await import('os');

            const tempPath = path.join(os.tmpdir(), `test_${Date.now()}.sqlite`);

            try {
                // Create a SQLite file with test data
                const instance = await DuckDBInstance.create(':memory:');
                const conn = await instance.connect();
                await conn.run('INSTALL sqlite; LOAD sqlite;');
                await conn.run(`ATTACH '${tempPath.replace(/\\/g, '/')}' AS testdb (TYPE SQLITE)`);
                await conn.run('CREATE TABLE testdb.products (id INTEGER, price DOUBLE)');
                await conn.run('INSERT INTO testdb.products VALUES (1, 10.5), (2, 20.0), (3, 5.0)');
                conn.closeSync();

                // Use target with query filters (the proper approach)
                const result = await getData({
                    source: {
                        type: DataSourceType.Connection,
                        module: 'sqlite',
                        connectionString: tempPath,
                        target: 'products'
                    },
                    query: {
                        filters: [
                            { field: 'price', operator: QueryFilterOperator.GreaterThan, value: 10 }
                        ]
                    }
                });

                expect(result).toHaveLength(2);
                expect(result.some(r => r.price === 20)).toBe(true);
                expect(result.some(r => r.price === 10.5)).toBe(true);
            } finally {
                try {
                    fs.unlinkSync(tempPath);
                } catch {
                    /* ignore */
                }
            }
        });

        it('should handle Stream data source type', async () => {
            const { Readable } = await import('stream');
            const data = [
                { id: 1, name: 'Alice' },
                { id: 2, name: 'Bob' }
            ];
            const jsonData = JSON.stringify(data);

            const stream = Readable.from([jsonData]);

            const result = await getData({
                source: {
                    type: DataSourceType.Stream,
                    stream: stream
                }
            });

            expect(result).toHaveLength(2);
            expect(result[0].name).toBe('Alice');
            expect(result[1].name).toBe('Bob');
        });

        it('should handle File data source with CSV format', async () => {
            const fs = await import('fs');
            const path = await import('path');
            const os = await import('os');

            const tempPath = path.join(os.tmpdir(), `test_${Date.now()}.csv`);
            fs.writeFileSync(tempPath, 'id,name,age\n1,Alice,30\n2,Bob,25');

            try {
                const result = await getData({
                    source: {
                        type: DataSourceType.File,
                        path: tempPath,
                        options: { format: DataSourceFormat.Csv }
                    }
                });

                expect(result).toHaveLength(2);
                expect(result[0].name).toBe('Alice');
            } finally {
                fs.unlinkSync(tempPath);
            }
        });

        it('should handle File data source with Auto format (explicit)', async () => {
            const fs = await import('fs');
            const path = await import('path');
            const os = await import('os');

            // Explicit Auto format should auto-detect JSON
            const tempPath = path.join(os.tmpdir(), `test_${Date.now()}.json`);
            fs.writeFileSync(
                tempPath,
                JSON.stringify([
                    { id: 1, name: 'Alice' },
                    { id: 2, name: 'Bob' }
                ])
            );

            try {
                const result = await getData({
                    source: {
                        type: DataSourceType.File,
                        path: tempPath,
                        options: { format: DataSourceFormat.Auto }
                    }
                });

                expect(result).toHaveLength(2);
                expect(result[0].name).toBe('Alice');
            } finally {
                fs.unlinkSync(tempPath);
            }
        });

        it('should handle File data source with Xml format (falls back to json auto)', async () => {
            const fs = await import('fs');
            const path = await import('path');
            const os = await import('os');

            // Xml format falls back to read_json_auto since DuckDB doesn't support native XML
            // Using JSON content since read_json_auto is used as fallback
            const tempPath = path.join(os.tmpdir(), `test_${Date.now()}.json`);
            fs.writeFileSync(
                tempPath,
                JSON.stringify([
                    { id: 1, name: 'Alice' },
                    { id: 2, name: 'Bob' }
                ])
            );

            try {
                const result = await getData({
                    source: {
                        type: DataSourceType.File,
                        path: tempPath,
                        options: { format: DataSourceFormat.Xml }
                    }
                });

                expect(result).toHaveLength(2);
                expect(result[0].name).toBe('Alice');
            } finally {
                fs.unlinkSync(tempPath);
            }
        });

        it('should handle File data source with NDJSON format', async () => {
            const fs = await import('fs');
            const path = await import('path');
            const os = await import('os');

            const tempPath = path.join(os.tmpdir(), `test_${Date.now()}.ndjson`);
            fs.writeFileSync(tempPath, '{"id":1,"name":"Alice"}\n{"id":2,"name":"Bob"}');

            try {
                const result = await getData({
                    source: {
                        type: DataSourceType.File,
                        path: tempPath,
                        options: { format: DataSourceFormat.Ndjson }
                    }
                });

                expect(result).toHaveLength(2);
                expect(result[0].name).toBe('Alice');
            } finally {
                fs.unlinkSync(tempPath);
            }
        });

        it('should handle File data source with Parquet format', async () => {
            // Use DuckDB to create a parquet file, then read it back
            const { DuckDBInstance } = await import('@duckdb/node-api');
            const fs = await import('fs');
            const path = await import('path');
            const os = await import('os');

            const tempPath = path.join(os.tmpdir(), `test_${Date.now()}.parquet`);

            try {
                // Create a parquet file using DuckDB
                const instance = await DuckDBInstance.create(':memory:');
                const connection = await instance.connect();
                await connection.run(
                    `COPY (SELECT 1 as id, 'Alice' as name UNION ALL SELECT 2, 'Bob') TO '${tempPath.replace(/\\/g, '/')}' (FORMAT PARQUET)`
                );

                const result = await getData({
                    source: {
                        type: DataSourceType.File,
                        path: tempPath,
                        options: { format: DataSourceFormat.Parquet }
                    }
                });

                expect(result).toHaveLength(2);
                expect(result[0].name).toBe('Alice');
            } finally {
                if (fs.existsSync(tempPath)) fs.unlinkSync(tempPath);
            }
        });

        it('should handle URL data source', async () => {
            // Use a public JSON endpoint for testing
            const result = await getData({
                source: {
                    type: DataSourceType.Url,
                    url: 'https://jsonplaceholder.typicode.com/users/1',
                    options: { format: DataSourceFormat.Json }
                }
            });

            // The API returns a single user object wrapped in array by DuckDB
            expect(result).toHaveLength(1);
            expect(result[0]).toHaveProperty('id');
            expect(result[0]).toHaveProperty('name');
        });

        it('should handle URL data source with custom headers', async () => {
            // Use a public JSON endpoint - headers are set via DuckDB CREATE SECRET
            const result = await getData({
                source: {
                    type: DataSourceType.Url,
                    url: 'https://jsonplaceholder.typicode.com/users/2',
                    options: {
                        format: DataSourceFormat.Json,
                        headers: {
                            Accept: 'application/json',
                            'X-Custom-Header': 'test-value'
                        }
                    }
                }
            });

            expect(result).toHaveLength(1);
            expect(result[0]).toHaveProperty('id', 2);
        });
    });
});
