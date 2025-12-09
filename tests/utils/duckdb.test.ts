import { describe, it, expect } from 'vitest';
import {
    buildSQLQuery,
    createDuckDBInstance,
    ingestStreamToDuckDB
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
    });
});
