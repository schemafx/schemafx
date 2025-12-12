import type { z } from 'zod';
import {
    AppActionType,
    AppFieldType,
    type AppTableRow,
    type Connector,
    type AppTable,
    type TableQueryOptions
} from '../types.js';
import { decrypt, encrypt } from './encryption.js';
import { extractKeys } from './schemaUtils.js';
import type { LRUCache } from 'lru-cache';
import knex from 'knex';
import {
    buildSQLQuery,
    createDuckDBInstance,
    ingestDataToDuckDB,
    ingestStreamToDuckDB
} from './duckdb.js';
import type { DuckDBValue } from '@duckdb/node-api';
import { zodFromTable } from './zodUtils.js';

const qb = knex({ client: 'pg' });

export function encodeRow(
    row: Record<string, unknown>,
    table: AppTable,
    encryptionKey?: string
): Record<string, unknown> {
    if (!encryptionKey) return row;
    const processedRow = { ...row };

    for (const field of table.fields) {
        if (
            field.encrypted &&
            (field.type === AppFieldType.Text || field.type === AppFieldType.JSON) &&
            processedRow[field.id] !== undefined &&
            processedRow[field.id] !== null
        ) {
            let valueToEncrypt = processedRow[field.id];
            if (field.type === AppFieldType.JSON) valueToEncrypt = JSON.stringify(valueToEncrypt);

            processedRow[field.id] = encrypt(String(valueToEncrypt), encryptionKey);
        }
    }

    return processedRow;
}

export function decodeRow(
    row: Record<string, unknown>,
    table: AppTable,
    encryptionKey?: string
): Record<string, unknown> {
    if (!encryptionKey) return row;
    const processedRow = { ...row };

    for (const field of table.fields) {
        let value = processedRow[field.id];

        if (
            field.encrypted &&
            value !== undefined &&
            value !== null &&
            typeof value === 'string' &&
            (field.type === AppFieldType.Text || field.type === AppFieldType.JSON)
        ) {
            const decrypted = decrypt(value, encryptionKey);
            if (field.type === AppFieldType.JSON) value = JSON.parse(decrypted);
            else value = decrypted;

            processedRow[field.id] = value;
        }
    }

    return processedRow;
}

export async function executeAction(opts: {
    appId: string;
    connector: Connector;
    table: AppTable;
    validatorCache: LRUCache<string, z.ZodType>;
    actId: string;
    currentRows: AppTableRow[];
    depth: number;
    maxDepth?: number;
    encryptionKey?: string;
}) {
    const {
        appId,
        connector,
        table,
        validatorCache,
        actId,
        currentRows,
        depth,
        maxDepth,
        encryptionKey
    } = opts;

    if (maxDepth && depth > maxDepth) {
        throw new Error('Max recursion depth exceeded.');
    }

    const action = table.actions?.find(a => a.id === actId);
    if (!action) throw new Error(`Action ${actId} not found.`);
    const keyFields = table.fields.filter(f => f.isKey).map(f => f.id);

    switch (action.type) {
        case AppActionType.Add: {
            const validator = zodFromTable(table, appId, validatorCache);
            for (const row of currentRows) {
                await connector?.addRow?.(
                    table,
                    encodeRow(
                        (await validator.parseAsync(row)) as Record<string, unknown>,
                        table,
                        encryptionKey
                    )
                );
            }

            return;
        }

        case AppActionType.Update: {
            const validator = zodFromTable(table, appId, validatorCache);
            for (const row of currentRows) {
                const key = extractKeys(row, keyFields);
                if (Object.keys(key).length === 0) continue;

                await connector?.updateRow?.(
                    table,
                    key,
                    encodeRow(
                        (await validator.parseAsync(row)) as Record<string, unknown>,
                        table,
                        encryptionKey
                    )
                );
            }

            return;
        }

        case AppActionType.Delete: {
            for (const row of currentRows) {
                const key = extractKeys(row, keyFields);
                if (Object.keys(key).length === 0) continue;

                await connector?.deleteRow?.(table, key);
            }

            return;
        }

        case AppActionType.Process: {
            const subActions = (action.config?.actions as string[]) || [];

            for (const subActionId of subActions) {
                await executeAction({ ...opts, actId: subActionId });
            }

            return;
        }
    }
}

export async function handleDataRetriever(
    table: AppTable,
    connector: Connector,
    query?: TableQueryOptions,
    encryptionKey?: string
) {
    if (!connector.getData && !connector.getDataStream) return [];

    const tempTableName = `t_${Math.random().toString(36).substring(7)}`;
    const dbInstance = await createDuckDBInstance();
    const connection = await dbInstance.connect();

    if (connector.getDataStream) {
        await ingestStreamToDuckDB(
            connection,
            await connector.getDataStream(table),
            table,
            tempTableName
        );
    } else {
        await ingestDataToDuckDB(connection, await connector.getData!(table), table, tempTableName);
    }

    const { sql, params } = buildSQLQuery(tempTableName, query || {});
    const reader = await connection.run(sql, params as unknown[] as DuckDBValue[]);
    const rows = await reader.getRows();

    const result = rows.map(row => {
        const obj: Record<string, unknown> = {};
        table.fields.forEach((field, index) => {
            obj[field.id] = row[index];
        });

        return decodeRow(obj, table, encryptionKey);
    });

    await connection.run(qb.schema.dropTableIfExists(tempTableName).toString());
    connection.closeSync();

    return result;
}
