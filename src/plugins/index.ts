import type { FastifyPluginAsyncZod } from 'fastify-type-provider-zod';
import type { AppSchema, Connector } from '../types.js';
import { z } from 'zod';
import { LRUCache } from 'lru-cache';

import authPlugin from './authPlugin.js';
import connectorsPlugin from './connectorsPlugin.js';
import schemaPlugin from './schemaPlugin.js';
import dataPlugin from './dataPlugin.js';

export type SchemaFXConnectorsOptions = {
    schemaConnector: string;
    connectors: Record<string, Connector>;
    maxRecursiveDepth?: number;
    validatorCacheOpts?: {
        max?: number;
        ttl?: number;
    };
    schemaCacheOpts?: {
        max?: number;
        ttl?: number;
    };
};

const plugin: FastifyPluginAsyncZod<SchemaFXConnectorsOptions> = async (
    fastify,
    { schemaConnector, connectors, maxRecursiveDepth, validatorCacheOpts, schemaCacheOpts }
) => {
    const validatorCache = new LRUCache<string, z.ZodAny>({
        max: validatorCacheOpts?.max ?? 500,
        ttl: validatorCacheOpts?.ttl ?? 1000 * 60 * 60
    });

    const schemaCache = new LRUCache<string, AppSchema>({
        max: schemaCacheOpts?.max ?? 100,
        ttl: schemaCacheOpts?.ttl ?? 1000 * 60 * 5 // 5 minutes TTL
    });

    const sConnector = connectors[schemaConnector];

    if (!sConnector) {
        throw new Error(`Unrecognized connector "${schemaConnector}".`);
    } else if (!sConnector.getSchema) {
        throw new Error(`Missing implementation "getSchema" on connector "${schemaConnector}".`);
    } else if (!sConnector.saveSchema) {
        throw new Error(`Missing implementation "saveSchema" on connector "${schemaConnector}".`);
    } else if (!sConnector.deleteSchema) {
        throw new Error(`Missing implementation "deleteSchema" on connector "${schemaConnector}".`);
    }

    async function getSchema(appId: string) {
        if (schemaCache.has(appId)) return schemaCache.get(appId)!;

        const schema = await sConnector.getSchema!(appId);
        schemaCache.set(appId, schema);
        return schema;
    }

    // Register Auth Plugin
    fastify.register(authPlugin);

    // Register Connectors Plugin
    fastify.register(connectorsPlugin, {
        connectors,
        sConnector,
        schemaCache,
        getSchema
    });

    // Register Schema Plugin
    fastify.register(schemaPlugin, {
        sConnector,
        schemaCache,
        validatorCache,
        getSchema
    });

    // Register Data Plugin
    fastify.register(dataPlugin, {
        connectors,
        getSchema,
        validatorCache,
        maxRecursiveDepth
    });
};

export default plugin;
