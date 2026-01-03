import type { FastifyPluginAsyncZod } from 'fastify-type-provider-zod';
import authPlugin from './authPlugin.js';
import connectorsPlugin from './connectorsPlugin.js';
import schemaPlugin from './schemaPlugin.js';
import dataPlugin from './dataPlugin.js';
import type DataService from '../services/DataService.js';

const plugin: FastifyPluginAsyncZod<{ dataService: DataService }> = async (
    fastify,
    { dataService }
) => {
    fastify.register(authPlugin, { dataService });
    fastify.register(connectorsPlugin, { dataService });
    fastify.register(schemaPlugin, { dataService });
    fastify.register(dataPlugin, { dataService });
};

export default plugin;
