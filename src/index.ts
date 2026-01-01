// Default
import SchemaFX from './core.js';
export default SchemaFX;

// Options
export type { SchemaFXOptions } from './core.js';

// Types
export {
    type AppAction,
    AppActionType,
    AppFieldType,
    type AppField,
    type AppTable,
    type AppTableRow,
    type AppView,
    AppViewType,
    type AppSchema,
    Connector,
    type ConnectorCapabilities,
    type ConnectorTable,
    ConnectorTableCapability,
    QueryFilterOperator,
    type TableQueryOptions,

    // Data Source Definitions
    DataSourceType,
    DataSourceFormat,
    type DataSourceOptions,
    type DataSourceDefinition,
    type InlineDataSource,
    type FileDataSource,
    type UrlDataSource,
    type StreamDataSource,
    type ConnectionDataSource
} from './types.js';

export { inferTable } from './utils/dataUtils.js';

// Connectors
export { default as MemoryConnector } from './connectors/memoryConnector.js';
export { default as FileConnector } from './connectors/fileConnector.js';
