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
    inferTable,
    QueryFilterOperator,
    type TableQueryOptions
} from './types.js';

// Connectors
export { default as MemoryConnector } from './connectors/memoryConnector.js';
export { default as FileConnector } from './connectors/fileConnector.js';
