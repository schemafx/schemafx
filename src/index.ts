import { type TableDefinition, type TableColumnDefinition, TableColumnType } from './schemas';
export { TableDefinition, TableColumnDefinition };

import { Connector, ConnectorAuthType, ConnectorAuthPropType } from './connector';
export { Connector };

export const Types = {
    TableColumnType,
    ConnectorAuthType,
    ConnectorAuthPropType
};

import { SchemaFX, type SchemaFXOptions } from './core';
export { SchemaFXOptions };
export default SchemaFX;
