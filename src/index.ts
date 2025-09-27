import { type TableDefinition, type TableColumnDefinition, TableColumnType } from './schemas';
export { TableDefinition, TableColumnDefinition };

import { zodToTableColumns } from './utils/zodToTableColumns';
export const utils = { zodToTableColumns };

import { Connector, ConnectorAuthType, ConnectorAuthPropType } from './connector';
export { Connector };

export const Enums = {
    TableColumnType,
    ConnectorAuthType,
    ConnectorAuthPropType
};

import { SchemaFX, type SchemaFXOptions } from './core';
export { SchemaFXOptions };
export default SchemaFX;
