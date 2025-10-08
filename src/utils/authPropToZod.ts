import z, { type ZodType } from 'zod';
import { ConnectorAuthPropType } from '../connector';

export default function authPropToZod(
    prop: ConnectorAuthPropType | { type: ConnectorAuthPropType; required: boolean }
): ZodType {
    if (prop && typeof prop === 'object') {
        return prop.required === false
            ? authPropToZod(prop.type).optional()
            : authPropToZod(prop.type);
    }

    switch (prop) {
        case ConnectorAuthPropType.Number:
            return z.number();
    }

    return z.string();
}
