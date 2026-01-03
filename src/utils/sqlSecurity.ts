const CONTROL_CHARS = /[\x00-\x1F\x7F]/;

export function hasControlChars(s: string) {
    return CONTROL_CHARS.test(s);
}

export function validatePathOrUrl(s: string, name = 'value') {
    if (typeof s !== 'string') throw new Error(`${name} must be a string`);
    if (hasControlChars(s)) throw new Error(`${name} contains control characters`);
    if (s.includes("'") || s.includes('"')) throw new Error(`${name} contains embedded quotes`);
    return s.replace(/\\/g, '/');
}

export function escapeSqlLiteral(s: string) {
    // Escape single quotes by doubling them for safe SQL literal embedding
    return s.replace(/'/g, "''");
}

export function validateIdentifier(id: string) {
    // Allow simple identifiers and dotted paths (schema.table) but no quotes/control chars
    if (typeof id !== 'string') throw new Error('identifier must be a string');
    if (hasControlChars(id)) throw new Error('identifier contains control characters');
    if (/['\"]/.test(id)) throw new Error('identifier contains quotes');
    if (!/^[A-Za-z0-9_.$]+$/.test(id)) throw new Error('identifier contains invalid characters');
    return id;
}
