/**
 * Efficiently checks if all elements of `subset` are present in `superset`.
 * @param subset The array whose elements are to be checked.
 * @param superset The array to check against.
 * @returns `true` if all elements in `subset` are in `superset`, otherwise `false`.
 */
export default function containsAll<T>(subset: T[], superset: T[]): boolean {
    if (subset.length > superset.length) {
        return false;
    }

    const supersetSet = new Set(superset);

    for (const element of subset) {
        if (!supersetSet.has(element)) {
            return false;
        }
    }

    return true;
}
