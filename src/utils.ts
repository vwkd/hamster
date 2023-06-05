/**
 * Convert async iterable iterator to array
 * 
 * Accepts optional transformer function that can change the values
 */
export async function gen2arr<T>(gen: AsyncIterable<T>): Promise<T[]> {
  const res: T[] = [];

  for await(const x of gen) {
    res.push(x);
  }
  return res;
}

/**
 * Convert array to object
 * 
 * Creates object where the keys are the values of the `keyName` array element property and the values are the array element without the `keyName` property
 */
// todo: make TS happy
export function arr2obj<KeyName extends string, ValueName extends string, Obj extends Record<KeyName, string>>(array: Obj[], keyName: KeyName, valueName: ValueName) {
  return array.reduce((acc, element) => {
    const key = element[keyName];
    const value = element[valueName];
    acc[key] = value;
    return acc;
  }, {});
}
