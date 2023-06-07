// note: somehow needs this otherwise errors all over the place
export type StringKeyOf<T extends object> = Extract<keyof T, string>;

/**
 * Check if object is not empty
 * @param obj object
 * @returns truthy if object has at least one field, falsy otherwise
 */
export function isNonempty(obj: object) {
  return Object.keys(obj).length;
}
