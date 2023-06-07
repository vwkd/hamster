import { fromZodError, ValidationError } from "../deps.ts";
import type { ZodError } from "../deps.ts";

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

/**
 * Create user error from Zod error
 * @param zodError Zod error
 * @returns user error
 */
export function createUserError(zodError: ZodError): ValidationError {
  return fromZodError(zodError, { prefix: "Invalid argument" });
}
