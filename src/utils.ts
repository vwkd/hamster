import { fromZodError, ValidationError, z, ZodParsedType } from "../deps.ts";
import type { ZodError } from "../deps.ts";

// note: somehow needs this otherwise errors all over the place
export type StringKeyOf<T> = Extract<keyof T, string>;

export type PartialStringKey<T> = {
  [P in StringKeyOf<T>]?: T[P];
};

// patch Object.entries from https://stackoverflow.com/a/60142095/2607891
export type Entries<T> = {
  [K in StringKeyOf<T>]: [K, T[K]];
}[StringKeyOf<T>][];

/**
 * Check if object is not empty
 * @param obj object
 * @returns truthy if object has at least one field, falsy otherwise
 */
export function isNonempty(obj: { [x in string]: unknown } | undefined) {
  return obj && Object.keys(obj).length;
}

/**
 * Create user error from Zod error
 * @param zodError Zod error
 * @returns user error
 */
export function createUserError(zodError: ZodError): ValidationError {
  return fromZodError(zodError, { prefix: "Invalid argument" });
}

/**
 * Custom error map for own schemas to reduce repetition
 */
export const ownErrorMap: z.ZodErrorMap = (iss, ctx) => {
  const fieldName = iss.path.at(-1);

  // todo: can iss.path be empty?
  if (iss.code !== z.ZodIssueCode.invalid_type || !iss.path.length) {
    return { message: ctx.defaultError };
  }

  if (iss.received === ZodParsedType.undefined) {
    return { message: `'${fieldName}' is required` };
  }

  return {
    message: `'${fieldName}' must be ${
      getArticle(iss.expected)
    }${iss.expected}`,
  };
};

/**
 * Custom error map for user-supplied database schema
 * to patch on descriptive errors afterwards
 */
export const userErrorMap: z.ZodErrorMap = (iss, ctx) => {
  const columnName = iss.path[0];
  const fieldPath = iss.path.slice(1);

  // note: iss.path is empty if user passes empty argument!
  if (iss.code !== z.ZodIssueCode.invalid_type || !iss.path.length) {
    return { message: ctx.defaultError };
  }

  if (iss.received === ZodParsedType.undefined) {
    if (iss.path.length > 1) {
      return {
        message: `field '${
          fieldPath.join(".")
        }' in column '${columnName}' is required`,
      };
    } else {
      return { message: `column '${columnName}' is required` };
    }
  }

  if (iss.path.length > 1) {
    return {
      message: `field '${
        fieldPath.join(".")
      }' in column '${columnName}' must be ${
        getArticle(iss.expected)
      }${iss.expected}`,
    };
  } else {
    return {
      message: `column '${columnName}' must be ${
        getArticle(iss.expected)
      }${iss.expected}`,
    };
  }
};

/**
 * Get English article for type name
 *
 * e.g. an object, a function, nan, etc.
 * @param firstLetter first letter of type name
 * @returns article "a " or "an ", or ""
 */
function getArticle(typeName: string) {
  if (
    ["nan", "undefined", "null", "unknown", "void", "never"].includes(typeName)
  ) {
    return "";
  } else if (["integer", "array", "object"].includes(typeName)) {
    return "an ";
  } else {
    return "a ";
  }
}
