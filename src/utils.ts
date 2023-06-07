import { fromZodError, ValidationError, z, ZodParsedType } from "../deps.ts";
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

/**
 * Custom error map to add more descriptive errors to user-supplied database schema
 */
export const customErrorMap: z.ZodErrorMap = (iss, ctx) => {
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
