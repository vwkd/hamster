// note: somehow needs this otherwise errors all over the place
export type StringKeyOf<T extends object> = Extract<keyof T, string>;
