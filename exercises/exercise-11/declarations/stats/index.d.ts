declare module "stats" {
  export function getMaxIndex<T>(
    input: T[],
    comparator: (a: T, b: T) => number
  ): number;

  export function getMaxElement<T>(
    input: T[],
    comparator: (a: T, b: T) => number
  ): T | null;

  export function getMinIndex<T>(
    input: T[],
    comparator: (a: T, b: T) => number
  ): number;

  export function getMinElement<T>(
    input: T[],
    comparator: (a: T, b: T) => number
  ): T | null;

  export function getMedianIndex<T>(
    input: T[],
    comparator: (a: T, b: T) => number
  ): number;

  export function getMedianElement<T>(
    input: T[],
    comparator: (a: T, b: T) => number
  ): T | null;

  export function getAverageValue<T>(
    input: T[],
    getValue: (input: T) => number
  ): number | null;
}
