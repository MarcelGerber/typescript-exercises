import { fs } from "mz";

type QueryOperator<S> =
  | { $eq: S }
  | { $gt: number }
  | { $lt: number }
  | { $in: S[] };

type Query<T> = { [key in keyof T]?: QueryOperator<T[key]> };

type TopLevelOperator<T> =
  | Query<T>
  | { $and: TopLevelOperator<T>[] }
  | { $or: TopLevelOperator<T>[] }
  | { $text: string };

interface QueryOptions<T> {
  sort?: { [key in keyof T]?: 1 | -1 };
  projection?: { [key in keyof T]?: 1 };
}

export class Database<T> {
  protected filename: string;
  protected fullTextSearchFieldNames: (keyof T)[];

  constructor(filename: string, fullTextSearchFieldNames: (keyof T)[]) {
    this.filename = filename;
    this.fullTextSearchFieldNames = fullTextSearchFieldNames;
  }

  operatorToPredicate<S>(query: QueryOperator<S>): (field: S) => boolean {
    return (field: S) => {
      if ("$eq" in query) {
        return field === query.$eq;
      } else if ("$lt" in query && typeof field === "number") {
        return field < query.$lt;
      } else if ("$gt" in query && typeof field === "number") {
        return field > query.$gt;
      } else if ("$in" in query) {
        return query.$in.includes(field);
      }
      return false;
    };
  }

  queryToPredicate(query: Query<T>): (data: T) => boolean {
    return (data: T) => {
      return (Object.keys(query) as (keyof T)[]).every((key) =>
        this.operatorToPredicate(query[key]!)(data[key])
      );
    };
  }

  topLevelPredicate(query: TopLevelOperator<T>): (data: T) => boolean {
    return (data: T) => {
      if ("$and" in query) {
        return query.$and.every((q) => this.topLevelPredicate(q)(data));
      } else if ("$or" in query) {
        return query.$or.some((q) => this.topLevelPredicate(q)(data));
      } else if ("$text" in query) {
        return this.fullTextSearchFieldNames
          .map((fieldName) => (data[fieldName] as unknown) as string)
          .some((text: string) =>
            new RegExp(`\\b${query.$text}\\b`, "i").test(text)
          );
      } else {
        return this.queryToPredicate(query)(data);
      }
    };
  }

  async find(
    query: TopLevelOperator<T>,
    options?: Omit<QueryOptions<T>, "projection">
  ): Promise<T[]>;

  async find(
    query: TopLevelOperator<T>,
    options?: QueryOptions<T>
  ): Promise<Partial<T>[]>;

  async find(
    query: TopLevelOperator<T>,
    options?: QueryOptions<T>
  ): Promise<Partial<T>[]> {
    const fileContent = await fs.readFile(this.filename, "utf8");
    const contents: T[] = fileContent
      .split("\n")
      .filter((line) => line.startsWith("E"))
      .map((line) => JSON.parse(line.substr(1)) as T);

    const predicate = this.topLevelPredicate(query);

    let results = contents.filter((data) => predicate(data));

    if (options?.sort) {
      Object.entries<1 | -1 | undefined>(options.sort).forEach(
        ([sortKey, sortOrder]) => {
          const [sKey, sOrder] = [sortKey as keyof T, sortOrder as 1 | -1];
          results = results.sort((a, b) =>
            a[sKey] > b[sKey] ? 1 * sOrder : -1 * sOrder
          );
        }
      );
    }

    if (options?.projection) {
      let newResults = [];
      for (const result of results) {
        let newResult: Partial<T> = {};
        for (const [pKey, pInclude] of Object.entries(options.projection)) {
          const key = pKey as keyof T;
          if (pInclude) newResult[key] = result[key];
        }
        newResults.push(newResult);
      }
      return newResults;
    }

    return results;
  }
}
