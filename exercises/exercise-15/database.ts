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

interface DatabaseRecord<T> {
  type: "E" | "D";
  data: T;
}

class Mutex {
  _locking: Promise<void>;
  _locks: number;

  constructor() {
    this._locking = Promise.resolve();
    this._locks = 0;
  }

  isLocked() {
    return this._locks > 0;
  }

  lock() {
    this._locks += 1;

    let unlockNext: () => void;

    let willLock = new Promise<void>(
      (resolve) =>
        (unlockNext = () => {
          this._locks -= 1;

          resolve();
        })
    );

    let willUnlock = this._locking.then(() => unlockNext);

    this._locking = this._locking.then(() => willLock);

    return willUnlock;
  }
}

export class Database<T> {
  protected filename: string;
  protected fullTextSearchFieldNames: (keyof T)[];
  private mutex: Mutex;

  constructor(filename: string, fullTextSearchFieldNames: (keyof T)[]) {
    this.filename = filename;
    this.fullTextSearchFieldNames = fullTextSearchFieldNames;
    this.mutex = new Mutex();
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

  async readFromFile(): Promise<DatabaseRecord<T>[]> {
    const fileContent = await fs.readFile(this.filename, "utf8");
    return fileContent
      .split("\n")
      .filter((line) => line.trim().length > 0)
      .map((line) => ({
        type: line.startsWith("E") ? "E" : "D",
        data: JSON.parse(line.substr(1)) as T,
      }));
  }

  async writeToFile(records: DatabaseRecord<T>[]) {
    const dbString =
      records
        .map((record) => `${record.type}${JSON.stringify(record.data)}`)
        .join("\n") + "\n";

    await fs.writeFileSync(this.filename, dbString, { encoding: "utf8" });
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
    const contents = await (await this.readFromFile())
      .filter((record) => record.type === "E")
      .map((record) => record.data);

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

  async delete(query: TopLevelOperator<T>) {
    const unlock = await this.mutex.lock();

    const records = await this.readFromFile();

    const queryResults = await (await this.find(query)).map((data) =>
      JSON.stringify(data)
    );

    const newRecords = records.map((record) => ({
      type: queryResults.includes(JSON.stringify(record.data))
        ? "D"
        : record.type,
      data: record.data,
    }));

    await this.writeToFile(newRecords);

    unlock();
  }

  async insert(data: T) {
    const unlock = await this.mutex.lock();

    const records = await this.readFromFile();

    const newRecords: DatabaseRecord<T>[] = [...records, { type: "E", data }];

    await fs.writeFileSync(this.filename, `E${JSON.stringify(data)}\n`, {
      encoding: "utf8",
      flag: "a",
    });

    // await this.writeToFile(newRecords);

    unlock();
  }
}
