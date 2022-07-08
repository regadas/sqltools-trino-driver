type Rows = Array<{ [key: string]: any }>;

type Columns = Array<{ name: string; type: string }>;

export type QueryResult = {
  rows?: Rows;
  columns?: Columns;
  error?: Error;
};
