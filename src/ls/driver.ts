import AbstractDriver from "@sqltools/base-driver";
import queries from "./queries";
import {
  IConnectionDriver,
  MConnectionExplorer,
  NSDatabase,
  ContextValue,
  Arg0,
  IQueryOptions,
} from "@sqltools/types";
import { v4 as generateId } from "uuid";
import { QueryResult } from "./types";
import { QueryParser } from "./parser";
import { BasicAuth, ConnectionOptions, QueryData, Trino } from "trino-client";

type DriverLib = Trino;
type DriverOptions = any;

export default class TrinoDriver
  extends AbstractDriver<DriverLib, DriverOptions>
  implements IConnectionDriver
{
  queries = queries;

  public async open(): Promise<Trino> {
    if (this.connection) {
      return this.connection;
    }

    const connOptions: ConnectionOptions = {
      server: this.credentials.server,
      catalog: this.credentials.catalog,
      schema: this.credentials.schema,
      source: "sqltools-driver",
      auth: new BasicAuth(this.credentials.user, this.credentials.password),
      ssl: this.credentials.trinoOptions?.ssl, 
      client_tags: this.credentials.clientTags,
    };

    try {
      const conn = Trino.create(connOptions);
      this.connection = Promise.resolve(conn);
    } catch (error) {
      return Promise.reject(error);
    }

    return this.connection;
  }

  public async close(): Promise<void> {
    if (!this.connection) return Promise.resolve();
    await this.connection;
    this.connection = null;
  }

  private buildResult = (
    query: string,
    queryResult: QueryResult,
    opts: IQueryOptions
  ): NSDatabase.IResult => {
    const columns = queryResult.columns ?? [];
    const rows = queryResult.rows ?? [];
    const msg = queryResult.error
      ? queryResult.error.message
      : `Successfully executed. ${rows.length} rows were affected.`;

    return {
      requestId: opts.requestId,
      resultId: generateId(),
      connId: this.getId(),
      cols: columns.map((col) => col.name),
      results: rows,
      messages: [this.prepareMessage(msg)],
      error: queryResult.error ? true : false,
      rawError: queryResult.error,
      query,
    };
  };

  private async executeQuery(db: Trino, query: string): Promise<QueryResult> {
    const empty: QueryResult = {} as QueryResult;
    return (await db.query(query)).fold(empty, (qr, acc) => {
      if (qr.error) {
        return {
          error: new Error(qr.error.message),
        };
      }

      if (!acc.columns || acc.columns.length == 0) {
        acc.columns = (qr.columns ?? []).map(
          (c) =>
            <{ name: string; type: string }>{
              name: c.name,
              type: c.type,
            }
        );
      }

      const rows = (qr.data ?? []).map((row: QueryData[]) => {
        const data = {};
        row.forEach((value, idx) => (data[acc.columns[idx].name] = value));
        return data;
      });
      acc.rows = acc.rows ? [...acc.rows, ...rows] : rows;

      return acc;
    });
  }

  public query: typeof AbstractDriver["prototype"]["query"] = async (
    query: string,
    opt = {}
  ) => {
    const resultsAgg: NSDatabase.IResult[] = [];
    const db = await this.open();

    for (const q of QueryParser.statements(query)) {
      const iresult: NSDatabase.IResult = await this.executeQuery(db, q)
        .then((result) => this.buildResult(q, result, opt))
        .catch((error) => this.buildResult(q, { error: error }, opt));

      resultsAgg.push(iresult);
    }

    return resultsAgg;
  };

  public async testConnection() {
    await this.open();
    const testSelect = await this.query("SELECT 1", {});

    if (testSelect.length > 0 && testSelect[0].error) {
      const msg = testSelect[0].messages
        .map((m: { message: string; date: Date }) => m.message)
        .join("\n");

      return Promise.reject({ message: msg });
    }
  }

  /**
   * This method is a helper to generate the connection explorer tree.
   * it gets the child items based on current item
   */
  public async getChildrenForItem({
    item,
    parent,
  }: Arg0<IConnectionDriver["getChildrenForItem"]>) {
    switch (item.type) {
      case ContextValue.CONNECTION:
      case ContextValue.CONNECTED_CONNECTION:
        return this.queryResults(
          queries.fetchSchemas({
            database: this.credentials.catalog,
          } as NSDatabase.IDatabase)
        );
      case ContextValue.SCHEMA:
        return <MConnectionExplorer.IChildItem[]>[
          {
            label: "Tables",
            type: ContextValue.RESOURCE_GROUP,
            iconId: "folder",
            childType: ContextValue.TABLE,
          },
          {
            label: "Views",
            type: ContextValue.RESOURCE_GROUP,
            iconId: "folder",
            childType: ContextValue.VIEW,
          },
        ];
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        return this.queryResults(
          queries.fetchColumns(item as NSDatabase.ITable)
        );
      case ContextValue.RESOURCE_GROUP:
        return this.getChildrenForGroup({ item, parent });
    }
    return [];
  }

  /**
   * This method is a helper to generate the connection explorer tree.
   * It gets the child based on child types
   */
  private async getChildrenForGroup({
    parent,
    item,
  }: Arg0<IConnectionDriver["getChildrenForItem"]>) {
    switch (item.childType) {
      case ContextValue.TABLE:
        return this.queryResults(
          queries.fetchTables(parent as NSDatabase.ISchema)
        );
      case ContextValue.VIEW:
        return this.queryResults(
          queries.fetchViews(parent as NSDatabase.ISchema)
        );
    }
    return [];
  }

  /**
   * This method is a helper for intellisense and quick picks.
   */
  public async searchItems(
    itemType: ContextValue,
    search: string,
    _extraParams: any = {}
  ): Promise<NSDatabase.SearchableItem[]> {
    switch (itemType) {
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        return this.queryResults(queries.searchTables({ search }));
      case ContextValue.COLUMN:
        return this.queryResults(
          queries.searchColumns({ search, ..._extraParams })
        );
    }
    return [];
  }

  public getStaticCompletions: IConnectionDriver["getStaticCompletions"] =
    async () => {
      return {};
    };
}
