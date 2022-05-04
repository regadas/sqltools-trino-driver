import AbstractDriver from "@sqltools/base-driver";
import queries from "./queries";
import {
  IConnectionDriver,
  MConnectionExplorer,
  NSDatabase,
  ContextValue,
  Arg0,
} from "@sqltools/types";
import { v4 as generateId } from "uuid";
import presto from "presto-client";

type DriverLib = typeof presto.Client;
type DriverOptions = any;

export default class TrinoDriver
  extends AbstractDriver<DriverLib, DriverOptions>
  implements IConnectionDriver {
  queries = queries;

  public async open(): Promise<presto.Client> {
    if (this.connection) {
      return this.connection;
    }

    const connOptions = {
      host: this.credentials.host,
      port: 8080,
      catalog: this.credentials.catalog,
      schema: this.credentials.schema,
      user: this.credentials.user,
      engine: "trino",
      source: "sqltools-driver",
    };

    try {
      const conn = new presto.Client(connOptions);
      this.connection = Promise.resolve(conn);
    } catch (error) {
      return Promise.reject(error);
    }

    return this.connection;
  }

  public async close() {
    if (!this.connection) return Promise.resolve();
    await this.connection;
    this.connection = null;
  }

  public query: typeof AbstractDriver["prototype"]["query"] = async (
    query,
    opt = {}
  ) => {
    const { requestId } = opt;
    return this.open()
      .then(async (db) => {
        const queriesResults: any[] = await new Promise((resolve, reject) => {
          let results = [];
          db.execute({
            query: query,
            data: function (error, data, columns, _) {
              if (error) return reject(error);

              data.forEach((row) => {
                const d = {};
                columns.forEach((element, ci) => (d[element.name] = row[ci]));
                results.push(d);
              });
            },
            success: function (error, _) {
              if (error) return reject(error);
              resolve(results);
            },
            error: function (error) {
              reject(error);
            },
          });
        });
        return queriesResults;
      })
      .then((results) => {
        return [
          <NSDatabase.IResult>{
            requestId,
            resultId: generateId(),
            connId: this.getId(),
            cols: Object.keys(results[0]),
            messages: [
              this.prepareMessage(
                [`Successfully executed. ${results.length} rows were affected.`]
                  .filter(Boolean)
                  .join(" ")
              ),
            ],
            query,
            results,
          },
        ];
      })
      .catch((error) => {
        return [
          <NSDatabase.IResult>{
            connId: this.getId(),
            requestId,
            resultId: generateId(),
            cols: [],
            messages: [
              this.prepareMessage(
                [error.message.replace(/\n/g, " ")].filter(Boolean).join(" ")
              ),
            ],
            error: true,
            rawError: error,
            query,
            results: [],
          },
        ];
      });
  };

  public async testConnection() {
    await this.open();
    const testSelect = await this.query("SELECT 1", {});
    if (testSelect[0].error) {
      return Promise.reject({
        message: `Connected but cannot run SQL. ${testSelect[0].rawError}`,
      });
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
        return this.queryResults(queries.fetchTables(parent as NSDatabase.ISchema));
      case ContextValue.VIEW:
        return this.queryResults(queries.fetchViews(parent as NSDatabase.ISchema));
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
        return this.queryResults(queries.searchColumns({ search, ..._extraParams }));
    }
    return [];
  }

  public getStaticCompletions: IConnectionDriver["getStaticCompletions"] =
    async () => {
      return {};
    };
}
