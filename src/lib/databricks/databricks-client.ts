import { DBSQLClient } from "@databricks/sql";
import type IDBSQLLogger from "@databricks/sql/dist/contracts/IDBSQLLogger";
import { LogLevel } from "@databricks/sql/dist/contracts/IDBSQLLogger";

export const databricksLogger: IDBSQLLogger = {
  log: (level: LogLevel, message: string): void => {
    if (level === LogLevel.error && !message.includes("LZ4")) {
      console.error(message);
    }
  },
};

export const databricksClient = new DBSQLClient({
  logger: databricksLogger,
});
