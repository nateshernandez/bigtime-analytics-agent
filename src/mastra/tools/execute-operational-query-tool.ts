import { config } from "@/lib/config";
import { databricksLogger } from "@/lib/databricks/databricks-client";
import { DBSQLClient } from "@databricks/sql";
import { createTool } from "@mastra/core/tools";
import { z } from "zod";

const MAX_ROWS = 500;
const QUERY_TIMEOUT_SEC = 30;

const description = `
  Executes a read-only SQL query against the operational database (Databricks).
  Supports SELECT, WITH (CTEs), EXPLAIN, TABLE, and other read operations.
  Write operations (INSERT, UPDATE, DELETE, DROP, etc.) are blocked.
  Results are capped at ${MAX_ROWS} rows and queries timeout after ${QUERY_TIMEOUT_SEC} seconds.
  Returns the query results on success, or an error message with details on failure.
`;

const inputSchema = z.object({
  sqlQuery: z
    .string()
    .describe(
      "Read-only SQL query to execute (SELECT, WITH, EXPLAIN, TABLE, etc.)"
    ),
});

const outputSchema = z.object({
  success: z.boolean(),
  rows: z.array(z.record(z.string(), z.unknown())).optional(),
  rowCount: z.number().optional(),
  error: z.string().optional(),
});

const DISALLOWED_OPERATIONS = [
  "insert",
  "update",
  "delete",
  "drop",
  "create",
  "alter",
  "truncate",
  "grant",
  "revoke",
  "copy",
  "call",
  "do",
];

function containsDisallowedOperation(query: string): string | null {
  const normalizedQuery = query.toLowerCase();

  for (const operation of DISALLOWED_OPERATIONS) {
    const pattern = new RegExp(`\\b${operation}\\b`);

    if (pattern.test(normalizedQuery)) {
      return operation.toUpperCase();
    }
  }

  return null;
}

function ensureRowLimit(sqlQuery: string): string {
  const normalizedQuery = sqlQuery.toLowerCase();
  const hasRowLimit =
    normalizedQuery.includes("limit") || normalizedQuery.includes("fetch");

  return hasRowLimit
    ? sqlQuery
    : `${sqlQuery.trimEnd().replace(/;?\s*$/, "")} LIMIT ${MAX_ROWS}`;
}

export const executeOperationalQueryTool = createTool({
  id: "execute-operational-query",
  description,
  inputSchema,
  outputSchema,
  execute: async ({ sqlQuery }) => {
    const disallowedOperation = containsDisallowedOperation(sqlQuery);

    if (disallowedOperation) {
      return {
        success: false as const,
        error: `${disallowedOperation} operations are not allowed. Only read-only queries are permitted.`,
      };
    }

    const queryWithLimit = ensureRowLimit(sqlQuery);
    const client = new DBSQLClient({ logger: databricksLogger });

    try {
      const { host, httpPath, accessToken, catalog, schema } =
        config.databricks;
      await client.connect({
        host,
        path: httpPath,
        token: accessToken,
        socketTimeout: QUERY_TIMEOUT_SEC * 1000,
      });

      const session = await client.openSession({
        initialCatalog: catalog,
        initialSchema: schema,
        configuration: { STATEMENT_TIMEOUT: String(QUERY_TIMEOUT_SEC) },
      });

      const queryOperation = await session.executeStatement(queryWithLimit, {
        queryTimeout: BigInt(QUERY_TIMEOUT_SEC),
        maxRows: MAX_ROWS,
      });

      const rawRows = await queryOperation.fetchAll();
      await queryOperation.close();
      await session.close();
      await client.close();

      const rows = (rawRows as Record<string, unknown>[]).slice(0, MAX_ROWS);

      return {
        success: true as const,
        rows,
        rowCount: rows.length,
      };
    } catch (error) {
      try {
        await client.close();
      } catch {
        // ignore close errors
      }

      const errorMessage =
        error instanceof Error ? error.message : "Unknown Error Occurred";

      return {
        success: false as const,
        error: `Query execution failed: ${errorMessage}`,
      };
    }
  },
});
