import { config } from "@/lib/config";
import { db } from "@/lib/db/client";
import { databricksClient } from "@/lib/databricks/databricks-client";
import {
  NewOperationalSchemaEmbedding,
  operationalSchemaEmbeddingsTable,
} from "@/lib/db/schemas/operational-schema-embeddings-schema";
import { DBSQLClient } from "@databricks/sql";
import { ModelRouterEmbeddingModel } from "@mastra/core/llm";
import { embed } from "ai";
import { sql } from "drizzle-orm";

type DBSQLSession = Awaited<ReturnType<DBSQLClient["openSession"]>>;

interface ColumnInfo {
  columnName: string;
  dataType: string;
  isNullable: boolean;
  isPrimaryKey: boolean;
  isForeignKey: boolean;
}

interface ForeignKeyInfo {
  columnName: string;
  referencedTable: string;
  referencedColumn: string;
}

interface EnumValue {
  columnName: string;
  values: string[];
}

interface TableMetadata {
  tableName: string;
  columns: ColumnInfo[];
  primaryKeys: string[];
  foreignKeys: ForeignKeyInfo[];
  enumValues: EnumValue[];
}

interface ShowTablesRow {
  tableName: string;
}

interface DescribeTableRow {
  col_name: string;
  data_type: string;
}

interface TablePropertyRow {
  key: string;
  value: string;
}

interface DistinctValueRow {
  value: string | null;
}

async function getTables(session: DBSQLSession): Promise<string[]> {
  const queryOperation = await session.executeStatement(
    `SHOW TABLES IN \`${config.databricks.catalog}\`.\`${config.databricks.schema}\``,
    {
      runAsync: true,
      maxRows: 10000,
    }
  );

  const result = await queryOperation.fetchAll();
  await queryOperation.close();

  return result.map((row) => (row as ShowTablesRow).tableName);
}

async function getColumns(
  session: DBSQLSession,
  tableName: string
): Promise<ColumnInfo[]> {
  const fullTableName = `\`${config.databricks.catalog}\`.\`${config.databricks.schema}\`.\`${tableName}\``;
  const queryOperation = await session.executeStatement(
    `DESCRIBE TABLE EXTENDED ${fullTableName}`,
    {
      runAsync: true,
      maxRows: 10000,
    }
  );

  const result = await queryOperation.fetchAll();
  await queryOperation.close();

  const columns: ColumnInfo[] = [];
  for (const row of result) {
    const typedRow = row as DescribeTableRow;

    if (!typedRow.col_name || typedRow.col_name.startsWith("#")) {
      break;
    }

    if (typedRow.col_name.trim() === "") {
      continue;
    }

    columns.push({
      columnName: typedRow.col_name,
      dataType: typedRow.data_type || "string",
      isNullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
    });
  }

  return columns;
}

async function getPrimaryKeys(
  session: DBSQLSession,
  tableName: string
): Promise<string[]> {
  try {
    const fullTableName = `\`${config.databricks.catalog}\`.\`${config.databricks.schema}\`.\`${tableName}\``;
    const queryOperation = await session.executeStatement(
      `SHOW TBLPROPERTIES ${fullTableName}`,
      {
        runAsync: true,
        maxRows: 10000,
      }
    );

    const result = await queryOperation.fetchAll();
    await queryOperation.close();

    for (const row of result) {
      const typedRow = row as TablePropertyRow;
      if (typedRow.key === "primaryKey" || typedRow.key === "primary_key") {
        return typedRow.value.split(",").map((col) => col.trim());
      }
    }

    return [];
  } catch {
    return [];
  }
}

async function getForeignKeys(
  session: DBSQLSession,
  tableName: string
): Promise<ForeignKeyInfo[]> {
  try {
    const fullTableName = `\`${config.databricks.catalog}\`.\`${config.databricks.schema}\`.\`${tableName}\``;
    const queryOperation = await session.executeStatement(
      `DESCRIBE TABLE EXTENDED ${fullTableName}`,
      {
        runAsync: true,
        maxRows: 10000,
      }
    );

    const result = await queryOperation.fetchAll();
    await queryOperation.close();

    const foreignKeys: ForeignKeyInfo[] = [];
    let inDetailSection = false;

    for (const row of result) {
      const typedRow = row as DescribeTableRow;

      if (typedRow.col_name === "# Detailed Table Information") {
        inDetailSection = true;
        continue;
      }

      if (inDetailSection && typedRow.col_name?.includes("Foreign Key")) {
        const fkMatch = typedRow.data_type?.match(/(\w+)\s*->\s*(\w+)\.(\w+)/);
        if (fkMatch) {
          foreignKeys.push({
            columnName: fkMatch[1],
            referencedTable: fkMatch[2],
            referencedColumn: fkMatch[3],
          });
        }
      }
    }

    return foreignKeys;
  } catch {
    return [];
  }
}

async function detectEnumValues(
  session: DBSQLSession,
  tableName: string,
  columns: ColumnInfo[]
): Promise<EnumValue[]> {
  const textTypes = ["string", "text", "varchar", "char"];

  const candidates = columns.filter((c) =>
    textTypes.includes(c.dataType.toLowerCase())
  );

  const enumValues: EnumValue[] = [];

  for (const column of candidates) {
    try {
      const fullTableName = `\`${config.databricks.catalog}\`.\`${config.databricks.schema}\`.\`${tableName}\``;
      const queryOperation = await session.executeStatement(
        `SELECT DISTINCT \`${column.columnName}\` AS value
           FROM ${fullTableName}
          WHERE \`${column.columnName}\` IS NOT NULL
          LIMIT 20`,
        {
          runAsync: true,
          maxRows: 20,
        }
      );

      const result = await queryOperation.fetchAll();
      await queryOperation.close();

      const values = result
        .map((row) => (row as DistinctValueRow).value)
        .filter(
          (value): value is string => value !== null && value !== undefined
        );

      if (values.length >= 2 && values.length <= 10) {
        enumValues.push({
          columnName: column.columnName,
          values: values.sort(),
        });
      }
    } catch {
      continue;
    }
  }

  return enumValues;
}

async function extractTableMetadata(
  session: DBSQLSession,
  tableName: string
): Promise<TableMetadata> {
  const [columns, primaryKeys, foreignKeys] = await Promise.all([
    getColumns(session, tableName),
    getPrimaryKeys(session, tableName),
    getForeignKeys(session, tableName),
  ]);

  const pkSet = new Set(primaryKeys);
  const fkSet = new Set(foreignKeys.map((fk) => fk.columnName));

  for (const col of columns) {
    col.isPrimaryKey = pkSet.has(col.columnName);
    col.isForeignKey = fkSet.has(col.columnName);
  }

  const enumValues = await detectEnumValues(session, tableName, columns);
  return { tableName, columns, primaryKeys, foreignKeys, enumValues };
}

function formatTableContent(metadata: TableMetadata): string {
  const lines: string[] = [`Table: ${metadata.tableName}`];

  const columnDescriptions = metadata.columns.map((col) => {
    const constraints: string[] = [];
    if (col.isPrimaryKey) constraints.push("primary key");
    if (col.isForeignKey) constraints.push("foreign key");
    if (!col.isNullable && !col.isPrimaryKey) constraints.push("not null");
    const suffix = constraints.length > 0 ? `, ${constraints.join(", ")}` : "";
    return `${col.columnName} (${col.dataType}${suffix})`;
  });

  lines.push(`Columns: ${columnDescriptions.join(", ")}`);

  if (metadata.foreignKeys.length > 0) {
    const fkDescriptions = metadata.foreignKeys.map(
      (fk) =>
        `${metadata.tableName}.${fk.columnName} → ${fk.referencedTable}.${fk.referencedColumn}`
    );

    lines.push(`Foreign Keys: ${fkDescriptions.join(", ")}`);
  }

  if (metadata.primaryKeys.length > 0) {
    lines.push(`Primary Keys: ${metadata.primaryKeys.join(", ")}`);
  }

  if (metadata.enumValues.length > 0) {
    const enumDescriptions = metadata.enumValues.map(
      (ev) =>
        `${ev.columnName} can be ${ev.values.map((v) => `'${v}'`).join(", ")}`
    );

    lines.push(`Sample Values: ${enumDescriptions.join("; ")}`);
  }

  return lines.join("\n");
}

async function main(): Promise<void> {
  let session: DBSQLSession | null = null;

  try {
    await databricksClient.connect({
      host: config.databricks.host,
      path: config.databricks.httpPath,
      token: config.databricks.accessToken,
    });
    console.log("Connected to Databricks.");

    session = await databricksClient.openSession({
      initialCatalog: config.databricks.catalog,
      initialSchema: config.databricks.schema,
    });
    console.log("Opened Databricks session.");

    await db.execute(sql`SELECT 1`);
    console.log("Connected to analytics database.");

    await db.delete(operationalSchemaEmbeddingsTable);
    console.log("Cleared existing embeddings.");

    const tableNames = await getTables(session);
    console.log(`Found ${tableNames.length} tables to process.\n`);

    const embeddings: NewOperationalSchemaEmbedding[] = [];

    for (let i = 0; i < tableNames.length; i++) {
      const tableName = tableNames[i];
      process.stdout.write(`[${i + 1}/${tableNames.length}] ${tableName}... `);

      const metadata = await extractTableMetadata(session, tableName);
      const content = formatTableContent(metadata);

      const { embedding } = await embed({
        model: "openai/text-embedding-3-small",
        value: content,
      });

      embeddings.push({ schemaDescription: content, tableName, embedding });
      console.log("done");
    }

    await db.transaction(async (tx) => {
      for (const record of embeddings) {
        await tx.insert(operationalSchemaEmbeddingsTable).values(record);
      }
    });

    console.log(`\nInserted ${embeddings.length} embeddings.`);
  } catch (error) {
    console.error("Fatal error:", error);
    process.exit(1);
  } finally {
    if (session) {
      await session.close();
    }
    await databricksClient.close();
    await db.$client.end();
  }
}

main();
