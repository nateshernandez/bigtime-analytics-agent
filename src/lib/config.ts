import "dotenv/config";

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
}

export const config = {
  db: {
    connectionString: required("DB_CONNECTION_STRING"),
  },
  databricks: {
    host: required("DATABRICKS_HOST"),
    httpPath: required("DATABRICKS_HTTP_PATH"),
    accessToken: required("DATABRICKS_ACCESS_TOKEN"),
    catalog: required("DATABRICKS_CATALOG"),
    schema: required("DATABRICKS_SCHEMA"),
  },
} as const;
