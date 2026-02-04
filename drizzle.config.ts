import { config } from "./src/lib/config";
import "dotenv/config";
import { defineConfig } from "drizzle-kit";

export default defineConfig({
  out: "./src/lib/db/migrations",
  schema: "./src/lib/db/schemas/*.ts",
  dialect: "postgresql",
  dbCredentials: {
    url: config.db.connectionString,
  },
});
