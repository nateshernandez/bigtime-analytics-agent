import { config } from "@/lib/config";
import { drizzle } from "drizzle-orm/node-postgres";

export const db = drizzle(config.db.connectionString);
