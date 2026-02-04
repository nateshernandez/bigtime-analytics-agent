import { bigserial, index, pgTable, text, vector } from "drizzle-orm/pg-core";

export const operationalSchemaEmbeddingsTable = pgTable(
  "operational_schema_embeddings",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    schemaDescription: text("schema_description").notNull(),
    tableName: text("table_name").notNull(),
    embedding: vector("embedding", { dimensions: 1536 }).notNull(),
  },
  (table) => [
    index("schema_embeddings_embedding_idx").using(
      "hnsw",
      table.embedding.op("vector_cosine_ops")
    ),
  ]
);

export type NewOperationalSchemaEmbedding =
  typeof operationalSchemaEmbeddingsTable.$inferInsert;
export type OperationalSchemaEmbedding =
  typeof operationalSchemaEmbeddingsTable.$inferSelect;
