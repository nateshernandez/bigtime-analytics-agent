CREATE TABLE "operational_schema_embeddings" (
	"id" bigserial PRIMARY KEY NOT NULL,
	"schema_description" text NOT NULL,
	"table_name" text NOT NULL,
	"embedding" vector(1536) NOT NULL
);
--> statement-breakpoint
CREATE INDEX "schema_embeddings_embedding_idx" ON "operational_schema_embeddings" USING hnsw ("embedding" vector_cosine_ops);