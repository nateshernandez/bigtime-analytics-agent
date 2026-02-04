import { db } from "@/lib/db/client";
import { operationalSchemaEmbeddingsTable } from "@/lib/db/schemas/operational-schema-embeddings-schema";
import { createTool } from "@mastra/core/tools";
import { embed } from "ai";
import { cosineDistance, desc, sql } from "drizzle-orm";
import { z } from "zod";

const description = `
  Vector similarity search over operational database table schemas.
  Embeds the query and returns top 10 tables by cosine similarity.
  Each result includes the table name, full schema description (columns, types, keys, relationships), and similarity score.
`;

const inputSchema = z.object({
  query: z
    .string()
    .describe(
      "Natural language search query for finding relevant table schemas"
    ),
});

const outputSchema = z.object({
  tables: z.array(
    z.object({
      tableName: z.string(),
      schemaDescription: z.string(),
      similarityScore: z.number(),
    })
  ),
});

export const searchOperationalSchemaTool = createTool({
  id: "search-operational-schema",
  description,
  inputSchema,
  outputSchema,
  execute: async ({ query }) => {
    const { embedding } = await embed({
      model: "openai/text-embedding-3-small",
      value: query,
    });

    const similarityScore = sql<number>`1 - (${cosineDistance(operationalSchemaEmbeddingsTable.embedding, embedding)})`;

    const tables = await db
      .select({
        tableName: operationalSchemaEmbeddingsTable.tableName,
        schemaDescription: operationalSchemaEmbeddingsTable.schemaDescription,
        similarityScore,
      })
      .from(operationalSchemaEmbeddingsTable)
      .orderBy(desc(similarityScore))
      .limit(10);

    return { tables };
  },
});
