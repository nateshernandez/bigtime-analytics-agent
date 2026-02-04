import { config } from "@/lib/config";
import { analyticsAgent } from "@/mastra/agents/analytics-agent";
import type { Config } from "@mastra/core";
import { Mastra } from "@mastra/core/mastra";
import { LibSQLStore } from "@mastra/libsql";
import { PinoLogger } from "@mastra/loggers";
import {
  Observability,
  DefaultExporter,
  CloudExporter,
  SensitiveDataFilter,
} from "@mastra/observability";

const storage = new LibSQLStore({
  id: "mastra-storage",
  url: config.isDevelopment ? ":memory:" : "file:./mastra.db",
});

const logger = new PinoLogger({
  name: "Mastra",
  level: "info",
});

const observability = new Observability({
  configs: {
    default: {
      serviceName: "mastra",
      exporters: [new DefaultExporter(), new CloudExporter()],
      spanOutputProcessors: [new SensitiveDataFilter()],
    },
  },
});

const bundlerConfig: NonNullable<Config["bundler"]> = {
  externals: ["pg", "lz4", "bufferutil", "utf-8-validate", "readable-stream"],
};

export const mastra = new Mastra({
  agents: { analyticsAgent },
  storage,
  logger,
  observability,
  bundler: bundlerConfig,
});
