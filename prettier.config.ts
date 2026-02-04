import type { Config } from "prettier";

const config: Config = {
  /* General Formatting */
  semi: true,
  singleQuote: false,
  tabWidth: 2,
  trailingComma: "es5",
  printWidth: 80,

  /* Plugins */
  plugins: ["@trivago/prettier-plugin-sort-imports"],
};

export default config;
