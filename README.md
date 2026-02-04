# BigTime Analytics Agent POC

## ⚡️ Requirements

- [Docker Desktop](https://docs.docker.com/desktop/)
- [Node.js](https://github.com/Schniz/fnm)
- [pnpm](https://pnpm.io/)

## 🚀 Getting Started

1. Copy the environment template and configure your variables.

   ```bash
   cp .env.example .env
   ```

2. Start the local database.

   ```bash
   docker compose up -d
   ```

3. Run migrations.

   ```bash
   pnpm exec drizzle-kit migrate
   ```

4. Install dependencies.

   ```bash
   pnpm install
   ```

5. Populate operational schema embeddings.

   ```bash
   pnpm run populate-embeddings
   ```

6. Start the dev server.

   ```bash
   pnpm run dev
   ```

The application will be available at `http://localhost:4111`.

## 🤝 Contributing

This project adheres to the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) specification for commit messages.
