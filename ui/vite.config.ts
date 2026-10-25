import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

const backend = process.env.TRIBUTARY_API ?? "http://127.0.0.1:8000";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: { "/api": { target: backend, ws: true, changeOrigin: true } },
  },
  test: { environment: "node" },
});
