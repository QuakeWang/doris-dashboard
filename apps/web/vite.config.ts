import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

const coopHeaders = {
  "Cross-Origin-Opener-Policy": "same-origin",
  "Cross-Origin-Embedder-Policy": "require-corp",
} as const;

export default defineConfig({
  plugins: [react()],
  server: {
    headers: coopHeaders,
    port: 12305,
  },
  preview: {
    headers: coopHeaders,
    port: 12305,
  },
  test: {
    environment: "node",
    include: ["src/**/*.test.ts"],
  },
});
