import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  // le build est servi directement par le serveur node
  build: { outDir: "../serveur/public", emptyOutDir: true },
  // en dev (npm run dev) on redirige l'api et le websocket vers le serveur node
  server: {
    proxy: {
      "/api": "http://localhost:3000",
      "/ws": { target: "ws://localhost:3000", ws: true },
    },
  },
});
