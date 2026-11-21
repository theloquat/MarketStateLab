import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";
export default defineConfig({
  plugins: [react()],
  test: {
    environment: "jsdom",
    include: ["src/contest.test.tsx", "src/events.test.tsx"],
    coverage: {
      provider: "v8",
      include: ["src/Contest*.tsx", "src/useContestEvents.ts"],
      reporter: ["json-summary", "text"],
      thresholds: { lines: 30, statements: 30 },
    },
  },
});
