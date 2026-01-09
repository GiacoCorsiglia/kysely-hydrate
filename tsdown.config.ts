import { defineConfig } from "tsdown";

export default defineConfig({
	entry: ["src/index.ts", "src/experimental.ts"],
	exports: true,
});
