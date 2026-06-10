/**
 * Schema-builder version of the seed database, used ONLY by
 * `src/experimental/scope-resolver.test.ts` (via the re-export in
 * `sqlite.ts`). It lives in its own file so the shared fixture layer
 * (`fixture.ts`) does not depend on `src/experimental`.
 *
 * Note: this intentionally tracks `SeedDB` in `fixture.ts`, but currently
 * omits the `replies` table.
 */

import { integer, text } from "../experimental/schema/sqlite.ts";
import { createDatabase } from "../experimental/schema/table.ts";

export const seedDb = createDatabase("public", {
	users: {
		id: integer().generated(),
		username: text(),
		email: text(),
	},
	posts: {
		id: integer().generated(),
		user_id: integer(),
		title: text(),
		content: text(),
	},
	comments: {
		id: integer().generated(),
		post_id: integer(),
		user_id: integer(),
		content: text(),
	},
	profiles: {
		id: integer().generated(),
		user_id: integer(),
		bio: text().nullable(),
		avatar_url: text().nullable(),
	},
});
