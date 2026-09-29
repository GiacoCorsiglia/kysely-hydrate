// A standalone hydrator over flat join rows: fields, extras, omit and nested hasMany/hasOne.
import { expectTypeOf } from "expect-type";

import { createHydrator } from "../../src/index.ts";

interface Row {
	id: number;
	username: string;
	email: string;
	password_hash: string;
	profile$$id: number | null;
	profile$$bio: string | null;
	posts$$id: number | null;
	posts$$title: string | null;
	posts$$published_at: Date | null;
	posts$$comments$$id: number | null;
	posts$$comments$$content: string | null;
	posts$$comments$$author$$id: number | null;
	posts$$comments$$author$$username: string | null;
}

const hydrator = createHydrator<Row>("id")
	.fields({ id: true, username: true, email: (email) => email.toLowerCase(), password_hash: true })
	.extras({ handle: (row) => `@${row.username}` })
	.omit(["password_hash"])
	.hasOne("profile", "profile$$", (h) => h("id").fields({ id: true, bio: true }))
	.hasMany("posts", "posts$$", (h) =>
		h("id")
			.fields({ id: true, title: true, published_at: (d) => d?.toISOString() ?? null })
			.extras({ isPublished: (post) => post.published_at !== null })
			.hasMany("comments", "comments$$", (h) =>
				h("id")
					.fields({ id: true, content: true })
					.hasOneOrThrow("author", "author$$", (h) => h("id").fields({ id: true, username: true })),
			),
	);

expectTypeOf(hydrator.hydrate([] as Row[])).resolves.toEqualTypeOf<
	{
		id: number;
		username: string;
		email: string;
		handle: string;
		profile: { id: number | null; bio: string | null } | null;
		posts: {
			id: number | null;
			title: string | null;
			published_at: string | null;
			isPublished: boolean;
			comments: {
				id: number | null;
				content: string | null;
				author: { id: number | null; username: string | null };
			}[];
		}[];
	}[]
>();
