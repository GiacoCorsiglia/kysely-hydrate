import { expectTypeOf } from "expect-type";

import { createHydrator } from "./hydrator.ts";

//
// Fields: selection and transformation
//

{
	interface User {
		id: number;
		name: string;
		age: number;
	}

	const hydrator = createHydrator<User>("id").fields({
		id: true,
		name: (name) => {
			expectTypeOf(name).toEqualTypeOf<string>();
			return name.toUpperCase();
		},
		age: (age) => {
			expectTypeOf(age).toEqualTypeOf<number>();
			return age.toString();
		},

		// Because of structural typing, you can provide a field that doesn't exist
		// in the input; however, it will not be included in the output type.
		nonExistentField: true,
	});

	const result = hydrator.hydrate([] as User[]);

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; name: string; age: string }[]>();

	expectTypeOf((await result)[0]!).toMatchObjectType<{ id: number }>();
	// @ts-expect-error - nonExistentField should not be included in the output type
	expectTypeOf((await result)[0]!).toMatchObjectType<{ nonExistentField: any }>();
}

//
// Extras: computed fields
//

{
	interface User {
		id: number;
		firstName: string;
		lastName: string;
	}

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, firstName: true, lastName: true })
		.extras({
			fullName: (user) => {
				// Extras receive the full input row
				expectTypeOf(user).toEqualTypeOf<User>();
				return `${user.firstName} ${user.lastName}`;
			},
			idSquared: (user) => {
				expectTypeOf(user).toEqualTypeOf<User>();
				return user.id * user.id;
			},
		});

	const result = hydrator.hydrate([] as User[]);

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			firstName: string;
			lastName: string;
			fullName: string;
			idSquared: number;
		}[]
	>();
}

//
// Nested collections: hasMany, hasOne, hasOneOrThrow
//

{
	interface UserRow {
		id: number;
		name: string;
		profile$$id: number | null;
		profile$$bio: string | null;
		posts$$id: number | null;
		posts$$title: string | null;
		settings$$id: number;
		settings$$theme: string;
	}

	const hydrator = createHydrator<UserRow>("id")
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (h) =>
			h("id").fields({
				id: true,
				title: (title) => {
					// Nested callbacks receive unprefixed types
					expectTypeOf(title).toEqualTypeOf<string | null>();
					return title;
				},
			}),
		)
		.hasOne("profile", "profile$$", (h) =>
			h("id").fields({
				id: true,
				bio: (bio) => {
					expectTypeOf(bio).toEqualTypeOf<string | null>();
					return bio;
				},
			}),
		)
		.hasOneOrThrow("settings", "settings$$", (h) =>
			h("id").fields({
				id: true,
				theme: (theme) => {
					expectTypeOf(theme).toEqualTypeOf<string>();
					return theme;
				},
			}),
		);

	const result = hydrator.hydrate([] as UserRow[]);

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			name: string;
			posts: Array<{ id: number | null; title: string | null }>;
			profile: { id: number | null; bio: string | null } | null;
			settings: { id: number; theme: string };
		}[]
	>();
}

//
// Attached collections: attachMany, attachOne, attachOneOrThrow
//

{
	interface User {
		id: number;
		name: string;
	}

	interface Post {
		id: number;
		userId: number;
		title: string;
	}

	interface Profile {
		id: number;
		userId: number;
		bio: string;
	}

	interface Settings {
		userId: number;
		theme: string;
	}

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachMany(
			"posts",
			async (users) => {
				// Attach callbacks receive array of parent rows
				expectTypeOf(users).toEqualTypeOf<User[]>();
				return [] as Post[];
			},
			{ matchChild: "userId" },
		)
		.attachOne(
			"profile",
			async (users) => {
				expectTypeOf(users).toEqualTypeOf<User[]>();
				return [] as Profile[];
			},
			{ matchChild: "userId" },
		)
		.attachOneOrThrow(
			"settings",
			async (users) => {
				expectTypeOf(users).toEqualTypeOf<User[]>();
				return [] as Settings[];
			},
			{ matchChild: "userId" },
		);

	const result = hydrator.hydrate([] as User[]);

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			name: string;
			posts: Post[];
			profile: Profile | null;
			settings: Settings;
		}[]
	>();
}

//
// Nested extras: receive unprefixed input
//

{
	interface UserRow {
		id: number;
		name: string;
		posts$$id: number | null;
		posts$$title: string | null;
		posts$$content: string | null;
	}

	const hydrator = createHydrator<UserRow>("id")
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (h) =>
			h("id")
				.fields({ id: true, title: true, content: true })
				.extras({
					summary: (post) => {
						// Nested extras receive unprefixed input
						expectTypeOf(post).toEqualTypeOf<{
							id: number | null;
							title: string | null;
							content: string | null;
						}>();
						return `${post.title}: ${post.content}`;
					},
				}),
		);

	const result = hydrator.hydrate([] as UserRow[]);

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			name: string;
			posts: Array<{
				id: number | null;
				title: string | null;
				content: string | null;
				summary: string;
			}>;
		}[]
	>();
}

//
// Nested has* calls: multiple levels of nesting
//

{
	interface UserRow {
		id: number;
		name: string;
		posts$$id: number | null;
		posts$$title: string | null;
		posts$$comments$$id: number | null;
		posts$$comments$$content: string | null;
		posts$$comments$$author$$id: number | null;
		posts$$comments$$author$$username: string | null;
	}

	const hydrator = createHydrator<UserRow>("id")
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (h) =>
			h("id")
				.fields({ id: true, title: true })
				.hasMany("comments", "comments$$", (h2) =>
					h2("id")
						.fields({
							id: true,
							content: (content) => {
								// Nested callbacks receive unprefixed types
								expectTypeOf(content).toEqualTypeOf<string | null>();
								return content;
							},
						})
						.hasOne("author", "author$$", (h3) =>
							h3("id").fields({
								id: true,
								username: (username) => {
									// Deeply nested callbacks still receive unprefixed types
									expectTypeOf(username).toEqualTypeOf<string | null>();
									return username;
								},
							}),
						),
				),
		);

	const result = hydrator.hydrate([] as UserRow[]);

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			name: string;
			posts: Array<{
				id: number | null;
				title: string | null;
				comments: Array<{
					id: number | null;
					content: string | null;
					author: { id: number | null; username: string | null } | null;
				}>;
			}>;
		}[]
	>();
}

//
// Nested attach* calls inside has* calls
//

{
	interface UserRow {
		id: number;
		name: string;
		posts$$id: number | null;
		posts$$title: string | null;
	}

	interface Comment {
		id: number;
		postId: number;
		content: string;
	}

	interface Tag {
		id: number;
		postId: number;
		name: string;
	}

	interface Metadata {
		postId: number;
		viewCount: number;
	}

	const hydrator = createHydrator<UserRow>("id")
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (h) =>
			h("id")
				.fields({ id: true, title: true })
				.attachMany(
					"comments",
					async (posts) => {
						// Attach callbacks in nested context receive unprefixed parent rows
						expectTypeOf(posts).toEqualTypeOf<{ id: number | null; title: string | null }[]>();
						return [] as Comment[];
					},
					{ matchChild: "postId", toParent: "id" },
				)
				.attachOne(
					"metadata",
					async (posts) => {
						expectTypeOf(posts).toEqualTypeOf<{ id: number | null; title: string | null }[]>();
						return [] as Metadata[];
					},
					{ matchChild: "postId", toParent: "id" },
				)
				.attachOneOrThrow(
					"primaryTag",
					async (posts) => {
						expectTypeOf(posts).toEqualTypeOf<{ id: number | null; title: string | null }[]>();
						return [] as Tag[];
					},
					{ matchChild: "postId", toParent: "id" },
				),
		);

	const result = hydrator.hydrate([] as UserRow[]);

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			name: string;
			posts: Array<{
				id: number | null;
				title: string | null;
				comments: Comment[];
				metadata: Metadata | null;
				primaryTag: Tag;
			}>;
		}[]
	>();
}

//
// Composite keys
//

{
	interface UserPost {
		userId: number;
		postId: number;
		content: string;
	}

	const hydrator = createHydrator<UserPost>(["userId", "postId"]).fields({
		userId: true,
		postId: true,
		content: true,
	});

	const result = hydrator.hydrate([] as UserPost[]);

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			userId: number;
			postId: number;
			content: string;
		}[]
	>();
}

//
// Invalid keys: should reject nonsense keys
//

{
	interface User {
		id: number;
		name: string;
	}

	// @ts-expect-error - nonExistentKey is not a valid key
	createHydrator<User>("nonExistentKey");

	// @ts-expect-error - composite key with invalid key
	createHydrator<User>(["id", "nonExistentKey"]);

	// @ts-expect-error - all keys invalid in composite
	createHydrator<User>(["foo", "bar"]);
}

{
	interface UserRow {
		id: number;
		name: string;
		posts$$id: number | null;
		posts$$title: string | null;
	}

	createHydrator<UserRow>("id")
		.fields({ id: true, name: true })
		.hasMany(
			"posts",
			"posts$$",
			// @ts-expect-error - invalid key for nested collection
			(h) => h("invalidKey").fields({ id: true, title: true }),
		);

	createHydrator<UserRow>("id")
		.fields({ id: true, name: true })
		.hasOne(
			"post",
			"posts$$",
			// @ts-expect-error - composite key with invalid key in nested collection
			(h) => h(["id", "invalidKey"]).fields({ id: true, title: true }),
		);
}

{
	interface User {
		id: number;
		name: string;
	}

	interface Post {
		id: number;
		userId: number;
		title: string;
	}

	createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachMany(
			"posts",
			async (_users) => [] as Post[],
			// @ts-expect-error - invalidChild is not a key in Post
			{ matchChild: "invalidChild" },
		);

	createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachOne(
			"post",
			async (_users) => [] as Post[],
			// @ts-expect-error - invalidParent is not a key in the parent row
			{ matchChild: "userId", toParent: "invalidParent" },
		);

	createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachOneOrThrow(
			"post",
			// @ts-expect-error - both keys invalid (error appears on callback)
			async (_users) => [] as Post[],
			{ matchChild: "invalidChild", toParent: "invalidParent" },
		);
}
