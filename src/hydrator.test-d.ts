import { expectTypeOf } from "expect-type";

import { createHydrator, hydrateData } from "./hydrator.ts";

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

//
// omit: removes fields from output
//

{
	interface User {
		id: number;
		name: string;
		email: string;
		password: string;
	}

	// Basic omit
	const hydrator1 = createHydrator<User>("id")
		.fields({ id: true, name: true, email: true, password: true })
		.omit(["password"]);

	const result1 = hydrator1.hydrate([] as User[]);

	expectTypeOf(result1).resolves.toEqualTypeOf<
		{
			id: number;
			name: string;
			email: string;
		}[]
	>();

	// Omit multiple fields
	const hydrator2 = createHydrator<User>("id")
		.fields({ id: true, name: true, email: true, password: true })
		.omit(["email", "password"]);

	const result2 = hydrator2.hydrate([] as User[]);

	expectTypeOf(result2).resolves.toEqualTypeOf<{ id: number; name: string }[]>();

	// @ts-expect-error - cannot omit non-existent field
	createHydrator<User>("id").fields({ id: true, name: true }).omit(["nonExistent"]);
}

//
// Nested omit
//

{
	interface UserRow {
		id: number;
		name: string;
		posts$$id: number | null;
		posts$$title: string | null;
		posts$$content: string | null;
		posts$$internalNotes: string | null;
	}

	const hydrator = createHydrator<UserRow>("id")
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (h) =>
			h("id")
				.fields({ id: true, title: true, content: true, internalNotes: true })
				.omit(["internalNotes"]),
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
			}>;
		}[]
	>();
}

//
// extend: composing hydrators
//

{
	interface User {
		id: number;
		name: string;
		email: string;
	}

	// Basic extend: merges fields
	const baseHydrator = createHydrator<User>("id").fields({ id: true, name: true });

	const emailHydrator = createHydrator<User>("id").fields({ email: true });

	const combined = baseHydrator.extend(emailHydrator);

	const result = combined.hydrate([] as User[]);

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; name: string; email: string }[]>();

	// Other hydrator's field types take precedence
	const upperHydrator = createHydrator<User>("id").fields({
		name: (name): string => name.toUpperCase(),
	});

	const lengthHydrator = createHydrator<User>("id").fields({
		name: (name): number => name.length,
	});

	const combined2 = upperHydrator.extend(lengthHydrator);

	const result2 = combined2.hydrate([] as User[]);

	// name is now number (from lengthHydrator)
	expectTypeOf(result2).resolves.toEqualTypeOf<{ name: number }[]>();

	// Merges extras
	const hydrator1 = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.extras({
			displayName: (user) => `${user.name}`,
		});

	const hydrator2 = createHydrator<User>("id").extras({
		emailLower: (user) => user.email.toLowerCase(),
	});

	const combined3 = hydrator1.extend(hydrator2);

	const result3 = combined3.hydrate([] as User[]);

	expectTypeOf(result3).resolves.toEqualTypeOf<
		{ id: number; name: string; displayName: string; emailLower: string }[]
	>();

	// Works with subtype (narrower input)
	interface AdminUser extends User {
		role: string;
	}

	const userHydrator = createHydrator<User>("id").fields({ id: true, name: true });

	const adminHydrator = createHydrator<AdminUser>("id").fields({ role: true });

	const combined4 = userHydrator.extend(adminHydrator);

	// Input type is intersection: User & AdminUser = AdminUser
	const result4 = combined4.hydrate([] as AdminUser[]);

	expectTypeOf(result4).resolves.toEqualTypeOf<{ id: number; name: string; role: string }[]>();

	// Also works in reverse direction (no constraint on OtherInput)
	const combined5 = adminHydrator.extend(userHydrator);

	// Input type is still AdminUser (AdminUser & User = AdminUser)
	const result5 = combined5.hydrate([] as AdminUser[]);

	expectTypeOf(result5).resolves.toEqualTypeOf<{ id: number; name: string; role: string }[]>();

	// Works with disjoint types (creates intersection)
	interface Post {
		id: number;
		title: string;
	}

	const nameHydrator = createHydrator<User>("id").fields({ name: true });

	const titleHydrator = createHydrator<Post>("id").fields({ title: true });

	const combined6 = nameHydrator.extend(titleHydrator);

	// Input type is User & Post = { id, name, email, title }
	type UserAndPost = User & Post;
	const result6 = combined6.hydrate([] as UserAndPost[]);

	expectTypeOf(result6).resolves.toEqualTypeOf<{ name: string; title: string }[]>();

	// Cannot extend with incompatible overlapping field types
	createHydrator<{ id: number; name: string }>("id").extend(
		// @ts-expect-error - id type mismatch (number vs string)
		createHydrator<{ id: string; role: string }>("id"),
	);

	// Incompatible field types in the other direction too
	createHydrator<{ id: string; role: string }>("id").extend(
		// @ts-expect-error - id type mismatch (string vs number)
		createHydrator<{ id: number; name: string }>("id"),
	);

	// Different keyBy will cause runtime error but not type error
	// (keyBy is not part of the type signature)
}

//
// Default keyBy: omit keyBy when input has 'id' property
//

{
	interface User {
		id: number;
		name: string;
		email: string;
	}

	// keyBy omitted - should work when input has 'id'
	const hydrator1 = createHydrator<User>().fields({ name: true });
	const result1 = hydrator1.hydrate([] as User[]);
	expectTypeOf(result1).resolves.toEqualTypeOf<{ name: string }[]>();

	// keyBy required when input doesn't have 'id'
	interface NoIdUser {
		userId: number;
		name: string;
	}

	// @ts-expect-error - keyBy required when input doesn't have 'id'
	createHydrator<NoIdUser>();

	// But works with explicit keyBy
	const hydrator4 = createHydrator<NoIdUser>("userId");
	const result4 = hydrator4.hydrate([] as NoIdUser[]);
	expectTypeOf(result4).resolves.toEqualTypeOf<{}[]>();
}

//
// Default keyBy with nested hydrators
//

{
	interface User {
		id: number;
		name: string;
	}

	type UserWithPosts = User & {
		posts$$id: number;
		posts$$title: string;
	};

	// Nested collection using default keyBy in factory function
	const hydrator = createHydrator<UserWithPosts>()
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (create) => create().fields({ id: true, title: true }));

	const result = hydrator.hydrate([] as UserWithPosts[]);
	expectTypeOf(result).resolves.toEqualTypeOf<
		{ id: number; name: string; posts: { id: number; title: string }[] }[]
	>();
}

//
// hydrateData function with default keyBy
//

{
	interface User {
		id: number;
		name: string;
	}

	// hydrateData with hydrator that uses default keyBy
	const hydrator = createHydrator<User>().fields({ id: true, name: true });
	const result1 = hydrateData([] as User[], hydrator);
	expectTypeOf(result1).resolves.toEqualTypeOf<{ id: number; name: string }[]>();

	// hydrateData with inline hydrator factory
	const result2 = hydrateData([] as User[], (create: typeof createHydrator<User>) =>
		create().fields({ id: true }),
	);
	expectTypeOf(result2).resolves.toEqualTypeOf<{ id: number }[]>();
}

//
// Tests: keyBy required when input lacks 'id'
//

{
	interface NoIdUser {
		userId: number;
		name: string;
	}

	// @ts-expect-error - createHydrator requires keyBy when input doesn't have 'id'
	createHydrator<NoIdUser>();

	// Works with explicit keyBy
	const validHydrator = createHydrator<NoIdUser>("userId").fields({
		userId: true,
		name: true,
	});
	expectTypeOf(validHydrator.hydrate([] as NoIdUser[])).resolves.toEqualTypeOf<
		{ userId: number; name: string }[]
	>();

	// Nested collections also require keyBy when child doesn't have 'id'
	interface User {
		id: number;
		name: string;
	}

	type UserWithNoIdPosts = User & {
		posts$$postId: number;
		posts$$title: string;
	};

	// @ts-expect-error - nested collection requires keyBy when child doesn't have 'id'
	createHydrator<UserWithNoIdPosts>().hasMany("posts", "posts$$", (create) => create());

	// Explicit keyBy works for nested collections without 'id'
	const nestedHydrator = createHydrator<UserWithNoIdPosts>().hasMany("posts", "posts$$", (create) =>
		create("postId").fields({ postId: true, title: true }),
	);
	expectTypeOf(nestedHydrator.hydrate([] as UserWithNoIdPosts[])).resolves.toEqualTypeOf<
		{ posts: { postId: number; title: string }[] }[]
	>();

	// @ts-expect-error - hydrateData with inline factory requires keyBy when input doesn't have 'id'
	hydrateData([] as NoIdUser[], (create: typeof createHydrator<NoIdUser>) => create());
}

//
// map(): transformations
//

{
	interface User {
		id: number;
		name: string;
	}

	// Basic map: transforms output type
	const hydrator1 = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.map((user) => ({ userId: user.id, userName: user.name }));

	const result1 = hydrator1.hydrate([] as User[]);
	expectTypeOf(result1).resolves.toEqualTypeOf<{ userId: number; userName: string }[]>();

	// Chaining maps
	const hydrator2 = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.map((user) => {
			expectTypeOf(user).toEqualTypeOf<{ id: number; name: string }>();
			return { ...user, upper: user.name.toUpperCase() };
		})
		.map((user) => {
			expectTypeOf(user).toEqualTypeOf<{ id: number; name: string; upper: string }>();
			return { final: user.upper };
		});

	const result2 = hydrator2.hydrate([] as User[]);
	expectTypeOf(result2).resolves.toEqualTypeOf<{ final: string }[]>();

	// After map, only map() and hydrate() are available
	const mapped = createHydrator<User>("id")
		.fields({ id: true })
		.map((u) => u.id);

	// @ts-expect-error - cannot call fields() after map()
	// oxlint-disable-next-line no-unused-expressions
	mapped.fields;

	// @ts-expect-error - cannot call extras() after map()
	// oxlint-disable-next-line no-unused-expressions
	mapped.extras;

	// @ts-expect-error - cannot call omit() after map()
	// oxlint-disable-next-line no-unused-expressions
	mapped.omit;

	// @ts-expect-error - cannot call hasMany() after map()
	// oxlint-disable-next-line no-unused-expressions
	mapped.hasMany;

	// @ts-expect-error - cannot call hasOne() after map()
	// oxlint-disable-next-line no-unused-expressions
	mapped.hasOne;

	// @ts-expect-error - cannot call hasOneOrThrow() after map()
	// oxlint-disable-next-line no-unused-expressions
	mapped.hasOneOrThrow;

	// @ts-expect-error - cannot call attachMany() after map()
	// oxlint-disable-next-line no-unused-expressions
	mapped.attachMany;

	// @ts-expect-error - cannot call attachOne() after map()
	// oxlint-disable-next-line no-unused-expressions
	mapped.attachOne;

	// @ts-expect-error - cannot call attachOneOrThrow() after map()
	// oxlint-disable-next-line no-unused-expressions
	mapped.attachOneOrThrow;

	// @ts-expect-error - cannot call extend() after map()
	// oxlint-disable-next-line no-unused-expressions
	mapped.extend;
}

//
// map() with nested collections
//

{
	interface UserRow {
		id: number;
		name: string;
		posts$$id: number | null;
		posts$$title: string | null;
	}

	// Nested map
	const hydrator = createHydrator<UserRow>("id")
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (h) =>
			h("id")
				.fields({ id: true, title: true })
				// Map child
				.map((post) => {
					expectTypeOf(post).toEqualTypeOf<{ id: number | null; title: string | null }>();
					return { postId: post.id, postTitle: post.title };
				}),
		)
		// Map parent.
		.map((user) => {
			expectTypeOf(user).toEqualTypeOf<{
				id: number;
				name: string;
				// The mapped post type:
				posts: { postId: number | null; postTitle: string | null }[];
			}>();
			return { userName: user.name, postCount: user.posts.length };
		});

	const result = hydrator.hydrate([] as UserRow[]);
	expectTypeOf(result).resolves.toEqualTypeOf<{ userName: string; postCount: number }[]>();
}

//
// map() with attached collections
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

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachMany("posts", async (): Promise<Post[]> => [], { matchChild: "userId" })
		.map((user) => {
			expectTypeOf(user).toEqualTypeOf<{ id: number; name: string; posts: Post[] }>();
			return { userName: user.name, postTitles: user.posts.map((p) => p.title) };
		});

	const result = hydrator.hydrate([] as User[]);
	expectTypeOf(result).resolves.toEqualTypeOf<{ userName: string; postTitles: string[] }[]>();
}

//
// map() with extend()
//

{
	interface User {
		id: number;
		name: string;
	}

	// Cannot extend a mapped hydrator
	const baseHydrator = createHydrator<User>("id").fields({ id: true, name: true });

	const mappedHydrator = createHydrator<User>("id")
		.fields({ id: true })
		.map((u) => ({ userId: u.id }));

	// This should work - extending with a mapped hydrator
	const extended = baseHydrator.extend(mappedHydrator);
	const result1 = extended.hydrate([] as User[]);
	expectTypeOf(result1).resolves.toEqualTypeOf<{ id: number; name: string; userId: number }[]>();

	// @ts-expect-error - cannot call fields() after extend() with a mapped hydrator
	// oxlint-disable-next-line no-unused-expressions
	extended.extras;

	const otherHydrator = createHydrator<User>("id").extras({ displayName: (u) => u.name });

	// However, extending with a full hydrator should produce a full hydrator.
	const extendedButNotMapped = baseHydrator.extend(otherHydrator);
	// This is allowed.
	const extendedWithExtras = extendedButNotMapped.extras({ idSquared: (u) => u.id ** 2 });
	expectTypeOf(extendedWithExtras.hydrate([] as User[])).resolves.toEqualTypeOf<
		{
			id: number;
			name: string;
			displayName: string;
			idSquared: number;
		}[]
	>();
}
