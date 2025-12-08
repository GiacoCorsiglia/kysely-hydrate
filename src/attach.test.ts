import assert from "node:assert";
import { test } from "node:test";

import { createHydratable, hydrate } from "./hydratable.ts";

// Test types
interface User {
	id: number;
	email: string;
}

interface Post {
	id: number;
	userId: number;
	title: string;
}

interface Comment {
	id: number;
	postId: number;
	content: string;
}

// Mock fetch functions that track call counts and return hydrated data
let postFetchCount = 0;
let commentFetchCount = 0;

interface HydratedPost {
	id: number;
	userId: number;
	title: string;
}

interface HydratedComment {
	id: number;
	content: string;
}

const mockFetchPosts = async (users: User[]): Promise<HydratedPost[]> => {
	postFetchCount++;
	const userIds = users.map((u) => u.id);

	// Simulate fetching posts for these users
	const rawPosts: Post[] = [
		{ id: 1, userId: 1, title: "Post 1 by User 1" },
		{ id: 2, userId: 1, title: "Post 2 by User 1" },
		{ id: 3, userId: 2, title: "Post 1 by User 2" },
	].filter((post) => userIds.includes(post.userId));

	// Hydrate the posts (include userId for matching)
	return hydrate(
		rawPosts,
		createHydratable<Post>("id").fields({
			id: true,
			userId: true,
			title: true,
		}),
	) as Promise<HydratedPost[]>;
};

const mockFetchComments = async (posts: HydratedPost[]): Promise<HydratedComment[]> => {
	commentFetchCount++;
	const postIds = posts.map((p) => p.id);

	// Simulate fetching comments for these posts
	const rawComments: Comment[] = [
		{ id: 1, postId: 1, content: "Comment 1 on Post 1" },
		{ id: 2, postId: 1, content: "Comment 2 on Post 1" },
		{ id: 3, postId: 2, content: "Comment 1 on Post 2" },
		{ id: 4, postId: 3, content: "Comment 1 on Post 3" },
	].filter((comment) => postIds.includes(comment.postId));

	// Hydrate the comments
	return hydrate(
		rawComments,
		createHydratable<Comment>("id").fields({ id: true, content: true }),
	) as Promise<HydratedComment[]>;
};

test("attachMany: fetches and attaches multiple related entities", async () => {
	postFetchCount = 0;

	const users: User[] = [
		{ id: 1, email: "user1@example.com" },
		{ id: 2, email: "user2@example.com" },
	];

	const hydratable = createHydratable<User>("id")
		.fields({ id: true, email: true })
		.attachMany("posts", mockFetchPosts, "userId");

	const result = await hydrate(users, hydratable);

	// Verify fetch was called exactly once
	assert.strictEqual(postFetchCount, 1, "Posts should be fetched exactly once");

	// Verify structure
	assert.strictEqual(result.length, 2);
	assert.strictEqual(result[0]?.email, "user1@example.com");
	assert.strictEqual(result[0]?.posts.length, 2);
	assert.strictEqual(result[0]?.posts[0]?.title, "Post 1 by User 1");
	assert.strictEqual(result[0]?.posts[1]?.title, "Post 2 by User 1");

	assert.strictEqual(result[1]?.email, "user2@example.com");
	assert.strictEqual(result[1]?.posts.length, 1);
	assert.strictEqual(result[1]?.posts[0]?.title, "Post 1 by User 2");
});

test("attachOne: fetches and attaches a single related entity", async () => {
	postFetchCount = 0;

	const users: User[] = [
		{ id: 1, email: "user1@example.com" },
		{ id: 2, email: "user2@example.com" },
	];

	const hydratable = createHydratable<User>("id")
		.fields({ id: true, email: true })
		.attachOne("latestPost", mockFetchPosts, "userId");

	const result = await hydrate(users, hydratable);

	// Verify fetch was called exactly once
	assert.strictEqual(postFetchCount, 1, "Posts should be fetched exactly once");

	// Verify structure - attachOne should return the first match or null
	assert.strictEqual(result.length, 2);
	assert.strictEqual(result[0]?.email, "user1@example.com");
	assert.strictEqual(result[0]?.latestPost?.title, "Post 1 by User 1");

	assert.strictEqual(result[1]?.email, "user2@example.com");
	assert.strictEqual(result[1]?.latestPost?.title, "Post 1 by User 2");
});

test("attachOne: returns null when no match found", async () => {
	const users: User[] = [{ id: 999, email: "user999@example.com" }];

	const hydratable = createHydratable<User>("id")
		.fields({ id: true, email: true })
		.attachOne("latestPost", mockFetchPosts, "userId");

	const result = await hydrate(users, hydratable);

	assert.strictEqual(result.length, 1);
	assert.strictEqual(result[0]?.latestPost, null);
});

test("nested attachMany: fetches data at multiple levels exactly once per level", async () => {
	postFetchCount = 0;
	commentFetchCount = 0;

	// Create a fetch function that returns posts with hydrated comments
	const mockFetchPostsWithComments = async (
		users: User[],
	): Promise<Array<HydratedPost & { comments: HydratedComment[] }>> => {
		postFetchCount++;
		const userIds = users.map((u) => u.id);

		// Simulate fetching posts for these users
		const rawPosts: Post[] = [
			{ id: 1, userId: 1, title: "Post 1 by User 1" },
			{ id: 2, userId: 1, title: "Post 2 by User 1" },
			{ id: 3, userId: 2, title: "Post 1 by User 2" },
		].filter((post) => userIds.includes(post.userId));

		// Hydrate posts (include userId for matching)
		const hydratedPosts = (await hydrate(
			rawPosts,
			createHydratable<Post>("id").fields({
				id: true,
				userId: true,
				title: true,
			}),
		)) as HydratedPost[];

		// Fetch and hydrate comments for these posts
		const hydratedComments = await mockFetchComments(hydratedPosts);

		// Get raw comments for matching
		const rawComments: Comment[] = [
			{ id: 1, postId: 1, content: "Comment 1 on Post 1" },
			{ id: 2, postId: 1, content: "Comment 2 on Post 1" },
			{ id: 3, postId: 2, content: "Comment 1 on Post 2" },
			{ id: 4, postId: 3, content: "Comment 1 on Post 3" },
		].filter((comment) => rawPosts.some((p) => p.id === comment.postId));

		// Attach comments to posts
		return hydratedPosts.map((post) => {
			const rawPost = rawPosts.find((p) => p.id === post.id);
			return {
				...post,
				comments: hydratedComments.filter((c) => {
					const rawComment = rawComments.find((rc) => rc.id === c.id);
					return rawComment?.postId === rawPost?.id;
				}),
			};
		});
	};

	const users: User[] = [
		{ id: 1, email: "user1@example.com" },
		{ id: 2, email: "user2@example.com" },
	];

	const hydratable = createHydratable<User>("id")
		.fields({ id: true, email: true })
		.attachMany("posts", mockFetchPostsWithComments, "userId");

	const result = await hydrate(users, hydratable);

	// Verify each fetch was called exactly once
	assert.strictEqual(postFetchCount, 1, "Posts should be fetched exactly once");
	assert.strictEqual(commentFetchCount, 1, "Comments should be fetched exactly once");

	// Verify structure
	assert.strictEqual(result.length, 2);

	// User 1 has 2 posts
	assert.strictEqual(result[0]?.posts.length, 2);
	// Post 1 has 2 comments
	assert.strictEqual(result[0]?.posts[0]?.comments.length, 2);
	assert.strictEqual(result[0]?.posts[0]?.comments[0]?.content, "Comment 1 on Post 1");
	assert.strictEqual(result[0]?.posts[0]?.comments[1]?.content, "Comment 2 on Post 1");
	// Post 2 has 1 comment
	assert.strictEqual(result[0]?.posts[1]?.comments.length, 1);
	assert.strictEqual(result[0]?.posts[1]?.comments[0]?.content, "Comment 1 on Post 2");

	// User 2 has 1 post with 1 comment
	assert.strictEqual(result[1]?.posts.length, 1);
	assert.strictEqual(result[1]?.posts[0]?.comments.length, 1);
	assert.strictEqual(result[1]?.posts[0]?.comments[0]?.content, "Comment 1 on Post 3");
});

test("attachMany with composite keys", async () => {
	interface CompositeKeyEntity {
		key1: string;
		key2: string;
		value: string;
	}

	interface RelatedEntity {
		relatedKey1: string;
		relatedKey2: string;
		data: string;
	}

	interface HydratedRelatedEntity {
		relatedKey1: string;
		relatedKey2: string;
		data: string;
	}

	const fetchRelated = async (entities: CompositeKeyEntity[]): Promise<HydratedRelatedEntity[]> => {
		// Simulate fetching based on composite keys
		const keys = entities.map((e) => `${e.key1}:${e.key2}`);
		const rawRelated: RelatedEntity[] = [
			{ relatedKey1: "a", relatedKey2: "1", data: "Related to a:1" },
			{ relatedKey1: "a", relatedKey2: "1", data: "Another for a:1" },
			{ relatedKey1: "b", relatedKey2: "2", data: "Related to b:2" },
		].filter((r) => keys.includes(`${r.relatedKey1}:${r.relatedKey2}`));

		// Hydrate the related entities (include keys for matching)
		return hydrate(
			rawRelated,
			createHydratable<RelatedEntity>(["relatedKey1", "relatedKey2"]).fields({
				relatedKey1: true,
				relatedKey2: true,
				data: true,
			}),
		) as Promise<HydratedRelatedEntity[]>;
	};

	const entities: CompositeKeyEntity[] = [
		{ key1: "a", key2: "1", value: "Entity A1" },
		{ key1: "b", key2: "2", value: "Entity B2" },
	];

	const hydratable = createHydratable<CompositeKeyEntity>(["key1", "key2"])
		.fields({ key1: true, key2: true, value: true })
		.attachMany("related", fetchRelated, ["relatedKey1", "relatedKey2"]);

	const result = await hydrate(entities, hydratable);

	assert.strictEqual(result.length, 2);
	assert.strictEqual(result[0]?.related.length, 2);
	assert.strictEqual(result[0]?.related[0]?.data, "Related to a:1");
	assert.strictEqual(result[1]?.related.length, 1);
	assert.strictEqual(result[1]?.related[0]?.data, "Related to b:2");
});

test("attachMany with no matching entities returns empty array", async () => {
	const users: User[] = [{ id: 999, email: "user999@example.com" }];

	const hydratable = createHydratable<User>("id")
		.fields({ id: true, email: true })
		.attachMany("posts", mockFetchPosts, "userId");

	const result = await hydrate(users, hydratable);

	assert.strictEqual(result.length, 1);
	assert.strictEqual(result[0]?.posts.length, 0);
});

test("mixing has and attach collections", async () => {
	postFetchCount = 0;

	// Simulate data that already includes some nested info via SQL joins
	interface UserWithProfile {
		id: number;
		email: string;
		profile$$name: string;
		profile$$age: number;
	}

	const users: UserWithProfile[] = [
		{
			id: 1,
			email: "user1@example.com",
			profile$$name: "John",
			profile$$age: 30,
		},
		{
			id: 2,
			email: "user2@example.com",
			profile$$name: "Jane",
			profile$$age: 25,
		},
	];

	const hydratable = createHydratable<UserWithProfile>("id")
		.fields({ id: true, email: true })
		.hasOne("profile", "profile$$", (h) => h("name").fields({ name: true, age: true }))
		.attachMany("posts", mockFetchPosts, "userId");

	const result = await hydrate(users, hydratable);

	// Verify fetch was called exactly once
	assert.strictEqual(postFetchCount, 1);

	// Verify both has and attach collections work
	assert.strictEqual(result.length, 2);
	assert.strictEqual(result[0]?.profile?.name, "John");
	assert.strictEqual(result[0]?.profile?.age, 30);
	assert.strictEqual(result[0]?.posts.length, 2);

	assert.strictEqual(result[1]?.profile?.name, "Jane");
	assert.strictEqual(result[1]?.profile?.age, 25);
	assert.strictEqual(result[1]?.posts.length, 1);
});
