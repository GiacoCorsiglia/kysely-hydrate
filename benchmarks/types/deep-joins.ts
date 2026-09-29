// A query set nested four levels deep: organizations -> users -> posts -> comments -> reactions.
import { expectTypeOf } from "expect-type";

import { querySet } from "../../src/index.ts";
import { db } from "./schema.ts";

const organizations = querySet(db)
	.selectAs("org", db.selectFrom("organizations").select(["id", "name", "slug", "plan"]))
	.innerJoinMany(
		"members",
		({ eb, qs }) =>
			qs(eb.selectFrom("users").select(["id", "organization_id", "username", "role"])).leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "user_id", "title", "status"])
							.where("status", "=", "published"),
					).leftJoinMany(
						"comments",
						({ eb, qs }) =>
							qs(eb.selectFrom("comments").select(["id", "post_id", "content"])).leftJoinMany(
								"reactions",
								({ eb, qs }) =>
									qs(eb.selectFrom("reactions").select(["id", "comment_id", "emoji"])),
								"reactions.comment_id",
								"comments.id",
							),
						"comments.post_id",
						"posts.id",
					),
				"posts.user_id",
				"members.id",
			),
		"members.organization_id",
		"org.id",
	)
	.where("organizations.plan", "!=", "free")
	.orderBy("name");

expectTypeOf(organizations.execute()).resolves.toEqualTypeOf<
	{
		id: number;
		name: string;
		slug: string;
		plan: "free" | "pro" | "enterprise";
		members: {
			id: number;
			organization_id: number;
			username: string;
			role: "admin" | "member";
			posts: {
				id: number;
				user_id: number;
				title: string;
				status: "draft" | "published";
				comments: {
					id: number;
					post_id: number;
					content: string;
					reactions: { id: number; comment_id: number; emoji: string }[];
				}[];
			}[];
		}[];
	}[]
>();
