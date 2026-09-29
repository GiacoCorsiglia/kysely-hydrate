// One user joined to seven siblings, each one level deep.
import { expectTypeOf } from "expect-type";

import { querySet } from "../../src/index.ts";
import { db } from "./schema.ts";

const users = querySet(db)
	.selectAs("user", db.selectFrom("users").select(["id", "organization_id", "username", "email"]))
	.innerJoinOne(
		"organization",
		({ eb, qs }) => qs(eb.selectFrom("organizations").select(["id", "name"])),
		"organization.id",
		"user.organization_id",
	)
	.leftJoinOne(
		"profile",
		({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "user_id", "bio", "avatar_url"])),
		"profile.user_id",
		"user.id",
	)
	.leftJoinOneOrThrow(
		"settings",
		({ eb, qs }) => qs(eb.selectFrom("settings").select(["id", "user_id", "theme", "locale"])),
		"settings.user_id",
		"user.id",
	)
	.leftJoinMany(
		"posts",
		({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "user_id", "title"])),
		"posts.user_id",
		"user.id",
	)
	.leftJoinMany(
		"comments",
		({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "user_id", "content"])),
		"comments.user_id",
		"user.id",
	)
	.leftJoinMany(
		"reactions",
		({ eb, qs }) => qs(eb.selectFrom("reactions").select(["id", "user_id", "emoji"])),
		"reactions.user_id",
		"user.id",
	)
	.innerJoinMany(
		"sessions",
		({ eb, qs }) => qs(eb.selectFrom("sessions").select(["id", "user_id", "expires_at"])),
		"sessions.user_id",
		"user.id",
	);

expectTypeOf(users.execute()).resolves.toEqualTypeOf<
	{
		id: number;
		organization_id: number;
		username: string;
		email: string;
		organization: { id: number; name: string };
		profile: { id: number; user_id: number; bio: string | null; avatar_url: string | null } | null;
		settings: { id: number; user_id: number; theme: "light" | "dark"; locale: string };
		posts: { id: number; user_id: number; title: string }[];
		comments: { id: number; user_id: number; content: string }[];
		reactions: { id: number; user_id: number; emoji: string }[];
		sessions: { id: number; user_id: number; expires_at: Date }[];
	}[]
>();
