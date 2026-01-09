import { type Generated } from "kysely";

import { integer, text } from "../experimental/schema/sqlite.ts";
import { createDatabase } from "../experimental/schema/table.ts";

export interface User {
	id: Generated<number>;
	username: string;
	email: string;
}

export interface Post {
	id: Generated<number>;
	user_id: number;
	title: string;
	content: string;
}

export interface Comment {
	id: Generated<number>;
	post_id: number;
	user_id: number;
	content: string;
}

export interface Profile {
	id: Generated<number>;
	user_id: number;
	bio: string | null;
	avatar_url: string | null;
}

export interface Reply {
	id: Generated<number>;
	comment_id: number;
	user_id: number;
	content: string;
}

// Kysely Database interface
export interface SeedDB {
	users: User;
	posts: Post;
	comments: Comment;
	profiles: Profile;
	replies: Reply;
}

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
