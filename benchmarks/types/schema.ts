/**
 * The schema the `types` fixtures query: a small SaaS app, with column counts
 * typical of real tables.  Fixtures only type-check, never run, so `db` is
 * declared rather than constructed.
 */
import type * as k from "kysely";

export interface DB {
	organizations: {
		id: k.Generated<number>;
		name: string;
		slug: string;
		plan: "free" | "pro" | "enterprise";
		created_at: k.Generated<Date>;
	};
	users: {
		id: k.Generated<number>;
		organization_id: number;
		username: string;
		email: string;
		display_name: string | null;
		role: "admin" | "member";
		created_at: k.Generated<Date>;
		deleted_at: Date | null;
	};
	profiles: {
		id: k.Generated<number>;
		user_id: number;
		bio: string | null;
		avatar_url: string | null;
		location: string | null;
	};
	settings: {
		id: k.Generated<number>;
		user_id: number;
		theme: "light" | "dark";
		locale: string;
		email_notifications: boolean;
	};
	posts: {
		id: k.Generated<number>;
		user_id: number;
		title: string;
		slug: string;
		content: string;
		status: "draft" | "published";
		published_at: Date | null;
		created_at: k.Generated<Date>;
	};
	comments: {
		id: k.Generated<number>;
		post_id: number;
		user_id: number;
		content: string;
		created_at: k.Generated<Date>;
	};
	reactions: {
		id: k.Generated<number>;
		comment_id: number;
		user_id: number;
		emoji: string;
	};
	tags: {
		id: k.Generated<number>;
		name: string;
		color: string;
	};
	post_tags: {
		post_id: number;
		tag_id: number;
	};
	sessions: {
		id: k.Generated<number>;
		user_id: number;
		token: string;
		expires_at: Date;
	};
}

export declare const db: k.Kysely<DB>;
