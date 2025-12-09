import { type Generated } from "kysely";

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

// Kysely Database interface
export interface SeedDB {
	users: User;
	posts: Post;
	comments: Comment;
	profiles: Profile;
}
