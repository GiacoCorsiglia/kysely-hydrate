import type SQLite from "better-sqlite3";

// Example table interfaces
export interface User {
	id: number;
	username: string;
	email: string;
}

export interface Post {
	id: number;
	user_id: number;
	title: string;
	content: string;
}

export interface Comment {
	id: number;
	post_id: number;
	user_id: number;
	content: string;
}

export interface Profile {
	id: number;
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

export async function seed(db: SQLite.Database) {
	db.pragma(`foreign_keys = ON;`);

	db.exec(`CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL,
    email TEXT NOT NULL
  );`);

	db.exec(`CREATE TABLE IF NOT EXISTS posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE ON UPDATE CASCADE
  );`);

	db.exec(`CREATE TABLE IF NOT EXISTS comments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    post_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    content TEXT NOT NULL,
    FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE ON UPDATE CASCADE
  );`);

	db.exec(`CREATE TABLE IF NOT EXISTS profiles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL UNIQUE,
    bio TEXT,
    avatar_url TEXT,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE ON UPDATE CASCADE
  );`);

	// Insert 10 users (no dates)
	db.exec(`INSERT INTO users (username, email) VALUES
    ('alice', 'alice@example.com'),
    ('bob', 'bob@example.com'),
    ('carol', 'carol@example.com'),
    ('dave', 'dave@example.com'),
    ('eve', 'eve@example.com'),
    ('frank', 'frank@example.com'),
    ('grace', 'grace@example.com'),
    ('heidi', 'heidi@example.com'),
    ('ivan', 'ivan@example.com'),
    ('judy', 'judy@example.com');
  `);

	// Insert sample posts with predictable titles and contents (no dates)
	db.exec(`INSERT INTO posts (user_id, title, content) VALUES
    (1, 'Post 1', 'Content for post 1'),
    (2, 'Post 2', 'Content for post 2'),
    (3, 'Post 3', 'Content for post 3'),
    (4, 'Post 4', 'Content for post 4'),
    (1, 'Post 5', 'Content for post 5'),
    (5, 'Post 6', 'Content for post 6'),
    (6, 'Post 7', 'Content for post 7'),
    (7, 'Post 8', 'Content for post 8'),
    (8, 'Post 9', 'Content for post 9'),
    (9, 'Post 10', 'Content for post 10'),
    (10, 'Post 11', 'Content for post 11'),
    (2, 'Post 12', 'Content for post 12'),
    (4, 'Post 13', 'Content for post 13'),
    (5, 'Post 14', 'Content for post 14'),
    (3, 'Post 15', 'Content for post 15');
  `);

	// Insert sample comments with predictable content (no dates)
	db.exec(`INSERT INTO comments (post_id, user_id, content) VALUES
    (1, 2, 'Comment 1 on post 1'),
    (1, 3, 'Comment 2 on post 1'),
    (2, 1, 'Comment 3 on post 2'),
    (4, 5, 'Comment 4 on post 4'),
    (5, 6, 'Comment 5 on post 5'),
    (6, 7, 'Comment 6 on post 6'),
    (7, 8, 'Comment 7 on post 7'),
    (8, 9, 'Comment 8 on post 8'),
    (9, 10, 'Comment 9 on post 9'),
    (10, 1, 'Comment 10 on post 10'),
    (11, 2, 'Comment 11 on post 11'),
    (3, 4, 'Comment 12 on post 3'),
    (13, 5, 'Comment 13 on post 13'),
    (14, 6, 'Comment 14 on post 14'),
    (15, 7, 'Comment 15 on post 15');
  `);

	// Insert one predictable profile per user
	db.exec(`INSERT INTO profiles (user_id, bio, avatar_url) VALUES
    (1, 'Bio for user 1', 'https://example.com/avatars/1.png'),
    (2, 'Bio for user 2', 'https://example.com/avatars/2.png'),
    (3, 'Bio for user 3', 'https://example.com/avatars/3.png'),
    (4, 'Bio for user 4', 'https://example.com/avatars/4.png'),
    (5, 'Bio for user 5', 'https://example.com/avatars/5.png'),
    (6, 'Bio for user 6', 'https://example.com/avatars/6.png'),
    (7, 'Bio for user 7', 'https://example.com/avatars/7.png'),
    (8, 'Bio for user 8', 'https://example.com/avatars/8.png'),
    (9, 'Bio for user 9', 'https://example.com/avatars/9.png'),
    (10, 'Bio for user 10', 'https://example.com/avatars/10.png');
  `);
}
