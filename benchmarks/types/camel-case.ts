// fixLongAliases over CamelCasePlugin, with a camelCase schema and long nested keys.
import { expectTypeOf } from "expect-type";
import * as k from "kysely";

import { fixLongAliases, querySet } from "../../src/index.ts";

interface CamelDB {
	userAccounts: { id: k.Generated<number>; displayName: string; primaryEmailAddress: string };
	blogPostEntries: { id: k.Generated<number>; userAccountId: number; headlineText: string };
	blogPostCommentThreads: { id: k.Generated<number>; blogPostEntryId: number; bodyText: string };
}

declare const dialect: k.Dialect;
const db = new k.Kysely<CamelDB>({ dialect, plugins: [fixLongAliases(new k.CamelCasePlugin())] });

const accounts = querySet(db)
	.selectAs("userAccount", db.selectFrom("userAccounts").select(["id", "displayName"]))
	.leftJoinMany(
		"authoredBlogPostEntries",
		({ eb, qs }) =>
			qs(
				eb.selectFrom("blogPostEntries").select(["id", "userAccountId", "headlineText"]),
			).leftJoinMany(
				"blogPostCommentThreads",
				({ eb, qs }) =>
					qs(eb.selectFrom("blogPostCommentThreads").select(["id", "blogPostEntryId", "bodyText"])),
				"blogPostCommentThreads.blogPostEntryId",
				"authoredBlogPostEntries.id",
			),
		"authoredBlogPostEntries.userAccountId",
		"userAccount.id",
	);

expectTypeOf(accounts.execute()).resolves.toEqualTypeOf<
	{
		id: number;
		displayName: string;
		authoredBlogPostEntries: {
			id: number;
			userAccountId: number;
			headlineText: string;
			blogPostCommentThreads: { id: number; blogPostEntryId: number; bodyText: string }[];
		}[];
	}[]
>();
