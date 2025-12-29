import { expectTypeOf } from "expect-type";

import {
	type ApplyPrefix,
	type ApplyPrefixes,
	type MakePrefix,
	type SelectAndStripPrefix,
} from "./prefixes.ts";

//
// MakePrefix: creates prefix with $$ separator
//

{
	type Result1 = MakePrefix<"", "posts">;
	expectTypeOf<Result1>().toEqualTypeOf<"posts$$">();

	type Result2 = MakePrefix<"posts$$", "comments">;
	expectTypeOf<Result2>().toEqualTypeOf<"posts$$comments$$">();

	type Result3 = MakePrefix<"a$$b$$", "c">;
	expectTypeOf<Result3>().toEqualTypeOf<"a$$b$$c$$">();
}

//
// ApplyPrefix: applies prefix to a key
//

{
	type Result1 = ApplyPrefix<"posts$$", "id">;
	expectTypeOf<Result1>().toEqualTypeOf<"posts$$id">();

	type Result2 = ApplyPrefix<"", "id">;
	expectTypeOf<Result2>().toEqualTypeOf<"id">();

	type Result3 = ApplyPrefix<"prefix_", "name">;
	expectTypeOf<Result3>().toEqualTypeOf<"prefix_name">();
}

//
// ApplyPrefixes: applies prefix to all keys in a type
//

{
	interface Post {
		id: number;
		title: string;
		content: string;
	}

	type Prefixed = ApplyPrefixes<"posts$$", Post>;
	expectTypeOf<Prefixed>().toEqualTypeOf<{
		"posts$$id": number;
		"posts$$title": string;
		"posts$$content": string;
	}>();

	type EmptyPrefix = ApplyPrefixes<"", Post>;
	expectTypeOf<EmptyPrefix>().toEqualTypeOf<{
		id: number;
		title: string;
		content: string;
	}>();
}

//
// SelectAndStripPrefix: extracts and strips prefixed properties
//

{
	interface Row {
		id: number;
		name: string;
		"posts$$id": number | null;
		"posts$$title": string | null;
		"comments$$id": number | null;
	}

	type PostFields = SelectAndStripPrefix<"posts$$", Row>;
	expectTypeOf<PostFields>().toEqualTypeOf<{
		id: number | null;
		title: string | null;
	}>();

	type CommentFields = SelectAndStripPrefix<"comments$$", Row>;
	expectTypeOf<CommentFields>().toEqualTypeOf<{
		id: number | null;
	}>();

	type NoPrefix = SelectAndStripPrefix<"", Row>;
	expectTypeOf<NoPrefix>().toEqualTypeOf<{
		id: number;
		name: string;
		"posts$$id": number | null;
		"posts$$title": string | null;
		"comments$$id": number | null;
	}>();
}
