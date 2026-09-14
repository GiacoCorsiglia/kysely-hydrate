/**
 * Structural stand-ins for the duck-typed values `sqlCompare` supports.
 * Neither Temporal (behind a flag on Node 22) nor the decimal libraries are
 * available here, and detection is by shape anyway.
 */

/**
 * `unorderable` models a library divergence: comparing against NaN returns
 * `NaN` in decimal.js but `null` in bignumber.js.
 */
abstract class BaseDecimalStub {
	readonly n: number;
	readonly #unorderable: number | null;

	constructor(n: number, unorderable: number | null = Number.NaN) {
		this.n = n;
		this.#unorderable = unorderable;
	}

	protected compareValue(other: unknown): number | null {
		const value = other instanceof BaseDecimalStub ? other.n : Number(other);
		if (Number.isNaN(this.n) || Number.isNaN(value)) {
			return this.#unorderable;
		}
		return this.n < value ? -1 : this.n > value ? 1 : 0;
	}

	toString(): string {
		return String(this.n);
	}
}

/** decimal.js and big.js expose `cmp`. */
export class DecimalStub extends BaseDecimalStub {
	cmp(other: unknown): number | null {
		return this.compareValue(other);
	}
}

/** bignumber.js exposes `comparedTo` but not `cmp`. */
export class ComparedToDecimalStub extends BaseDecimalStub {
	comparedTo(other: unknown): number | null {
		return this.compareValue(other);
	}
}

/**
 * Temporal stubs reproduce the shape V8 implements: tag as a prototype data
 * property, a static `compare` that throws across types, no `compare` on
 * `PlainMonthDay`, and a throwing `valueOf`.
 */
abstract class TemporalStub {
	declare readonly [Symbol.toStringTag]: string;

	readonly value: string;

	constructor(value: string) {
		this.value = value;
	}

	valueOf(): never {
		throw new TypeError(
			"Do not use built-in arithmetic operators with Temporal objects. When comparing, use Temporal.PlainDate.compare(obj1, obj2), not obj1 > obj2.",
		);
	}

	toString(): string {
		return this.value;
	}
}

/** ISO 8601 strings order chronologically as strings. */
function compareIso<T extends TemporalStub>(kind: new (value: string) => T) {
	return (a: T, b: T): number => {
		if (!(a instanceof kind) || !(b instanceof kind)) {
			throw new TypeError("invalid argument");
		}
		return a.value < b.value ? -1 : a.value > b.value ? 1 : 0;
	};
}

function defineTag(kind: abstract new (value: string) => TemporalStub, tag: string): void {
	Object.defineProperty(kind.prototype, Symbol.toStringTag, { value: tag, configurable: true });
}

export class PlainDateStub extends TemporalStub {
	static compare = compareIso(PlainDateStub);

	static {
		defineTag(this, "Temporal.PlainDate");
	}
}

export class PlainTimeStub extends TemporalStub {
	static compare = compareIso(PlainTimeStub);

	static {
		defineTag(this, "Temporal.PlainTime");
	}
}

export class PlainMonthDayStub extends TemporalStub {
	static {
		defineTag(this, "Temporal.PlainMonthDay");
	}
}

/** `compare` throws once calendar units are involved, as the real one does. */
export class DurationStub extends TemporalStub {
	static compare(a: DurationStub, b: DurationStub): number {
		if (a.value === b.value) {
			return 0;
		}
		if (a.#hasCalendarUnits() || b.#hasCalendarUnits()) {
			throw new RangeError("A starting point is required for years, months, or weeks comparison");
		}
		const aTotal = a.seconds + (a.minutes + (a.hours + a.days * 24) * 60) * 60;
		const bTotal = b.seconds + (b.minutes + (b.hours + b.days * 24) * 60) * 60;
		return aTotal < bTotal ? -1 : aTotal > bTotal ? 1 : 0;
	}

	readonly years: number;
	readonly months: number;
	readonly weeks: number;
	readonly days: number;
	readonly hours: number;
	readonly minutes: number;
	readonly seconds: number;

	constructor(value: string) {
		super(value);
		const [datePart = "", timePart = ""] = value.replace(/^P/, "").split("T");
		const field = (part: string, unit: string) =>
			Number(new RegExp(`(\\d+)${unit}`).exec(part)?.[1] ?? 0);

		this.years = field(datePart, "Y");
		this.months = field(datePart, "M");
		this.weeks = field(datePart, "W");
		this.days = field(datePart, "D");
		this.hours = field(timePart, "H");
		this.minutes = field(timePart, "M");
		this.seconds = field(timePart, "S");
	}

	#hasCalendarUnits(): boolean {
		return this.years !== 0 || this.months !== 0 || this.weeks !== 0;
	}

	static {
		defineTag(this, "Temporal.Duration");
	}
}
