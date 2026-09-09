/**
 * Structural stand-ins for the duck-typed value types `sqlCompare` supports.
 *
 * These deliberately are not the real libraries. Temporal is unavailable on
 * the runtimes this package supports, and decimal libraries are not
 * dependencies -- but more importantly, what needs testing is that detection
 * keys off *shape* rather than identity. A stub that reproduces the shape is
 * the more faithful test of that contract.
 *
 * Shapes verified against the reference implementations: @js-temporal/polyfill
 * for Temporal, and decimal.js / big.js / bignumber.js for the decimals.
 */

/** How a decimal library reports a comparison it cannot order. */
type UnorderableResult = number | null | undefined;

/**
 * Shared behavior for the decimal stubs. The comparison method lives on each
 * subclass's prototype rather than on the instance, which is where decimal.js,
 * big.js, and bignumber.js all put theirs -- and what detection keys off.
 */
abstract class BaseDecimalStub {
	readonly n: number;
	readonly #unorderable: UnorderableResult;
	readonly #throws: boolean;

	constructor(n: number, unorderable: UnorderableResult = Number.NaN, throws = false) {
		this.n = n;
		this.#unorderable = unorderable;
		this.#throws = throws;
	}

	protected compareValue(other: unknown): number | null | undefined {
		if (this.#throws) {
			throw new Error("[DecimalError] Invalid argument");
		}
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

/**
 * Stands in for decimal.js and big.js, which expose `cmp`.
 *
 * The `unorderable` parameter covers a real divergence between libraries:
 * decimal.js returns `NaN` for a comparison against NaN while bignumber.js
 * returns `null`, and `null` is the dangerous one because it coerces to 0 and
 * would silently report unequal values as equal.
 */
export class DecimalStub extends BaseDecimalStub {
	cmp(other: unknown): number | null | undefined {
		return this.compareValue(other);
	}
}

/** Stands in for bignumber.js, which exposes `comparedTo` but not `cmp`. */
export class ComparedToDecimalStub extends BaseDecimalStub {
	comparedTo(other: unknown): number | null | undefined {
		return this.compareValue(other);
	}
}

/**
 * Base for the Temporal stubs. Every Temporal type tags itself, exposes
 * ordering as a *static* `compare` rather than an instance method, and throws
 * from `valueOf` to block accidental `a < b` coercion.
 */
abstract class TemporalStub {
	readonly value: string;

	constructor(value: string) {
		this.value = value;
	}

	abstract get [Symbol.toStringTag](): string;

	valueOf(): never {
		throw new TypeError(
			"Do not use built-in arithmetic operators with Temporal objects. When comparing, use Temporal.PlainDate.compare(obj1, obj2), not obj1 > obj2.",
		);
	}

	toString(): string {
		return this.value;
	}
}

/** ISO 8601 date strings order chronologically under a plain string compare. */
function compareIso(a: TemporalStub, b: TemporalStub): number {
	return a.value < b.value ? -1 : a.value > b.value ? 1 : 0;
}

export class PlainDateStub extends TemporalStub {
	static compare = compareIso;

	override get [Symbol.toStringTag](): string {
		return "Temporal.PlainDate";
	}
}

export class PlainTimeStub extends TemporalStub {
	static compare = compareIso;

	override get [Symbol.toStringTag](): string {
		return "Temporal.PlainTime";
	}
}

/**
 * `Temporal.PlainMonthDay` is the one Temporal type with no static `compare`
 * at all -- a month/day pair has no inherent order.
 */
export class PlainMonthDayStub extends TemporalStub {
	override get [Symbol.toStringTag](): string {
		return "Temporal.PlainMonthDay";
	}
}

/**
 * `Temporal.Duration.compare` works for durations made of exact time units but
 * throws a RangeError once years, months, or weeks are involved, because
 * relating those to days needs a starting point.
 */
export class DurationStub extends TemporalStub {
	static compare(a: DurationStub, b: DurationStub): number {
		// Mirrors the spec: identical field sets short-circuit before the
		// calendar-unit check, so this succeeds even for values that cannot be
		// compared against anything else.
		if (a.value === b.value) {
			return 0;
		}
		if (a.#hasCalendarUnits() || b.#hasCalendarUnits()) {
			throw new RangeError("A starting point is required for years, months, or weeks comparison");
		}
		const aTotal = a.minutes + a.hours * 60 + a.days * 24 * 60;
		const bTotal = b.minutes + b.hours * 60 + b.days * 24 * 60;
		return aTotal < bTotal ? -1 : aTotal > bTotal ? 1 : 0;
	}

	readonly years: number;
	readonly months: number;
	readonly weeks: number;
	readonly days: number;
	readonly hours: number;
	readonly minutes: number;

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
	}

	#hasCalendarUnits(): boolean {
		return this.years !== 0 || this.months !== 0 || this.weeks !== 0;
	}

	override get [Symbol.toStringTag](): string {
		return "Temporal.Duration";
	}
}
