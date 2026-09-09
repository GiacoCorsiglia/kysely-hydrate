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
 * Stands in for decimal.js, big.js, and bignumber.js.
 *
 * The real libraries differ in ways that matter: decimal.js exposes both `cmp`
 * and `comparedTo`, big.js only `cmp`, bignumber.js only `comparedTo`. They
 * also disagree on unorderable results -- decimal.js returns `NaN` while
 * bignumber.js returns `null`, and `null` is the dangerous one because it
 * coerces to 0 and would silently report unequal values as equal.
 */
export class DecimalStub {
	readonly n: number;
	readonly #unorderable: UnorderableResult;
	readonly #throws: boolean;

	constructor(
		n: number,
		method: "cmp" | "comparedTo" = "cmp",
		unorderable: UnorderableResult = Number.NaN,
		throws = false,
	) {
		this.n = n;
		this.#unorderable = unorderable;
		this.#throws = throws;

		// Expose only the method the emulated library has, so detection cannot
		// pass by finding the other one.
		const compare = (other: DecimalStub | number | bigint): number | null | undefined => {
			if (this.#throws) {
				throw new Error("[DecimalError] Invalid argument");
			}
			const value = other instanceof DecimalStub ? other.n : Number(other);
			if (Number.isNaN(this.n) || Number.isNaN(value)) {
				return this.#unorderable;
			}
			return this.n < value ? -1 : this.n > value ? 1 : 0;
		};
		(this as Record<string, unknown>)[method] = compare;
	}

	toString(): string {
		return String(this.n);
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
		const aMinutes = DurationStub.#toMinutes(a.value);
		const bMinutes = DurationStub.#toMinutes(b.value);
		return aMinutes < bMinutes ? -1 : aMinutes > bMinutes ? 1 : 0;
	}

	static #toMinutes(iso: string): number {
		const [datePart = "", timePart = ""] = iso.replace(/^P/, "").split("T");
		if (/[YMW]/.test(datePart)) {
			throw new RangeError("A starting point is required for years, months, or weeks comparison");
		}

		const days = Number(/(\d+)D/.exec(datePart)?.[1] ?? 0);
		const hours = Number(/(\d+)H/.exec(timePart)?.[1] ?? 0);
		const minutes = Number(/(\d+)M/.exec(timePart)?.[1] ?? 0);
		return days * 24 * 60 + hours * 60 + minutes;
	}

	override get [Symbol.toStringTag](): string {
		return "Temporal.Duration";
	}
}
