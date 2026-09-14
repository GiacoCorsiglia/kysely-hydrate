/**
 * Structural stand-ins for `Temporal.*` values, for testing `sqlCompare`.
 *
 * Temporal is not available on the runtimes this package supports (Node 22
 * ships it only behind `--harmony-temporal`), and `sqlCompare` detects it by
 * shape rather than identity anyway, so a stub that reproduces the shape is
 * the faithful test. Shapes verified against V8's implementation: the tag is
 * a data property on the prototype, ordering is a *static* `compare` on the
 * constructor that throws for a different Temporal type, `PlainMonthDay` has
 * no `compare`, and `valueOf` throws to block `a < b`.
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

/** ISO 8601 strings order chronologically under a plain string compare. */
function compareIso<T extends TemporalStub>(kind: new (value: string) => T) {
	return (a: T, b: T): number => {
		if (!(a instanceof kind) || !(b instanceof kind)) {
			throw new TypeError("invalid argument");
		}
		return a.value < b.value ? -1 : a.value > b.value ? 1 : 0;
	};
}

/** Mirrors the spec: the tag is a non-writable data property on the prototype. */
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

/**
 * `Temporal.PlainMonthDay` is the one Temporal type with no static `compare`
 * at all -- a month/day pair has no inherent order.
 */
export class PlainMonthDayStub extends TemporalStub {
	static {
		defineTag(this, "Temporal.PlainMonthDay");
	}
}

/**
 * `Temporal.Duration.compare` works for durations made of exact time units but
 * throws a RangeError once years, months, or weeks are involved. Fields are
 * exposed like the real thing, which is all `sqlCompare` reads.
 */
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
