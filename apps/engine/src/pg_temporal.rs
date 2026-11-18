//! Timestamps as text: predicate literals for `timestamptz`, `timestamp` and `date` columns
//! respelled the way Postgres prints a cell of that type, and replicated `timestamptz` cells
//! respelled the way a UTC session prints them.
//!
//! The engine keeps these columns as coarse `Text` (see [`crate::schema::ColumnType`]): a cell is
//! Postgres's own text output — `2026-10-19 16:00:00+00` — and every comparison the engine makes
//! itself is code-point order. Postgres compares the same predicate as instants, in the backfill
//! and in every subset page. The two agree only when literal and cells are all spelled ONE way,
//! and neither side promises that on its own:
//!
//! * **A client is free to send any spelling Postgres accepts.** The usual one is ISO 8601,
//!   `2026-10-19T07:00:00Z`, whose `T` sorts after the space in every cell of that UTC date. A
//!   window bounded by such a literal then matches a row in SQL and not on the live path, so the
//!   snapshot holds the row and the feed never reports its changes. So a recognised literal is
//!   rewritten once, at compile time, into the cell spelling ([`canonical_literal`]), and
//!   everything downstream — leaf comparison, the access-leaf range index, equality templates —
//!   keeps comparing text.
//! * **Postgres prints a `timestamptz` in the session's time zone.** Text order is instant order
//!   only when every cell carries one offset, and a backfill row must spell an instant exactly as
//!   that row's next replicated change does. The spelling the engine uses is UTC, ISO style, and
//!   each road is brought to it differently: query sessions ask for it at startup
//!   ([`crate::pg::SESSION_OPTIONS`], and a `timestamptz` literal with no zone is then read as
//!   UTC by SQL exactly as it is here); the replication session takes no startup options, so its
//!   cells are respelled as they are decoded ([`utc_cell`]). The boot refuses a replication
//!   session that would not print ISO at all ([`crate::pg::check_replication_session`]).
//!
//! Years are 0001–9999 (four digits, no `BC` suffix). A literal or cell this module does not
//! recognise is left exactly as it was — `infinity` and `-infinity` already are what Postgres
//! prints in every zone, and anything else keeps the behaviour it had rather than being guessed
//! at.

/// Respell `literal` for a column of Postgres type `pg_type`, or `None` when the type is not a
/// timestamp/date type or the literal is not a timestamp this module recognises.
pub(crate) fn canonical_literal(literal: &str, pg_type: &str) -> Option<String> {
    let kind = match pg_type {
        "timestamptz" => Kind::TimestampTz,
        "timestamp" => Kind::Timestamp,
        "date" => Kind::Date,
        _ => return None,
    };
    let parsed = parse(literal)?;
    Some(match kind {
        Kind::Date => format!("{:04}-{:02}-{:02}", parsed.year, parsed.month, parsed.day),
        // Postgres ignores a zone when it casts a literal to `timestamp`.
        Kind::Timestamp => render(parsed.local_micros, "")?,
        Kind::TimestampTz => render(parsed.local_micros - i64::from(parsed.offset_secs.unwrap_or(0)) * MICROS, "+00")?,
    })
}

/// Respell a replicated cell of a time-zone-dependent type the way a UTC session prints it, or
/// `None` to leave the cell as it is: it is already UTC, its type's output does not depend on the
/// session zone, or it is not something this module reads (see the module docs).
///
/// The zone-dependent types are `timestamptz` and the arrays, ranges and multiranges built on it,
/// by `pg_type.typname`. Every other type prints the same in every zone — `timestamp` and `date`
/// have none, `timetz` carries its own — and is never touched, whatever its text looks like.
pub(crate) fn utc_cell(cell: &str, pg_type: &str) -> Option<String> {
    match pg_type {
        "timestamptz" => {
            // A zero offset prints as a bare `+00`: the common case once a deployment runs in UTC.
            if cell.ends_with("+00") {
                return None;
            }
            let s = cell.as_bytes();
            let (parsed, len) = parse_prefix(s)?;
            if len != s.len() {
                return None;
            }
            render(parsed.local_micros - i64::from(parsed.offset_secs?) * MICROS, "+00")
        }
        "_timestamptz" | "tstzrange" | "_tstzrange" | "tstzmultirange" | "_tstzmultirange" => utc_elements(cell),
        _ => None,
    }
}

/// [`utc_cell`] for a container of `timestamptz`: every timestamp in it respelled in UTC, the
/// punctuation around them (quotes, escapes, brackets, `NULL`) copied as it stands. `None` when
/// nothing changes, or when any element is one this module does not read — a container is
/// respelled whole or not at all.
fn utc_elements(cell: &str) -> Option<String> {
    if cell.contains(" BC") {
        return None;
    }
    let s = cell.as_bytes();
    let mut out = String::new();
    let mut copied = 0usize;
    let mut at = 0usize;
    while at < s.len() {
        // A timestamp starts at a digit no digit precedes; in a cell of these types nothing else
        // parses as one.
        if s[at].is_ascii_digit()
            && (at == 0 || !s[at - 1].is_ascii_digit())
            && let Some((parsed, len)) = parse_prefix(&s[at..])
            && let Some(offset) = parsed.offset_secs
        {
            if offset != 0 {
                let utc = render(parsed.local_micros - i64::from(offset) * MICROS, "+00")?;
                out.push_str(&cell[copied..at]);
                out.push_str(&utc);
                copied = at + len;
            }
            at += len;
        } else {
            at += 1;
        }
    }
    if copied == 0 {
        return None;
    }
    out.push_str(&cell[copied..]);
    Some(out)
}

#[derive(Clone, Copy)]
enum Kind {
    TimestampTz,
    Timestamp,
    Date,
}

const MICROS: i64 = 1_000_000;
const DAY_MICROS: i64 = 86_400 * MICROS;

struct Parsed {
    year: i64,
    month: i64,
    day: i64,
    /// The literal's own date and time as microseconds since 1970-01-01 00:00:00, zone ignored.
    local_micros: i64,
    /// Seconds east of UTC, when the literal names a zone.
    offset_secs: Option<i32>,
}

/// `YYYY-MM-DD[(T| )HH:MM[:SS[.fraction]]][ ][Z|±HH[[:]MM[[:]SS]]]`, nothing else.
fn parse(literal: &str) -> Option<Parsed> {
    let s = literal.trim_matches(|c: char| c.is_ascii_whitespace()).as_bytes();
    let (parsed, len) = parse_prefix(s)?;
    (len == s.len()).then_some(parsed)
}

/// The timestamp `s` begins with, in [`parse`]'s grammar, and how many bytes of `s` it takes. A
/// time or a zone that does not parse is simply not part of it: the caller decides whether what is
/// left over matters.
fn parse_prefix(s: &[u8]) -> Option<(Parsed, usize)> {
    let mut at = 0usize;
    let year = digits(s, &mut at, 4)?;
    expect(s, &mut at, b'-')?;
    let month = digits(s, &mut at, 2)?;
    expect(s, &mut at, b'-')?;
    let day = digits(s, &mut at, 2)?;
    if !(1..=9999).contains(&year) || !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month) {
        return None;
    }
    let mut micros_of_day = 0i64;
    let mut offset_secs = None;
    if matches!(s.get(at), Some(b'T' | b't' | b' '))
        && let Some((micros, end)) = time_of_day(s, at + 1)
    {
        micros_of_day = micros;
        at = end;
        let mut zone_at = at;
        while s.get(zone_at) == Some(&b' ') {
            zone_at += 1;
        }
        if let Some(offset) = zone(s, &mut zone_at) {
            offset_secs = Some(offset);
            at = zone_at;
        }
    }
    let parsed = Parsed {
        year,
        month,
        day,
        local_micros: days_from_civil(year, month, day) * DAY_MICROS + micros_of_day,
        offset_secs,
    };
    Some((parsed, at))
}

/// `HH:MM[:SS[.fraction]]` starting at `at`: microseconds into the day, and where it ends.
fn time_of_day(s: &[u8], mut at: usize) -> Option<(i64, usize)> {
    let hour = digits(s, &mut at, 2)?;
    expect(s, &mut at, b':')?;
    let minute = digits(s, &mut at, 2)?;
    let mut second = 0;
    let mut fraction = 0;
    if s.get(at) == Some(&b':') {
        at += 1;
        second = digits(s, &mut at, 2)?;
        if s.get(at) == Some(&b'.') {
            at += 1;
            fraction = fraction_micros(s, &mut at)?;
        }
    }
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    Some(((hour * 3600 + minute * 60 + second) * MICROS + fraction, at))
}

/// Exactly `n` ASCII digits.
fn digits(s: &[u8], at: &mut usize, n: usize) -> Option<i64> {
    let field = s.get(*at..*at + n)?;
    if !field.iter().all(u8::is_ascii_digit) {
        return None;
    }
    *at += n;
    Some(field.iter().fold(0i64, |acc, d| acc * 10 + i64::from(d - b'0')))
}

fn expect(s: &[u8], at: &mut usize, byte: u8) -> Option<()> {
    (s.get(*at) == Some(&byte)).then(|| *at += 1)
}

/// One or more fraction digits as microseconds, rounded half-to-even past the sixth — Postgres
/// keeps microseconds and rounds the rest. A carry into the next second is the caller's sum.
fn fraction_micros(s: &[u8], at: &mut usize) -> Option<i64> {
    let start = *at;
    while s.get(*at).is_some_and(u8::is_ascii_digit) {
        *at += 1;
    }
    let field = &s[start..*at];
    if field.is_empty() {
        return None;
    }
    let mut micros = 0i64;
    for i in 0..6 {
        micros = micros * 10 + field.get(i).map_or(0, |d| i64::from(d - b'0'));
    }
    if let Some(&next) = field.get(6) {
        let rest_nonzero = field[7..].iter().any(|d| *d != b'0');
        if next > b'5' || (next == b'5' && (rest_nonzero || micros % 2 == 1)) {
            micros += 1;
        }
    }
    Some(micros)
}

/// `Z`, or `±HH`, `±HHMM`, `±HH:MM`, each optionally followed by seconds: seconds east of UTC.
fn zone(s: &[u8], at: &mut usize) -> Option<i32> {
    let sign = match s.get(*at)? {
        b'Z' | b'z' => {
            *at += 1;
            return Some(0);
        }
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    *at += 1;
    let hours = digits(s, at, 2)?;
    let field = |at: &mut usize| -> Option<i64> {
        let mut probe = *at;
        if s.get(probe) == Some(&b':') {
            probe += 1;
        }
        let value = digits(s, &mut probe, 2)?;
        *at = probe;
        Some(value)
    };
    // Seconds can only follow minutes. What comes after the zone is the caller's to judge.
    let minutes = field(at);
    let seconds = if minutes.is_some() { field(at).unwrap_or(0) } else { 0 };
    let minutes = minutes.unwrap_or(0);
    if hours > 15 || minutes > 59 || seconds > 59 {
        return None;
    }
    i32::try_from(sign * (hours * 3600 + minutes * 60 + seconds)).ok()
}

/// `YYYY-MM-DD HH:MM:SS[.ffffff]<suffix>`, the fraction with its trailing zeros trimmed, as
/// Postgres prints it. `None` outside years 0001–9999, where the spelling changes.
fn render(micros: i64, suffix: &str) -> Option<String> {
    let (days, of_day) = (micros.div_euclid(DAY_MICROS), micros.rem_euclid(DAY_MICROS));
    let (year, month, day) = civil_from_days(days);
    if !(1..=9999).contains(&year) {
        return None;
    }
    let (seconds, fraction) = (of_day / MICROS, of_day % MICROS);
    let mut out =
        format!("{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}", seconds / 3600, seconds / 60 % 60, seconds % 60);
    if fraction != 0 {
        let digits = format!("{fraction:06}");
        out.push('.');
        out.push_str(digits.trim_end_matches('0'));
    }
    out.push_str(suffix);
    Some(out)
}

fn is_leap(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        4 | 6 | 9 | 11 => 30,
        2 if is_leap(year) => 29,
        2 => 28,
        _ => 31,
    }
}

/// Days since 1970-01-01 in the proleptic Gregorian calendar (Howard Hinnant's `days_from_civil`).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let year_of_era = y.rem_euclid(400);
    let day_of_year = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// The inverse of [`days_from_civil`].
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era = (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let mp = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tz(literal: &str) -> Option<String> {
        canonical_literal(literal, "timestamptz")
    }

    /// Each pair is `(what a client may send, what Postgres prints for that instant in UTC)` —
    /// the right-hand side is `SELECT '<left>'::timestamptz::text` under `SET TimeZone = 'UTC'`.
    #[test]
    fn timestamptz_literals_become_postgres_utc_output() {
        for (literal, printed) in [
            ("2026-10-19T07:00:00Z", "2026-10-19 07:00:00+00"),
            ("2026-10-19t07:00:00z", "2026-10-19 07:00:00+00"),
            ("2026-10-19 07:00:00+00", "2026-10-19 07:00:00+00"),
            ("2026-10-19 07:00:00", "2026-10-19 07:00:00+00"),
            ("2026-10-19T07:00Z", "2026-10-19 07:00:00+00"),
            ("2026-10-19", "2026-10-19 00:00:00+00"),
            ("  2026-10-19T07:00:00Z ", "2026-10-19 07:00:00+00"),
            ("2026-10-19T00:00:00-07:00", "2026-10-19 07:00:00+00"),
            ("2026-10-19T00:00:00-0700", "2026-10-19 07:00:00+00"),
            ("2026-10-19T00:00:00-07", "2026-10-19 07:00:00+00"),
            ("2026-10-19 00:00:00 -07", "2026-10-19 07:00:00+00"),
            ("2026-10-19T12:30:00+05:30", "2026-10-19 07:00:00+00"),
            // The offset carries the instant across a day, a month, a year and a leap day.
            ("2026-10-19T23:30:00-01:00", "2026-10-20 00:30:00+00"),
            ("2026-11-01T00:30:00+01:00", "2026-10-31 23:30:00+00"),
            ("2027-01-01T00:00:00+00:30", "2026-12-31 23:30:00+00"),
            ("2028-03-01T00:00:00+01:00", "2028-02-29 23:00:00+00"),
            ("2027-03-01T00:00:00+01:00", "2027-02-28 23:00:00+00"),
            // Fractions: trailing zeros trimmed, microseconds kept, the rest rounded.
            ("2026-10-19T07:00:00.000Z", "2026-10-19 07:00:00+00"),
            ("2026-10-19T07:00:00.500Z", "2026-10-19 07:00:00.5+00"),
            ("2026-10-19T07:00:00.123456Z", "2026-10-19 07:00:00.123456+00"),
            ("2026-10-19T07:00:00.1234567Z", "2026-10-19 07:00:00.123457+00"),
            ("2026-10-19T07:00:00.1234564Z", "2026-10-19 07:00:00.123456+00"),
            ("2026-10-19T23:59:59.9999996Z", "2026-10-20 00:00:00+00"),
        ] {
            assert_eq!(tz(literal).as_deref(), Some(printed), "{literal}");
        }
    }

    /// Canonical spellings order as their instants do — the property text comparison rests on.
    #[test]
    fn canonical_timestamptz_text_orders_like_the_instants() {
        let ascending = [
            "2026-10-18T23:59:59.999999Z",
            "2026-10-19T00:00:00Z",
            "2026-10-19T00:00:00.000001Z",
            "2026-10-19T00:00:00.1Z",
            "2026-10-19T00:00:00.12Z",
            "2026-10-19T00:00:00.5Z",
            "2026-10-19T00:00:01Z",
            "2026-10-19T09:00:00Z",
            "2026-10-19T10:00:00Z",
            "2026-10-20T00:00:00-07:00",
            "2026-11-01T00:00:00Z",
            "2027-01-01T00:00:00Z",
        ];
        let canonical: Vec<String> = ascending.iter().map(|l| tz(l).unwrap()).collect();
        let mut sorted = canonical.clone();
        sorted.sort();
        assert_eq!(canonical, sorted);
    }

    #[test]
    fn timestamp_and_date_literals_take_their_own_types_output() {
        assert_eq!(canonical_literal("2026-10-19T07:00:00Z", "timestamp").as_deref(), Some("2026-10-19 07:00:00"));
        assert_eq!(
            canonical_literal("2026-10-19T07:00:00.25-07:00", "timestamp").as_deref(),
            Some("2026-10-19 07:00:00.25"),
            "a zone is ignored when casting to timestamp"
        );
        assert_eq!(canonical_literal("2026-10-19", "timestamp").as_deref(), Some("2026-10-19 00:00:00"));
        assert_eq!(canonical_literal("2026-10-19T23:30:00-07:00", "date").as_deref(), Some("2026-10-19"));
        assert_eq!(canonical_literal("2026-10-19", "date").as_deref(), Some("2026-10-19"));
    }

    /// Anything else is not rewritten: other column types, the infinities, impossible dates,
    /// trailing text, and spellings outside the grammar.
    #[test]
    fn unrecognised_literals_and_other_types_are_left_alone() {
        for pg_type in ["text", "uuid", "timetz", "interval", "jsonb"] {
            assert_eq!(canonical_literal("2026-10-19T07:00:00Z", pg_type), None, "{pg_type}");
        }
        for literal in [
            "",
            "infinity",
            "-infinity",
            "now",
            "next tuesday",
            "2026-02-30T00:00:00Z",
            "2027-02-29",
            "2026-13-01",
            "2026-10-19T24:00:00Z",
            "2026-10-19T07:60:00Z",
            "2026-10-19T07:00:60Z",
            "2026-10-19T07:00:00.Z",
            "2026-10-19T07:00:00+16:00",
            "2026-10-19T07:00:00Z trailing",
            "2026-10-19T07:00:00 UTC",
            "20261019T070000Z",
            "26-10-19",
            "0000-01-01",
            "10000-01-01",
            "0001-01-01T00:00:00+01:00",
        ] {
            assert_eq!(tz(literal), None, "{literal:?}");
        }
    }

    /// Each pair is `(a cell as a session in some zone prints it, the same instant as a UTC
    /// session prints it)` — the two sides of `SELECT x::text` under `SET TimeZone` to that zone
    /// and to `UTC`.
    #[test]
    fn timestamptz_cells_are_respelled_as_a_utc_session_prints_them() {
        for (printed, utc) in [
            // America/Los_Angeles: daylight time, then the hour it repeats when that ends.
            ("2026-10-19 01:00:00-07", "2026-10-19 08:00:00+00"),
            ("2026-11-01 01:45:00-07", "2026-11-01 08:45:00+00"),
            ("2026-11-01 01:15:00-08", "2026-11-01 09:15:00+00"),
            // Half-hour and quarter-hour zones (Asia/Kolkata, Asia/Kathmandu).
            ("2026-10-19 13:30:00+05:30", "2026-10-19 08:00:00+00"),
            ("2026-10-19 13:45:00+05:45", "2026-10-19 08:00:00+00"),
            // Fractions survive as printed: trailing zeros already trimmed, microseconds kept.
            ("2026-10-21 18:04:56.789+05:30", "2026-10-21 12:34:56.789+00"),
            ("2026-10-21 05:34:56.000001-07", "2026-10-21 12:34:56.000001+00"),
            // The offset carries the instant across a year and back across a leap day.
            ("2026-12-31 20:00:00-07", "2027-01-01 03:00:00+00"),
            ("2028-03-01 05:00:00+09", "2028-02-29 20:00:00+00"),
            // Local mean time, before standard zones: the offset has seconds.
            ("1883-11-18 12:00:00-07:52:58", "1883-11-18 19:52:58+00"),
        ] {
            assert_eq!(utc_cell(printed, "timestamptz").as_deref(), Some(utc), "{printed}");
        }
    }

    /// A cell that is already UTC needs nothing, a cell this module cannot read is not guessed at
    /// (the infinities, a `BC` year, any non-ISO `DateStyle`), and only a type whose output
    /// depends on the session zone is ever touched.
    #[test]
    fn utc_cells_unreadable_cells_and_zone_independent_types_are_left_alone() {
        for cell in [
            "2026-10-19 08:00:00+00",
            "2026-10-19 08:00:00.5+00",
            "infinity",
            "-infinity",
            "0044-03-15 04:07:02-07:52:58 BC",
            "19.10.2026 01:00:00 PDT",
            "Mon Oct 19 01:00:00 2026 PDT",
            "10/19/2026 01:00:00 PDT",
            "2026-10-19 01:00:00",
            "",
        ] {
            assert_eq!(utc_cell(cell, "timestamptz"), None, "{cell:?}");
        }
        for pg_type in ["timestamp", "date", "timetz", "text", "jsonb", "_text", "_timestamp", "tsrange"] {
            assert_eq!(utc_cell("2026-10-19 01:00:00-07", pg_type), None, "{pg_type}");
        }
    }

    /// Arrays and ranges of `timestamptz` print each element in the session zone too.
    #[test]
    fn arrays_and_ranges_of_timestamptz_are_respelled_element_by_element() {
        for (pg_type, printed, utc) in [
            (
                "_timestamptz",
                r#"{"2026-10-19 01:00:00-07",NULL,"2026-10-20 13:30:00+05:30"}"#,
                r#"{"2026-10-19 08:00:00+00",NULL,"2026-10-20 08:00:00+00"}"#,
            ),
            (
                "tstzrange",
                r#"["2026-10-19 01:00:00-07","2026-10-20 01:00:00-07")"#,
                r#"["2026-10-19 08:00:00+00","2026-10-20 08:00:00+00")"#,
            ),
            ("tstzrange", r#"[,"2026-10-20 01:00:00.25-07")"#, r#"[,"2026-10-20 08:00:00.25+00")"#),
            (
                "tstzmultirange",
                r#"{["2026-10-19 01:00:00-07","2026-10-19 02:00:00-07"),["2026-10-20 01:00:00-07",)}"#,
                r#"{["2026-10-19 08:00:00+00","2026-10-19 09:00:00+00"),["2026-10-20 08:00:00+00",)}"#,
            ),
            (
                "_tstzrange",
                r#"{"[\"2026-10-19 01:00:00-07\",\"2026-10-20 01:00:00-07\")"}"#,
                r#"{"[\"2026-10-19 08:00:00+00\",\"2026-10-20 08:00:00+00\")"}"#,
            ),
        ] {
            assert_eq!(utc_cell(printed, pg_type).as_deref(), Some(utc), "{pg_type} {printed}");
        }
        for (pg_type, cell) in [
            ("_timestamptz", "{}"),
            ("_timestamptz", r#"{"2026-10-19 08:00:00+00",NULL}"#),
            ("tstzrange", "empty"),
            ("tstzrange", r#"["2026-10-19 08:00:00+00",infinity)"#),
            ("tstzrange", r#"["0044-03-15 04:07:02-07:52:58 BC","2026-10-19 01:00:00-07")"#),
        ] {
            assert_eq!(utc_cell(cell, pg_type), None, "{pg_type} {cell}");
        }
    }

    #[test]
    fn civil_days_round_trip() {
        for days in (-719_162..=2_932_896).step_by(997) {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(days_from_civil(y, m, d), days);
        }
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(days_from_civil(2000, 3, 1), 11_017);
    }
}
