pub const MS_PER_DAY: i64 = 86_400_000;
pub const SESSION_GAP_MS: i64 = 1_800_000;
pub const CLICK_REACH_MS: i64 = 7 * MS_PER_DAY;
pub const LATE_CUTOFF_MS: i64 = MS_PER_DAY;

#[inline(always)]
fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

#[inline(always)]
pub fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// Howard Hinnant's proleptic Gregorian civil -> days since 1970-01-01.
#[inline(always)]
pub fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = (y - era * 400) as u32;
    let m_adj = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * m_adj + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + (doe as i64) - 719_468
}

/// Inverse of `days_from_civil`: days since 1970-01-01 -> (year, month, day).
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m as u32, d as u32)
}

#[derive(Clone, Copy)]
pub struct YmEntry {
    /// Raw 8 bytes of `"YYYY-MM-"` as native-endian u64 for 1-instruction comparison.
    pub ym_u64: u64,
    /// Unix seconds of `YYYY-MM-01T00:00:00Z`.
    pub day1_sec: i64,
    pub max_day: u32,
}

/// Fast cache of up to 4 `"YYYY-MM-"` prefixes covering the active dataset window.
/// Hits >99.99% of all F1/F2 timestamps in a 14-day window with a single u64 compare!
#[derive(Clone, Copy)]
pub struct YmCache {
    entries: [YmEntry; 4],
    len: usize,
}

impl YmCache {
    pub fn for_window(click_start_ms: i64, window_end_ms: i64) -> Self {
        let mut entries = [YmEntry {
            ym_u64: 0,
            day1_sec: 0,
            max_day: 0,
        }; 4];
        let mut len = 0usize;

        // Cover from (click_start - 2 days) to (window_end + 10 days) for F2 offsets & late ingest_ts
        let start_day = click_start_ms.div_euclid(MS_PER_DAY) - 2;
        let end_day = window_end_ms.div_euclid(MS_PER_DAY) + 10;
        let (mut y, mut m, _) = civil_from_days(start_day);
        let (ey, em, _) = civil_from_days(end_day);

        while (y < ey || (y == ey && m <= em)) && len < 4 {
            if (0..=9999).contains(&y) {
                let s = format!("{y:04}-{m:02}-");
                let ym_u64 = u64::from_ne_bytes(s.as_bytes().try_into().unwrap());
                let day1_sec = days_from_civil(y, m, 1) * 86_400;
                let max_day = days_in_month(y, m);
                entries[len] = YmEntry {
                    ym_u64,
                    day1_sec,
                    max_day,
                };
                len += 1;
            }
            if m == 12 {
                y += 1;
                m = 1;
            } else {
                m += 1;
            }
        }
        Self { entries, len }
    }

    #[inline(always)]
    fn lookup(&self, b: &[u8]) -> Option<YmEntry> {
        let key = u64::from_ne_bytes(unsafe { *(b.as_ptr() as *const [u8; 8]) });
        for i in 0..self.len {
            let e = unsafe { *self.entries.get_unchecked(i) };
            if e.ym_u64 == key {
                return Some(e);
            }
        }
        None
    }
}

#[inline(always)]
fn parse_2_digits(a: u8, b: u8) -> Option<u32> {
    let d0 = a.wrapping_sub(b'0');
    let d1 = b.wrapping_sub(b'0');
    if d0 <= 9 && d1 <= 9 {
        Some((d0 as u32) * 10 + (d1 as u32))
    } else {
        None
    }
}

#[inline(always)]
fn parse_4_digits(b: &[u8]) -> Option<i64> {
    let d0 = b[0].wrapping_sub(b'0');
    let d1 = b[1].wrapping_sub(b'0');
    let d2 = b[2].wrapping_sub(b'0');
    let d3 = b[3].wrapping_sub(b'0');
    if d0 <= 9 && d1 <= 9 && d2 <= 9 && d3 <= 9 {
        Some((d0 as i64) * 1000 + (d1 as i64) * 100 + (d2 as i64) * 10 + (d3 as i64))
    } else {
        None
    }
}

/// General exact civil 19-byte prefix `YYYY-MM-DDTHH:MM:SS` -> Unix seconds.
#[inline]
fn parse_civil_19_sec_slow(b: &[u8]) -> Option<i64> {
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let year = parse_4_digits(&b[0..4])?;
    let month = parse_2_digits(b[5], b[6])?;
    if !(1..=12).contains(&month) {
        return None;
    }
    let day = parse_2_digits(b[8], b[9])?;
    if day < 1 || day > days_in_month(year, month) {
        return None;
    }
    let hour = parse_2_digits(b[11], b[12])?;
    if hour > 23 {
        return None;
    }
    let minute = parse_2_digits(b[14], b[15])?;
    if minute > 59 {
        return None;
    }
    let second = parse_2_digits(b[17], b[18])?;
    if second > 59 {
        return None;
    }
    let days = days_from_civil(year, month, day);
    Some(days * 86_400 + (hour as i64) * 3600 + (minute as i64) * 60 + (second as i64))
}

#[inline(always)]
fn parse_civil_19_sec_fast(b: &[u8], ym_cache: &YmCache) -> Option<i64> {
    if let Some(entry) = ym_cache.lookup(b) {
        let sep_bad = (b[10] ^ b'T') | (b[13] ^ b':') | (b[16] ^ b':');
        let d0 = b[8].wrapping_sub(b'0');
        let d1 = b[9].wrapping_sub(b'0');
        let h0 = b[11].wrapping_sub(b'0');
        let h1 = b[12].wrapping_sub(b'0');
        let m0 = b[14].wrapping_sub(b'0');
        let m1 = b[15].wrapping_sub(b'0');
        let s0 = b[17].wrapping_sub(b'0');
        let s1 = b[18].wrapping_sub(b'0');
        if (sep_bad != 0)
            | (d0 > 9)
            | (d1 > 9)
            | (h0 > 9)
            | (h1 > 9)
            | (m0 > 9)
            | (m1 > 9)
            | (s0 > 9)
            | (s1 > 9)
        {
            return None;
        }
        let day = (d0 as u32) * 10 + (d1 as u32);
        let hour = (h0 as u32) * 10 + (h1 as u32);
        let minute = (m0 as u32) * 10 + (m1 as u32);
        let second = (s0 as u32) * 10 + (s1 as u32);
        if (day < 1) | (day > entry.max_day) | (hour > 23) | (minute > 59) | (second > 59) {
            return None;
        }
        return Some(
            entry.day1_sec
                + ((day - 1) as i64) * 86_400
                + (hour as i64) * 3600
                + (minute as i64) * 60
                + (second as i64),
        );
    }
    parse_civil_19_sec_slow(b)
}

/// Strict F1 parser (`YYYY-MM-DDTHH:MM:SSZ`) returning Unix seconds (no cache, used for manifest).
#[inline(always)]
pub fn parse_f1_sec(b: &[u8]) -> Option<i64> {
    if b.len() != 20 || b[19] != b'Z' {
        return None;
    }
    parse_civil_19_sec_slow(b)
}

/// Fast cached strict F1 parser (`YYYY-MM-DDTHH:MM:SSZ`) returning Unix seconds.
#[inline(always)]
pub fn parse_f1_sec_fast(b: &[u8], ym_cache: &YmCache) -> Option<i64> {
    if b.len() != 20 || b[19] != b'Z' {
        return None;
    }
    parse_civil_19_sec_fast(b, ym_cache)
}

/// Strict F1/F2/F3 timestamp parser with window check `[min_ms, max_ms_excl)`.
#[inline(always)]
pub fn parse_timestamp_in_window(
    b: &[u8],
    min_ms: i64,
    max_ms_excl: i64,
    ym_cache: &YmCache,
) -> Option<i64> {
    let len = b.len();
    if len == 0 {
        return None;
    }
    let ts_ms = if len == 20 && b[19] == b'Z' {
        // F1: YYYY-MM-DDTHH:MM:SSZ
        parse_civil_19_sec_fast(b, ym_cache)? * 1000
    } else if len == 13 {
        // Fast path for standard 13-digit epoch ms (99.9% of F3 timestamps!):
        // SWAR 8-digit + 4-digit + 1-digit parse.
        let w8 = u64::from_le_bytes(unsafe { *(b.as_ptr() as *const [u8; 8]) }).wrapping_sub(0x3030_3030_3030_3030);
        let w4 = u32::from_le_bytes(unsafe { *(b.as_ptr().add(8) as *const [u8; 4]) }).wrapping_sub(0x3030_3030);
        let d12 = b[12].wrapping_sub(b'0');
        let bad8 = ((w8 | w8.wrapping_add(0x7676_7676_7676_7676)) & 0x8080_8080_8080_8080)
            | (w8 & 0x7070_7070_7070_7070);
        let bad4 = ((w4 | w4.wrapping_add(0x7676_7676)) & 0x8080_8080)
            | (w4 & 0x7070_7070);
        if (bad8 != 0) | (bad4 != 0) | (d12 > 9) {
            return None;
        }
        let p8 = ((w8 & 0x00FF_00FF_00FF_00FF) * 10) + ((w8 >> 8) & 0x00FF_00FF_00FF_00FF);
        let p8 = ((p8 & 0x0000_FFFF_0000_FFFF) * 100) + ((p8 >> 16) & 0x0000_FFFF_0000_FFFF);
        let hi8 = (((p8 & 0xFFFF_FFFF) * 10_000) + (p8 >> 32)) as i64;
        let p4 = ((w4 & 0x00FF_00FF) * 10) + ((w4 >> 8) & 0x00FF_00FF);
        let lo4 = (((p4 & 0xFFFF) * 100) + (p4 >> 16)) as i64;
        hi8 * 100_000 + lo4 * 10 + (d12 as i64)
    } else if len == 25 && (b[19] == b'+' || b[19] == b'-') {
        // F2: YYYY-MM-DDTHH:MM:SS±HH:MM
        if b[22] != b':' {
            return None;
        }
        let off_h = parse_2_digits(b[20], b[21])?;
        let off_m = parse_2_digits(b[23], b[24])?;
        if off_h > 23 || off_m > 59 {
            return None;
        }
        let base_sec = parse_civil_19_sec_fast(b, ym_cache)?;
        let off_sec = (off_h as i64) * 3600 + (off_m as i64) * 60;
        let utc_sec = if b[19] == b'+' {
            base_sec - off_sec
        } else {
            base_sec + off_sec
        };
        utc_sec * 1000
    } else {
        // General F3: ASCII digits only (epoch ms), arbitrary length / leading zeros.
        let mut i = 0usize;
        while i < len && b[i] == b'0' {
            i += 1;
        }
        if i == len {
            0i64
        } else {
            let mut val: i64 = 0;
            let mut overflowed = false;
            while i < len {
                let d = b[i].wrapping_sub(b'0');
                if d > 9 {
                    return None;
                }
                if !overflowed {
                    if let Some(next) = val.checked_mul(10).and_then(|v| v.checked_add(d as i64)) {
                        val = next;
                    } else {
                        overflowed = true;
                    }
                }
                i += 1;
            }
            if overflowed {
                return None;
            }
            val
        }
    };

    if ts_ms >= min_ms && ts_ms < max_ms_excl {
        Some(ts_ms)
    } else {
        None
    }
}

/// Strict price_cents parser: non-empty, ASCII digits only, value in 1..=10_000_000.
#[inline(always)]
pub fn parse_price_cents(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut val: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        if val <= 10_000_000 {
            val = val * 10 + (d as u64);
        }
    }
    if (1..=10_000_000).contains(&val) {
        Some(val as u32)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamps() {
        let start = parse_f1_sec(b"2026-10-01T00:00:00Z").unwrap() * 1000;
        let end = parse_f1_sec(b"2026-10-15T00:00:00Z").unwrap() * 1000;
        let ym = YmCache::for_window(start - CLICK_REACH_MS, end);

        let f1 = parse_f1_sec_fast(b"2026-10-03T23:58:10Z", &ym).unwrap() * 1000;
        assert_eq!(f1, 1791071890000);
        assert_eq!(
            parse_timestamp_in_window(b"2026-10-03T19:58:10-04:00", 0, i64::MAX, &ym),
            Some(1791071890000)
        );
        assert_eq!(
            parse_timestamp_in_window(b"0001791071890000", 0, i64::MAX, &ym),
            Some(1791071890000)
        );
        assert_eq!(
            parse_timestamp_in_window(b"2026-10-04T09:59:00+23:59", 0, i64::MAX, &ym),
            Some(parse_f1_sec(b"2026-10-03T10:00:00Z").unwrap() * 1000)
        );
        assert_eq!(
            parse_timestamp_in_window(b"2026-10-03T12:00:00-00:00", 0, i64::MAX, &ym),
            Some(parse_f1_sec(b"2026-10-03T12:00:00Z").unwrap() * 1000)
        );
        assert_eq!(
            parse_timestamp_in_window(b"2026-10-03T10:00:00+24:00", 0, i64::MAX, &ym),
            None
        );
        assert_eq!(
            parse_timestamp_in_window(b"2026-10-03T10:00:00+05:60", 0, i64::MAX, &ym),
            None
        );
        assert_eq!(
            parse_timestamp_in_window(b"2026-10-03T10:00:00+0200", 0, i64::MAX, &ym),
            None
        );
        assert_eq!(
            parse_timestamp_in_window(b"2027-02-29T10:00:00Z", 0, i64::MAX, &ym),
            None
        );
        assert!(parse_timestamp_in_window(b"2028-02-29T10:00:00Z", 0, i64::MAX, &ym).is_some());
        assert_eq!(
            parse_timestamp_in_window(b"179102160000000000000", 0, i64::MAX, &ym),
            None
        );
        assert_eq!(parse_timestamp_in_window(b"", 0, i64::MAX, &ym), None);
        assert_eq!(
            parse_timestamp_in_window(b"2026-10-03T10:00:00z", 0, i64::MAX, &ym),
            None
        );
        assert_eq!(
            parse_timestamp_in_window(b"2026-10-03t10:00:00Z", 0, i64::MAX, &ym),
            None
        );
    }

    #[test]
    fn test_price() {
        assert_eq!(parse_price_cents(b"0250"), Some(250));
        assert_eq!(parse_price_cents(b"1"), Some(1));
        assert_eq!(parse_price_cents(b"10000000"), Some(10_000_000));
        assert_eq!(parse_price_cents(b"10000001"), None);
        assert_eq!(parse_price_cents(b"0"), None);
        assert_eq!(parse_price_cents(b""), None);
        assert_eq!(parse_price_cents(b"-5"), None);
        assert_eq!(parse_price_cents(b"1000\r"), None);
    }
}
