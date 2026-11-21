/// Ultra-fast strict CSV line parser matching rules.md 100% exactly:
/// - Empty line -> None
/// - Wrong number of fields (!= N) -> None
/// - Quotes valid ONLY wrapping the entire field (`"..."`), with zero internal quotes
/// - Spaces and `\r` are ordinary characters
///
/// Uses SIMD bitmask extraction (AVX2 on x86_64, NEON on aarch64) for lines <= 128 bytes
/// (99.999%+ of all lines) and a single-pass scalar scanner for longer lines.

#[inline(always)]
fn split_fields_scalar<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let len = line.len();
    if len == 0 {
        return None;
    }
    let mut out = [&[][..]; N];
    let mut field_idx = 0usize;
    let mut field_start = 0usize;
    let mut quotes = 0u32;

    let mut i = 0usize;
    while i < len {
        let b = unsafe { *line.get_unchecked(i) };
        if b == b',' {
            if field_idx >= N - 1 {
                return None;
            }
            let raw = unsafe { line.get_unchecked(field_start..i) };
            let val = if quotes == 0 {
                raw
            } else if quotes == 2
                && raw.len() >= 2
                && unsafe { *raw.get_unchecked(0) } == b'"'
                && unsafe { *raw.get_unchecked(raw.len() - 1) } == b'"'
            {
                unsafe { raw.get_unchecked(1..raw.len() - 1) }
            } else {
                return None;
            };
            unsafe { *out.get_unchecked_mut(field_idx) = val };
            field_idx += 1;
            field_start = i + 1;
            quotes = 0;
        } else if b == b'"' {
            quotes += 1;
        }
        i += 1;
    }

    if field_idx != N - 1 {
        return None;
    }

    let raw = unsafe { line.get_unchecked(field_start..len) };
    let val = if quotes == 0 {
        raw
    } else if quotes == 2
        && raw.len() >= 2
        && unsafe { *raw.get_unchecked(0) } == b'"'
        && unsafe { *raw.get_unchecked(raw.len() - 1) } == b'"'
    {
        unsafe { raw.get_unchecked(1..raw.len() - 1) }
    } else {
        return None;
    };
    unsafe { *out.get_unchecked_mut(N - 1) = val };
    Some(out)
}

#[inline(always)]
fn extract_from_masks<const N: usize>(
    line: &[u8],
    mut comma_mask: u128,
    quote_mask: u128,
) -> Option<[&[u8]; N]> {
    if comma_mask.count_ones() as usize != N - 1 {
        return None;
    }
    let mut out = [&[][..]; N];
    let mut start = 0usize;

    if quote_mask == 0 {
        for slot in &mut out[..N - 1] {
            let c = comma_mask.trailing_zeros() as usize;
            comma_mask &= comma_mask - 1;
            *slot = unsafe { line.get_unchecked(start..c) };
            start = c + 1;
        }
        out[N - 1] = unsafe { line.get_unchecked(start..) };
        Some(out)
    } else {
        for slot in &mut out[..N - 1] {
            let c = comma_mask.trailing_zeros() as usize;
            comma_mask &= comma_mask - 1;
            let flen = c - start;
            if flen == 0 {
                *slot = &[];
            } else {
                let fmask = if flen == 128 {
                    u128::MAX
                } else {
                    (1u128 << flen) - 1
                };
                let fq = (quote_mask >> start) & fmask;
                if fq == 0 {
                    *slot = unsafe { line.get_unchecked(start..c) };
                } else if flen >= 2 && fq == (1u128 | (1u128 << (flen - 1))) {
                    *slot = unsafe { line.get_unchecked(start + 1..c - 1) };
                } else {
                    return None;
                }
            }
            start = c + 1;
        }
        let end = line.len();
        let flen = end - start;
        if flen == 0 {
            out[N - 1] = &[];
        } else {
            let fmask = if flen == 128 {
                u128::MAX
            } else {
                (1u128 << flen) - 1
            };
            let fq = (quote_mask >> start) & fmask;
            if fq == 0 {
                out[N - 1] = unsafe { line.get_unchecked(start..end) };
            } else if flen >= 2 && fq == (1u128 | (1u128 << (flen - 1))) {
                out[N - 1] = unsafe { line.get_unchecked(start + 1..end - 1) };
            } else {
                return None;
            }
        }
        Some(out)
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
fn scan_masks_128(line: &[u8]) -> (u128, u128) {
    use std::arch::x86_64::*;
    let len = line.len();
    let ptr = line.as_ptr();
    let mut comma_mask = 0u128;
    let mut quote_mask = 0u128;
    unsafe {
        let v_comma = _mm256_set1_epi8(b',' as i8);
        let v_quote = _mm256_set1_epi8(b'"' as i8);
        let mut off = 0usize;
        while off + 32 <= len {
            let chunk = _mm256_loadu_si256(ptr.add(off) as *const __m256i);
            let cm = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, v_comma)) as u32 as u128;
            let qm = _mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, v_quote)) as u32 as u128;
            comma_mask |= cm << off;
            quote_mask |= qm << off;
            off += 32;
        }
        if off < len {
            let rem = len - off;
            let mut tail = [0u8; 32];
            std::ptr::copy_nonoverlapping(ptr.add(off), tail.as_mut_ptr(), rem);
            let chunk = _mm256_loadu_si256(tail.as_ptr() as *const __m256i);
            let valid_bits = if rem == 32 { u32::MAX } else { (1u32 << rem) - 1 };
            let cm = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, v_comma)) as u32 & valid_bits)
                as u128;
            let qm = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, v_quote)) as u32 & valid_bits)
                as u128;
            comma_mask |= cm << off;
            quote_mask |= qm << off;
        }
    }
    (comma_mask, quote_mask)
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
fn scan_masks_128(line: &[u8]) -> (u128, u128) {
    use std::arch::aarch64::*;
    let len = line.len();
    let ptr = line.as_ptr();
    let mut comma_mask = 0u128;
    let mut quote_mask = 0u128;

    #[inline(always)]
    unsafe fn movemask_neon(v: uint8x16_t, bit_mask: uint8x16_t) -> u16 {
        let masked = vandq_u8(v, bit_mask);
        let lo = vaddv_u8(vget_low_u8(masked)) as u16;
        let hi = vaddv_u8(vget_high_u8(masked)) as u16;
        lo | (hi << 8)
    }

    unsafe {
        let bit_mask_arr: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
        let bit_mask = vld1q_u8(bit_mask_arr.as_ptr());
        let v_comma = vdupq_n_u8(b',');
        let v_quote = vdupq_n_u8(b'"');

        let mut off = 0usize;
        while off + 16 <= len {
            let chunk = vld1q_u8(ptr.add(off));
            let cm = movemask_neon(vceqq_u8(chunk, v_comma), bit_mask) as u128;
            let qm = movemask_neon(vceqq_u8(chunk, v_quote), bit_mask) as u128;
            comma_mask |= cm << off;
            quote_mask |= qm << off;
            off += 16;
        }
        if off < len {
            let rem = len - off;
            let mut tail = [0u8; 16];
            std::ptr::copy_nonoverlapping(ptr.add(off), tail.as_mut_ptr(), rem);
            let chunk = vld1q_u8(tail.as_ptr());
            let valid_bits = ((1u32 << rem) - 1) as u16;
            let cm = (movemask_neon(vceqq_u8(chunk, v_comma), bit_mask) & valid_bits) as u128;
            let qm = (movemask_neon(vceqq_u8(chunk, v_quote), bit_mask) & valid_bits) as u128;
            comma_mask |= cm << off;
            quote_mask |= qm << off;
        }
    }
    (comma_mask, quote_mask)
}

#[inline(always)]
pub fn split_fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let len = line.len();
    if len == 0 {
        return None;
    }
    #[cfg(any(
        target_arch = "aarch64",
        all(target_arch = "x86_64", target_feature = "avx2")
    ))]
    {
        if len <= 128 {
            let (cm, qm) = scan_masks_128(line);
            return extract_from_masks::<N>(line, cm, qm);
        }
    }
    split_fields_scalar::<N>(line)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csv_dialect() {
        let cases: &[&[u8]] = &[
            b"u1,u2",
            b"\"u1\",\" u2\"",
            b"\"\",u2",
            b"u1,u2\r",
            b"u1,\"u2\"\r",
            b"u1,u5\"",
            b"u1,u2,u3",
            b"u1",
            b"",
            b"\"\"\"\",u2",
            b"\"a\"b,u2",
            b"\"\",\"",
            b"\"a\",",
            b",\"\"",
            b"\"",
            b",",
        ];
        for &c in cases {
            assert_eq!(split_fields::<2>(c), split_fields_scalar::<2>(c), "mismatch on {:?}", String::from_utf8_lossy(c));
        }

        // Test 7-field lines of various lengths up to 160 bytes
        let ev = b"e4k9z2m1p,\"u8x7y6z5w\",2026-10-03T23:58:10-04:00,2026-10-03T23:59:10Z,\"purchase\",p12345,\"0250\"";
        assert_eq!(split_fields::<7>(ev), split_fields_scalar::<7>(ev));
        assert!(split_fields::<7>(ev).is_some());

        let long_ev = format!("e1,u1,{},2026-10-03T23:59:10Z,view,p1,", "a".repeat(120));
        assert_eq!(
            split_fields::<7>(long_ev.as_bytes()),
            split_fields_scalar::<7>(long_ev.as_bytes())
        );
    }
}
