pub const EVENT_BITS: u32 = 6;
pub const EVENT_PARTS: usize = 1 << EVENT_BITS;
pub const PERSON_BITS: u32 = 6;
pub const PERSON_PARTS: usize = 1 << PERSON_BITS;

#[inline(always)]
pub fn prefetch_read<T>(ptr: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(ptr as *const i8, std::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        std::arch::asm!(
            "prfm pldl1keep, [{p}]",
            p = in(reg) ptr,
            options(nostack, readonly, preserves_flags)
        );
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        let _ = ptr;
    }
}

const WY_S0: u64 = 0xa076_1d64_78bd_642f;
const WY_S1: u64 = 0xe703_7ed1_a0b4_28db;
const WY_S2: u64 = 0x8ebc_6af0_9c88_c6e3;

#[inline(always)]
const fn wymix(a: u64, b: u64) -> u64 {
    let r = (a as u128).wrapping_mul(b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}

const LEN_WYMIX: [u64; 17] = [
    wymix(0 ^ WY_S0, WY_S1),
    wymix(1 ^ WY_S0, WY_S1),
    wymix(2 ^ WY_S0, WY_S1),
    wymix(3 ^ WY_S0, WY_S1),
    wymix(4 ^ WY_S0, WY_S1),
    wymix(5 ^ WY_S0, WY_S1),
    wymix(6 ^ WY_S0, WY_S1),
    wymix(7 ^ WY_S0, WY_S1),
    wymix(8 ^ WY_S0, WY_S1),
    wymix(9 ^ WY_S0, WY_S1),
    wymix(10 ^ WY_S0, WY_S1),
    wymix(11 ^ WY_S0, WY_S1),
    wymix(12 ^ WY_S0, WY_S1),
    wymix(13 ^ WY_S0, WY_S1),
    wymix(14 ^ WY_S0, WY_S1),
    wymix(15 ^ WY_S0, WY_S1),
    wymix(16 ^ WY_S0, WY_S1),
];

#[inline(always)]
unsafe fn read_u64_unaligned(p: *const u8) -> u64 {
    u64::from_le_bytes(*(p as *const [u8; 8]))
}

#[inline(always)]
unsafe fn read_u32_unaligned(p: *const u8) -> u32 {
    u32::from_le_bytes(*(p as *const [u8; 4]))
}

/// wyhash-style hash for short byte strings (IDs are usually 6 to 12 bytes).
#[inline(always)]
pub fn hash_id(bytes: &[u8]) -> u64 {
    let len = bytes.len();
    let ptr = bytes.as_ptr();
    if (8..=16).contains(&len) {
        unsafe {
            let a = read_u64_unaligned(ptr);
            let b = read_u64_unaligned(ptr.add(len - 8));
            wymix(a ^ WY_S1, b ^ WY_S2) ^ *LEN_WYMIX.get_unchecked(len)
        }
    } else {
        hash_bytes(bytes)
    }
}

/// wyhash-style hash for short byte strings (IDs are usually 6 to 12 bytes).
#[inline(always)]
pub fn hash_bytes(bytes: &[u8]) -> u64 {
    let len = bytes.len();
    let ptr = bytes.as_ptr();
    unsafe {
        if (4..=16).contains(&len) {
            let (a, b) = if len >= 8 {
                (read_u64_unaligned(ptr), read_u64_unaligned(ptr.add(len - 8)))
            } else {
                let lo = read_u32_unaligned(ptr) as u64;
                let hi = read_u32_unaligned(ptr.add(len - 4)) as u64;
                (lo | (hi << 32), lo ^ (hi << 32))
            };
            return wymix(a ^ WY_S1, b ^ WY_S2) ^ *LEN_WYMIX.get_unchecked(len);
        }
        if len < 4 {
            let v = if len == 0 {
                0u64
            } else {
                ((*ptr as u64) << 16) | ((*ptr.add(len >> 1) as u64) << 8) | (*ptr.add(len - 1) as u64)
            };
            return wymix(v ^ WY_S1, (len as u64) ^ WY_S0);
        }
        let mut seed = WY_S0 ^ (len as u64);
        let mut p = ptr;
        let mut rem = len;
        while rem > 16 {
            let a = read_u64_unaligned(p);
            let b = read_u64_unaligned(p.add(8));
            seed = wymix(a ^ WY_S1, b ^ seed);
            p = p.add(16);
            rem -= 16;
        }
        let a = read_u64_unaligned(ptr.add(len - 16));
        let b = read_u64_unaligned(ptr.add(len - 8));
        wymix(a ^ WY_S1, b ^ seed) ^ wymix(WY_S2, len as u64)
    }
}

#[inline(always)]
pub fn fast_build_hasher() -> ahash::RandomState {
    ahash::RandomState::with_seeds(
        0x517c_c1b7_2722_0a95,
        0x9e37_79b9_7f4a_7c15,
        0xbf58_476d_1ce4_e5b9,
        0x94d0_49bb_1331_11eb,
    )
}

#[inline(always)]
pub fn event_part(h: u64) -> usize {
    (h >> (64 - EVENT_BITS)) as usize
}

#[inline(always)]
pub fn person_part_from_hash(h: u64) -> usize {
    (h >> (64 - PERSON_BITS)) as usize
}

#[inline(always)]
pub fn person_part_from_comp(comp_id: u32) -> usize {
    (comp_id.wrapping_mul(0x9E37_79B1) >> (32 - PERSON_BITS)) as usize
}

#[inline(always)]
pub fn read_varint(data: &[u8], pos: &mut usize) -> usize {
    let b0 = unsafe { *data.get_unchecked(*pos) };
    *pos += 1;
    if b0 < 0x80 {
        return b0 as usize;
    }
    let mut val = (b0 & 0x7f) as usize;
    let mut shift = 7;
    loop {
        let b = unsafe { *data.get_unchecked(*pos) };
        *pos += 1;
        val |= ((b & 0x7f) as usize) << shift;
        if b < 0x80 {
            return val;
        }
        shift += 7;
    }
}

#[inline(always)]
pub fn rd_u32(data: &[u8], pos: usize) -> u32 {
    debug_assert!(pos + 4 <= data.len());
    u32::from_le(unsafe { (data.as_ptr().add(pos) as *const u32).read_unaligned() })
}

#[inline(always)]
pub fn rd_u64(data: &[u8], pos: usize) -> u64 {
    debug_assert!(pos + 8 <= data.len());
    u64::from_le(unsafe { (data.as_ptr().add(pos) as *const u64).read_unaligned() })
}

/// Copy a short slice without calling libc. Writes exactly `len` bytes using
/// overlapping fixed-width moves; never reads or writes outside either range.
#[inline(always)]
pub unsafe fn copy_small(dst: *mut u8, src: *const u8, len: usize) {
    if len >= 16 {
        std::ptr::copy_nonoverlapping(src, dst, len);
    } else if len >= 8 {
        let a = (src as *const u64).read_unaligned();
        let b = (src.add(len - 8) as *const u64).read_unaligned();
        (dst as *mut u64).write_unaligned(a);
        (dst.add(len - 8) as *mut u64).write_unaligned(b);
    } else if len >= 4 {
        let a = (src as *const u32).read_unaligned();
        let b = (src.add(len - 4) as *const u32).read_unaligned();
        (dst as *mut u32).write_unaligned(a);
        (dst.add(len - 4) as *mut u32).write_unaligned(b);
    } else if len > 0 {
        *dst = *src;
        *dst.add(len >> 1) = *src.add(len >> 1);
        *dst.add(len - 1) = *src.add(len - 1);
    }
}

/// Write cursor into spare capacity reserved by `append_record`. Only
/// `append_record` can construct one, so every write stays inside the
/// reservation as long as callers pass an honest upper bound.
pub struct Cur {
    pub p: *mut u8,
}

impl Cur {
    #[inline(always)]
    pub fn u8(&mut self, v: u8) {
        unsafe {
            *self.p = v;
            self.p = self.p.add(1);
        }
    }

    #[inline(always)]
    pub fn u32(&mut self, v: u32) {
        unsafe {
            (self.p as *mut u32).write_unaligned(v.to_le());
            self.p = self.p.add(4);
        }
    }

    #[inline(always)]
    pub fn u64(&mut self, v: u64) {
        unsafe {
            (self.p as *mut u64).write_unaligned(v.to_le());
            self.p = self.p.add(8);
        }
    }

    #[inline(always)]
    pub fn varint(&mut self, mut v: usize) {
        while v >= 0x80 {
            self.u8((v as u8) | 0x80);
            v >>= 7;
        }
        self.u8(v as u8);
    }

    #[inline(always)]
    pub fn bytes(&mut self, s: &[u8]) {
        unsafe {
            copy_small(self.p, s.as_ptr(), s.len());
            self.p = self.p.add(s.len());
        }
    }
}

/// Append one record of at most `max` bytes to `buf` with a single capacity check.
#[cfg(test)]
#[inline(always)]
pub fn append_record(buf: &mut Vec<u8>, max: usize, f: impl FnOnce(&mut Cur)) {
    buf.reserve(max);
    unsafe {
        let len = buf.len();
        let start = buf.as_mut_ptr().add(len);
        let written = write_raw(start, f);
        debug_assert!(written <= max);
        buf.set_len(len + written);
    }
}

/// Write one record at `start` and return its length. The caller guarantees
/// enough room for the record.
#[inline(always)]
pub unsafe fn write_raw(start: *mut u8, f: impl FnOnce(&mut Cur)) -> usize {
    let mut c = Cur { p: start };
    f(&mut c);
    c.p.offset_from(start) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_append_record_roundtrip() {
        let mut buf = Vec::new();
        for len in 0..40usize {
            let s: Vec<u8> = (0..len as u8).map(|b| b.wrapping_mul(7)).collect();
            append_record(&mut buf, len + 32, |c| {
                c.varint(len);
                c.bytes(&s);
                c.u32(0xDEAD_BEEF);
                c.u64(len as u64 * 3);
            });
        }
        append_record(&mut buf, 16, |c| c.varint(300));
        let mut pos = 0;
        for len in 0..40usize {
            assert_eq!(read_varint(&buf, &mut pos), len);
            let expect: Vec<u8> = (0..len as u8).map(|b| b.wrapping_mul(7)).collect();
            assert_eq!(&buf[pos..pos + len], &expect[..]);
            pos += len;
            assert_eq!(rd_u32(&buf, pos), 0xDEAD_BEEF);
            pos += 4;
            assert_eq!(rd_u64(&buf, pos), len as u64 * 3);
            pos += 8;
        }
        assert_eq!(read_varint(&buf, &mut pos), 300);
        assert_eq!(pos, buf.len());
    }
}
