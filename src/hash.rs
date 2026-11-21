use std::sync::atomic::{AtomicU64, Ordering};

pub const EVENT_PARTS: usize = 128;
pub const PERSON_PARTS: usize = 128;

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
fn wymix(a: u64, b: u64) -> u64 {
    let r = (a as u128).wrapping_mul(b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}

#[inline(always)]
unsafe fn read_u64_unaligned(p: *const u8) -> u64 {
    u64::from_le_bytes(*(p as *const [u8; 8]))
}

#[inline(always)]
unsafe fn read_u32_unaligned(p: *const u8) -> u32 {
    u32::from_le_bytes(*(p as *const [u8; 4]))
}

/// Ultra-fast wyhash for byte slices (4-5 CPU cycles for 8-16 byte IDs, full 64-bit avalanche).
#[inline(always)]
pub fn hash_bytes(bytes: &[u8]) -> u64 {
    let len = bytes.len();
    let ptr = bytes.as_ptr();
    unsafe {
        if len >= 4 && len <= 16 {
            let (a, b) = if len >= 8 {
                (
                    read_u64_unaligned(ptr),
                    read_u64_unaligned(ptr.add(len - 8)),
                )
            } else {
                let lo = read_u32_unaligned(ptr) as u64;
                let hi = read_u32_unaligned(ptr.add(len - 4)) as u64;
                (lo | (hi << 32), lo ^ (hi << 32))
            };
            return wymix(a ^ WY_S1, b ^ WY_S2) ^ wymix(len as u64 ^ WY_S0, WY_S1);
        }
        if len < 4 {
            let v = if len == 0 {
                0u64
            } else {
                ((*ptr as u64) << 16)
                    | ((*ptr.add(len >> 1) as u64) << 8)
                    | (*ptr.add(len - 1) as u64)
            };
            return wymix(v ^ WY_S1, (len as u64) ^ WY_S0);
        }
        // len > 16
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
    (h >> 57) as usize
}

#[inline(always)]
pub fn person_part_from_hash(h: u64) -> usize {
    (h >> 57) as usize
}

#[inline(always)]
pub fn person_part_from_comp(comp_id: u32) -> usize {
    (comp_id.wrapping_mul(0x9E37_79B1) >> 25) as usize
}

#[inline(always)]
pub fn mix_u32(v: u32) -> u64 {
    let mut x = (v as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

#[inline(always)]
pub fn write_varint(buf: &mut Vec<u8>, mut v: usize) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
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

/// Concurrent bitset Bloom filter (single-write-phase, then read-only) to skip clicks
/// for people with zero surviving events. Zero false negatives guaranteed.
pub struct ActiveBloom {
    words: Vec<AtomicU64>,
    bit_mask: u64,
}

impl ActiveBloom {
    pub fn new_for_bytes(event_bytes: u64) -> Self {
        let num_words: usize = if event_bytes < 1 << 20 {
            1 << 10
        } else if event_bytes < 1 << 28 {
            1 << 17
        } else {
            1 << 21
        };
        let mut words = Vec::with_capacity(num_words);
        for _ in 0..num_words {
            words.push(AtomicU64::new(0));
        }
        Self {
            words,
            bit_mask: ((num_words as u64) << 6) - 1,
        }
    }

    #[inline(always)]
    pub fn insert(&self, h: u64) {
        let b1 = h & self.bit_mask;
        let b2 = (h >> 27) & self.bit_mask;
        let w1 = (b1 >> 6) as usize;
        let m1 = 1u64 << (b1 & 63);
        let w2 = (b2 >> 6) as usize;
        let m2 = 1u64 << (b2 & 63);
        if (self.words[w1].load(Ordering::Relaxed) & m1) == 0 {
            self.words[w1].fetch_or(m1, Ordering::Relaxed);
        }
        if (self.words[w2].load(Ordering::Relaxed) & m2) == 0 {
            self.words[w2].fetch_or(m2, Ordering::Relaxed);
        }
    }

    #[inline(always)]
    pub fn may_contain(&self, h: u64) -> bool {
        let b1 = h & self.bit_mask;
        let w1 = (b1 >> 6) as usize;
        let m1 = 1u64 << (b1 & 63);
        if (self.words[w1].load(Ordering::Relaxed) & m1) == 0 {
            return false;
        }
        let b2 = (h >> 27) & self.bit_mask;
        let w2 = (b2 >> 6) as usize;
        let m2 = 1u64 << (b2 & 63);
        (self.words[w2].load(Ordering::Relaxed) & m2) != 0
    }
}
