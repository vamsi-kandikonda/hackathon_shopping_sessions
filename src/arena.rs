//! Bump arena for the parse-phase record buffers. Regions are mapped
//! directly, marked for transparent huge pages and pre-faulted in one call, so
//! filling them costs no per-page faults. Everything is freed when the arena drops.

use std::ptr;

const REGION: usize = 64 << 20;

pub struct Arena {
    regions: Vec<(*mut u8, usize)>,
    cur: *mut u8,
    left: usize,
    /// Spans from other arenas that are dead and may be overwritten. Their
    /// owning arenas must outlive this one's users.
    free: Vec<(*mut u8, usize)>,
}

unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}

impl Default for Arena {
    fn default() -> Self {
        Self {
            regions: Vec::new(),
            cur: ptr::null_mut(),
            left: 0,
            free: Vec::new(),
        }
    }
}

fn map_region(len: usize) -> *mut u8 {
    unsafe {
        let p = libc::mmap(
            ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        );
        if p == libc::MAP_FAILED {
            panic!("arena mmap of {len} bytes failed: {}", std::io::Error::last_os_error());
        }
        #[cfg(target_os = "linux")]
        {
            libc::madvise(p, len, libc::MADV_HUGEPAGE);
        }
        p as *mut u8
    }
}

/// A dead span of arena memory that another arena may reuse.
pub struct Span(*mut u8, usize);
unsafe impl Send for Span {}

impl Arena {
    /// Remove and return this arena's unused recycled spans.
    pub fn take_spare(&mut self) -> Vec<Span> {
        self.free.drain(..).map(|(p, l)| Span(p, l)).collect()
    }

    /// Give this arena spans to reuse. Their owners must outlive every chunk
    /// carved from them.
    pub fn add_spare(&mut self, spans: Vec<Span>) {
        self.free.extend(spans.into_iter().map(|Span(p, l)| (p, l)));
    }

    /// Hand a dead chunk's memory to this arena for reuse. The caller
    /// guarantees nothing reads the chunk again and its owner stays alive.
    #[allow(dead_code)]
    pub fn recycle(&mut self, c: RawChunk) {
        if c.len >= 1024 {
            self.free.push((c.ptr as *mut u8, c.len));
        }
    }

    /// A block of at least `need` bytes, preferably recycled; returns its capacity.
    fn block(&mut self, need: usize, want: usize) -> (*mut u8, usize) {
        while let Some(&(p, len)) = self.free.last() {
            self.free.pop();
            if len >= need {
                return (p, len);
            }
        }
        let cap = want.max(need);
        (self.alloc(cap), cap)
    }

    /// Allocate `size` bytes (8-byte aligned), valid until the arena drops.
    pub fn alloc(&mut self, size: usize) -> *mut u8 {
        let size = (size + 7) & !7;
        if size > self.left {
            let len = REGION.max((size + 4095) & !4095);
            let p = map_region(len);
            self.regions.push((p, len));
            self.cur = p;
            self.left = len;
        }
        let p = self.cur;
        unsafe {
            self.cur = self.cur.add(size);
        }
        self.left -= size;
        p
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        for &(p, len) in &self.regions {
            unsafe {
                libc::munmap(p as *mut libc::c_void, len);
            }
        }
    }
}

/// A filled buffer that lives in an `Arena`.
#[derive(Clone, Copy)]
pub struct RawChunk {
    ptr: *const u8,
    len: usize,
}

unsafe impl Send for RawChunk {}
unsafe impl Sync for RawChunk {}

impl RawChunk {
    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

/// Append-only buffer of fixed capacity inside an arena.
pub struct FixedBuf {
    ptr: *mut u8,
    len: usize,
    cap: usize,
}

impl FixedBuf {
    pub fn empty() -> Self {
        Self {
            ptr: ptr::null_mut(),
            len: 0,
            cap: 0,
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Ensure `need` free bytes, sealing the current buffer into a chunk if
    /// it is too full. Returns the sealed chunk, if any.
    #[inline(always)]
    pub fn reserve(&mut self, need: usize, chunk_size: usize, arena: &mut Arena) -> Option<RawChunk> {
        if self.cap - self.len >= need {
            return None;
        }
        let sealed = (self.len > 0).then(|| self.take());
        let (p, cap) = arena.block(need, chunk_size);
        self.ptr = p;
        self.len = 0;
        self.cap = cap;
        sealed
    }

    /// Pointer to the free space; caller must have reserved enough.
    #[inline(always)]
    pub fn tail(&mut self) -> *mut u8 {
        unsafe { self.ptr.add(self.len) }
    }

    #[inline(always)]
    pub fn advance(&mut self, n: usize) {
        debug_assert!(self.len + n <= self.cap);
        self.len += n;
    }

    /// Seal the filled part as a chunk and keep the rest for later appends.
    pub fn take(&mut self) -> RawChunk {
        let c = RawChunk {
            ptr: self.ptr,
            len: self.len,
        };
        unsafe {
            self.ptr = self.ptr.add(self.len);
        }
        self.cap -= self.len;
        self.len = 0;
        c
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arena_chunks_roundtrip() {
        let mut arena = Arena::default();
        let mut buf = FixedBuf::empty();
        let mut chunks = Vec::new();
        let mut expect = Vec::new();
        for i in 0..100_000u32 {
            let rec = i.to_le_bytes();
            let need = if i % 9999 == 0 { 70_000 } else { 4 };
            if let Some(c) = buf.reserve(need, 1024, &mut arena) {
                chunks.push(c);
            }
            unsafe {
                std::ptr::copy_nonoverlapping(rec.as_ptr(), buf.tail(), 4);
            }
            buf.advance(4);
            expect.extend_from_slice(&rec);
        }
        chunks.push(buf.take());
        let got: Vec<u8> = chunks.iter().flat_map(|c| c.as_slice().to_vec()).collect();
        assert_eq!(got, expect);
    }
}
