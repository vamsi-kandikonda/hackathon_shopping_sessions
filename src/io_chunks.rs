use std::ffi::OsStr;
use std::fs::{self, File};
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

use memchr::memchr;

pub const DEFAULT_CHUNK_SIZE: usize = 4 * 1024 * 1024;

pub fn list_csv_files(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if !dir.exists() {
        return Ok(files);
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension() == Some(OsStr::new("csv")) && path.is_file() {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

/// Zero-copy read-only memory map of a CSV file. The file handle stays open
/// for `pread`-based readers.
pub struct MappedFile {
    ptr: *mut libc::c_void,
    len: usize,
    file: File,
}

unsafe impl Send for MappedFile {}
unsafe impl Sync for MappedFile {}

impl MappedFile {
    fn open_inner(path: &Path, do_mmap: bool) -> io::Result<Option<Self>> {
        let file = File::open(path)?;
        let len = file.metadata()?.len() as usize;
        if len == 0 {
            return Ok(None);
        }
        let ptr = if do_mmap {
            let p = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    len,
                    libc::PROT_READ,
                    libc::MAP_PRIVATE,
                    file.as_raw_fd(),
                    0,
                )
            };
            if p == libc::MAP_FAILED {
                return Err(io::Error::last_os_error());
            }
            unsafe {
                libc::madvise(p, len, libc::MADV_SEQUENTIAL);
            }
            p
        } else {
            std::ptr::null_mut()
        };
        #[cfg(target_os = "linux")]
        unsafe {
            libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_SEQUENTIAL);
        }
        Ok(Some(Self { ptr, len, file }))
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr as *const u8, self.len) }
    }

    #[inline(always)]
    pub fn advise_dontneed(&self, start: usize, end: usize) {
        #[cfg(target_os = "linux")]
        {
            if self.len > DEFAULT_CHUNK_SIZE {
                let page = 4096usize;
                let a_start = (start + page - 1) & !(page - 1);
                let a_end = end & !(page - 1);
                if a_end > a_start {
                    unsafe {
                        libc::madvise(
                            (self.ptr as *mut u8).add(a_start) as *mut libc::c_void,
                            a_end - a_start,
                            libc::MADV_DONTNEED,
                        );
                    }
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (start, end);
        }
    }
}

impl Drop for MappedFile {
    fn drop(&mut self) {
        if !self.ptr.is_null() && self.len > 0 {
            #[cfg(target_os = "linux")]
            unsafe {
                libc::madvise(self.ptr, self.len, libc::MADV_DONTNEED);
            }
            unsafe {
                libc::munmap(self.ptr, self.len);
            }
        }
    }
}

#[derive(Clone, Copy)]
pub struct FileChunk {
    pub file_idx: usize,
    pub data_start: usize,
    pub start: usize,
    pub end: usize,
}

pub fn plan_chunks(
    files: &[PathBuf],
    chunk_size: usize,
) -> io::Result<(Vec<MappedFile>, Vec<FileChunk>, u64)> {
    plan_chunks_inner(files, chunk_size, true)
}

pub fn plan_chunks_for_read(
    files: &[PathBuf],
    chunk_size: usize,
) -> io::Result<(Vec<MappedFile>, Vec<FileChunk>, u64)> {
    plan_chunks_inner(files, chunk_size, !use_pread())
}

fn plan_chunks_inner(
    files: &[PathBuf],
    chunk_size: usize,
    do_mmap: bool,
) -> io::Result<(Vec<MappedFile>, Vec<FileChunk>, u64)> {
    use std::os::unix::fs::FileExt;
    let mut mapped = Vec::with_capacity(files.len());
    let mut chunks = Vec::new();
    let mut total_bytes = 0u64;
    let mut hdr_buf = [0u8; 4096];

    for path in files {
        let Some(mf) = MappedFile::open_inner(path, do_mmap)? else {
            continue;
        };
        let file_len = mf.len;
        let nl = if !mf.ptr.is_null() {
            memchr(b'\n', mf.as_slice())
        } else {
            let n = mf.file.read_at(&mut hdr_buf, 0)?;
            memchr(b'\n', &hdr_buf[..n])
        };
        let Some(nl) = nl else {
            continue;
        };
        let data_start = nl + 1;
        if data_start >= file_len {
            continue;
        }
        let file_idx = mapped.len();
        total_bytes += (file_len - data_start) as u64;
        mapped.push(mf);

        let mut pos = data_start;
        while pos < file_len {
            let end = (pos + chunk_size).min(file_len);
            chunks.push(FileChunk {
                file_idx,
                data_start,
                start: pos,
                end,
            });
            pos = end;
        }
    }

    Ok((mapped, chunks, total_bytes))
}

/// Zero-copy line iterator over every line whose first byte lies in `[chunk.start, chunk.end)`.
/// Strips trailing `\n` only (never `\r`).
#[cfg(any(test, not(any(target_arch = "aarch64", all(target_arch = "x86_64", target_feature = "avx2")))))]
#[inline]
pub fn for_each_line_in_chunk<'a>(
    file: &'a MappedFile,
    chunk: &FileChunk,
    mut f: impl FnMut(&'a [u8]),
) {
    let data = file.as_slice();
    let file_len = data.len();

    let mut pos = if chunk.start > chunk.data_start {
        let Some(rel_nl) = memchr(b'\n', &data[chunk.start - 1..chunk.end]) else {
            return;
        };
        let next = chunk.start - 1 + rel_nl + 1;
        if next >= chunk.end {
            return;
        }
        next
    } else {
        chunk.start
    };

    let limit = chunk.end;

    // Linux 5.14+: fault in this chunk's page-cache pages in one call instead of
    // one fault per 64 KB. Older kernels return EINVAL and we fall back to faults.
    #[cfg(target_os = "linux")]
    {
        const MADV_POPULATE_READ: libc::c_int = 22;
        let page = 4096usize;
        let a_start = pos & !(page - 1);
        let a_end = (limit + 512).min(file_len);
        if a_end > a_start {
            unsafe {
                libc::madvise(
                    (data.as_ptr() as *mut u8).add(a_start) as *mut libc::c_void,
                    a_end - a_start,
                    MADV_POPULATE_READ,
                );
            }
        }
    }

    while pos < limit {
        if let Some(rel_nl) = memchr(b'\n', &data[pos..]) {
            let end = pos + rel_nl;
            f(unsafe { data.get_unchecked(pos..end) });
            pos = end + 1;
        } else {
            f(unsafe { data.get_unchecked(pos..file_len) });
            pos = file_len;
            break;
        }
    }

    file.advise_dontneed(chunk.start, pos);
}

/// Like `for_each_line_in_chunk`, but splits each line into exactly `N`
/// fields and calls `f` only for well-formed lines. Lines up to 128 bytes are
/// found and split in a single SIMD pass; longer lines and the last few lines
/// of a file take the scalar path.
#[inline]
pub fn for_each_record<'a, const N: usize>(
    file: &'a MappedFile,
    chunk: &FileChunk,
    mut f: impl FnMut([&'a [u8]; N]),
) {
    #[cfg(any(target_arch = "aarch64", all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        let data = file.as_slice();
        let file_len = data.len();
        let mut pos = if chunk.start > chunk.data_start {
            let Some(rel_nl) = memchr(b'\n', &data[chunk.start - 1..chunk.end]) else {
                return;
            };
            let next = chunk.start - 1 + rel_nl + 1;
            if next >= chunk.end {
                return;
            }
            next
        } else {
            chunk.start
        };
        let limit = chunk.end;

        #[cfg(target_os = "linux")]
        {
            const MADV_POPULATE_READ: libc::c_int = 22;
            let a_start = pos & !4095;
            let a_end = (limit + 512).min(file_len);
            if a_end > a_start {
                unsafe {
                    libc::madvise(
                        (data.as_ptr() as *mut u8).add(a_start) as *mut libc::c_void,
                        a_end - a_start,
                        MADV_POPULATE_READ,
                    );
                }
            }
        }

        let fast_end = file_len.saturating_sub(128);
        while pos < limit {
            if pos <= fast_end {
                if let Some((len, cm, qm)) = unsafe { crate::csv_parse::scan_line_128(data.as_ptr().add(pos)) } {
                    let line = unsafe { data.get_unchecked(pos..pos + len) };
                    if let Some(fields) = crate::csv_parse::extract_from_masks::<N>(line, cm, qm) {
                        f(fields);
                    }
                    pos += len + 1;
                    continue;
                }
            }
            // Long line or near the end of the file.
            let end = memchr(b'\n', &data[pos..]).map_or(file_len, |r| pos + r);
            if let Some(fields) = crate::csv_parse::split_fields::<N>(unsafe { data.get_unchecked(pos..end) }) {
                f(fields);
            }
            pos = end + 1;
        }
        file.advise_dontneed(chunk.start, pos.min(file_len));
    }
    #[cfg(not(any(target_arch = "aarch64", all(target_arch = "x86_64", target_feature = "avx2"))))]
    for_each_line_in_chunk(file, chunk, |line| {
        if let Some(fields) = crate::csv_parse::split_fields::<N>(line) {
            f(fields);
        }
    });
}

/// Read `[base + buf.len(), upto)` of the file onto the end of `buf`.
fn fill_to(file: &File, buf: &mut Vec<u8>, base: usize, upto: usize) {
    use std::os::unix::fs::FileExt;
    let old = buf.len();
    let new = upto - base;
    if new <= old {
        return;
    }
    buf.reserve(new - old + READ_PAD);
    unsafe {
        let spare = std::slice::from_raw_parts_mut(buf.as_mut_ptr().add(old), new - old);
        let mut done = 0usize;
        while done < spare.len() {
            match file.read_at(&mut spare[done..], (base + old + done) as u64) {
                Ok(0) => break,
                Ok(n) => done += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => panic!("read failed: {e}"),
            }
        }
        buf.set_len(old + done);
    }
}

const READ_PAD: usize = 128;
const READ_EXTRA: usize = 64 * 1024;

/// `pread`-based version of `for_each_record`: reads the chunk (plus the tail
/// of its last line) into `buf`, so input pages are never faulted into our
/// address space. Fields borrow from `buf` and are valid until the next call.
pub fn for_each_record_read<'b, const N: usize>(
    file: &MappedFile,
    chunk: &FileChunk,
    buf: &'b mut Vec<u8>,
    mut f: impl FnMut([&'b [u8]; N]),
) {
    let file_len = file.len;
    let base = if chunk.start > chunk.data_start { chunk.start - 1 } else { chunk.start };
    buf.clear();
    let mut want = (chunk.end + READ_EXTRA).min(file_len);
    loop {
        fill_to(&file.file, buf, base, want);
        // The last line starting before `chunk.end` must end inside `buf`.
        let tail_from = chunk.end - 1 - base;
        if want >= file_len || memchr(b'\n', &buf[tail_from.min(buf.len())..]).is_some() {
            break;
        }
        want = (want + READ_EXTRA).min(file_len);
    }
    // Zero padding so the 128-byte SIMD scan never reads uninitialised memory.
    unsafe {
        buf.reserve(READ_PAD);
        std::ptr::write_bytes(buf.as_mut_ptr().add(buf.len()), 0, READ_PAD);
    }
    let buf: &'b [u8] = buf;
    let len = buf.len();
    let limit = chunk.end - base;
    let mut pos = if chunk.start > chunk.data_start {
        let Some(nl) = memchr(b'\n', &buf[..limit.min(len)]) else {
            return;
        };
        if nl + 1 >= limit {
            return;
        }
        nl + 1
    } else {
        0
    };

    while pos < limit && pos < len {
        #[cfg(any(target_arch = "aarch64", all(target_arch = "x86_64", target_feature = "avx2")))]
        {
            if let Some((l, cm, qm)) = unsafe { crate::csv_parse::scan_line_128(buf.as_ptr().add(pos)) } {
                if pos + l <= len {
                    let line = &buf[pos..pos + l];
                    if let Some(fields) = crate::csv_parse::extract_from_masks::<N>(line, cm, qm) {
                        f(fields);
                    }
                    pos += l + 1;
                    continue;
                }
            }
        }
        let end = memchr(b'\n', &buf[pos..]).map_or(len, |r| pos + r);
        if let Some(fields) = crate::csv_parse::split_fields::<N>(&buf[pos..end]) {
            f(fields);
        }
        pos = end + 1;
    }
}

/// Use `pread` instead of `mmap` for event and click input (`SS_IO=mmap` to switch back).
pub fn use_pread() -> bool {
    static FLAG: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *FLAG.get_or_init(|| std::env::var("SS_IO").map_or(true, |v| v != "mmap"))
}

const STREAM_BLOCK: usize = 256 * 1024;

/// Streaming `pread` reader: walks a chunk in 256 KB reads so the bytes being
/// parsed are still in L2 cache, carrying a partial line over to the next
/// read. Calls `f` for every well-formed line that starts in the chunk.
/// Fields borrow from `buf` and are valid only during the call.
pub fn for_each_record_stream<const N: usize>(
    file: &MappedFile,
    chunk: &FileChunk,
    buf: &mut Vec<u8>,
    mut f: impl FnMut([&[u8]; N]),
) {
    use std::os::unix::fs::FileExt;
    let file_len = file.len;
    let skip_first = chunk.start > chunk.data_start;
    let mut buf_base = if skip_first { chunk.start - 1 } else { chunk.start };
    let mut file_pos = buf_base;
    let mut carry = 0usize;
    let mut need_skip = skip_first;
    loop {
        // Refill after the carried bytes.
        let want = STREAM_BLOCK.max(carry * 2);
        if buf.capacity() < carry + want + READ_PAD {
            buf.reserve(carry + want + READ_PAD - buf.len());
        }
        let mut got = 0usize;
        unsafe {
            let dst = std::slice::from_raw_parts_mut(buf.as_mut_ptr().add(carry), want);
            while got < want && file_pos + got < file_len {
                match file.file.read_at(&mut dst[got..], (file_pos + got) as u64) {
                    Ok(0) => break,
                    Ok(n) => got += n,
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(e) => panic!("read failed: {e}"),
                }
            }
            buf.set_len(carry + got);
            std::ptr::write_bytes(buf.as_mut_ptr().add(carry + got), 0, READ_PAD);
        }
        file_pos += got;
        let eof = file_pos >= file_len;
        let valid = buf.len();
        let data: &[u8] = &buf[..];
        let mut pos = 0usize;

        if need_skip {
            match memchr(b'\n', data) {
                Some(nl) => {
                    need_skip = false;
                    pos = nl + 1;
                }
                None if eof => return,
                None => {
                    // The skipped line continues; drop what we have and read on.
                    buf_base += valid;
                    carry = 0;
                    buf.clear();
                    continue;
                }
            }
        }

        loop {
            if pos >= valid || buf_base + pos >= chunk.end {
                break;
            }
            #[cfg(any(target_arch = "aarch64", all(target_arch = "x86_64", target_feature = "avx2")))]
            {
                if let Some((l, cm, qm)) = unsafe { crate::csv_parse::scan_line_128(data.as_ptr().add(pos)) } {
                    let line = &data[pos..pos + l];
                    if let Some(fields) = crate::csv_parse::extract_from_masks::<N>(line, cm, qm) {
                        f(fields);
                    }
                    pos += l + 1;
                    continue;
                }
            }
            match memchr(b'\n', &data[pos..valid]) {
                Some(r) => {
                    if let Some(fields) = crate::csv_parse::split_fields::<N>(&data[pos..pos + r]) {
                        f(fields);
                    }
                    pos += r + 1;
                }
                None if eof => {
                    if let Some(fields) = crate::csv_parse::split_fields::<N>(&data[pos..valid]) {
                        f(fields);
                    }
                    pos = valid;
                }
                None => break,
            }
        }

        if eof || buf_base + pos >= chunk.end {
            return;
        }
        // Carry the partial line to the front and read more.
        carry = valid - pos;
        buf.copy_within(pos..valid, 0);
        buf.truncate(carry);
        buf_base += pos;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect_lines<const N: usize>(bytes: &[u8], chunk_size: usize) -> (Vec<Vec<Vec<u8>>>, Vec<Vec<Vec<u8>>>) {
        let dir = std::env::temp_dir().join(format!("ss_io_test_{}_{}", std::process::id(), chunk_size));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("t.csv");
        std::fs::write(&path, bytes).unwrap();
        let (mapped, chunks, _) = plan_chunks(&[path.clone()], chunk_size).unwrap();
        let mut fast = Vec::new();
        let mut slow = Vec::new();
        let mut viaread = Vec::new();
        let mut viastream = Vec::new();
        let mut rbuf = Vec::new();
        for c in &chunks {
            for_each_record::<N>(&mapped[c.file_idx], c, |f| fast.push(f.iter().map(|x| x.to_vec()).collect()));
            for_each_record_stream::<N>(&mapped[c.file_idx], c, &mut rbuf, |f| {
                viastream.push(f.iter().map(|x| x.to_vec()).collect::<Vec<_>>())
            });
            for_each_record_read::<N>(&mapped[c.file_idx], c, &mut rbuf, |f| {
                viaread.push(f.iter().map(|x| x.to_vec()).collect::<Vec<_>>())
            });
            for_each_line_in_chunk(&mapped[c.file_idx], c, |line| {
                if let Some(f) = crate::csv_parse::split_fields::<N>(line) {
                    slow.push(f.iter().map(|x| x.to_vec()).collect());
                }
            });
        }
        std::fs::remove_dir_all(&dir).ok();
        assert_eq!(viaread, slow, "pread path, chunk {chunk_size}");
        assert_eq!(viastream, slow, "stream path, chunk {chunk_size}");
        (fast, slow)
    }

    #[test]
    fn fused_scanner_matches_line_splitter() {
        let mut rng = 0x1234_5678_9abc_def0u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut body = b"a,b,c,d\n".to_vec();
        for _ in 0..20_000 {
            let len = (next() % 200) as usize;
            for _ in 0..len {
                let r = next() % 20;
                body.push(match r {
                    0..=2 => b',',
                    3 => b'"',
                    4 => b'\r',
                    5 => b' ',
                    _ => b'a' + (r as u8),
                });
            }
            body.push(b'\n');
        }
        // One line far longer than a stream block, then more normal lines.
        body.extend_from_slice(b"big,");
        body.extend(std::iter::repeat(b'q').take(700_000));
        body.extend_from_slice(b",x,y\n");
        for i in 0..3000u32 {
            body.extend_from_slice(format!("a{i},b,c,d\n").as_bytes());
        }
        body.extend_from_slice(b"x,y,z,\"w\"");
        for cs in [64usize, 1000, 4096, 1 << 20] {
            let (fast, slow) = collect_lines::<4>(&body, cs);
            assert_eq!(fast.len(), slow.len(), "chunk {cs}");
            assert_eq!(fast, slow, "chunk {cs}");
            let (fast, slow) = collect_lines::<2>(&body, cs);
            assert_eq!(fast, slow, "chunk {cs}");
        }
    }
}
