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

/// Zero-copy read-only memory map of a CSV file.
pub struct MappedFile {
    ptr: *mut libc::c_void,
    len: usize,
}

unsafe impl Send for MappedFile {}
unsafe impl Sync for MappedFile {}

impl MappedFile {
    pub fn open(path: &Path) -> io::Result<Option<Self>> {
        let file = File::open(path)?;
        let len = file.metadata()?.len() as usize;
        if len == 0 {
            return Ok(None);
        }
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                file.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        unsafe {
            libc::madvise(ptr, len, libc::MADV_SEQUENTIAL);
        }
        Ok(Some(Self { ptr, len }))
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
    let mut mapped = Vec::with_capacity(files.len());
    let mut chunks = Vec::new();
    let mut total_bytes = 0u64;

    for path in files {
        let Some(mf) = MappedFile::open(path)? else {
            continue;
        };
        let bytes = mf.as_slice();
        let Some(nl) = memchr(b'\n', bytes) else {
            continue;
        };
        let data_start = nl + 1;
        if data_start >= bytes.len() {
            continue;
        }
        let file_idx = mapped.len();
        let file_len = bytes.len();
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
