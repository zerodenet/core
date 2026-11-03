use super::*;
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use windows_sys::Win32::{
    Foundation::{ERROR_NO_MORE_FILES, HANDLE, INVALID_HANDLE_VALUE},
    Storage::FileSystem::{
        FindClose, FindFirstFileW, FindNextFileW, FILE_ATTRIBUTE_DIRECTORY,
        FILE_ATTRIBUTE_REPARSE_POINT, WIN32_FIND_DATAW,
    },
};
pub(in crate::file::tree) struct Directory {
    handle: HANDLE,
    first: Option<WIN32_FIND_DATAW>,
}
unsafe impl Send for Directory {}
impl Directory {
    pub(in crate::file::tree) fn new() -> Self {
        Self {
            handle: INVALID_HANDLE_VALUE,
            first: None,
        }
    }
    pub(in crate::file::tree) fn acquire(&mut self, path: &Path) -> io::Result<()> {
        let path: Vec<_> = path
            .join("*")
            .as_os_str()
            .encode_wide()
            .chain(Some(0))
            .collect();
        let mut data = unsafe { std::mem::zeroed() };
        self.handle = unsafe { FindFirstFileW(path.as_ptr(), &mut data) };
        if self.handle == INVALID_HANDLE_VALUE {
            Err(io::Error::last_os_error())
        } else {
            self.first = Some(data);
            Ok(())
        }
    }
    pub(in crate::file::tree) fn entries(&mut self, _: &Path) -> io::Result<Vec<String>> {
        let mut names = Vec::new();
        loop {
            let data = if let Some(first) = self.first.take() {
                first
            } else {
                let mut data = unsafe { std::mem::zeroed() };
                if unsafe { FindNextFileW(self.handle, &mut data) } == 0 {
                    let source = io::Error::last_os_error();
                    if source.raw_os_error() == Some(ERROR_NO_MORE_FILES as i32) {
                        break;
                    }
                    return Err(source);
                }
                data
            };
            let n = data
                .cFileName
                .iter()
                .position(|v| *v == 0)
                .unwrap_or(data.cFileName.len());
            let mut name = std::ffi::OsString::from_wide(&data.cFileName[..n])
                .to_string_lossy()
                .into_owned();
            if name == "." || name == ".." {
                continue;
            }
            if data.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY != 0
                && data.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT == 0
            {
                name.push('/');
            }
            names.push(name);
            if names.len() > 65536 {
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "directory entry bound exceeded",
                ));
            }
        }
        Ok(names)
    }
    pub(in crate::file::tree) fn close(mut self) -> io::Result<bool> {
        if self.handle == INVALID_HANDLE_VALUE {
            return Ok(false);
        }
        let handle = std::mem::replace(&mut self.handle, INVALID_HANDLE_VALUE);
        if unsafe { FindClose(handle) } != 0 {
            Ok(true)
        } else {
            Err(io::Error::last_os_error())
        }
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        if self.handle != INVALID_HANDLE_VALUE {
            unsafe {
                FindClose(self.handle);
            }
        }
    }
}
