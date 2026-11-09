use super::arguments;
use crate::contract::component::ProcessRequest;
use std::{
    cmp::Ordering,
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    io,
    os::windows::ffi::OsStringExt,
    path::{Path, PathBuf},
};
use windows_sys::Win32::{
    Globalization::{CompareStringOrdinal, CSTR_EQUAL, CSTR_LESS_THAN},
    Storage::FileSystem::{GetFileAttributesW, GetFullPathNameW, INVALID_FILE_ATTRIBUTES},
    System::SystemInformation::{GetSystemDirectoryW, GetWindowsDirectoryW},
};

#[derive(Eq)]
struct Key(Vec<u16>);
impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        match unsafe {
            CompareStringOrdinal(
                self.0.as_ptr(),
                self.0.len() as i32,
                other.0.as_ptr(),
                other.0.len() as i32,
                1,
            )
        } {
            CSTR_LESS_THAN => Ordering::Less,
            CSTR_EQUAL => Ordering::Equal,
            _ => Ordering::Greater,
        }
    }
}
pub(super) struct Launch {
    pub(super) application: Vec<u16>,
    pub(super) command: Vec<u16>,
    pub(super) environment: Vec<u16>,
}
impl Launch {
    pub(super) fn prepare(request: &ProcessRequest) -> io::Result<Self> {
        let mut environment = BTreeMap::new();
        for (name, value) in std::env::vars_os() {
            environment.insert(key(&name)?, arguments::wide(&value)?);
        }
        let path_key = key(OsStr::new("PATH"))?;
        let mut child_path = None;
        for (name, value) in &request.environment {
            let name = key(name)?;
            if name.0.is_empty() || name.0.contains(&61) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid process environment name",
                ));
            }
            if name == path_key {
                child_path = value.clone();
            }
            match value {
                Some(value) => {
                    environment.insert(name, arguments::wide(value)?);
                }
                None => {
                    environment.remove(&name);
                }
            }
        }
        let mut application = resolve(&request.program, child_path.as_deref())?;
        application = user_path(application)?;
        // Win32 normalizes non-verbatim trailing dots/spaces. Detect the
        // executable that CreateProcess would actually select, as Command does.
        let normalized = if application.starts_with(&[92, 92, 63, 92]) {
            application[..application.len() - 1].to_vec()
        } else {
            full_path(&application)?
        };
        let is_batch = matches!(
            normalized
                .len()
                .checked_sub(4)
                .map(|start| &normalized[start..]),
            Some([46, 98 | 66, 97 | 65, 116 | 84] | [46, 99 | 67, 109 | 77, 100 | 68])
        );
        let command = if is_batch {
            let command = arguments::batch(&application, &request.arguments)?;
            let mut system = system_directory(true)?;
            system.push("cmd.exe");
            application = terminated(system.as_os_str())?;
            command
        } else {
            arguments::executable(request.program.as_os_str(), &request.arguments)?
        };
        let mut block = Vec::new();
        for (name, value) in environment {
            block.extend(name.0);
            block.push(61);
            block.extend(value);
            block.push(0);
        }
        if block.is_empty() {
            block.push(0);
        }
        block.push(0);
        Ok(Self {
            application,
            command,
            environment: block,
        })
    }
}
fn key(value: &OsStr) -> io::Result<Key> {
    let wide = arguments::wide(value)?;
    if wide.len() > i32::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "process environment key too long",
        ));
    }
    Ok(Key(wide))
}
pub(super) fn terminated(value: &OsStr) -> io::Result<Vec<u16>> {
    let mut wide = arguments::wide(value)?;
    wide.push(0);
    Ok(wide)
}
fn exists(path: &Path) -> Option<Vec<u16>> {
    let wide = terminated(path.as_os_str()).ok()?;
    (unsafe { GetFileAttributesW(wide.as_ptr()) } != INVALID_FILE_ATTRIBUTES).then_some(wide)
}
fn resolve(program: &Path, child_path: Option<&OsStr>) -> io::Result<Vec<u16>> {
    let bytes = arguments::wide(program.as_os_str())?;
    if bytes.is_empty() || bytes.last().is_some_and(|unit| matches!(*unit, 47 | 92)) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "program path has no file name",
        ));
    }
    let exe_suffix = bytes.len().checked_sub(4).is_some_and(|start| {
        bytes[start..].iter().all(|unit| *unit < 128)
            && bytes[start..]
                .iter()
                .map(|unit| *unit as u8)
                .collect::<Vec<_>>()
                .eq_ignore_ascii_case(b".exe")
    });
    if bytes.iter().any(|unit| matches!(*unit, 47 | 92 | 58)) {
        if exe_suffix {
            return terminated(program.as_os_str());
        }
        let mut extended = program.as_os_str().to_owned();
        extended.push(".exe");
        return Ok(exists(Path::new(&extended)).unwrap_or(terminated(program.as_os_str())?));
    }
    let mut directories = Vec::new();
    if let Some(path) = child_path {
        directories.extend(std::env::split_paths(path).filter(|path| !path.as_os_str().is_empty()));
    }
    if let Ok(mut executable) = std::env::current_exe() {
        executable.pop();
        directories.push(executable);
    }
    directories.push(system_directory(true)?);
    directories.push(system_directory(false)?);
    if let Some(paths) = std::env::var_os("PATH") {
        directories
            .extend(std::env::split_paths(&paths).filter(|path| !path.as_os_str().is_empty()));
    }
    for mut directory in directories {
        directory.push(program);
        if !bytes.contains(&46) {
            directory.set_extension("exe");
        }
        if let Some(program) = exists(&directory) {
            return Ok(program);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "process program not found",
    ))
}
fn system_directory(system: bool) -> io::Result<PathBuf> {
    let mut buffer = vec![0u16; 260];
    loop {
        let size = unsafe {
            if system {
                GetSystemDirectoryW(buffer.as_mut_ptr(), buffer.len() as u32)
            } else {
                GetWindowsDirectoryW(buffer.as_mut_ptr(), buffer.len() as u32)
            }
        };
        if size == 0 {
            return Err(io::Error::last_os_error());
        }
        if (size as usize) < buffer.len() {
            buffer.truncate(size as usize);
            return Ok(PathBuf::from(OsString::from_wide(&buffer)));
        }
        buffer.resize(size as usize + 1, 0);
    }
}

fn full_path(path: &[u16]) -> io::Result<Vec<u16>> {
    let mut buffer = vec![0; 260];
    loop {
        let size = unsafe {
            GetFullPathNameW(
                path.as_ptr(),
                buffer.len() as u32,
                buffer.as_mut_ptr(),
                std::ptr::null_mut(),
            )
        };
        if size == 0 {
            return Err(io::Error::last_os_error());
        }
        if (size as usize) < buffer.len() {
            buffer.truncate(size as usize);
            return Ok(buffer);
        }
        buffer.resize(size as usize + 1, 0);
    }
}
fn user_path(path: Vec<u16>) -> io::Result<Vec<u16>> {
    // Remove a short verbatim prefix only when normalization preserves every
    // code unit. cmd.exe cannot execute an ordinary short verbatim batch path.
    if path.len() > 260 {
        return Ok(path);
    }
    let candidate = match path.as_slice() {
        [92, 92, 63, 92, _, 58, 92, ..] => Some(path[4..].to_vec()),
        [92, 92, 63, 92, 85, 78, 67, 92, ..] => {
            let mut candidate = vec![92, 92];
            candidate.extend_from_slice(&path[8..]);
            Some(candidate)
        }
        _ => None,
    };
    if let Some(candidate) = candidate {
        if full_path(&candidate)? == candidate[..candidate.len() - 1] {
            return Ok(candidate);
        }
    }
    Ok(path)
}
