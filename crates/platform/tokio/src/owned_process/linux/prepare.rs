//! Prepare all launch data in the parent. The post-fork supervisor never enters
//! Rust allocation, environment access, logging, unwinding or a runtime.
use crate::contract::component::ProcessRequest;
use std::{
    collections::BTreeMap,
    ffi::{CString, OsString},
    io,
    os::unix::ffi::OsStrExt,
};
pub(super) struct Launch {
    arguments: Vec<CString>,
    environment: Vec<CString>,
    candidates: Vec<CString>,
    shell: Vec<Vec<*const libc::c_char>>,
    argv: Vec<*const libc::c_char>,
    envp: Vec<*const libc::c_char>,
    paths: Vec<*const libc::c_char>,
    pub(super) signals: [libc::sigaction; 65],
    pub(super) mask: libc::sigset_t,
}
#[derive(Clone, Copy)]
pub(super) struct RawLaunch {
    pub(super) argv: *const *const libc::c_char,
    pub(super) envp: *const *const libc::c_char,
    pub(super) paths: *const *const libc::c_char,
    pub(super) count: usize,
    pub(super) shells: *const Vec<*const libc::c_char>,
    pub(super) signals: *const libc::sigaction,
    pub(super) mask: libc::sigset_t,
}
fn string(bytes: &[u8]) -> io::Result<CString> {
    CString::new(bytes).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "process launch contains an interior NUL",
        )
    })
}
impl Launch {
    pub(super) fn prepare(request: &ProcessRequest) -> io::Result<Self> {
        let mut variables: BTreeMap<OsString, OsString> = std::env::vars_os().collect();
        for (name, value) in &request.environment {
            if name.as_bytes().is_empty()
                || name.as_bytes().contains(&b'=')
                || name.as_bytes().contains(&0)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid process environment name",
                ));
            }
            match value {
                Some(value) => {
                    variables.insert(name.clone(), value.clone());
                }
                None => {
                    variables.remove(name);
                }
            }
        }
        let path = variables
            .get(std::ffi::OsStr::new("PATH"))
            .map(|value| value.as_bytes())
            .unwrap_or(b"/bin:/usr/bin");
        let program = request.program.as_os_str().as_bytes();
        if program.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "empty process program",
            ));
        }
        let candidates = if program.contains(&b'/') {
            vec![string(program)?]
        } else {
            path.split(|byte| *byte == b':')
                .map(|directory| {
                    let mut candidate = Vec::new();
                    candidate
                        .try_reserve(
                            directory
                                .len()
                                .checked_add(program.len())
                                .and_then(|size| size.checked_add(1))
                                .ok_or_else(|| io::Error::other("process PATH size overflow"))?,
                        )
                        .map_err(io::Error::other)?;
                    if !directory.is_empty() {
                        candidate.extend_from_slice(directory);
                        candidate.push(b'/');
                    }
                    candidate.extend_from_slice(program);
                    string(&candidate)
                })
                .collect::<io::Result<Vec<_>>>()?
        };
        let mut arguments = Vec::new();
        arguments.push(string(program)?);
        for argument in &request.arguments {
            arguments.push(string(argument.as_bytes())?);
        }
        let mut environment = Vec::new();
        for (name, value) in variables {
            let mut entry = name.as_bytes().to_vec();
            entry.push(b'=');
            entry.extend_from_slice(value.as_bytes());
            environment.push(string(&entry)?);
        }
        let mut argv: Vec<_> = arguments.iter().map(|value| value.as_ptr()).collect();
        argv.push(std::ptr::null());
        let mut envp: Vec<_> = environment.iter().map(|value| value.as_ptr()).collect();
        envp.push(std::ptr::null());
        let paths = candidates.iter().map(|value| value.as_ptr()).collect();
        let shell = candidates
            .iter()
            .map(|candidate| {
                let mut values = vec![c"/bin/sh".as_ptr(), candidate.as_ptr()];
                values.extend(arguments.iter().skip(1).map(|value| value.as_ptr()));
                values.push(std::ptr::null());
                values
            })
            .collect();
        // SAFETY: zeroed sigaction/sigset_t are initialized exclusively by the
        // corresponding native getters before any child can inspect them.
        let mut signals = unsafe { std::mem::zeroed::<[libc::sigaction; 65]>() };
        for signal in 1..65 {
            if signal == libc::SIGKILL as usize || signal == libc::SIGSTOP as usize {
                continue;
            }
            // glibc reserves two realtime signals; EINVAL means no disposition
            // exists to restore through its sigaction interface.
            if unsafe { libc::sigaction(signal as i32, std::ptr::null(), &mut signals[signal]) } < 0
                && io::Error::last_os_error().raw_os_error() != Some(libc::EINVAL)
            {
                return Err(io::Error::last_os_error());
            }
        }
        let mut mask = unsafe { std::mem::zeroed() };
        let code = unsafe { libc::pthread_sigmask(libc::SIG_SETMASK, std::ptr::null(), &mut mask) };
        if code != 0 {
            return Err(io::Error::from_raw_os_error(code));
        }
        Ok(Self {
            arguments,
            environment,
            candidates,
            shell,
            argv,
            envp,
            paths,
            signals,
            mask,
        })
    }
    pub(super) fn raw(&self) -> RawLaunch {
        // These allocations remain in the parent's Launch until fork returns.
        // The child has its own identical address space thereafter.
        let _keep = (&self.arguments, &self.environment);
        RawLaunch {
            argv: self.argv.as_ptr(),
            envp: self.envp.as_ptr(),
            paths: self.paths.as_ptr(),
            count: self.candidates.len(),
            shells: self.shell.as_ptr(),
            signals: self.signals.as_ptr(),
            mask: self.mask,
        }
    }
}
