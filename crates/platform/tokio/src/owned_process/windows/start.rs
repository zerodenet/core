use super::*;
use windows_sys::Win32::{
    Foundation::INVALID_HANDLE_VALUE,
    Security::SECURITY_ATTRIBUTES,
    Storage::FileSystem::{
        CreateFileW, FILE_ATTRIBUTE_NORMAL, FILE_GENERIC_READ, FILE_GENERIC_WRITE, FILE_SHARE_READ,
        FILE_SHARE_WRITE, OPEN_EXISTING,
    },
    System::{
        JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
            JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
            SetInformationJobObject,
        },
        Threading::{
            CREATE_SUSPENDED, CREATE_UNICODE_ENVIRONMENT, CreateProcessW,
            DeleteProcThreadAttributeList, EXTENDED_STARTUPINFO_PRESENT,
            InitializeProcThreadAttributeList, PROC_THREAD_ATTRIBUTE_HANDLE_LIST,
            PROCESS_INFORMATION, ResumeThread, STARTF_USESTDHANDLES, STARTUPINFOEXW,
            UpdateProcThreadAttribute,
        },
    },
};
impl OwnedProcess {
    pub fn start(&self, actor: TaskJoinWitness) -> io::Result<()> {
        if !self.scope.resource_open()
            || !self.scope.same_scope(&actor.scope_identity())
            || actor.is_joined()
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "process actor is not in its original open Scope",
            ));
        }
        if let Some(endpoint) = self.endpoint() {
            endpoint.check_start(&actor, self)?;
        }
        let mut state = self.lock();
        if state.stop
            || state.attempted
            || (state.endpoint.is_some() && !state.endpoint_request_ready)
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "process start is terminal",
            ));
        }
        if let Some(actual) = &state.actor {
            if !actual.same_registration(&actor) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "process start crossed its bound original actor",
                ));
            }
        } else {
            state.actor = Some(actor);
        }
        state.attempted = true;
        let result = self.start_inner(&mut state);
        state.request = None;
        if let Err(error) = result {
            state.fault(error);
            state.stop = true;
            return Err(io::Error::other(SharedFailure(
                state
                    .first_error
                    .clone()
                    .expect("retained original failure"),
            )));
        }
        Ok(())
    }
    fn start_inner(&self, state: &mut State) -> io::Result<()> {
        let mut launch = launch::Launch::prepare(
            state
                .request
                .as_ref()
                .ok_or_else(|| io::Error::other("process request missing"))?,
        )?;
        let job = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if job.is_null() {
            return Err(io::Error::last_os_error());
        }
        state.retain(JOB, job);
        let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
        // Child processes cannot break out of this unnamed original job.
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        if unsafe {
            SetInformationJobObject(
                job,
                JobObjectExtendedLimitInformation,
                (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                std::mem::size_of_val(&limits) as u32,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        let security = SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: std::ptr::null_mut(),
            bInheritHandle: 1,
        };
        let null_name: Vec<_> = "NUL\0".encode_utf16().collect();
        let null = unsafe {
            CreateFileW(
                null_name.as_ptr(),
                FILE_GENERIC_READ | FILE_GENERIC_WRITE,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                &security,
                OPEN_EXISTING,
                FILE_ATTRIBUTE_NORMAL,
                std::ptr::null_mut(),
            )
        };
        if null == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        state.retain(NULL, null);
        let mut attributes = Attributes::new()?;
        let mut handles = [null];
        if unsafe {
            UpdateProcThreadAttribute(
                attributes.pointer(),
                0,
                PROC_THREAD_ATTRIBUTE_HANDLE_LIST as usize,
                handles.as_mut_ptr().cast(),
                std::mem::size_of_val(&handles),
                std::ptr::null_mut(),
                std::ptr::null(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        let mut startup: STARTUPINFOEXW = unsafe { std::mem::zeroed() };
        startup.StartupInfo.cb = std::mem::size_of::<STARTUPINFOEXW>() as u32;
        startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
        startup.StartupInfo.hStdInput = null;
        startup.StartupInfo.hStdOutput = null;
        startup.StartupInfo.hStdError = null;
        startup.lpAttributeList = attributes.pointer();
        let mut process: PROCESS_INFORMATION = unsafe { std::mem::zeroed() };
        if unsafe {
            CreateProcessW(
                launch.application.as_ptr(),
                launch.command.as_mut_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                1,
                CREATE_SUSPENDED | CREATE_UNICODE_ENVIRONMENT | EXTENDED_STARTUPINFO_PRESENT,
                launch.environment.as_ptr().cast(),
                std::ptr::null(),
                &startup.StartupInfo,
                &mut process,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        // Fixed original slots need no allocation after CreateProcess succeeds.
        state.retain(PROCESS, process.hProcess);
        state.retain(THREAD, process.hThread);
        if unsafe { AssignProcessToJobObject(job, process.hProcess) } == 0 {
            return Err(io::Error::last_os_error());
        }
        state.assigned = true;
        let count = unsafe { ResumeThread(process.hThread) };
        if count == u32::MAX {
            return Err(io::Error::last_os_error());
        }
        if count != 1 {
            return Err(io::Error::other(
                "original suspended thread state was changed",
            ));
        }
        state.started = true;
        Ok(())
    }
}
struct Attributes {
    memory: Vec<usize>,
    initialized: bool,
}
impl Attributes {
    fn new() -> io::Result<Self> {
        let mut bytes = 0;
        unsafe {
            InitializeProcThreadAttributeList(std::ptr::null_mut(), 1, 0, &mut bytes);
        }
        if bytes == 0 {
            return Err(io::Error::last_os_error());
        }
        let words = bytes
            .checked_add(std::mem::size_of::<usize>() - 1)
            .ok_or_else(|| io::Error::other("process attribute size overflow"))?
            / std::mem::size_of::<usize>();
        let mut result = Self {
            memory: vec![0; words],
            initialized: false,
        };
        if unsafe { InitializeProcThreadAttributeList(result.pointer(), 1, 0, &mut bytes) } == 0 {
            return Err(io::Error::last_os_error());
        }
        result.initialized = true;
        Ok(result)
    }
    fn pointer(&mut self) -> windows_sys::Win32::System::Threading::LPPROC_THREAD_ATTRIBUTE_LIST {
        self.memory.as_mut_ptr().cast()
    }
}
impl Drop for Attributes {
    fn drop(&mut self) {
        // This releases only the parent-allocated attribute description; all
        // inherited originals remain explicit slots in the native job owner.
        if self.initialized {
            unsafe {
                DeleteProcThreadAttributeList(self.pointer());
            }
        }
    }
}
