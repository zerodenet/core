use super::*;
use crate::contract::{
    lifecycle::ChannelControl,
    scope::{TaskClass, TaskScope},
};
use std::{
    num::NonZeroUsize,
    os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
    task::Poll,
};
use tokio::{sync::oneshot, time::Instant};
use windows_sys::Win32::{
    Foundation::WAIT_OBJECT_0,
    Security::SECURITY_ATTRIBUTES,
    System::Threading::{
        CreateEventW, OpenProcess, PROCESS_SYNCHRONIZE, SetEvent, WaitForSingleObject,
    },
};
fn end() -> Instant {
    Instant::now() + std::time::Duration::from_secs(5)
}
fn request() -> ProcessRequest {
    let mut request = ProcessRequest::new(std::env::current_exe().unwrap());
    request.arguments = vec![
        "--ignored".into(),
        "--exact".into(),
        "owned_process::windows::tests::native_job_fixture".into(),
    ];
    request
}
/// Only entered by the native job tests using this exact executable. It is a
/// real root and descendant, not a replacement process implementation.
#[test]
#[ignore = "native Windows subprocess fixture; invoked by the owning tests"]
fn native_job_fixture() {
    let role = std::env::var("ZERO_R2_PROCESS_FIXTURE").unwrap();
    let directory =
        std::path::PathBuf::from(std::env::var_os("ZERO_R2_PROCESS_DIRECTORY").unwrap());
    let event: usize = std::env::var("ZERO_R2_PROCESS_EVENT")
        .unwrap()
        .parse()
        .unwrap();
    unsafe {
        SetEvent(event as HANDLE);
    }
    if role == "CHILD" {
        std::fs::write(directory.join("ready"), std::process::id().to_string()).unwrap();
        loop {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "owned_process::windows::tests::native_job_fixture",
        ])
        .env("ZERO_R2_PROCESS_FIXTURE", "CHILD")
        .spawn()
        .unwrap();
    while !directory.join("release").exists() {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    // The child is intentionally still alive when the actual root exits.
    assert!(child.try_wait().unwrap().is_none());
    std::process::exit(23);
}
async fn tree(natural: bool) {
    let directory = tempfile::tempdir().unwrap();
    let security = SECURITY_ATTRIBUTES {
        nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: std::ptr::null_mut(),
        bInheritHandle: 1,
    };
    let event = unsafe { CreateEventW(&security, 1, 0, std::ptr::null()) };
    assert!(!event.is_null());
    let event = unsafe { OwnedHandle::from_raw_handle(event) };
    let mut request = request();
    request.environment = [
        ("ZERO_R2_PROCESS_FIXTURE".into(), Some("ROOT".into())),
        (
            "ZERO_R2_PROCESS_DIRECTORY".into(),
            Some(directory.path().as_os_str().to_owned()),
        ),
        (
            "ZERO_R2_PROCESS_EVENT".into(),
            Some((event.as_raw_handle() as usize).to_string().into()),
        ),
    ]
    .into_iter()
    .collect();
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let original = OwnedProcess::prepare(request, &scope.registrar()).unwrap();
    let (begin, begun) = oneshot::channel();
    let (ready, started) = oneshot::channel();
    let (returned, body) = oneshot::channel();
    let actual = original.clone();
    let (_, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            actual.start(begun.await.unwrap()).unwrap();
            actual.wait_started(end()).await.unwrap();
            ready.send(()).unwrap();
            actual.wait_tree_ended(end()).await.unwrap();
            returned.send(()).unwrap();
            Ok(())
        })
        .unwrap();
    begin.send(witness.clone()).unwrap();
    tokio::time::timeout_at(end(), started)
        .await
        .unwrap()
        .unwrap();
    let ready = directory.path().join("ready");
    let pid: u32 = tokio::time::timeout_at(end(), async {
        loop {
            if let Ok(value) = std::fs::read_to_string(&ready) {
                if let Ok(pid) = value.parse() {
                    break pid;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let descendant = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, pid) };
    assert!(
        !descendant.is_null(),
        "actual descendant handle: {}",
        io::Error::last_os_error()
    );
    let descendant = unsafe { OwnedHandle::from_raw_handle(descendant) };
    assert_ne!(
        unsafe { WaitForSingleObject(descendant.as_raw_handle(), 0) },
        WAIT_OBJECT_0
    );
    assert_eq!(
        unsafe { WaitForSingleObject(event.as_raw_handle(), 0) },
        windows_sys::Win32::Foundation::WAIT_TIMEOUT,
        "the suspended root inherited only its explicit NUL handles, not this live core original"
    );
    assert!(original.try_finish().unwrap().is_none());
    if natural {
        std::fs::write(directory.path().join("release"), b"x").unwrap();
    } else {
        original.request_stop();
    }
    tokio::time::timeout_at(end(), body).await.unwrap().unwrap();
    assert_eq!(
        unsafe { WaitForSingleObject(descendant.as_raw_handle(), 0) },
        WAIT_OBJECT_0
    );
    assert!(
        original.try_finish().unwrap().is_none(),
        "native job emptiness does not replace the actual actor join"
    );
    let exit = tokio::time::timeout_at(end(), scope.next_exit())
        .await
        .unwrap()
        .unwrap();
    assert!(exit.result.is_ok());
    let closed = original.try_finish().unwrap().unwrap();
    assert!(closed.matches(&original) && closed.had_native_acquisition());
    assert!(
        original.first_failure().is_none(),
        "{:?}",
        original.first_failure()
    );
    assert_eq!(
        original.root_status().unwrap().code(),
        Some(if natural { 23 } else { 1 })
    );
    scope.begin_shutdown();
    assert!(std::future::poll_fn(|cx| Poll::Ready(scope.poll_exit(cx).is_ready())).await);
    assert!(scope.is_drained());
}
#[tokio::test]
async fn suspended_job_stop_joins_real_descendants_and_all_original_handles() {
    tree(false).await;
}
#[tokio::test]
async fn natural_root_status_is_preserved_while_actual_job_descendants_are_retired() {
    tree(true).await;
}
#[tokio::test]
async fn missing_program_retains_original_failure_and_cold_actor_join_until_cleanup() {
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let original = OwnedProcess::prepare(
        ProcessRequest::new("zero-nonexistent-native-process-24172.exe"),
        &scope.registrar(),
    )
    .unwrap();
    let (begin, begun) = oneshot::channel();
    let actual = original.clone();
    let (_, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            assert!(actual.start(begun.await.unwrap()).is_err());
            actual.wait_tree_ended(end()).await.unwrap();
            Ok(())
        })
        .unwrap();
    begin.send(witness).unwrap();
    scope.next_exit().await.unwrap();
    assert!(original.first_failure().is_some());
    assert!(
        !original
            .try_finish()
            .unwrap()
            .unwrap()
            .had_native_acquisition()
    );
    scope.begin_shutdown();
    assert!(std::future::poll_fn(|cx| Poll::Ready(scope.poll_exit(cx).is_ready())).await);
}
