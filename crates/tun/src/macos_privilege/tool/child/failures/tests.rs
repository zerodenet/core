use super::*;
use std::{os::unix::process::ExitStatusExt, sync::Arc};
#[test]
fn independent_fixed_child_read_close_and_wait_failures_keep_their_exact_original_sources() {
    let operation = Arc::new(io::Error::from_raw_os_error(libc::EIO));
    let close = Arc::new(io::Error::from_raw_os_error(libc::EBADF));
    let wait = Arc::new(io::Error::from_raw_os_error(libc::EINTR));
    let error = finish(
        Ok(()),
        Err(io::Error::other(operation.clone())),
        Err(io::Error::other(close.clone())),
        Err(io::Error::other(wait.clone())),
    )
    .unwrap_err();
    let actual = error.get_ref().unwrap().downcast_ref::<Failures>().unwrap();
    for (index, original) in [(1, &operation), (2, &close), (3, &wait)] {
        assert!(Arc::ptr_eq(
            actual.phases[index]
                .as_ref()
                .unwrap()
                .get_ref()
                .unwrap()
                .downcast_ref::<Arc<io::Error>>()
                .unwrap(),
            original
        ));
    }
}
#[test]
fn a_single_original_error_is_returned_without_an_additional_aggregate() {
    let original = Arc::new(io::Error::from_raw_os_error(libc::EIO));
    let error = finish(
        Ok(()),
        Err(io::Error::other(original.clone())),
        Ok(()),
        Ok(ExitStatus::from_raw(0)),
    )
    .unwrap_err();
    assert!(Arc::ptr_eq(
        error
            .get_ref()
            .unwrap()
            .downcast_ref::<Arc<io::Error>>()
            .unwrap(),
        &original
    ));
}
