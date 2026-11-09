use super::*;
#[test]
fn only_exact_private_authorization_cancel_result_matches_its_original_nonce() {
    assert!(cancelled(b"zero.utun.cancelled.131\n", 131));
    for value in [
        b"zero.utun.cancelled.132\n".as_slice(),
        b"zero.utun.completed.131\n",
        b"zero.utun.cancelled.131",
        b"zero.utun.cancelled.+131\n",
        b"untrusted 131\n",
    ] {
        assert!(!cancelled(value, 131));
    }
}
#[test]
fn foreground_authorization_script_has_no_detached_helper_and_cancellation_does_not_execute_it() {
    let script = super::super::native::OWNED_SCRIPT;
    assert!(!script.contains("2>&1 &"));
    assert!(script.contains("with administrator privileges"));
    assert!(script.contains("if errorNumber is -128 then return"));
    assert!(script.contains("zero.utun.cancelled."));
    assert!(script.contains("zero.utun.completed."));
}
