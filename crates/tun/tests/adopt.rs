#![cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn host_descriptor_adoption_rejects_regular_files_and_wrong_descriptor_without_closing_host_fd(
) {
    use std::os::fd::AsRawFd;
    let file = std::fs::File::open("/dev/null").unwrap();
    assert!(zero_tun::adopt(file.as_raw_fd(), "not-tun").is_err());
    assert!(file.metadata().is_ok(), "host original fd still owned");
    assert!(zero_tun::adopt(0, "not-tun").is_err());
}
