use super::*;
use crate::TunDevice;

#[test]
fn only_ring_capacity_exhaustion_is_retryable() {
    assert!(buffer_full(&wintun::Error::Io(
        io::Error::from_raw_os_error(ERROR_BUFFER_OVERFLOW as i32)
    )));
    assert!(!buffer_full(&wintun::Error::Io(
        io::Error::from_raw_os_error(5)
    )));
    assert!(!buffer_full(&wintun::Error::ShuttingDown));
}

#[tokio::test]
#[ignore = "requires Administrator privileges and wintun.dll"]
async fn existing_host_wintun_is_read_only_staged_and_releases_its_reader() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let name = "ZeroHostPacketTest";
    let driver = load_wintun().unwrap();
    let host = wintun::Adapter::create(&driver, name, "ZeroHostTest", None).unwrap();
    let setup = host.start_session(wintun::MAX_RING_CAPACITY).unwrap();
    let address: IpAddr = "10.67.3.1".parse().unwrap();
    super::super::configure_adapter(name, &[(address, "255.255.255.0".parse().unwrap())], 1400)
        .unwrap();
    drop(setup);
    assert!(ExistingWindowsTun::open(name, &["10.67.3.99".parse().unwrap()], 1400).is_err());
    assert!(ExistingWindowsTun::open(name, &[address], 1500).is_err());
    let mut device = ExistingWindowsTun::open(name, &[address], 1400).unwrap();
    assert!(
        device.reader.is_none(),
        "preparation must not consume host packets"
    );
    assert_eq!(device.read(&mut []).await.unwrap(), 0);
    assert_eq!(device.write(&[]).await.unwrap(), 0);
    assert!(device.reader.is_none(), "empty reads must not activate I/O");
    assert!(
        ExistingWindowsTun::open(name, &[address], 1400).is_err(),
        "another candidate must not take over the committed Wintun session"
    );
    assert_eq!(
        device
            .configure(address, "255.255.255.0".parse().unwrap(), 1200)
            .unwrap_err()
            .kind(),
        io::ErrorKind::PermissionDenied
    );
    validate_addresses(name, &[address], 1400).unwrap();
    let socket = std::net::UdpSocket::bind((address, 0)).unwrap();
    socket
        .send_to(b"host-packet-probe", "10.67.3.2:9000")
        .unwrap();
    let mut packet = [0; 65536];
    let size = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let size = device.read(&mut packet).await?;
            if size == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "host reader stopped",
                ));
            }
            if packet[..size].ends_with(b"host-packet-probe") {
                return Ok(size);
            }
        }
    })
    .await
    .unwrap()
    .unwrap();
    assert!(size >= 20);
    assert!(device.reader.is_some());
    device.write_all(&packet[..size]).await.unwrap();
    assert!(device.write_all(&[0; 65536]).await.is_err());
    drop(device); // Wakes and joins the blocking reader, preserving host configuration.
    validate_addresses(name, &[address], 1400).unwrap();
    let reopened = ExistingWindowsTun::open(name, &[address], 1400).unwrap();
    drop(reopened);
    drop(host);
}

#[test]
#[ignore = "requires Administrator privileges and wintun.dll"]
fn missing_existing_host_adapter_does_not_create_one() {
    let name = "ZeroMissingHostPacket";
    assert_eq!(
        ExistingWindowsTun::open(name, &["10.67.4.1".parse().unwrap()], 1400)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::NotFound
    );
    let driver = load_wintun().unwrap();
    assert!(wintun::Adapter::open(&driver, name).is_err());
}
