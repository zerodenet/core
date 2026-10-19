//! A real TLS 1.3 handshake followed by a deterministic target rejection.
//! Keep the target alive until the probe's original Finished flight arrives.
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

pub(super) fn spawn(
    mut io: DuplexStream,
    config: Arc<rustls::ServerConfig>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut connection = rustls::ServerConnection::new(config).unwrap();
        let mut ccs = 0;
        loop {
            let record = read_record(&mut io).await;
            if record == super::ccs::CCS_RECORD {
                ccs += 1;
                if ccs == 2 {
                    // The production probe detects this target's CCS rejection
                    // immediately, without spending three one-second windows.
                    io.write_all(&[21, 3, 3, 0, 2, 2, 10]).await.unwrap();
                    let mut finished = read_record(&mut io).await;
                    assert!(matches!(finished.first(), Some(20 | 23)));
                    while finished.first() == Some(&20) {
                        finished = read_record(&mut io).await;
                    }
                    assert_eq!(finished.first(), Some(&23));
                    return;
                }
                continue;
            }
            connection.read_tls(&mut &record[..]).unwrap();
            connection.process_new_packets().unwrap();
            while connection.wants_write() {
                let mut output = Vec::new();
                connection.write_tls(&mut output).unwrap();
                io.write_all(&output).await.unwrap();
            }
        }
    })
}
async fn read_record(io: &mut DuplexStream) -> Vec<u8> {
    let mut header = [0; 5];
    io.read_exact(&mut header).await.unwrap();
    let mut record = header.to_vec();
    record.resize(5 + u16::from_be_bytes([header[3], header[4]]) as usize, 0);
    io.read_exact(&mut record[5..]).await.unwrap();
    record
}
