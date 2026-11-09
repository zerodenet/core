//! The original read/write algorithms run inside the existing owning poller.
use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
pub(super) struct Input {
    pub(super) raw: TcpRelayStream,
    pub(super) plan: PreparedMasks,
    pub(super) transform: super::super::stream::Transform,
    pub(super) shared: PacketMetadataArc<queue::Shared>,
    pub(super) scratch: memory::Scratch,
    pub(super) codec: CarrierBufferCredit,
}
pub(super) async fn run(input: Input) -> io::Result<()> {
    // Pure byte credit is retained through every encoder/decoder temporary.
    // Tokio's internal split bookkeeping remains an opaque implementation
    // allocation, not a claimed packet header, native provider or new task.
    let _plan = input.plan; // its original tables outlive every codec clone
    let _codec = input.codec;
    let mut scratch = input.scratch;
    let shared = input.shared;
    let (mut decoder, mut encoder) = input.transform.codecs();
    let (mut reader, mut writer) = tokio::io::split(input.raw);
    let read = async {
        loop {
            let n = reader.read(&mut scratch.bytes).await?;
            if n == 0 {
                shared.finish_read();
                return Ok::<_, io::Error>(());
            }
            let credit = shared.memory.receive.reserve(memory::READ + 4).await?;
            let bytes = decoder.decode(&scratch.bytes[..n])?;
            if bytes.is_empty() {
                drop(bytes);
                drop(credit);
                continue;
            }
            shared
                .publish(queue::Frame {
                    bytes,
                    _credit: credit,
                })
                .await?;
        }
    };
    let write = async {
        while let Some(frame) = shared.next_output().await {
            encoder.write(&mut writer, &frame.bytes).await?;
            writer.flush().await?;
            shared.commit(frame.bytes.len());
            drop(frame);
        }
        writer.shutdown().await?;
        shared.finish_write();
        Ok::<_, io::Error>(())
    };
    tokio::try_join!(read, write).map(|_| ())
}
