//! Immutable carrier masks materialized once per transport plan.
use super::*;
use std::sync::Arc;
#[derive(Clone, Default)]
pub struct PreparedMasks(Arc<[Prepared]>);
enum Prepared {
    Custom(Custom),
    Fragment(Fragment),
    Sudoku(super::super::sudoku::Profile),
}
impl PreparedMasks {
    pub fn new(masks: &[Mask]) -> io::Result<Self> {
        if masks.len() > 64 {
            return Err(invalid("too many TCP masks"));
        }
        let masks = masks
            .iter()
            .map(|mask| {
                if let Mask::Sudoku(settings) = mask {
                    return super::super::sudoku::Profile::new(settings).map(Prepared::Sudoku);
                }
                mask.validate()?;
                Ok(match mask {
                    Mask::Custom(config) => Prepared::Custom(config.clone()),
                    Mask::Fragment(config) => Prepared::Fragment(config.clone()),
                    Mask::Sudoku(_) => unreachable!(),
                })
            })
            .collect::<io::Result<Vec<_>>>()?;
        Ok(Self(masks.into()))
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
pub async fn wrap_prepared(
    mut stream: TcpRelayStream,
    masks: &PreparedMasks,
    server: bool,
) -> io::Result<TcpRelayStream> {
    for mask in masks.0.iter() {
        stream = match mask {
            Prepared::Custom(config) => {
                tokio::time::timeout(
                    Duration::from_secs(30),
                    custom::handshake(&mut stream, config, server),
                )
                .await
                .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))??;
                stream
            }
            Prepared::Fragment(config) => TcpRelayStream::from_client(stream::Stream::new(
                stream,
                stream::Transform::Fragment(config.clone()),
            )),
            Prepared::Sudoku(profile) => TcpRelayStream::from_client(stream::Stream::new(
                stream,
                stream::Transform::Sudoku(profile.clone(), server),
            )),
        };
    }
    Ok(stream)
}
