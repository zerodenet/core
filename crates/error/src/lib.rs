use std::io;

use thiserror::Error;

pub mod channel;
pub use channel::ChannelError;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Core(#[from] zero_core::Error),
}
