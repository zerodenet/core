use super::{LocalProcessInfo, LocalProcessLookup};
use crate::contract::ChannelError;
use std::sync::Arc;
pub(super) fn lookup(
    _: &LocalProcessLookup,
) -> Result<Option<LocalProcessInfo>, Arc<ChannelError>> {
    Ok(None)
}
