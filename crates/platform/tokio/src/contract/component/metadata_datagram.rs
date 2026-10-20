//! Ancillary datagrams are a separate exact native declaration. A plain UDP
//! listener cannot authorize capability probes or additional write pollers.
use crate::controlled_datagram::metadata::{
    MetadataDatagramObservation, MetadataDatagramRx, MetadataDatagramTx, MetadataLimits,
};
use std::net::SocketAddr;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetadataDatagramRequest {
    /// Exact position in this component's complete resource declaration.
    pub ordinal: usize,
    pub source: SocketAddr,
    pub limits: MetadataLimits,
}

/// Unique RX, bounded TX clones and the unique observation authority. The
/// granting host retains the actual main/probe owner and native control.
pub struct GrantedMetadataListener {
    pub rx: MetadataDatagramRx,
    pub tx: MetadataDatagramTx,
    pub observation: MetadataDatagramObservation,
}
