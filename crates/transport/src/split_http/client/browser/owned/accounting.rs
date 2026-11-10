//! Known immutable profile copies and the original duplex byte promises.
use super::*;
use zero_traits::packet_metadata::{PacketMetadataCredit, PacketMetadataLayout};

fn strings(options: &zero_traits::SplitHttpOptions) -> [&str; 11] {
    [
        &options.x_padding_key,
        &options.x_padding_header,
        &options.x_padding_placement,
        &options.x_padding_method,
        &options.session_placement,
        &options.session_key,
        &options.seq_placement,
        &options.seq_key,
        &options.uplink_http_method,
        &options.uplink_data_placement,
        &options.uplink_data_key,
    ]
}
fn add_string(layout: PacketMetadataLayout, length: usize) -> io::Result<PacketMetadataLayout> {
    if length == 0 {
        return Ok(layout);
    }
    layout
        .checked_add(PacketMetadataLayout::array::<u8>(length).map_err(io::Error::other)?)
        .map_err(io::Error::other)
}
pub(super) fn profile(
    input: &crate::profile::OwnedSplitHttpProfile,
    memory: &PacketDriverMetadataView,
) -> io::Result<(Profile, PacketMetadataCredit)> {
    let mut layout = PacketMetadataLayout::array::<(String, String)>(input.options.headers.len())
        .map_err(io::Error::other)?;
    for (name, value) in &input.options.headers {
        layout = add_string(add_string(layout, name.len())?, value.len())?;
    }
    for text in strings(&input.options) {
        layout = add_string(layout, text.len())?;
    }
    layout = add_string(layout, input.host.as_deref().unwrap_or("localhost").len())?;
    let (path, query) = input.path.split_once('?').unwrap_or((&input.path, ""));
    layout = add_string(layout, query.len())?;
    // Exact String clones above use len capacity. The two slash insertions
    // can coexist with the former path backing during its single growth.
    let path_peak = path
        .len()
        .checked_add(2)
        .and_then(|n| n.checked_mul(3))
        .map(|n| n.max(8))
        .ok_or(io::ErrorKind::InvalidInput)?;
    layout = layout
        .checked_add(PacketMetadataLayout::new(2, path_peak))
        .map_err(io::Error::other)?;
    // Profile::new constructs these pinned default strings even when its
    // caller supplies every field. Charge their real simultaneous backing.
    for text in [
        "x_padding",
        "x-padding",
        "queryInHeader",
        "repeat-x",
        "path",
        "path",
        "POST",
        "auto",
    ] {
        layout = add_string(layout, text.len())?;
    }
    // Empty fields receive separately allocated fallbacks while those defaults
    // and the input's original clones still coexist.
    let options = &input.options;
    for (empty, fallback) in [
        (options.session_placement.is_empty(), "path"),
        (options.seq_placement.is_empty(), "path"),
        (options.uplink_data_placement.is_empty(), "auto"),
        (options.uplink_http_method.is_empty(), "POST"),
        (
            options.uplink_data_key.is_empty(),
            if options.uplink_data_placement == "cookie" {
                "x_data"
            } else {
                "X-Data"
            },
        ),
    ] {
        if empty {
            layout = add_string(layout, fallback.len())?;
        }
    }
    let credit = memory.reserve(layout).map_err(io::Error::other)?;
    let value = Profile::new(input);
    Ok((value, credit))
}

pub(super) struct Buffer {
    pub bytes: Box<[u8]>,
    _credit: CarrierBufferCredit,
}
impl Buffer {
    pub fn prepare(book: &CarrierBufferBudget, length: usize) -> io::Result<Self> {
        let credit = book.try_reserve(length)?;
        let mut bytes = Box::<[u8]>::new_uninit_slice(length);
        for byte in &mut bytes {
            byte.write(0);
        }
        Ok(Self {
            bytes: unsafe { bytes.assume_init() },
            _credit: credit,
        })
    }
}
