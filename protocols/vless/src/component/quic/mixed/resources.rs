//! Exact declaration ordinals for the two actual physical half owners.
//! Both halves still consume the one common Entry table and Host totals.
use super::super::*;
use super::options::{Half, Options};
#[derive(Clone, Copy)]
pub(super) enum BoundHalf {
    Quic { metadata: usize, carrier: usize },
    Tcp { carrier: usize },
}
pub(super) struct Description {
    pub resources: Vec<ResourceDeclaration>,
    pub upload: BoundHalf,
    pub download: Option<BoundHalf>,
    pub tasks: NonZeroUsize,
}
impl Description {
    pub fn new(options: &Options) -> Self {
        let mut resources = Vec::new();
        let (upload, mut tasks) = append(&mut resources, &options.upload);
        let download = options.download.as_ref().map(|half| {
            let (binding, peak) = append(&mut resources, half);
            // Shared Entry admission bounds the sum of actual originals by N.
            // Taking the maximum per-N peak covers every mixture without
            // granting each half its own independent N or task allocation.
            tasks = tasks.max(peak);
            binding
        });
        Self {
            resources,
            upload,
            download,
            tasks: n(tasks),
        }
    }
}
fn append(resources: &mut Vec<ResourceDeclaration>, half: &Half) -> (BoundHalf, usize) {
    match half {
        Half::Quic(options) => {
            let spec = super::super::Factory.describe(options.as_ref());
            let first = resources.len();
            // The same real factory puts its Metadata and borrowed originals
            // first. Do not infer an ordinal from Role/instance numeric equality.
            assert!(matches!(
                spec.resources.first(),
                Some(ResourceDeclaration::ControlledMetadataCarrier { .. })
            ));
            assert!(matches!(
                spec.resources.get(1),
                Some(ResourceDeclaration::ControlledCarrierPool { datagram: true, .. })
            ));
            for resource in spec.resources {
                append_auxiliary(resources, resource);
            }
            (
                BoundHalf::Quic {
                    metadata: first,
                    carrier: first + 1,
                },
                spec.task_limit.unwrap().get(),
            )
        }
        Half::Tcp(options) => {
            let carrier = resources.len();
            resources.push(ResourceDeclaration::ControlledCarrierPool {
                endpoint: options.protocol.endpoint.clone(),
                dial: options.protocol.dial.clone(),
                datagram: false,
                max_sockets: options.origins,
            });
            if let Some(file) = options
                .protocol
                .material
                .as_ref()
                .and_then(|material| material.resource())
            {
                append_auxiliary(resources, file);
            }
            // One actual setup Aux and its real HTTP driver. Hyper delegates
            // remain finite work in that driver. A rejected REALITY job must
            // use the same original handoff, never a third ad-hoc worker.
            (
                BoundHalf::Tcp { carrier },
                options
                    .origins
                    .get()
                    .checked_mul(2)
                    .expect("validated exact TCP task peak"),
            )
        }
    }
}
fn append_auxiliary(resources: &mut Vec<ResourceDeclaration>, resource: ResourceDeclaration) {
    let repeated = resources
        .iter()
        .any(|previous| match (previous, &resource) {
            (
                ResourceDeclaration::File {
                    path: a,
                    max_bytes: x,
                },
                ResourceDeclaration::File {
                    path: b,
                    max_bytes: y,
                },
            ) => a == b && x == y,
            (
                ResourceDeclaration::NativeIcmp { max_sockets: a },
                ResourceDeclaration::NativeIcmp { max_sockets: b },
            ) => a == b,
            _ => false,
        });
    // Only immutable file permission and the common ancillary bound coalesce.
    // Actual Native/borrowed registrations retain their distinct exact ordinals.
    if !repeated {
        resources.push(resource);
    }
}
