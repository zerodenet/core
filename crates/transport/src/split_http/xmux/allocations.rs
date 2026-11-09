//! Concrete pool storage; explicit standalone callers retain their old Arc.
//! Installed groups/usage use sealed same-Root metadata custody, not raw Weak.
use super::*;
use std::ops::Deref;
use zero_platform_tokio::contract::component::PacketDriverMetadataView;
use zero_traits::packet_metadata::{PacketMetadataArc, PacketMetadataCredit, PacketMetadataLayout};
#[derive(Clone)]
pub(super) enum GroupRef {
    Library(Arc<Group>),
    Hosted(PacketMetadataArc<Group>),
}
impl Deref for GroupRef {
    type Target = Group;
    fn deref(&self) -> &Group {
        match self {
            Self::Library(group) => group,
            Self::Hosted(group) => group,
        }
    }
}
impl GroupRef {
    pub fn library_weak(&self) -> io::Result<std::sync::Weak<Group>> {
        match self {
            Self::Library(group) => Ok(Arc::downgrade(group)),
            Self::Hosted(_) => Err(io::ErrorKind::PermissionDenied.into()),
        }
    }
    pub fn new(config: SplitHttpXmux, owner: Option<OwnerKey>) -> io::Result<Self> {
        if let Some(owner) = owner {
            let credit = owner
                .memory
                .reserve(PacketMetadataArc::<Group>::layout().map_err(io::Error::other)?)
                .map_err(io::Error::other)?;
            let group = PacketMetadataArc::from_prepaid(Group::new(config, Some(owner)), credit)
                .map_err(|(source, _, _)| io::Error::other(source))?;
            Ok(Self::Hosted(group))
        } else {
            Ok(Self::Library(Arc::new(Group::new(config, None))))
        }
    }
}
#[derive(Clone)]
pub(in crate::split_http) enum UsageRef {
    Library(Arc<Mutex<Usage>>),
    Hosted(PacketMetadataArc<Mutex<Usage>>),
}
impl Deref for UsageRef {
    type Target = Mutex<Usage>;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Library(usage) => usage,
            Self::Hosted(usage) => usage,
        }
    }
}
impl UsageRef {
    pub fn new(usage: Usage, owner: Option<&OwnerKey>) -> io::Result<Self> {
        match owner {
            Some(owner) => {
                let credit = owner
                    .memory
                    .reserve(PacketMetadataArc::<Mutex<Usage>>::layout().map_err(io::Error::other)?)
                    .map_err(io::Error::other)?;
                let usage = PacketMetadataArc::from_prepaid(Mutex::new(usage), credit)
                    .map_err(|(source, _, _)| io::Error::other(source))?;
                Ok(Self::Hosted(usage))
            }
            None => Ok(Self::Library(Arc::new(Mutex::new(usage)))),
        }
    }
}
#[derive(Default)]
pub(super) struct Groups {
    values: Vec<GroupRef>,
    // Actual Vec allocation is destroyed before its credit, also on pool Drop.
    credit: Option<PacketMetadataCredit>,
    memory: Option<PacketDriverMetadataView>,
}
impl Deref for Groups {
    type Target = [GroupRef];
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}
impl Groups {
    pub fn bind(&mut self, memory: &PacketDriverMetadataView) -> io::Result<()> {
        if let Some(actual) = &self.memory {
            return if actual.same_root(memory) {
                Ok(())
            } else {
                Err(io::ErrorKind::PermissionDenied.into())
            };
        }
        // A genuine standalone allocation is moved only after the actual Host
        // has promised its replacement storage. Foreign Group headers remain
        // their original library owner's responsibility.
        if self.values.capacity() != 0 {
            self.reallocate(self.values.capacity(), memory)?;
        }
        self.memory = Some(memory.clone());
        Ok(())
    }
    pub fn push(&mut self, group: GroupRef) -> io::Result<()> {
        if self.values.len() == self.values.capacity() {
            if let Some(memory) = self.memory.clone() {
                let needed = self
                    .values
                    .len()
                    .checked_add(1)
                    .ok_or(io::ErrorKind::InvalidInput)?;
                let capacity = self.values.capacity().saturating_mul(2).max(needed);
                self.reallocate(capacity, &memory)?;
            } else {
                self.values.try_reserve(1).map_err(io::Error::other)?;
            }
        }
        self.values.push(group);
        Ok(())
    }
    fn reallocate(&mut self, capacity: usize, memory: &PacketDriverMetadataView) -> io::Result<()> {
        let layout = PacketMetadataLayout::array::<GroupRef>(capacity).map_err(io::Error::other)?;
        let credit = memory.reserve(layout).map_err(io::Error::other)?;
        let slots = Box::<[GroupRef]>::new_uninit_slice(capacity);
        let ptr = Box::into_raw(slots) as *mut GroupRef;
        // SAFETY: identical array layout/alignment, zero live elements; this
        // Vec owns the same exact prepaid allocation and sufficient capacity.
        let next = unsafe { Vec::from_raw_parts(ptr, 0, capacity) };
        let mut previous = std::mem::replace(&mut self.values, next);
        self.values.extend(previous.drain(..));
        drop(previous);
        drop(self.credit.replace(credit));
        Ok(())
    }
    pub fn remove(&mut self, index: usize) -> GroupRef {
        self.values.remove(index)
    }
    pub fn take(&mut self) -> Self {
        let memory = self.memory.clone();
        std::mem::replace(
            self,
            Self {
                memory,
                ..Default::default()
            },
        )
    }
}
