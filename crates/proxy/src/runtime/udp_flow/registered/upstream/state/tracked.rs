pub(in crate::runtime::udp_flow::registered::upstream) struct TrackedUpstreamAssociation<T, A> {
    target: T,
    association: A,
}

pub(crate) struct TrackedUpstreamAssociationState<T, A> {
    upstream: AssociationState<T, A>,
}

// An association cannot be active while another original awaits closure.
// The retired state carries the actual owner, never a cancellation flag.
enum AssociationState<T, A> {
    Empty,
    Active(TrackedUpstreamAssociation<T, A>),
    Retiring(A),
}

impl<T, A> TrackedUpstreamAssociation<T, A> {
    pub(in crate::runtime::udp_flow::registered::upstream) fn new(
        target: T,
        association: A,
    ) -> Self {
        Self {
            target,
            association,
        }
    }

    pub(in crate::runtime::udp_flow::registered::upstream) fn target(&self) -> &T {
        &self.target
    }

    pub(in crate::runtime::udp_flow::registered::upstream) fn association(&self) -> &A {
        &self.association
    }

    pub(in crate::runtime::udp_flow::registered::upstream) fn into_parts(self) -> (T, A) {
        (self.target, self.association)
    }
}

impl<T, A> TrackedUpstreamAssociationState<T, A> {
    pub(crate) fn new() -> Self {
        Self {
            upstream: AssociationState::Empty,
        }
    }

    pub(crate) fn target(&self) -> Option<&T> {
        match &self.upstream {
            AssociationState::Active(upstream) => Some(upstream.target()),
            _ => None,
        }
    }

    pub(crate) fn association(&self) -> Option<&A> {
        match &self.upstream {
            AssociationState::Active(upstream) => Some(upstream.association()),
            _ => None,
        }
    }

    pub(in crate::runtime::udp_flow::registered::upstream) fn insert(
        &mut self,
        target: T,
        association: A,
    ) -> Option<TrackedUpstreamAssociation<T, A>> {
        self.track(TrackedUpstreamAssociation::new(target, association))
    }

    pub(in crate::runtime::udp_flow::registered::upstream) fn track(
        &mut self,
        upstream: TrackedUpstreamAssociation<T, A>,
    ) -> Option<TrackedUpstreamAssociation<T, A>> {
        assert!(
            !matches!(self.upstream, AssociationState::Retiring(_)),
            "unconfirmed original blocks insertion"
        );
        let previous = self.take();
        self.upstream = AssociationState::Active(upstream);
        previous
    }

    pub(in crate::runtime::udp_flow::registered::upstream) fn take(
        &mut self,
    ) -> Option<TrackedUpstreamAssociation<T, A>> {
        match std::mem::replace(&mut self.upstream, AssociationState::Empty) {
            AssociationState::Active(upstream) => Some(upstream),
            state => {
                self.upstream = state;
                None
            }
        }
    }
}

impl<T, A> Default for TrackedUpstreamAssociationState<T, A> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, A> TrackedUpstreamAssociationState<T, A>
where
    T: PartialEq,
{
    pub(crate) fn matches_target(&self, target: &T) -> bool {
        self.target().is_some_and(|current| current == target)
    }
}

impl<T, A> TrackedUpstreamAssociationState<T, A> {
    pub(in crate::runtime::udp_flow::registered::upstream) fn retire(&mut self, owner: A) {
        assert!(
            matches!(self.upstream, AssociationState::Empty),
            "an original owner cannot be displaced"
        );
        self.upstream = AssociationState::Retiring(owner);
    }
    pub(in crate::runtime::udp_flow::registered::upstream) fn retiring_mut(
        &mut self,
    ) -> Option<&mut A> {
        match &mut self.upstream {
            AssociationState::Retiring(owner) => Some(owner),
            _ => None,
        }
    }
    pub(in crate::runtime::udp_flow::registered::upstream) fn release_confirmed(&mut self) {
        assert!(
            matches!(self.upstream, AssociationState::Retiring(_)),
            "only a retiring original may complete"
        );
        self.upstream = AssociationState::Empty;
    }
}
