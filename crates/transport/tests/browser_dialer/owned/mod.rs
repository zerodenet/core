//! Actual declared listener, native controller and hosted Scope regressions.
use super::*;
use std::{future::Future, num::NonZeroUsize, sync::Arc, time::Duration};
use tokio::time::{timeout, Instant};
use zero_platform_tokio::contract::{
    component::{GrantedDatagramListener, ResourceAccess},
    lifecycle::ResourceLifecycle,
    scope::{HostedScope, TaskScope},
    BoxFuture, ChannelError, CloseReason, Impact, Reason, Stage,
};
mod fixture;
mod ownership;
