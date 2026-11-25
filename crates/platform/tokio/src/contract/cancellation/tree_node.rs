//! Inner tree structure for [`super::CancellationToken`].
//!
//! Locks are always acquired in creation order (parent before child) to prevent
//! deadlocks, and all tree locks are released before invoking external wakers
//! so re-entrant cancellation or panicking wakers can never poison tree mutexes.

use std::sync::{Arc, Mutex, MutexGuard, TryLockError};

pub(super) struct TreeNode {
    inner: Mutex<Inner>,
    waker: tokio::sync::Notify,
}

impl TreeNode {
    pub(super) fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                parent: None,
                parent_idx: 0,
                children: Vec::new(),
                is_cancelled: false,
                num_handles: 1,
            }),
            waker: tokio::sync::Notify::new(),
        }
    }

    pub(super) fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.waker.notified()
    }
}

struct Inner {
    parent: Option<Arc<TreeNode>>,
    parent_idx: usize,
    children: Vec<Arc<TreeNode>>,
    is_cancelled: bool,
    num_handles: usize,
}

pub(super) fn is_cancelled(node: &Arc<TreeNode>) -> bool {
    node.inner.lock().unwrap().is_cancelled
}

pub(super) fn child_node(parent: &Arc<TreeNode>) -> Arc<TreeNode> {
    let mut locked_parent = parent.inner.lock().unwrap();
    if locked_parent.is_cancelled {
        return Arc::new(TreeNode {
            inner: Mutex::new(Inner {
                parent: None,
                parent_idx: 0,
                children: Vec::new(),
                is_cancelled: true,
                num_handles: 1,
            }),
            waker: tokio::sync::Notify::new(),
        });
    }

    let child = Arc::new(TreeNode {
        inner: Mutex::new(Inner {
            parent: Some(parent.clone()),
            parent_idx: locked_parent.children.len(),
            children: Vec::new(),
            is_cancelled: false,
            num_handles: 1,
        }),
        waker: tokio::sync::Notify::new(),
    });
    locked_parent.children.push(child.clone());
    child
}

fn disconnect_children(node: &mut Inner) {
    for child in std::mem::take(&mut node.children) {
        let mut locked_child = child.inner.lock().unwrap();
        locked_child.parent_idx = 0;
        locked_child.parent = None;
    }
}

fn with_locked_node_and_parent<F, Ret>(node: &Arc<TreeNode>, func: F) -> Ret
where
    F: FnOnce(MutexGuard<'_, Inner>, Option<MutexGuard<'_, Inner>>) -> Ret,
{
    let mut locked_node = node.inner.lock().unwrap();
    loop {
        let potential_parent = match locked_node.parent.as_ref() {
            Some(parent) => parent.clone(),
            None => return func(locked_node, None),
        };
        let locked_parent = match potential_parent.inner.try_lock() {
            Ok(locked_parent) => locked_parent,
            Err(TryLockError::WouldBlock) => {
                drop(locked_node);
                let locked_parent = potential_parent.inner.lock().unwrap();
                locked_node = node.inner.lock().unwrap();
                locked_parent
            }
            #[allow(clippy::unnecessary_literal_unwrap)]
            Err(TryLockError::Poisoned(err)) => Err(err).unwrap(),
        };
        if let Some(actual_parent) = locked_node.parent.as_ref() {
            if Arc::ptr_eq(actual_parent, &potential_parent) {
                return func(locked_node, Some(locked_parent));
            }
        }
    }
}

fn move_children_to_parent(node: &mut Inner, parent: &mut Inner) {
    parent.children.reserve(node.children.len());
    for child in std::mem::take(&mut node.children) {
        {
            let mut child_locked = child.inner.lock().unwrap();
            child_locked.parent.clone_from(&node.parent);
            child_locked.parent_idx = parent.children.len();
        }
        parent.children.push(child);
    }
}

fn remove_child(parent: &mut Inner, mut node: MutexGuard<'_, Inner>) {
    let pos = node.parent_idx;
    node.parent = None;
    node.parent_idx = 0;
    drop(node);

    if parent.children.len() == pos + 1 {
        parent.children.pop().unwrap();
    } else {
        let replacement_child = parent.children.pop().unwrap();
        replacement_child.inner.lock().unwrap().parent_idx = pos;
        parent.children[pos] = replacement_child;
    }

    let len = parent.children.len();
    if 4 * len <= parent.children.capacity() {
        parent.children.shrink_to(2 * len);
    }
}

pub(super) fn increase_handle_refcount(node: &Arc<TreeNode>) {
    let mut locked_node = node.inner.lock().unwrap();
    assert!(locked_node.num_handles > 0);
    locked_node.num_handles += 1;
}

pub(super) fn decrease_handle_refcount(node: &Arc<TreeNode>) {
    let num_handles = {
        let mut locked_node = node.inner.lock().unwrap();
        locked_node.num_handles -= 1;
        locked_node.num_handles
    };
    if num_handles == 0 {
        with_locked_node_and_parent(node, |mut node, parent| match parent {
            Some(mut parent) => {
                move_children_to_parent(&mut node, &mut parent);
                remove_child(&mut parent, node);
            }
            None => disconnect_children(&mut node),
        });
    }
}

pub(super) fn cancel(node: &Arc<TreeNode>) {
    let mut locked_node = node.inner.lock().unwrap();
    if locked_node.is_cancelled {
        return;
    }

    let mut notify = Vec::new();
    while let Some(child) = locked_node.children.pop() {
        let mut locked_child = child.inner.lock().unwrap();
        locked_child.parent = None;
        locked_child.parent_idx = 0;
        if locked_child.is_cancelled {
            continue;
        }

        while let Some(grandchild) = locked_child.children.pop() {
            let mut locked_grandchild = grandchild.inner.lock().unwrap();
            locked_grandchild.parent = None;
            locked_grandchild.parent_idx = 0;
            if locked_grandchild.is_cancelled {
                continue;
            }
            if locked_grandchild.children.is_empty() {
                locked_grandchild.is_cancelled = true;
                locked_grandchild.children = Vec::new();
                drop(locked_grandchild);
                notify.push(grandchild);
            } else {
                locked_grandchild.parent = Some(node.clone());
                locked_grandchild.parent_idx = locked_node.children.len();
                drop(locked_grandchild);
                locked_node.children.push(grandchild);
            }
        }

        locked_child.is_cancelled = true;
        locked_child.children = Vec::new();
        drop(locked_child);
        notify.push(child);
    }

    locked_node.is_cancelled = true;
    locked_node.children = Vec::new();
    drop(locked_node);
    notify.push(node.clone());

    let mut first_panic = None;
    for node in notify {
        if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            node.waker.notify_waiters();
        })) {
            if first_panic.is_none() {
                first_panic = Some(payload);
            }
        }
    }
    if let Some(payload) = first_panic {
        std::panic::resume_unwind(payload);
    }
}
