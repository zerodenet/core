use super::{AtomicUsize, QueryKey};
use std::{
    collections::{HashMap, HashSet},
    io,
    sync::{atomic::Ordering, Arc, Mutex},
};

#[derive(Clone, PartialEq, Eq, Hash)]
struct Node {
    coordinator: usize,
    query: QueryKey,
}
type EdgeKey = (usize, QueryKey, usize, QueryKey);
#[derive(Debug, Default)]
pub(crate) struct Graph {
    pub(super) edges: HashMap<EdgeKey, usize>,
}

#[derive(Clone)]
pub(super) struct Dependency {
    node: Node,
    graph: Arc<Mutex<Graph>>,
    pub(super) observers: Option<Arc<AtomicUsize>>,
}
impl Dependency {
    pub(super) fn new(coordinator: usize, query: QueryKey, graph: Arc<Mutex<Graph>>) -> Self {
        Self {
            node: Node { coordinator, query },
            graph,
            observers: None,
        }
    }
}
tokio::task_local! { pub(super) static ACTIVE: Vec<Dependency>; }

pub(super) fn extend(next: Dependency) -> io::Result<Vec<Dependency>> {
    let mut chain = ACTIVE.try_with(Clone::clone).unwrap_or_default();
    if chain.iter().any(|entry| entry.node == next.node) {
        return Err(cycle());
    }
    chain.push(next);
    Ok(chain)
}

/// Active await edges also catch two independently started leaders waiting on
/// each other. Query and wire coordinators of one real resolver share this graph.
pub(super) fn connect(next: &Dependency) -> io::Result<Option<Edge>> {
    let Some(previous) = ACTIVE
        .try_with(|chain| chain.last().cloned())
        .ok()
        .flatten()
    else {
        return Ok(None);
    };
    if !Arc::ptr_eq(&previous.graph, &next.graph) {
        return Ok(None);
    }
    let key = (
        previous.node.coordinator,
        previous.node.query,
        next.node.coordinator,
        next.node.query.clone(),
    );
    let mut graph = next.graph.lock().expect("DNS dependency graph poisoned");
    let mut pending = vec![next.node.clone()];
    let mut visited = HashSet::new();
    while let Some(node) = pending.pop() {
        if node.coordinator == key.0 && node.query == key.1 {
            return Err(cycle());
        }
        if !visited.insert(node.clone()) {
            continue;
        }
        for (owner, query, target, target_query) in graph.edges.keys() {
            if *owner == node.coordinator && *query == node.query {
                pending.push(Node {
                    coordinator: *target,
                    query: target_query.clone(),
                });
            }
        }
    }
    *graph.edges.entry(key.clone()).or_default() += 1;
    Ok(Some(Edge {
        graph: next.graph.clone(),
        key,
    }))
}

pub(super) struct Edge {
    graph: Arc<Mutex<Graph>>,
    key: EdgeKey,
}
impl Drop for Edge {
    fn drop(&mut self) {
        let mut graph = self.graph.lock().expect("DNS dependency graph poisoned");
        if let Some(count) = graph.edges.get_mut(&self.key) {
            *count -= 1;
            if *count == 0 {
                graph.edges.remove(&self.key);
            }
        }
    }
}
fn cycle() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "DNS resolution dependency cycle",
    )
}

/// Checked at the actual cache mutation point. Independent coalesced callers
/// keep a shared query observed after its original caller has cancelled.
pub(crate) fn query_is_observed() -> bool {
    ACTIVE
        .try_with(|chain| {
            chain
                .last()
                .and_then(|entry| entry.observers.as_ref())
                .is_none_or(|observers| observers.load(Ordering::Acquire) != 0)
        })
        .unwrap_or(true)
}
