# 事件系统与 Sink 设计

> [!IMPORTANT]
> 本文是事件系统的历史设计材料。当前事件名称和负载以[公开事件 API](https://docs.zerodenet.org/projects/core/control-plane/events)为准。

本文档详细规划事件总线、归一化事件格式和事件 Sink 框架的实现。

## 整体架构

```
                    ┌─────────────────┐
zero-engine ───────▶│  Event Producer │───────┐
                    └─────────────────┘       │
                                              ▼
                                    ┌─────────────────┐
                                    │   Event Bus     │  (tokio::sync::broadcast)
                                    └────────┬────────┘
                                             │
                    ┌────────────────────────┼────────────────────────┐
                    ▼                        ▼                        ▼
            ┌─────────────┐          ┌─────────────┐          ┌─────────────┐
            │  File Sink  │          │  HTTP Sink  │          │  SSE Stream │
            └─────────────┘          └─────────────┘          └─────────────┘
                    │                        │
                    ▼                        ▼
              ┌──────────┐             ┌──────────┐
              │  文件    │             │ Webhook  │
              │ (JSONL) │             │  Server  │
              └──────────┘             └──────────┘
```

---

## 核心类型定义

### 事件 Envelope

```rust
// crates/api/src/event.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub schema_id: &'static str,
    pub event_id: String,
    pub event_type: String,
    pub occurred_at_unix_ms: u64,
    pub source_id: Option<String>,
    pub sequence: Option<u64>,
    pub principal_key: Option<String>,
    pub labels: Option<HashMap<String, String>>,
    pub payload: EventPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum EventPayload {
    FlowStarted(FlowEvent),
    FlowCompleted(FlowEvent),
    PolicySelected(PolicySelectedEvent),
    PolicyProbeCompleted(PolicyProbeEvent),
    StatsSampled(StatsSnapshot),
    ConfigChanged(ConfigChangeEvent),
    EngineStarted(EngineLifecycleEvent),
    EngineStopped(EngineLifecycleEvent),
    EngineWarning(WarningEvent),
}
```

---

### Flow 事件

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowEvent {
    pub flow_id: String,
    pub flow_type: String,
    pub inbound_tag: String,
    pub target_tag: String,
    pub remote_addr: String,
    pub started_at_unix_ms: u64,
    pub ended_at_unix_ms: Option<u64>,
    pub bytes_up: u64,
    pub bytes_down: u64,
    pub outcome: FlowOutcome,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlowOutcome {
    InProgress,
    Succeeded,
    Failed,
    Cancelled,
    Blocked,
}
```

---

### Policy 事件

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicySelectedEvent {
    pub policy_tag: String,
    pub previous: String,
    pub current: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyProbeEvent {
    pub policy_tag: String,
    pub results: Vec<ProbeResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeResult {
    pub target_tag: String,
    pub rtt_ms: u32,
    pub status: ProbeStatus,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbeStatus {
    Alive,
    Dead,
    Unknown,
}
```

---

## Event Source Trait

```rust
// crates/api/src/traits.rs
#[async_trait]
pub trait EventSource: Send + Sync {
    async fn subscribe(&self, type_filter: Option<Vec<String>>) -> Result<EventSubscriber>;
    async fn latest(&self, limit: usize, type_filter: Option<Vec<String>>) -> Result<Vec<EventEnvelope>>;
}

pub struct EventSubscriber {
    receiver: broadcast::Receiver<EventEnvelope>,
    type_filter: Option<HashSet<String>>,
}

impl Stream for EventSubscriber {
    type Item = Result<EventEnvelope, Error>;
}
```

---

## Event Bus 实现

```rust
// crates/api/src/event_bus.rs
#[derive(Clone)]
pub struct EventBus {
    sender: broadcast::Sender<EventEnvelope>,
    buffer: Arc<Mutex<VecDeque<EventEnvelope>>>,
    max_buffer_size: usize,
    next_sequence: AtomicU64,
}

impl EventBus {
    pub fn new(capacity: usize, max_buffer_size: usize) -> Self;
    pub fn publish(&self, mut event: EventEnvelope) -> Result<()>;
    pub fn subscribe(&self, type_filter: Option<Vec<String>>) -> EventSubscriber;
    pub fn get_latest(&self, limit: usize, type_filter: Option<Vec<String>>) -> Vec<EventEnvelope>;
}
```

---

## Event Sink Trait

```rust
// crates/api/src/sink.rs
#[async_trait]
pub trait EventSink: Send + Sync {
    fn name(&self) -> &str;
    async fn send(&self, events: &[EventEnvelope]) -> Result<SinkResult>;
    async fn flush(&self) -> Result<()>;
    fn filter(&self) -> &Option<HashSet<String>>;
    fn batch_size(&self) -> usize;
}

pub struct SinkResult {
    pub succeeded: usize,
    pub failed: usize,
    pub retry_after_ms: Option<u64>,
}
```

---

## Sink Manager

```rust
// crates/api/src/sink/manager.rs
pub struct SinkManager {
    sinks: Vec<Arc<dyn EventSink>>,
    event_bus: EventBus,
    join_handles: Vec<JoinHandle<()>>,
}

impl SinkManager {
    pub fn new(event_bus: EventBus) -> Self;
    pub fn register_sink(&mut self, sink: Arc<dyn EventSink>);
    pub fn start(&mut self);
}
```

---

## 内置 Sink 实现

### File Sink

```rust
pub struct FileSink {
    name: String,
    path: PathBuf,
    filter: Option<HashSet<String>>,
    file: Arc<Mutex<File>>,
    rotate_config: Option<RotateConfig>,
}
```

### HTTP Sink

```rust
pub struct HttpSink {
    name: String,
    url: String,
    filter: Option<HashSet<String>>,
    client: reqwest::Client,
    retry_config: RetryConfig,
    headers: HashMap<String, String>,
}
```

---

## 配置模型

```rust
// crates/config/src/api.rs
#[derive(Debug, Clone, Deserialize)]
pub struct EventSinkConfig {
    pub tag: String,
    #[serde(rename = "type")]
    pub sink_type: String,
    pub events: Option<Vec<String>>,
    #[serde(flatten)]
    pub config: serde_json::Value,
}
```

---

## 与 zero-engine 集成

```rust
// crates/engine/src/events.rs
pub struct EngineEventProducer {
    event_bus: EventBus,
}

impl EngineEventProducer {
    pub fn new(event_bus: EventBus) -> Self;
    pub fn on_flow_completed(&self, flow: &FlowInfo);
    pub fn on_policy_selected(&self, policy_tag: &str, previous: &str, current: &str);
    pub fn on_probe_completed(&self, policy_tag: &str, results: Vec<ProbeResult>);
}
```

---

## 实现顺序

1. **Phase 1**: 核心类型
   - EventEnvelope 和 EventPayload 定义
   - EventBus 基础实现
   - EventSource trait 定义

2. **Phase 2**: Engine 集成
   - EngineEventProducer 实现
   - flow.completed 事件发布
   - policy.selected 事件发布

3. **Phase 3**: Sink 框架
   - EventSink trait 定义
   - SinkManager 实现
   - FileSink 实现

4. **Phase 4**: HTTP Sink
   - HttpSink 实现
   - 重试和退避策略
   - 批量发送

5. **Phase 5**: SSE 集成
   - HTTP Adapter 接入 EventSource
   - /api/v1/events/stream 端点
   - 断点续传支持

6. **Phase 6**: 测试
   - 事件发布和订阅测试
   - Sink 投递测试
   - 错误处理和重试测试
