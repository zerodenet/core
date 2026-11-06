//! Runtime owner limits and production options.
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeOptionsConfig {
    /// Per-file bound for component-owned file snapshots. Omission keeps the
    /// legacy exported config shape; production readers use 8 MiB by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_read_max_bytes: Option<usize>,
    /// On-demand physical carriers per native MUX pool. Omission preserves the
    /// exported configuration shape and uses a conservative four-carrier cap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_mux_max_carriers: Option<usize>,
    /// Actual native or borrowed original slots per taskless Stream role.
    /// Four by default, shared by its pending and delivered carrier uses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_stream_max_originals: Option<usize>,
    /// Actual pending, failed and retiring carrier custody reservations across
    /// this one Host. Default1024, on demand. Distinct from FD/socket count,
    /// per-role N, per-protocol pool N and logical user/task/byte limits.
    /// Each actual local-process metadata lookup is one reservation through
    /// its native cleanup, actual helper join and registration ACK. Linux
    /// owns at most three File originals internally; Windows at most one
    /// process HANDLE. These are not Child jobs or a native FD total.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub carrier_max_total_originals: Option<usize>,
    /// Actual native process jobs across this kernel, including pending,
    /// failed and retiring originals. Allocated on demand; 64 by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_max_jobs: Option<usize>,
    /// Concurrent original jobs per actual component generation, including
    /// unreleased task acknowledgements. One by default; not a descendant cap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_max_jobs_per_instance: Option<usize>,
    /// Total live or unconfirmed physical carriers in the one Mieru adapter
    /// pool, across every profile and both underlays. Four by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mieru_max_carriers: Option<usize>,
    /// Total original native/borrowed carriers per actual VMess gRPC Node,
    /// shared by TCP, UDP and Service opens, including pending and retiring.
    /// On demand, 4096 by default; separate from logical UDP association limits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vmess_grpc_max_originals: Option<usize>,
    /// Original Packet return allocations per device, including held/retiring buffers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub packet_return_max_frames: Option<usize>,
    /// Original Packet return payload bytes per device, retained until allocation release.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub packet_return_max_bytes: Option<usize>,
    /// Original Packet return allocations across the same actual device root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub packet_return_total_max_frames: Option<usize>,
    /// Original Packet return payload bytes across the same actual device root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub packet_return_total_max_bytes: Option<usize>,
    /// Authenticated UDP2022 fixed replay windows in the same SS adapter; live history is never evicted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ss_udp_replay_max_windows: Option<usize>,
    /// Actual retained UDP sender session IDs and counters across the same SS adapter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ss_udp_max_senders: Option<usize>,
    /// Authenticated UDP associations across the same SS adapter, including retiring originals.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ss_udp_max_associations: Option<usize>,
    /// Actual UDP return bindings across the same SS adapter; one association can have several targets.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ss_udp_max_return_bindings: Option<usize>,
    /// Successfully authenticated UDP user codecs retained by the same SS adapter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ss_udp_max_authenticated_codecs: Option<usize>,
    /// Response registrations in each actual SS cached Flow, including send
    /// Pending, failed/retiring and received-but-not-consumed responses.
    /// On demand,4096 by default; distinct from native originals and replay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ss_udp_max_pending_responses: Option<usize>,
    /// Same response registrations across the actual SS adapter's one pending
    /// book, shared by every cached Flow/consumer.4096 by default, on demand.
    /// Local and adapter bounds both apply; this is a count, not RSS/FD/bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ss_udp_total_max_pending_responses: Option<usize>,
    /// Actual physical carriers in one cached gRPC pool across every binding and retirement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grpc_pool_max_carriers: Option<usize>,
    /// Actual logical streams in one cached gRPC pool including pending and retiring streams.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grpc_pool_max_streams: Option<usize>,
    /// Actual physical carriers across the same gRPC registry, including pending, failed and retiring originals.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grpc_total_max_carriers: Option<usize>,
    /// Actual logical streams across the same gRPC registry until worker and view return.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grpc_total_max_streams: Option<usize>,
    /// Actual diagnostic and health probes per kernel, including pending and retiring originals; the process-wide ceiling remains eight.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic_max_operations: Option<usize>,
    /// Tightens the same System TUN Source's total task bound, including real
    /// main/route/stack/query workers. Omission retains the actual declared sum.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_tun_task_limit: Option<usize>,
    /// Actual allocated legacy SS Bloom bitmap bytes across this adapter's
    /// original books. On demand, 64 MiB by default; valid history is retained.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ss_legacy_replay_max_bytes: Option<usize>,
    /// Total retained TCP2022 replay salts in this actual SS adapter, across
    /// all inbound/outbound histories and per-profile capacity views. On demand,
    /// 500000 by default; live security history is never evicted for admission.
    /// UDP/Bloom replay owners have separate resource accounting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ss_tcp_replay_max_salts: Option<usize>,
    /// Live HY2 pool entries plus retained original failure groups, across
    /// every profile. On demand, 256 by default; failures are never evicted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hysteria2_max_entries: Option<usize>,
    /// Actual UDP logical registrations per authenticated HY2 pool entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hysteria2_max_udp_sessions: Option<usize>,
    /// Queued and fragmented UDP payload bytes shared by all registrations of
    /// one actual authenticated HY2 entry. Four MiB by default, on demand.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hysteria2_udp_pending_bytes: Option<usize>,
    /// Total active, retiring or unconfirmed control connections across the
    /// actual HTTP, IPC and gRPC root. Allocated on demand, 256 by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_max_connections: Option<usize>,
    /// Actual event bridge/writer subscriptions shared by the same control root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_max_subscriptions: Option<usize>,
    /// Retained serialized event payload bytes across the same control root's
    /// adapter queues and IPC/SSE pending writes. One MiB by default. gRPC
    /// releases on Tonic handoff; physical H2 buffers and replay are separate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_max_queued_event_bytes: Option<usize>,
    /// Actual event-network Scope tasks, including connecting and blocking
    /// lookup workers until their original joins/acknowledgements. Default1024.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_network_max_tasks: Option<usize>,
    /// Actual pending, failed and retiring event-network TCP originals per
    /// generation, separate from logical requests and the shared Host total.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_network_max_originals: Option<usize>,
    /// Actual blocking event-network lookups until original join/ACK.
    /// Default256, on demand, independently constrained by the Scope task cap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_network_max_lookups: Option<usize>,
    /// Original DNS custody shared by Node/bootstrap and Default query owners.
    /// An unconfirmed close still consumes one position; DoQ needs two.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_dns_max_originals: Option<usize>,
    /// Actual registered DNS query tasks shared by both purpose owners.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_dns_max_queries: Option<usize>,
    /// Actual blocking DNS jobs, retained through their original task join.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_dns_max_blocking_jobs: Option<usize>,
    /// Actual DNS H2 connection drivers, retained through their original join.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_dns_max_h2_drivers: Option<usize>,
    /// Native Direct egress originals per standard accepted UDP association.
    /// On demand, 64 by default; omission preserves the legacy export shape.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub udp_direct_max_sockets: Option<usize>,
    #[serde(default = "default_udp_upstream_idle_timeout_seconds")]
    pub udp_upstream_idle_timeout_seconds: u64,
    /// Maximum number of runtime events retained for replay.
    #[serde(default = "default_event_log_capacity")]
    pub event_log_capacity: usize,
    /// Global URL used by end-to-end outbound latency probes.
    #[serde(default)]
    pub latency_test_url: Option<String>,
    /// Optional crash-recovery snapshot for shared principal quota balances.
    /// Relative paths resolve beside the runtime config.
    #[serde(default)]
    pub principal_quota_state_path: Option<String>,
    #[serde(default)]
    pub network: NetworkOptionsConfig,
    #[serde(default)]
    pub udp: UdpPolicyConfig,
    #[serde(default)]
    pub log: LogConfig,
    /// Optional DNS subsystem configuration. Omit for system resolver.
    #[serde(default)]
    pub dns: Option<DnsConfig>,
    /// Optional declarative TUN inbound managed by the proxy lifecycle.
    #[serde(default)]
    pub tun: Option<TunConfig>,
}

impl Default for RuntimeOptionsConfig {
    fn default() -> Self {
        Self {
            file_read_max_bytes: None,
            native_mux_max_carriers: None,
            native_stream_max_originals: None,
            carrier_max_total_originals: None,
            process_max_jobs: None,
            process_max_jobs_per_instance: None,
            mieru_max_carriers: None,
            vmess_grpc_max_originals: None,
            packet_return_max_frames: None,
            packet_return_max_bytes: None,
            packet_return_total_max_frames: None,
            packet_return_total_max_bytes: None,
            ss_udp_replay_max_windows: None,
            ss_udp_max_senders: None,
            ss_udp_max_associations: None,
            ss_udp_max_return_bindings: None,
            ss_udp_max_authenticated_codecs: None,
            ss_udp_max_pending_responses: None,
            ss_udp_total_max_pending_responses: None,
            grpc_pool_max_carriers: None,
            grpc_pool_max_streams: None,
            grpc_total_max_carriers: None,
            grpc_total_max_streams: None,
            diagnostic_max_operations: None,
            system_tun_task_limit: None,
            ss_legacy_replay_max_bytes: None,
            ss_tcp_replay_max_salts: None,
            hysteria2_max_entries: None,
            hysteria2_max_udp_sessions: None,
            hysteria2_udp_pending_bytes: None,
            control_max_connections: None,
            control_max_subscriptions: None,
            control_max_queued_event_bytes: None,
            event_network_max_tasks: None,
            event_network_max_originals: None,
            event_network_max_lookups: None,
            native_dns_max_originals: None,
            native_dns_max_queries: None,
            native_dns_max_blocking_jobs: None,
            native_dns_max_h2_drivers: None,
            udp_direct_max_sockets: None,
            udp_upstream_idle_timeout_seconds: default_udp_upstream_idle_timeout_seconds(),
            event_log_capacity: default_event_log_capacity(),
            latency_test_url: None,
            principal_quota_state_path: None,
            network: NetworkOptionsConfig::default(),
            udp: UdpPolicyConfig::default(),
            log: LogConfig::default(),
            dns: None,
            tun: None,
        }
    }
}

pub const DEFAULT_LATENCY_TEST_URL: &str = "http://www.gstatic.com/generate_204";
pub const DEFAULT_EVENT_LOG_CAPACITY: usize = 1024;

impl RuntimeOptionsConfig {
    pub fn effective_process_max_jobs(&self) -> usize {
        self.process_max_jobs.unwrap_or(64)
    }
    pub fn effective_process_max_jobs_per_instance(&self) -> usize {
        self.process_max_jobs_per_instance.unwrap_or(1)
    }
    pub fn effective_carrier_max_total_originals(&self) -> usize {
        self.carrier_max_total_originals.unwrap_or(1024)
    }
    pub fn effective_native_stream_max_originals(&self) -> usize {
        self.native_stream_max_originals.unwrap_or(4)
    }
    pub fn effective_packet_return_max_frames(&self) -> usize {
        self.packet_return_max_frames.unwrap_or(256)
    }
    pub fn effective_packet_return_max_bytes(&self) -> usize {
        self.packet_return_max_bytes.unwrap_or(16777216)
    }
    pub fn effective_packet_return_total_max_frames(&self) -> usize {
        self.packet_return_total_max_frames.unwrap_or(1024)
    }
    pub fn effective_packet_return_total_max_bytes(&self) -> usize {
        self.packet_return_total_max_bytes.unwrap_or(67108864)
    }
    pub fn effective_ss_udp_replay_max_windows(&self) -> usize {
        self.ss_udp_replay_max_windows.unwrap_or(8192)
    }
    pub fn effective_ss_udp_max_senders(&self) -> usize {
        self.ss_udp_max_senders.unwrap_or(8192)
    }
    pub fn effective_ss_udp_max_associations(&self) -> usize {
        self.ss_udp_max_associations.unwrap_or(8192)
    }
    pub fn effective_ss_udp_max_return_bindings(&self) -> usize {
        self.ss_udp_max_return_bindings.unwrap_or(65536)
    }
    pub fn effective_ss_udp_max_pending_responses(&self) -> usize {
        self.ss_udp_max_pending_responses.unwrap_or(4096)
    }
    pub fn effective_ss_udp_total_max_pending_responses(&self) -> usize {
        self.ss_udp_total_max_pending_responses.unwrap_or(4096)
    }
    pub fn effective_ss_udp_max_authenticated_codecs(&self) -> usize {
        self.ss_udp_max_authenticated_codecs.unwrap_or(4096)
    }
    pub fn effective_grpc_pool_max_carriers(&self) -> usize {
        self.grpc_pool_max_carriers.unwrap_or(4)
    }
    pub fn effective_grpc_pool_max_streams(&self) -> usize {
        self.grpc_pool_max_streams.unwrap_or(256)
    }
    pub fn effective_grpc_total_max_carriers(&self) -> usize {
        self.grpc_total_max_carriers.unwrap_or(16)
    }
    pub fn effective_grpc_total_max_streams(&self) -> usize {
        self.grpc_total_max_streams.unwrap_or(1024)
    }
    pub fn effective_diagnostic_max_operations(&self) -> usize {
        self.diagnostic_max_operations.unwrap_or(8)
    }
    pub fn effective_system_tun_task_limit(&self, actual_default: usize) -> usize {
        self.system_tun_task_limit
            .unwrap_or(actual_default)
            .min(actual_default)
    }
    pub fn effective_ss_legacy_replay_max_bytes(&self) -> usize {
        self.ss_legacy_replay_max_bytes.unwrap_or(64 * 1024 * 1024)
    }
    pub fn effective_ss_tcp_replay_max_salts(&self) -> usize {
        self.ss_tcp_replay_max_salts.unwrap_or(500_000)
    }
    pub fn effective_vmess_grpc_max_originals(&self) -> usize {
        self.vmess_grpc_max_originals.unwrap_or(4096)
    }
    pub fn effective_mieru_max_carriers(&self) -> usize {
        self.mieru_max_carriers.unwrap_or(4)
    }
    pub fn effective_hysteria2_max_entries(&self) -> usize {
        self.hysteria2_max_entries.unwrap_or(256)
    }
    pub fn effective_hysteria2_max_udp_sessions(&self) -> usize {
        self.hysteria2_max_udp_sessions.unwrap_or(4096)
    }
    pub fn effective_hysteria2_udp_pending_bytes(&self) -> usize {
        self.hysteria2_udp_pending_bytes.unwrap_or(4 * 1024 * 1024)
    }
    pub fn effective_control_max_connections(&self) -> usize {
        self.control_max_connections.unwrap_or(256)
    }
    pub fn effective_control_max_subscriptions(&self) -> usize {
        self.control_max_subscriptions.unwrap_or(256)
    }
    pub fn effective_control_max_queued_event_bytes(&self) -> usize {
        self.control_max_queued_event_bytes.unwrap_or(1024 * 1024)
    }
    pub fn effective_event_network_max_tasks(&self) -> usize {
        self.event_network_max_tasks.unwrap_or(1024)
    }
    pub fn effective_event_network_max_originals(&self) -> usize {
        self.event_network_max_originals.unwrap_or(256)
    }
    pub fn effective_event_network_max_lookups(&self) -> usize {
        self.event_network_max_lookups.unwrap_or(256)
    }
    pub fn effective_native_dns_max_originals(&self) -> usize {
        self.native_dns_max_originals.unwrap_or(16)
    }
    pub fn effective_native_dns_max_queries(&self) -> usize {
        self.native_dns_max_queries.unwrap_or(16)
    }
    pub fn effective_native_dns_max_blocking_jobs(&self) -> usize {
        self.native_dns_max_blocking_jobs.unwrap_or(16)
    }
    pub fn effective_native_dns_max_h2_drivers(&self) -> usize {
        self.native_dns_max_h2_drivers.unwrap_or(16)
    }
    pub fn effective_udp_direct_max_sockets(&self) -> usize {
        self.udp_direct_max_sockets.unwrap_or(64)
    }
    pub fn effective_native_mux_max_carriers(&self) -> usize {
        self.native_mux_max_carriers.unwrap_or(4)
    }

    pub fn effective_file_read_max_bytes(&self) -> usize {
        self.file_read_max_bytes.unwrap_or(8 * 1024 * 1024)
    }

    pub fn effective_latency_test_url(&self) -> &str {
        self.latency_test_url_or(None)
    }

    pub fn latency_test_url_or<'a>(&'a self, fallback: Option<&'a str>) -> &'a str {
        self.latency_test_url
            .as_deref()
            .or(fallback)
            .unwrap_or(DEFAULT_LATENCY_TEST_URL)
    }
}
