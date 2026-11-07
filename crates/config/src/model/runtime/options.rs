//! Flat compatible runtime options; owner-limit defaults remain in their modules.
use super::*;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeOptionsConfig {
    /// Actual Source accepted sockets; None preserves the producer default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbound_max_accepted_sockets: Option<usize>,
    /// Actual Source datagram associations; None preserves the producer default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbound_max_datagram_associations: Option<usize>,
    /// Actual Source datagram peers; None preserves the producer default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbound_max_datagram_peers: Option<usize>,
    /// Actual Source packets per peer; None preserves the producer default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbound_max_packets_per_peer: Option<usize>,
    /// Actual Source pending sends; None preserves the producer default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbound_max_pending_sends: Option<usize>,
    /// Actual Source bind attempts; None preserves the producer default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbound_max_bind_attempts: Option<usize>,
    /// Actual Source target sockets; None preserves the producer default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbound_max_target_sockets: Option<usize>,
    /// Actual Source file sessions; None preserves the producer default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbound_max_file_sessions: Option<usize>,
    /// Actual Source handoff probe attempts; None preserves the producer default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbound_max_handoff_probe_attempts: Option<usize>,
    /// On-demand pending classifications per actual inbound HY2 HTTP/3 connection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hysteria2_h3_max_pending_classifications: Option<usize>,
    /// On-demand pending tcp streams per actual inbound HY2 HTTP/3 connection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hysteria2_h3_max_pending_tcp_streams: Option<usize>,
    /// On-demand pending web streams per actual inbound HY2 HTTP/3 connection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hysteria2_h3_max_pending_web_streams: Option<usize>,
    /// On-demand pending web requests per actual inbound HY2 HTTP/3 connection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hysteria2_h3_max_pending_web_requests: Option<usize>,
    /// Maximum same-original accepted/peer tasks. Omission retains the actual protocol default; no Scope or worker is allocated from the number alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbound_max_tasks_per_carrier: Option<usize>,
    /// Auxiliary resources in the same actual accepted/peer graph. Zero grants none; omitted defaults differ across protocols.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbound_max_resources_per_carrier: Option<usize>,
    /// Concurrent real logical requests per multiplexed carrier. Sequential HTTP framing remains structurally one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbound_max_in_flight_requests: Option<usize>,
    /// Concurrent capability-probe socket originals per actual Metadata owner; omitted keeps that owner's existing default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_max_probe_sockets: Option<usize>,
    /// Total native capability-probe attempts per actual Metadata initialization; not a per-socket refund.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_max_probe_attempts: Option<usize>,
    /// Concurrent whole synchronous sends per actual Metadata original; failed/incomplete calls keep their permits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_max_send_calls: Option<usize>,
    /// On-demand connection drivers per actual QUIC Source. Default8192; original endpoint and mask drivers are additional structural work.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quic_source_max_connections: Option<usize>,
    /// Sealed users across one shared QUIC endpoint. Default65536. Single-query DoQ retains its structural three-user bound.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quic_shared_endpoint_max_users: Option<usize>,
    /// Actual packet slots per existing QUIC/Mask FIFO. Default256, on demand.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quic_max_queue_packets: Option<usize>,
    /// Shared actual QUIC/Mask frame, codec/copy and receive workspace bytes
    /// in the same carrier root. Default64MiB; pending/retiring storage counts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quic_total_max_buffered_bytes: Option<usize>,
    /// Per-file bound for component-owned file snapshots. Omission keeps the legacy exported config shape; production readers use 8 MiB by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_read_max_bytes: Option<usize>,
    /// On-demand physical carriers per native MUX pool. Omission preserves the exported configuration shape and uses a conservative four-carrier cap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_mux_max_carriers: Option<usize>,
    /// Actual native or borrowed original slots per taskless Stream role. Four by default, shared by its pending and delivered carrier uses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_stream_max_originals: Option<usize>,
    /// Actual TCP originals per Direct component generation, including pending,
    /// failed and retiring candidates. On demand; default4096.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direct_max_originals: Option<usize>,
    /// Actual Direct initialization/refresh tasks in the same original Scope.
    /// On demand; default256. Separate from the shared carrier total.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direct_max_tasks: Option<usize>,
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
    /// Prepaid transmit allocations across the same kernel Packet memory root,
    /// including queued, retransmitted and retiring packets; allocated on demand.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub packet_transmit_total_max_frames: Option<usize>,
    /// Original transmit allocation bytes and unconsumed framing/fragmentation
    /// promises across that same root, never a separate per-device byte pool.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub packet_transmit_total_max_bytes: Option<usize>,
    /// Original native read/decoder/workspace frame promises across the same
    /// kernel Packet memory root, including pending and retiring allocations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub packet_native_read_total_max_frames: Option<usize>,
    /// Original native read/workspace allocation bytes across that same root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub packet_native_read_total_max_bytes: Option<usize>,
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
    /// Noncached preparation jobs in this RuntimeHost's actual original book.
    /// Pending, failed and retiring jobs remain charged; default64, range1..64.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub navigation_max_jobs: Option<usize>,
    /// Outstanding requests/body futures across those same owned jobs; default32.
    /// This bounds the existing4MiB-per-response payload, not total process RSS.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub navigation_max_requests: Option<usize>,
    /// Actual kernel PacketRoute originals, including closing routes until
    /// their execution lease and response worker both return. Default4096.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub packet_route_max_routes: Option<usize>,
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
    /// Serialized statistics event bytes retained by this Engine's original
    /// history. Eight MiB by default; not a budget for every event or RSS.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_log_stats_max_bytes: Option<usize>,
    /// Pending statistics bytes in each original Engine subscriber queue.
    /// One MiB by default; independent of control serialized output custody.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_subscriber_stats_max_bytes: Option<usize>,
    // Same Engine subscriber originals; count includes dequeued/retiring custody.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_subscriber_max_pending_events: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_subscriber_total_max_pending_events: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_subscriber_max_registrations: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_subscriber_total_stats_max_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_subscriber_max_filter_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_subscriber_total_max_storage_bytes: Option<usize>,
    /// Same-kernel Windows pipe payload workspaces, including native pending
    /// read/write and queued buffers. Default2MiB, on demand; not RSS or events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_pipe_max_workspace_bytes: Option<usize>,
    /// Control payload envelope storage; not RSS.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_max_in_flight_request_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_max_in_flight_response_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_ipc_max_frame_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_http_max_header_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_http_max_body_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_max_response_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_ipc_max_pending_commands: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_max_pending_commands: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_ipc_max_event_frames: Option<usize>,
    /// Retained serialized frames per actual SSE connection, including a pending write.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_sse_max_event_frames: Option<usize>,
    /// Actual gRPC stream jobs, retained through original joins and output return.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_grpc_max_requests: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_grpc_max_streams_per_connection: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_grpc_max_request_bytes: Option<usize>,
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
