//! Effective values project the same actual owners without preallocation.
use super::*;

impl RuntimeOptionsConfig {
    pub fn effective_raw_ip_max_tcp_connections(&self) -> usize {
        self.raw_ip_max_tcp_connections.unwrap_or(4096)
    }
    pub fn effective_raw_ip_max_udp_associations(&self) -> usize {
        self.raw_ip_max_udp_associations.unwrap_or(1024)
    }
    pub fn effective_raw_ip_max_dns_queries(&self) -> usize {
        self.raw_ip_max_dns_queries.unwrap_or(256)
    }
    pub fn effective_raw_ip_max_icmp_probes(&self) -> usize {
        self.raw_ip_max_icmp_probes.unwrap_or(64)
    }
    pub fn effective_system_tun_max_tcp_dns_connections(&self) -> usize {
        self.system_tun_max_tcp_dns_connections.unwrap_or(256)
    }
    pub fn effective_packet_return_max_routes(&self) -> usize {
        self.packet_return_max_routes.unwrap_or(1024)
    }
    pub fn effective_packet_return_max_conversations(&self) -> usize {
        self.packet_return_max_conversations.unwrap_or(8192)
    }
    pub fn effective_packet_return_max_observations(&self) -> usize {
        self.packet_return_max_observations.unwrap_or(4096)
    }
    pub fn effective_packet_windows_ring_capacity(&self) -> usize {
        self.packet_windows_ring_capacity
            .unwrap_or(64 * 1024 * 1024)
    }
    pub fn effective_packet_windows_ring_total_max_bytes(&self) -> usize {
        self.packet_windows_ring_total_max_bytes
            .unwrap_or(128 * 1024 * 1024)
    }
    pub fn effective_control_max_in_flight_request_bytes(&self) -> usize {
        self.control_max_in_flight_request_bytes.unwrap_or(67108864)
    }
    pub fn effective_control_max_in_flight_response_bytes(&self) -> usize {
        self.control_max_in_flight_response_bytes
            .unwrap_or(67108864)
    }
    pub fn effective_control_ipc_max_frame_bytes(&self) -> usize {
        self.control_ipc_max_frame_bytes.unwrap_or(67108864)
    }
    pub fn effective_control_http_max_header_bytes(&self) -> usize {
        self.control_http_max_header_bytes.unwrap_or(8192)
    }
    pub fn effective_control_http_max_body_bytes(&self) -> usize {
        self.control_http_max_body_bytes.unwrap_or(262144)
    }
    pub fn effective_control_max_response_bytes(&self) -> usize {
        self.control_max_response_bytes.unwrap_or(67108864)
    }
    pub fn effective_control_ipc_max_pending_commands(&self) -> usize {
        self.control_ipc_max_pending_commands.unwrap_or(32)
    }
    pub fn effective_control_max_pending_commands(&self) -> usize {
        self.control_max_pending_commands.unwrap_or(1024)
    }
    pub fn effective_control_ipc_max_event_frames(&self) -> usize {
        self.control_ipc_max_event_frames.unwrap_or(32)
    }

    pub fn effective_control_pipe_max_workspace_bytes(&self) -> usize {
        self.control_pipe_max_workspace_bytes
            .unwrap_or(2 * 1024 * 1024)
    }
    pub fn effective_inbound_max_accepted_sockets(&self) -> usize {
        self.inbound_max_accepted_sockets.unwrap_or(4096)
    }

    pub fn effective_inbound_max_datagram_associations(&self) -> usize {
        self.inbound_max_datagram_associations.unwrap_or(4096)
    }

    pub fn effective_inbound_max_datagram_peers(&self, actual_default: usize) -> usize {
        self.inbound_max_datagram_peers.unwrap_or(actual_default)
    }

    pub fn effective_inbound_max_packets_per_peer(&self, actual_default: usize) -> usize {
        self.inbound_max_packets_per_peer.unwrap_or(actual_default)
    }

    pub fn effective_inbound_max_pending_sends(&self, actual_default: usize) -> usize {
        self.inbound_max_pending_sends.unwrap_or(actual_default)
    }

    pub fn effective_inbound_max_bind_attempts(&self) -> usize {
        self.inbound_max_bind_attempts.unwrap_or(50)
    }

    pub fn effective_inbound_max_target_sockets(&self) -> usize {
        self.inbound_max_target_sockets.unwrap_or(4096)
    }

    pub fn effective_inbound_max_file_sessions(&self) -> usize {
        self.inbound_max_file_sessions.unwrap_or(16)
    }

    pub fn effective_inbound_max_handoff_probe_attempts(&self) -> usize {
        self.inbound_max_handoff_probe_attempts.unwrap_or(1000)
    }

    pub fn effective_direct_max_originals(&self) -> usize {
        self.direct_max_originals.unwrap_or(4096)
    }
    pub fn effective_direct_max_tasks(&self) -> usize {
        self.direct_max_tasks.unwrap_or(256)
    }
    pub fn effective_process_max_jobs(&self) -> usize {
        self.process_max_jobs.unwrap_or(64)
    }
    pub fn effective_process_max_jobs_per_instance(&self) -> usize {
        self.process_max_jobs_per_instance.unwrap_or(1)
    }
    pub fn effective_carrier_max_total_originals(&self) -> usize {
        self.carrier_max_total_originals.unwrap_or(1024)
    }
    pub fn effective_component_total_max_tasks(&self) -> usize {
        self.component_total_max_tasks.unwrap_or(4096)
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
    pub fn effective_packet_transmit_total_max_frames(&self) -> usize {
        self.packet_transmit_total_max_frames.unwrap_or(16384)
    }
    pub fn effective_packet_transmit_total_max_bytes(&self) -> usize {
        self.packet_transmit_total_max_bytes.unwrap_or(67108864)
    }
    pub fn effective_packet_native_read_total_max_frames(&self) -> usize {
        self.packet_native_read_total_max_frames.unwrap_or(256)
    }
    pub fn effective_packet_native_read_total_max_bytes(&self) -> usize {
        self.packet_native_read_total_max_bytes.unwrap_or(16777216)
    }
    pub fn effective_packet_driver_metadata_total_max_allocations(&self) -> usize {
        self.packet_driver_metadata_total_max_allocations
            .unwrap_or(16384)
    }
    pub fn effective_packet_driver_metadata_total_max_bytes(&self) -> usize {
        self.packet_driver_metadata_total_max_bytes
            .unwrap_or(16777216)
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
    pub fn effective_metadata_max_probe_sockets(&self, actual_default: usize) -> usize {
        self.metadata_max_probe_sockets.unwrap_or(actual_default)
    }
    pub fn effective_metadata_max_probe_attempts(&self, actual_default: usize) -> usize {
        self.metadata_max_probe_attempts.unwrap_or(actual_default)
    }
    pub fn effective_metadata_max_send_calls(&self, actual_default: usize) -> usize {
        self.metadata_max_send_calls.unwrap_or(actual_default)
    }
    pub fn effective_quic_source_max_connections(&self) -> usize {
        self.quic_source_max_connections.unwrap_or(8192)
    }
    pub fn effective_quic_shared_endpoint_max_users(&self) -> usize {
        self.quic_shared_endpoint_max_users.unwrap_or(65536)
    }
    pub fn effective_quic_max_queue_packets(&self) -> usize {
        self.quic_max_queue_packets.unwrap_or(256)
    }
    pub fn effective_dns_wire_total_max_buffered_bytes(&self) -> usize {
        self.dns_wire_total_max_buffered_bytes
            .unwrap_or(16 * 1024 * 1024)
    }

    pub fn effective_quic_total_max_buffered_bytes(&self) -> usize {
        self.quic_total_max_buffered_bytes
            .unwrap_or(64 * 1024 * 1024)
    }
    pub fn effective_inbound_max_tasks_per_carrier(&self, actual_default: usize) -> usize {
        self.inbound_max_tasks_per_carrier.unwrap_or(actual_default)
    }
    pub fn effective_inbound_max_resources_per_carrier(&self, actual_default: usize) -> usize {
        self.inbound_max_resources_per_carrier
            .unwrap_or(actual_default)
    }
    pub fn effective_inbound_max_in_flight_requests(&self, actual_default: usize) -> usize {
        self.inbound_max_in_flight_requests
            .unwrap_or(actual_default)
    }
    pub fn effective_hysteria2_h3_max_pending_classifications(&self) -> usize {
        self.hysteria2_h3_max_pending_classifications.unwrap_or(128)
    }
    pub fn effective_hysteria2_h3_max_pending_tcp_streams(&self) -> usize {
        self.hysteria2_h3_max_pending_tcp_streams.unwrap_or(128)
    }
    pub fn effective_hysteria2_h3_max_pending_web_streams(&self) -> usize {
        self.hysteria2_h3_max_pending_web_streams.unwrap_or(128)
    }
    pub fn effective_hysteria2_h3_max_pending_web_requests(&self) -> usize {
        self.hysteria2_h3_max_pending_web_requests.unwrap_or(64)
    }
    pub fn effective_event_log_stats_max_bytes(&self) -> usize {
        self.event_log_stats_max_bytes.unwrap_or(8 * 1024 * 1024)
    }
    pub fn effective_event_subscriber_stats_max_bytes(&self) -> usize {
        self.event_subscriber_stats_max_bytes.unwrap_or(1024 * 1024)
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

impl RuntimeOptionsConfig {
    pub fn effective_control_grpc_max_requests(&self) -> usize {
        self.control_grpc_max_requests.unwrap_or(1024)
    }
    pub fn effective_control_grpc_max_streams_per_connection(&self) -> usize {
        self.control_grpc_max_streams_per_connection.unwrap_or(32)
    }
    pub fn effective_control_grpc_max_request_bytes(&self) -> usize {
        self.control_grpc_max_request_bytes
            .unwrap_or(4 * 1024 * 1024)
    }
}

impl RuntimeOptionsConfig {
    pub fn effective_control_sse_max_event_frames(&self) -> usize {
        self.control_sse_max_event_frames.unwrap_or(32)
    }
}

mod subscriber;
