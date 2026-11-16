# macOS plugin execution in a Linux VM

Status: selected by the owner on 2026-11-16; production provider unfinished.
Apple Developer enrollment and Endpoint Security are no longer prerequisites
for the selected implementation. The existing ES probe remains optional and
outside production. This document is not native-close or runtime evidence.

The local Docker Desktop Linux VM is available as a development transport.
Its engine was started without building an image or running a container.
The runtime provider must bind the actual selected local engine/VM identity;
a disconnected daemon, a changed Docker context or a restarted VM invalidates
admission and retains the original job's unresolved cleanup. A VM is shared
infrastructure, not one VM per session and not a second Zero proxy instance.

## Ownership and launch

Reuse the existing Linux process owner, helper, subreaper/pidfd and derived
endpoint authorization rather than adding a process-group cleanup fallback.
One generation receives one private PID namespace and finite cgroup process,
memory and job ceilings. The guest supervisor is namespace init and retains
every actual plugin/descendant and native endpoint before readiness. Do not
share host PID/network namespaces, grant privileged mode, or expose the engine
socket to the plugin. A guest executable must be a Linux binary of the selected
plugin baseline. Never try to execute a host Mach-O binary, infer a substitute
image, download a different plugin version or rewrite its arguments silently.

The host installs one persistent standard Process owner before VM/container
creation, then retains the immutable VM/engine epoch, exact job/container ID,
guest supervisor identity and bounded control-channel originals. Commands carry
that same generation and invocation identity. Persist the creation outcome
before start; a cancelled create response remains an unknown acquisition until
the same provider resolves it. Readiness waits for the guest owner to install
the actual plugin and endpoints, not merely a running VM or CLI exit zero.

The guest launch preserves the existing protocol-owned arguments, environment,
working directory and startup/error semantics. Secrets travel only over the
owned bounded control input, never command-line strings, image layers, labels
or logs. Host file inputs require explicit bounded snapshot transfer; a blanket
host-directory or Docker-socket mount is not an implementation shortcut.

## Stream/Datagram integration

Expose actual standard Stream/Datagram providers in the original host catalog.
The guest checks the real process namespace and endpoint owner before granting
an exact TCP/UDP endpoint. The host retains original native transport controllers
and the exact guest users/driver lineage, including dynamic addresses and UDP
associations. Framing and SIP003/SIP003U stay in Shadowsocks; VM transport belongs
to platform. Preserve zero-length datagrams, target changes, half-close, full
dial/egress constraints, network epochs and inner/outer accounting. A published
container port alone is neither endpoint authorization nor proof that the
original host/guest I/O has closed. If the VM cannot enforce a configured dial
policy, preserve the prior compatible path until its real provider is migrated;
never claim a standard role then bypass that policy.

## Confirmed retirement

Revoke admission and guest endpoint leases, drain transferred users and join
the original host drivers. Stop the actual guest process owner and consume its
root wait status and descendant retirement attestation. The host also requires
matching guest endpoint native receipts, the owned VM-channel/launcher close
outcomes and its own actual TaskScope joins. Namespace init exit prevents new
processes in that namespace and the Linux kernel kills remaining members;
this kernel guarantee does not itself prove that host forwarding I/O, a CLI
handle, the guest supervisor wait or Scope debts have been consumed.

Timeout, connection loss, VM restart, task abort, process-group absence, a
container name, a port probe or removal of a metadata row never fabricates those
receipts. Retain the original ID and failure source until the same provider can
settle it. A shared VM remains alive for unaffected jobs. StopOld-before-BuildNew
and the existing host's serial replacement/recovery transaction stay mandatory.

## Remaining implementation and evidence

- Actual host Process/Stream/Datagram factory and VM control/input owners.
- Guest agent reusing the existing Linux original process/endpoint provider.
- Exact binary/image selection, bounded file transfer and dial-policy handling.
- Original namespace/cgroup, channel and endpoint close receipts and fault archive.
- Wiring macOS SIP003/SIP003U to those actual providers, then deleting their old
  claim/Prepared/response-task path; keep existing capability until that point.
- Real fork/double-fork/setsid, interrupted create/start/readiness, daemon loss,
  exact-owner TCP/UDP, network replacement and original receipt regressions.

No production gate is widened and no macOS plugin migration is marked complete.
TEMP-WG-OUTER-UDP-01 runtime acceptance remains separately deferred.

References: [Linux PID namespace lifecycle](https://man7.org/linux/man-pages/man7/pid_namespaces.7.html),
[Docker container run and isolation options](https://docs.docker.com/reference/cli/docker/container/run/).
