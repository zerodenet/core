# Withdrawn macOS plugin VM proposal

Status: withdrawn by the owner on 2026-11-16. This is historical context,
not an active implementation plan or a product requirement.

Requiring a Linux VM would add a runtime dependency and require a Linux build
of an existing native plugin. That deployment and compatibility change is outside
the intended ownership refactor. The preceding VM choice is superseded.

The uncommitted VM Engine, guest primitives, wire code and their regression
sources were removed from the checkout and archived outside the repository.
Their platform module export and serialization dependency were removed as well.
No VM provider was connected to the ordinary catalog or plugin production path;
no plugin image/container was built or run. Existing native plugin behavior and
independent Browser/prefix migration changes are retained.

The active direction is [native plugin lifecycle ownership](macos-plugin-lifecycle.md).
Apple authorization and a VM are not prerequisites. Windows keeps its native
Job Object provider and Linux keeps its native process provider. No native
migration or TEMP-WG runtime acceptance is marked complete by this withdrawal.
