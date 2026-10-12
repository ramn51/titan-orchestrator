# Cross-host data-path benchmark

This suite forces producers onto a `HOST_A` worker and consumers onto a
`GENERAL` worker. It verifies three distinct behaviors:

1. TitanStore transports a deterministic payload with matching SHA-256.
2. Master asset upload/fetch transports the same payload with matching SHA-256.
3. A host-local file is not visible on the other worker host.

The intended two-VM topology runs the `HOST_A` worker beside the master and
store, so results prove correctness but are not a clean worker-scaling result.
