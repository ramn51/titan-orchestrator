# Data-flow correctness benchmark

This suite verifies that dependent Titan tasks consume and transform real bytes,
not only that their scripts run in dependency order. It covers chain, diamond,
fan-out/fan-in and layered DAGs. Each pattern runs five times; pass five injects
one intentional nonzero exit and requires Titan to retry the stage successfully.

Each run creates a deterministic payload in the worker's shared workspace,
passes file names through job arguments, transforms the data in dependent tasks,
and checks the final SHA-256 against a separately computed model. The verifier
writes `verification.json` beneath its job artifact directory and emits the same
record to its task log.

This proves worker-workspace data flow on one worker VM. It does not claim
automatic artifact transfer between separate worker machines; that requires
shared storage, object storage or an explicit TitanStore data path.
