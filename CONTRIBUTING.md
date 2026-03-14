# Contributing

## Local Test Policy

Before opening a PR:

1. run the local quick gate first;
2. then run the full gate (or rely on CI full/stress follow-up if explicitly agreed for large runs).

Commands:

```bash
scripts/test/quick.sh
scripts/test/full.sh
```

`quick.sh`:
- configures/builds tests
- runs CTest label `smoke`
- intended to stay fast (target: laptop-friendly pre-push gate)

`full.sh`:
- configures/builds tests
- runs `smoke` + `stress`
- supports stress repeat via `STRESS_REPEAT=<n>`

## CI Policy

- PR CI path uses the quick gate (`smoke`) as the mandatory signal.
- Stress is tracked separately in the stress-flake workflow.
- TLS build/smoke validation remains a dedicated CI job.

## Scope Discipline

- Keep alpha line focused on stabilization, validation, and transparent measurement.
- Avoid scope creep into unrelated major features in hardening iterations.
