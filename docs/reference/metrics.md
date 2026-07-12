# Metrics and alarms reference

The adapter publishes the standard per-instance `southbound_health` measure with low-cardinality instance
identity. It contains `connectionState`, `publishLatencyMs`, `pollLatencyMs`, `readErrors`, `staleSignals`,
and `reconnects`. `readErrors` and `reconnects` are interval counters; latency values are last-observation
gauges. A camera becomes stale when no successful observation occurs inside
`healthThresholds.staleSignalSecs` (default 300 seconds).

Readiness is a component gate, not a claim that every camera is online. It requires validated
configuration, recovered catalog, usable output, active acknowledged command subscription, constructed
supervisors, at least one accepted enabled camera, available state capacity, and no shutdown.

| Alarm | Severity | Raised when | Cleared when |
|---|---|---|---|
| `storage-low` | critical | Output or state root is unreadable or falls below the configured free-space floor. | Every configured root is usable again. |
| `message-delivery-delayed` | warning | Durable terminal outbox pressure crosses its threshold. | The outbox recovers. |

Alarm context carries bounded storage/free-space or outbox-age/count information. It intentionally excludes
camera URLs, file paths beyond the affected root, credentials, request metadata, and arbitrary camera error
text. Capture-level outcomes belong in terminal application messages rather than metrics dimensions.
