# Data types

The baseline signal value-mapping page — how a protocol value becomes a `SouthboundSignalUpdate` —
does not apply to this adapter. camera-adapter is a **camera**, not a signal adapter: it produces
images, not signals. The image bytes are the data plane and travel as files (delivered by
`file-replicator`); the bus carries only control and terminal metadata.

The data shapes this adapter publishes are documented in the
[messaging interface reference](messaging-interface.md):

- **Capture announcement** — the terminal `app/image/*` application message for a completed capture:
  the capture and group identifiers, the camera summary, the durable output paths (`absolutePath`,
  `relativePath`, `fileUri`), the effective profile, per-stage durations, and any failure summary. See
  [Terminal application messages](messaging-interface.md#terminal-application-messages).
- **Capture thumbnail** — an optional, bounded JPEG preview carried inside the announcement only, as
  native protobuf bytes, never in the durable record. See
  [Capture thumbnail](messaging-interface.md#capture-thumbnail).
- **Metadata sidecar** — the optional on-disk JSON companion written next to the image. See the
  [configuration reference](configuration.md).

Command request/reply bodies (the `sb/*` verbs), the `JobState` vocabulary, and the stable error
codes are likewise specified in the [messaging interface reference](messaging-interface.md).
