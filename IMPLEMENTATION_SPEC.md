# Camera Adapter — Binding Implementation Specification

> **Status:** Accepted implementation addendum  
> **Date:** 2026-07-10  
> **Applies to:** `camera-adapter` and the required EdgeCommons core messaging work  
> **Primary design:** [`DESIGN.md`](DESIGN.md)  
> **Audience:** implementers, reviewers, test authors, release engineers, and operators

## 1. Authority and completion rule

This document is the binding implementation addendum to `DESIGN.md`. It resolves the review questions
and underspecified behavior that would otherwise permit incompatible implementations. `DESIGN.md`
remains binding in full except where this addendum makes a more specific choice. If the two documents
conflict, this addendum controls. Line references in this document refer to the current 2,373-line
`DESIGN.md` dated 2026-07-10.

The request to implement the design accepts:

- the shipped `main` command inbox with camera selection in the request body;
- the design's stated defaults unless this addendum adds a more specific default;
- the 256-connected-camera target;
- opt-in metadata sidecars;
- `raw` output in v1;
- Linux as the Tier-1 GenICam and RTSP host;
- PTZ preset mutation disabled by default;
- the single aggregated group reply, without a new scatter-gather core primitive; and
- component-local implementation of the common engine for the initial delivery.

The first release defers Bayer demosaicing and additional PFNC formats. It must reject those formats with
`UNSUPPORTED_PIXEL_FORMAT`; it must not label undecoded Bayer bytes as RGB or another supported format.

An implementation is complete only when:

1. every applicable requirement in `DESIGN.md` and this addendum is implemented;
2. the required phase gates and validation evidence are present;
3. all required cross-language core behavior is available and interoperable;
4. all required documentation describes shipped behavior only; and
5. every unrun lab gate is reported explicitly as an acceptance gap, and any owner-approved
   physical-camera waiver is recorded with its excluded compatibility claims.

Passing unit tests, compiling, or reaching a simulator milestone does not by itself satisfy the full
design. See `DESIGN.md` §§23.8–26, lines 2273–2347.

## 2. Resolved design decisions

| ID | Binding resolution | Design trace |
|---|---|---|
| `R-01` | Use `ecv1/{device}/camera-adapter/main/cmd/sb/{verb}` and select the camera with body `instance` for this delivery. Command addressing is resolved by core decision D-U28 (optional-instance UNS addressing); the pending migration target is instance-scope `.../{instance}/cmd/sb/{verb}` + component/fleet `.../cmd/sb/{verb}`, applied when the D-U28 rollout reaches this adapter (which updates the core southbound design and reference docs). | D-CAM-18 and §§12.1, 13, 26, 27 |
| `R-02` | `backend.type = "sim"` is a supported, configurable production backend in addition to `genicam-aravis` and `onvif-rtsp`. | §§6.2, 10.9: lines 227–263, 754–765 |
| `R-03` | ONVIF v1 binds with exactly one of `deviceServiceUrl` or `selector.endpointReference`. Endpoint-reference binding is resolved through bounded WS-Discovery. | §10.12: lines 800–819 |
| `R-04` | Initial startup reports and skips invalid instances if at least one enabled valid instance remains. A reload candidate is all-or-nothing: any invalid instance rejects the candidate and leaves the complete prior configuration running. | §§10.5, 20.1: lines 690–698, 1934–1945 |
| `R-05` | `capture-submit`, `capture-group`, and `capture-group-submit` use `waitUntilDeadline` as the offline default. Scheduled capture retains `failFast`; direct capture retains `waitUntilDeadline`. | §§10.10, 16.3: lines 767–783, 1775–1784 |
| `R-06` | `ACCEPTED` and `QUEUED` are separate durable states and separate commits. Recovery can therefore observe either state. | §§8.1, 12.2, 17.1: lines 310–341, 987–1011, 1788–1811 |
| `R-07` | Cancellation wins only while it can acquire the terminal/install compare-and-set before atomic installation starts. After installation starts, persistence and the terminal-state CAS win. | §§8.4, 9.2, 13.9: lines 402–409, 426–452, 1338–1351 |
| `R-08` | A group-cancel ledger entry uses `(main, sb/capture-cancel, requestId)` and hashes the canonical group target and reason. | §§8.2.1, 13.7, 13.9: lines 364–384, 1247–1307, 1338–1351 |
| `R-09` | Every command has a closed request/result schema as specified in §8 of this addendum. Unknown fields are rejected. | §13: lines 1071–1446 |
| `R-10` | Add `limits.maxQueuedControlsPerCamera`, default 32, range 1–1024. Safety stop has a reserved, non-starvable lane outside this ordinary control capacity. | §§6.1, 8.3, 16: lines 208–225, 386–400, 1741–1784 |
| `R-11` | `requestId` is 1–256 UTF-8 bytes, contains no Unicode control characters, and is compared by exact bytes. Duplicate lookup hashes canonical original request arguments before resolving current configuration. | §§8.2–8.2.1: lines 343–384 |
| `R-12` | `stopAndSettle` uses PTZ idle status when supported. Without status capability, a successful stop acknowledgement followed by `settleMs` is sufficient. | §11.5: lines 916–928 |
| `R-13` | RTSP readiness, fallback errors, and warm-session bounds are fixed by §10.3 of this addendum. At most one warm RTSP session per camera is retained for 30 seconds. | §§10.12, 11.3: lines 800–819, 879–899 |
| `R-14` | Each camera has one transport `resourceGroup`. Global encoder and writer gates protect shared CPU and storage resources. | §§6.1, 10.7, 16: lines 208–225, 710–742, 1741–1784 |
| `R-15` | Encoders stream output to the same-directory partial file and do not allocate a second image-sized output buffer. Admission reserves both in-flight memory and disk capacity. | §§9.2, 10.7, 16.2: lines 426–452, 710–742, 1759–1773 |
| `R-16` | Linux path creation and installation use capability-scoped, no-follow operations and a no-clobber final install. Windows uses the accepted portable profile in §9.1: standard-library no-overwrite finalization and safe collision failure, without Linux-equivalent hostile-local-actor containment. | §§9.1–9.2, 18.3: lines 413–452, 1867–1875 |
| `R-17` | With a sidecar, install and flush the sidecar before installing the final image. Consumers can never see a final image before its required sidecar. | §9.6 and §22.6: lines 495–502, 2077–2096 |
| `R-18` | Scheduler DST, coalescing, overlap, and jitter behavior is fixed by §7.4 of this addendum. | §§10.4, 23.1: lines 669–688, 2115–2131 |
| `R-19` | Add ONVIF limits `maxHeaderBytes = 65536`, `maxDecompressionRatio = 100`, and `allowBasicOverPlaintext = false`; apply DNS/IP pinning rules in §10.1 of this addendum. | §§10.12, 18.1, 18.4: lines 800–819, 1843–1852, 1876–1884 |
| `R-20` | Terminal announcements are published once, best effort, after the durable commit, and are never retried or stored for retry. A publish failure raises a stateful alarm and never fails a capture, rejects a capture, or changes readiness. | §§9.5, 14.3, 17.2, 19.4: lines 483–493, 1557–1567, 1813–1818, 1924–1930 |
| `R-21` | Queued jobs survive a compatible same-backend reload. Removal or backend-type change interrupts queued jobs transactionally with terminal failure messages. | §§17.1, 20.1: lines 1788–1811, 1934–1945 |
| `R-22` | Rust panics and reported callback errors are isolated at the supervisor boundary. Native segmentation faults and process aborts are not catchable in-process; sharding or process isolation is the mitigation. | §§17.3, 21.6: lines 1820–1828, 2034–2038 |
| `R-23` | SQLite uses transactional migrations, WAL, `synchronous=FULL`, a bounded busy timeout, startup integrity checking, and an exclusive state-directory process lock. | §17.1: lines 1788–1811 |
| `R-24` | Apply the Unix modes and deployment ownership rules in §9.3 of this addendum. Windows output ACL restriction is deployment guidance; the adapter does not enforce output DACLs. | §§18.3, 21: lines 1867–1875, 1957–2038 |
| `R-25` | Physical-camera validation is waived for this project because the owner has no hardware. The register must retain the waiver and explicitly exclude all model, firmware, device-timing, and hardware-compatibility claims; a hardware-certified release must restore §23.6. | §§23.6–23.8 |
| `R-26` | Keep 256 as the supported connected-camera target; retain 1,024-entry simulator coverage. | §§1, 16.1, 23.2, 23.5: lines 35–57, 1743–1757, 2137–2149, 2214–2228 |
| `R-27` | Keep sidecars opt-in and `raw` in v1. A `raw` capture without a sidecar is still valid, but its interpretation remains available through status and terminal metadata. | §§9.4, 9.6, 27: lines 467–502, 2333–2340 |
| `R-28` | Defer Bayer/PFNC demosaicing; keep Linux Tier 1; keep preset mutation false; do not add scatter-gather; do not extract the common engine into a reusable template during initial delivery. | §§21.3, 24, 27: lines 2004–2011, 2288–2301, 2349–2372 |
| `R-29` | P1 adds a four-language pre-commit configuration validator/veto. A rejected reload candidate never replaces current config, reaches effective-config publication, or invokes applied-config listeners. | §§10.5, 20.1: lines 690–698, 1934–1945 |
| `R-30` | P1 adds builder control of initial readiness. Camera adapter construction uses `initialReady(false)` and readiness stays false until every startup gate succeeds. | §§7, 19.4: lines 295–297, 1924–1930 |
| `R-31` | P1 makes CommandInbox startup observable. The adapter may become ready only after inbox subscription/registration reports success; startup failure is retained and reported. | §§13, 19.4: lines 1071–1082, 1924–1930 |
| `R-32` | Resolve `state.directory` deterministically for each platform. Greengrass and Kubernetes deployment artifacts set explicit durable paths; they never inherit transient work/container storage. | §§10.6, 21.3–21.5: lines 700–708, 2004–2032 |
| `R-33` | Support both ONVIF Media1 and Media2 services. `mediaService = "auto"` prefers an advertised Media2 profile with the configured token/name, then Media1; an explicit `media1` or `media2` setting never silently crosses service versions. | §§10.12, 11.3: lines 800–819, 879–899 |
| `R-34` | Support HTTP Digest and WS-Security UsernameToken PasswordDigest. `authenticationMode = "auto"` negotiates and validates authentication through read-only service calls before any mutating PTZ request; Basic remains subject to the plaintext prohibition. | §§10.12, 18.1: lines 800–819, 1843–1852 |
| `R-35` | Transient reconnect delay starts at `reconnectBackoffMinMs`, doubles to `reconnectBackoffMaxMs`, then adds deterministic positive jitter in `[0%, 20%]`. Permanent/auth/config failures use a base floor of `min(max, max(10000ms, 8 * min))`. A successful new session resets the attempt. The jitter key is `(instance, config generation, retry class, attempt)` so tests and fleet behavior are reproducible without synchronizing cameras. | §7: lines 274–306 |
| `R-36` | Add `component.global.discovery.eligibleInterfaces`, default empty. Values are distinct OS interface names, 1–256 UTF-8 bytes without controls. Periodic WS-Discovery and any ONVIF endpoint-reference selector require at least one explicit eligible interface; no wildcard/all-interface fallback is permitted. | Addendum §7.3 and DESIGN §§10.9, 18.4: explicit eligible-interface discovery and credential-free selection |

## 3. Delivery dependencies and phase gates

### 3.1 Dependency order

```text
P0 dependency/capacity spike
  ├─> P1 four-language core messaging plumbing
  └─> P2 Rust common engine with SimBackend
          ├─> P3 ONVIF snapshot, discovery, PTZ, and security
          └─> P4 GenICam/Aravis
P3 ─> P5 RTSP capture and ONVIF fallback
P1 + P2 + P3 + P4 + P5 ─> P6 platform/system/scale validation
P6 + physical compatibility + security review ─> P7 general release
```

P1 and the portions of P2 that do not consume the new APIs may proceed in parallel after P0. The camera
adapter must not ship a private bypass for missing P1 behavior. P3 and P4 may proceed in parallel after
P2. P5 depends on the ONVIF profile and URI work in P3. P6 cannot claim completion until every backend
selected for the release is integrated with the real P1 APIs.

### 3.2 Phase exit matrix

| Phase | Required work | Hard exit evidence | Design trace |
|---|---|---|---|
| P0 | Rust skeleton; dependency/license inventory; pinned Aravis, GLib, GStreamer and SQLite approach; Windows feasibility; SimBackend memory/thread baseline | One frame through each native stack available on the development platform; 256 idle simulations; approved OS/feature matrix; versioned resource baseline | §24 P0, line 2292 |
| P1 | Deferred command outcomes, correlation-aware `app`, confirmed publish, pre-commit config veto, initial readiness control, and observable CommandInbox startup in Java, Python, Rust, and TypeScript; update skeletons/templates and core docs | Per-language unit/coverage gates; reload-veto and startup-readiness races; 4×4 local MQTT requester/responder interop; PUBACK tests; four-language deployed Greengrass IPC interop; scaffold-to-build regression | §§10.5, 12.2, 19.4, 20.1, 23.3, 24 P1, lines 690–698, 960–1014, 1924–1945, 2186–2194, 2293 |
| P2 | Config, SQLite catalog/ledger, job engine, scheduler, admission, safe storage, commands/messages, metrics/health, SimBackend | Full SimBackend and EMQX contract; crash checkpoints; property/fuzz tests; at least 90% line coverage | §24 P2, line 2294 |
| P3 | WS-Discovery, ONVIF services/media/auth/TLS/snapshot, PTZ/presets, deterministic in-repo simulators | Simulator security/fault suite; physical ONVIF/PTZ compatibility is waived with no hardware claim | §24 P3 |
| P4 | Aravis GigE/USB3 backend, features, bounded buffers, timestamp quality, formats | Fake camera and packet faults; physical vendor compatibility is waived with no hardware claim | §24 P4 |
| P5 | GStreamer RTSP extraction, bounded warm session, fallback | RTSP codec/fault suite; physical fallback-camera compatibility is waived with no hardware claim | §24 P5 |
| P6 | HOST, Greengrass, Kubernetes, file-replicator, bottling-company integration, scale/soak | All current platform gates, short capacity evidence, docs and registry ready; 24-hour fleet-soak execution is deferred to a later validation phase | §24 P6, line 2304 |
| P7 | Compatibility register, threat/security review, operations, release status | No unresolved blocking findings; physical and lab gaps either closed or release explicitly withheld | §24 P7, line 2299 |

## 4. Implementation module map

The Rust implementation uses small modules with explicit ownership. Equivalent names are permitted only
when the same boundaries remain reviewable.

| Module | Required responsibility | May not own | Primary trace |
|---|---|---|---|
| `main` / `cli` | Parse standard EdgeCommons CLI, initialize runtime, order startup/shutdown | Camera protocol logic | §§19.4, 20.2, 21.1: lines 1924–1968 |
| `config` | Closed camera schema, defaults/ranges, startup partial-instance validation, pre-commit reload veto, atomic reload diff, redacted effective view, durable-state path resolution | Secret values or live sessions | §§10, 20.1, 21: lines 504–837, 1934–2038 |
| `registry` | Camera roster, stable selectors, immutable capability snapshots, supervisor lookup | Frame buffers | §§6.1, 7: lines 208–225, 274–306 |
| `supervisor` | Per-camera lifecycle, reconnect/backoff/jitter, session generations, panic isolation, alarms | Global admission policy | §§7, 17.3: lines 274–306, 1820–1828 |
| `actor` | Per-camera backend session, capture queue, control queue, safety-stop lane, device serialization | SQLite transactions | §§6.1, 8.3, 16: lines 208–225, 386–400, 1741–1784 |
| `backend` | Shared factory/session/frame/capability/error contracts | EdgeCommons topics or SQLite | §§6.2, 11: lines 227–263, 839–928 |
| `backend::sim` | Deterministic configurable simulated capture/PTZ/faults for production and tests | Conditional-test-only behavior | §§6.2, 23.2: lines 227–263, 2137–2149 |
| `backend::genicam_aravis` | Stable binding, feature validation, bounded buffers, metadata/timestamps, reconnect behavior | Image publishing | §§10.11, 11.2: lines 785–798, 857–877 |
| `backend::onvif` | Discovery, SOAP/media/auth/TLS, snapshot URI, URI safety, PTZ/presets | RTSP decoding internals | §§10.12, 11.3–11.5, 18: lines 800–831, 879–928, 1841–1884 |
| `backend::rtsp` | GStreamer negotiation, readiness selection, decode, one-session warm pool | ONVIF SOAP | §§11.3, 23.4: lines 879–899, 2206–2212 |
| `jobs` | State machine, deadlines, immutable effective profiles, terminal CAS, group aggregation | Protocol I/O | §§8, 13.5–13.9: lines 308–409, 1168–1351 |
| `catalog` | SQLite schema/migrations, jobs, groups, command ledger, schedule dedup, retention, queries | Camera I/O | §§8.2, 17.1: lines 343–384, 1788–1811 |
| `admission` | Priority aging, camera/global/resource/byte/disk/encoder/writer permits | Backend-specific feature selection | §§8.3, 10.7, 16: lines 386–400, 710–742, 1741–1784 |
| `scheduler` | Six-field cron, timezone/DST, stable jitter, misfire/overlap, durable occurrence keys | Wall-clock-only tests | §§8.2, 10.4, 23.1: lines 358–359, 669–688, 2115–2131 |
| `encoding` | Bounded worker pool and streaming jpeg/png/tiff/raw/passthrough output | Unbounded in-memory output | §§6.3, 9.4: lines 265–272, 467–481 |
| `storage` | Linux capability-scoped paths and no-clobber install; Windows portable persistence; disk reservation, partial files, stream/checksum/fsync, reconciliation | Retention deletion | §§9, 18.3: lines 411–502, 1867–1875 |
| `commands` | Closed schemas, validation, CommandInbox registration, deferred/immediate settlement | Direct publish to `reply_to` | §§12.2, 13: lines 960–1014, 1071–1446 |
| `messages` | Terminal body construction, correlation, exact topic/name, announcement serialization | Image bytes | §§12–15: lines 930–1739 |
| `observability` | Standard southbound health, bounded camera metrics, redacted/rate-limited logs/events | High-cardinality dimensions | §19: lines 1886–1930 |
| `runtime` | Reload/shutdown orchestration, initial-not-ready construction, observable command-inbox startup, readiness gates, and startup state | Platform-specific business branches | §§19.4–21.1: lines 1924–1970 |

The in-repository simulator services live outside the production Rust module tree but are versioned with
the component: ONVIF/WS-Discovery/snapshot fixtures, RTSP media service, and Docker orchestration. Native
third-party simulator images and packages must be pinned by digest or exact package version.

## 5. P1 cross-language core contract

This section fixes behavior, not language-specific signatures. Before implementation, the integration map
must reconcile each language's existing CommandInbox, messaging service, builders, error model, async
runtime, and template idioms. Public names and parameter types may be idiomatic, but observable behavior
must remain identical.

### 5.1 Deferred command outcome

Every language exposes three handler outcomes with the semantics from `DESIGN.md` §12.2, lines 960–1014:

- immediate success causes exactly one standard command success reply;
- immediate error causes exactly one standard command error reply; and
- deferred returns an opaque registry token, suppresses automatic reply, and releases the normal command
  dispatcher permit immediately.

The inbox-owned deferred registry must:

1. validate that the received message has guarded `reply_to`, correlation ID, verb, and a future bounded
   expiration;
2. create a `PROVISIONAL` token that contains no caller-controlled publish capability;
3. expose only an opaque, unforgeable token to application code;
4. activate the token to `OPEN` only after application durable acceptance succeeds;
5. discard it when durable acceptance fails;
6. settle with compare-and-set so at most one settlement wins;
7. build the standard reply wrapper and call guarded messaging reply using retained request metadata;
8. retry transport publication within a bounded policy until expiration;
9. record a stable diagnostic when an open token expires;
10. reject settlement after `SETTLED`, `EXPIRED`, or `CANCELLED_ON_SHUTDOWN`; and
11. attempt `COMPONENT_STOPPING` on shutdown while messaging remains available.

Returning a deferred outcome without first obtaining a provisional token is invalid. A handler must not
retain the full request solely to publish later, and component code must never publish directly to
`reply_to`. Deferred reply state is ephemeral across process restart; durable application status is the
recovery path.

Required race semantics:

- settlement versus expiration uses one atomic winner;
- two settlers produce one reply and one deterministic already-settled result;
- consumer timeout does not cancel application work;
- a late reply is sent only through the guarded reply path and is harmless if the request subscription is
  already gone;
- dispatcher concurrency counts the handler until it returns `Deferred`, not the lifetime of the job; and
- registry capacity and expiration are bounded and observable.

### 5.2 Correlation-aware application messages

Every language adds an `app` construction/publish path that accepts either a validated received request or
an explicit correlation ID. It must:

- preserve the normal application facade's topic generation, instance identity, envelope UUID, timestamp,
  encoding, and caller-owned body;
- copy the supplied correlation into the standard envelope header;
- reject malformed correlation values according to the existing envelope rules;
- never treat correlation as an idempotency key; and
- work with both ordinary publish and confirmed publish.

The camera adapter also copies the correlation into its schema-v1 terminal body for convenient consumers.
That duplicate body field must equal the envelope header. Scheduled messages receive one newly generated
correlation used in both places.

### 5.3 Acknowledgement-capable publish

Every language adds a bounded confirmed-publish operation with these semantics:

- MQTT completion at QoS 1 occurs only after the matching broker PUBACK is observed;
- Greengrass IPC completion occurs only after the publish operation completes successfully;
- enqueueing into a client library, writing to a socket, or receiving an ambiguous timeout is not success;
- cancellation or timeout returns a non-success result without claiming broker acknowledgement;
- disconnect before acknowledgement leaves delivery ambiguous/non-successful;
- implementations bound in-flight confirmed publishes and expose backpressure rather than allocating
  unbounded waiter state; and
- the existing immediate `publish` behavior remains source- and behavior-compatible.

A terminal announcement is published once, best effort. Application consumers therefore see
at-most-once delivery: an announcement may be lost, and none is ever duplicated by this component. The
durable capture result, not the announcement, is authoritative.

Each language exposes a prepared application publication that contains the validated topic, the logical
message, and the encoded envelope. A confirmed, exact-byte publication also exists in core and is the
foundation on which a durable-delivery augmentation would be built for components that need one; this
component does not use it.

### 5.4 Configuration and startup lifecycle plumbing

These are public four-language core behaviors because the adapter cannot implement them reliably outside
the configuration service, component builder, and CommandInbox that own the relevant state.

#### Pre-commit configuration validation

Every language provides a way to register one or more side-effect-free candidate validators before the
configuration provider starts. For every initial load and reload, the configuration service:

1. parses and schema-validates the candidate without replacing the current snapshot;
2. invokes registered validators with the candidate, the redacted current snapshot when one exists, and a
   phase of `INITIAL` or `RELOAD`;
3. applies a bounded validation deadline and collects stable validator errors;
4. commits the candidate with one atomic current-snapshot swap only when all validators accept;
5. publishes effective configuration and invokes applied-config listeners only after that commit; and
6. returns a standard reload error while retaining the exact prior snapshot when any validator rejects,
   times out, or fails.

A validator cannot publish effective configuration, mutate the current snapshot, resolve secret contents,
or perform session changes. Camera adapter initial validation accepts the candidate when at least one
enabled camera instance is valid and records diagnostics for skipped instances. Its reload validator
rejects the entire candidate when any global field or instance is invalid. This phase distinction preserves
`R-04` without allowing partial reload.

The current configuration object, effective-config publisher, applied-config listeners, and adapter
reload diff must all observe the same committed generation. A rejected generation is never externally
visible as current, even transiently.

#### Initial readiness control

Every language's component builder exposes an initial-readiness setting with equivalent behavior. The
camera adapter explicitly constructs the component with `initialReady(false)`. Construction may publish a
starting/not-ready state, but must not emit or report ready before application code calls the guarded
ready transition.

The camera adapter sets ready only after all of these gates succeed:

1. initial camera validation leaves at least one enabled valid instance;
2. durable state directory resolution, exclusive lock, migrations, integrity check, and recovery succeed;
3. output-root validation and initial disk reservation checks succeed;
4. every required command and built-in verb is registered and CommandInbox reports active;
5. camera supervisor objects are created; and
6. metrics, health, and configuration publication are initialized.

Camera sessions need not be online. Any failed gate leaves readiness false and exposes a stable startup
error. A concurrent callback cannot race the guarded transition and publish ready early.

#### Observable CommandInbox startup

Every language makes CommandInbox startup return or expose a bounded observable state with at least
`STARTING`, `ACTIVE`, `FAILED`, and `STOPPED`, plus a sanitized stable error when failed. `ACTIVE` means:

- all built-in and component verb handlers are installed;
- the exact command filters have been submitted to the selected transport;
- MQTT subscription acknowledgement or Greengrass IPC subscription operation completion has succeeded;
  and
- dispatch is able to accept work.

Partial registration failure tears down the subscriptions created by that start attempt and reports
`FAILED`; it must not report active with only some verbs. Repeated start/stop operations are deterministic
and do not leak subscriptions. The camera adapter's readiness follows the inbox state: it cannot become
ready before `ACTIVE`, and it becomes not-ready if the command plane later enters `FAILED` or `STOPPED`
while the component is otherwise running.

### 5.5 P1 integration map and proof

The P1 design record must map the behavioral contract above to the actual Java, Python, Rust, and
TypeScript source symbols before signatures are selected. The map records:

- current handler type and dispatcher ownership;
- proposed idiomatic outcome type;
- deferred registry owner, capacity, expiration clock, and shutdown hook;
- guarded reply entry point;
- application builder/view and correlation field representation;
- MQTT and IPC acknowledgement primitive;
- pre-commit configuration validator registration, deadline, veto error, generation swap, and effective
  publication ordering;
- builder initial-readiness state and guarded ready/not-ready transitions;
- CommandInbox start-state representation, transport acknowledgement, partial-start cleanup, and health
  integration;
- public exception/result model;
- skeleton/template updates; and
- exact unit, shared-vector, MQTT, and Greengrass interop cases.

Java remains the canonical API design, but no language may omit or weaken observable behavior. Required
interop is the full requester/responder matrix, not four self-tests.

## 6. Durable state and concurrency rules

### 6.1 SQLite lifecycle

The state directory is single-writer. Startup obtains an exclusive OS-level lock on
`<state.directory>/camera-adapter.lock` before opening SQLite. Failure to obtain the lock fails startup
with an operator-safe error; it must not fall back to an unlocked database.

On each open, configure and verify:

```text
PRAGMA foreign_keys = ON
PRAGMA journal_mode = WAL
PRAGMA synchronous = FULL
PRAGMA busy_timeout = 5000
PRAGMA integrity_check
```

`integrity_check` must return `ok`; otherwise readiness stays false and no commands or schedules are
accepted. Migrations are monotonic, versioned, transactional, and update `user_version` only in the same
commit as the schema change. A migration failure rolls back and fails startup. Runtime code never silently
recreates or discards a corrupt catalog.

Database work uses a bounded connection/worker pool and never blocks Tokio executor threads. The 5-second
busy timeout is a database lock bound, not permission to exceed the applicable command or job deadline.

### 6.2 Durable acceptance

Capture acceptance uses two durable commits:

1. insert the job as `ACCEPTED` with the original canonical request, request hash, immutable resolved
   profile, deadlines, trigger, origin correlation, and intended output; then
2. transition it to `QUEUED` and make it visible to the actor queue.

For a deferred direct capture, provisional token creation occurs before commit 1 and activation occurs
after commit 1. If commit 1 fails, discard the token and return immediate error. A crash after commit 1 but
before commit 2 leaves a durable `ACCEPTED` job, which startup marks `INTERRUPTED` and pairs with a terminal
terminal record as specified by `DESIGN.md` §17.1.

Submitted acceptance replies only after the `QUEUED` transition is durable. Group acceptance inserts the
group ledger, group record, and every `ACCEPTED` member in one transaction, then transitions all members to
`QUEUED` in a second all-or-nothing transaction.

### 6.3 Idempotency canonicalization

Each mutating request is parsed into its closed schema and normalized into canonical JSON:

- object keys sorted by Unicode code point;
- no insignificant whitespace;
- integers rendered in shortest decimal form;
- finite decimal values rendered in the implementation's shared canonical numeric form;
- arrays preserve order except `instances`, which is rejected if duplicated and then sorted only for the
  hash while original order remains in the result;
- omitted fields remain omitted and are not replaced with current config defaults in the original-args
  hash; and
- `requestId` is excluded from the immutable-arguments hash but remains part of the ledger key.

Duplicate detection first looks up the ledger key and compares this original-arguments hash. Only a new
key resolves the current default profile and stores the immutable effective profile. Consequently, an
identical retry after a config reload returns the existing job even if the named/default profile has since
changed. A changed explicit argument under the same key returns `IDEMPOTENCY_CONFLICT`.

`requestId` must be a string of 1–256 UTF-8 bytes with no Unicode general-category `Cc` or `Cf`
characters. It is not trimmed or case-folded.

### 6.4 Admission and resource ownership

Admission obtains permits in this order and releases them in reverse order:

1. per-camera descriptor queue;
2. global acquisition permit;
3. the camera's optional transport `resourceGroup` permit;
4. in-flight memory reservation;
5. output-filesystem disk reservation;
6. backend acquisition;
7. global encoder permit when conversion is required; and
8. global writer permit before persistence.

The single camera `resourceGroup` represents its NIC or USB transport. Decoder CPU is protected by
`maxConcurrentEncodes`; storage is protected by `maxConcurrentWrites` and disk reservations. A deployment
that needs separate transport constraints must shard cameras or assign the tighter shared group.

Before acquisition, reserve the effective `maximumFrameBytes` in memory and on the target filesystem.
Disk availability is computed as free bytes minus outstanding reservations and must remain above both
configured floors. Passthrough/raw writes consume the reservation directly. Streaming encoders write
bounded chunks to the partial file while retaining at most the source frame plus a small bounded codec
workspace; they must not construct a second image-sized `Vec`. Once actual payload and encoded byte counts
are known, shrink reservations, but never grow beyond the accepted maximum. An unexpected larger frame
fails before persistence with `RESOURCE_LIMIT` or the protocol-specific oversize error.

### 6.5 Camera control lanes

Each actor owns:

- a capture descriptor queue bounded by `maxQueuedCapturesPerCamera`;
- an ordinary control queue bounded by `maxQueuedControlsPerCamera` (default 32, range 1–1024); and
- one coalescing safety-stop lane that is always serviced before ordinary control and capture work.

If a safety stop is already pending, another stop expands the requested axes and tightens the deadline; it
does not consume another slot. Shutdown, PTZ timeout, configuration disable, and explicit `stop` all use
this lane. A full ordinary control queue returns `QUEUE_FULL`. It cannot block or evict the safety stop.

### 6.6 Cancellation and terminal arbitration

The catalog stores an internal `install_started` flag. Cancellation and persistence use transactional
compare-and-set:

- before installation, cancellation may transition a queued/acquiring/encoding/persisting job to
  `CANCELLED`; workers then stop or discard work and remove partial files;
- entering atomic installation CASes `install_started` from false to true only while the job is still
  nonterminal;
- if cancellation wins first, installation is forbidden;
- if installation wins first, cancellation returns `cancelled: false`, observed state `PERSISTING`, and
  `cancellationInProgress: false`; and
- after installation starts, only persistence/reconciliation may choose `SUCCEEDED` or `FAILED`.

A backend cancellation request is an optimization and does not determine the durable winner. The terminal
catalog CAS is the source of truth. Exactly one terminal announcement is published for that winner.

## 7. Configuration addendum

### 7.1 New and clarified global fields

| Field | Default | Range / validation | Reload |
|---|---:|---|---|
| `limits.maxQueuedControlsPerCamera` | `32` | 1–1024 | live for new control admission; existing entries are not dropped |
| `security.maxHeaderBytes` | `65536` | 4096–1048576 | live for the next HTTP/RTSP response |
| `security.maxDecompressionRatio` | `100` | 1–1000 | live for the next compressed response/frame |
| `security.allowBasicOverPlaintext` | `false` | may be true only when the instance also sets `allowInsecure: true`; emits a startup warning per affected camera | session replacement |
| `output.directoryMode` | `0750` | four-digit octal Unix mode; no setuid/setgid/sticky bits | new directories |
| `output.fileMode` | `0640` | four-digit octal Unix mode; no execute/special bits | new image and sidecar files |

The HTTP header limit includes the status line and all header names and values. The decompression ratio is
`decoded bytes / compressed bytes`; zero-length or indeterminate compressed input is rejected before
decode. Existing `maxSnapshotBytes` and `maximumFrameBytes` remain hard absolute limits.

### 7.2 Sim backend schema

`backend.type = "sim"` accepts the following closed schema:

```jsonc
{
  "type": "sim",
  "simulatedId": "camera-a",             // default: instance id
  "seed": 42,                             // default: stable SHA-256-derived seed
  "frame": {
    "width": 640,                         // 1..16384
    "height": 480,                        // 1..16384
    "pixelFormat": "RGB8",               // Mono8, RGB8, BGR8, or JPEG
    "pattern": "color-bars"               // color-bars, gradient, checkerboard, solid
  },
  "connectDelayMs": 0,                    // 0..300000
  "captureDelayMs": 10,                   // 0..600000
  "ptz": {
    "supported": false,
    "statusSupported": true,
    "presetsSupported": false
  },
  "faults": {
    "disconnectAfterCaptures": null,       // null or positive integer
    "failEveryNthCapture": null,           // null or positive integer
    "incompleteEveryNthCapture": null      // null or positive integer
  }
}
```

The simulator derives output bytes solely from its seed, capture ordinal, frame settings, and effective
profile, so the same configuration produces stable checksums. Tests may inject richer fault scripts
through the backend test harness, but production config accepts only the fields above. Simulator frames
still pass through normal admission, encoding, storage, catalog, messaging, metrics, and cancellation.

### 7.3 ONVIF endpoint-reference binding

An ONVIF backend config must provide exactly one of:

```json
{ "deviceServiceUrl": "https://10.0.8.25/onvif/device_service" }
```

or:

```json
{ "selector": { "endpointReference": "urn:uuid:camera-device-identity" } }
```

`endpointReference` is a 1–1024-byte non-control string compared exactly. When selected, bounded
WS-Discovery runs on explicitly eligible interfaces, matches the endpoint reference, validates every
reported XAddr through the same URI policy as runtime requests, and requires one unique device identity.
No match leaves the camera in reconnect backoff. Conflicting devices or a response whose endpoint identity
changes under the same session is a permanent configuration/security error until a later discovery cycle
produces one unambiguous match. Discovery never sends credentials.

The resolved service URL and addresses are session data, not rewritten configuration. Status may expose a
sanitized URL host but never credentials or user information.

ONVIF instances additionally accept these closed fields:

| Field | Default | Meaning |
|---|---|---|
| `mediaService` | `auto` | `auto`, `media1`, or `media2`. Auto prefers an advertised Media2 profile matching `mediaProfile`, then Media1. Explicit selection fails with `UNSUPPORTED_CAPABILITY` when unavailable. |
| `authenticationMode` | `auto` | `auto`, `http-digest`, `wsse-digest`, or `basic`. Auto establishes authentication with read-only capability/profile calls before any mutating operation. |

Media profile tokens remain opaque within their service version. An identically spelled Media1 and Media2
token is not assumed to identify the same profile. HTTP Digest challenge state and WS-Security nonce/time
state are session-scoped, bounded, and never logged. Authentication fallback never repeats a mutating PTZ
or preset operation: the chosen mechanism must already be established before actuation.

### 7.4 Scheduler semantics

For six-field cron expressions in an IANA timezone:

- a nonexistent local time during a forward DST transition is skipped and is not a misfire;
- a repeated local time during a backward transition fires only at the earlier occurrence;
- `misfirePolicy = skip` discards missed occurrences;
- `misfirePolicy = coalesce` creates exactly one job for the latest missed intended occurrence;
- overlap means any nonterminal job with the same `(instance, scheduleId)`, including an offline queued
  job;
- `overlapPolicy = skip` emits `schedule-skipped`; `queue` submits one ordinary bounded job and remains
  subject to queue capacity;
- stable jitter is the unsigned first 64 bits of
  `SHA-256(instance || 0x00 || scheduleId || 0x00 || intendedFireTimeUtc)`, modulo
  `jitterSeconds + 1`; `intendedFireTimeUtc` is encoded as ASCII RFC3339 at whole-second
  precision (`YYYY-MM-DDTHH:MM:SSZ`), which is exact because cron occurrences have second
  granularity; and
- occurrence dedup always uses the unjittered `intendedFireTime`, while requested/admitted timestamps show
  actual time.

All scheduler calculations use an injected wall/monotonic clock pair. Deadline duration uses monotonic
time within a process and persisted UTC instants for restart recovery.

### 7.5 Startup and reload validity

At initial startup, parse each instance independently against the closed backend schema. Report every
invalid instance with a stable error path and code. If at least one enabled valid instance remains, start
with only those valid instances and publish the redacted effective configuration. If none remains, fail
startup.

For reload, parse and validate the entire candidate before changing any live state. One invalid global
field or instance rejects the entire candidate. No partial schedule/profile/instance update occurs, and the
previous complete valid configuration continues.

Queued jobs survive a reload only when the camera remains present, enabled, and on the same backend type,
and the snapshotted profile remains executable by that backend contract. They retain their snapshot even
if the configured profile is edited or removed. Removing/disabling the camera or changing backend type
transactionally marks its queued jobs `INTERRUPTED` with `PROCESS_INTERRUPTED`, and announces each.
The active job follows the configured drain timeout.

### 7.6 Durable state directory resolution

An explicit absolute `component.global.state.directory` always wins after path validation. If it is
omitted, resolution is deterministic and must never use the process current directory, a temporary
directory, a Greengrass component work directory, or an unmounted container filesystem.

| Platform | Binding resolution |
|---|---|
| HOST/Linux | `/var/lib/edgecommons/camera-adapter-state` |
| HOST/Windows | `%ProgramData%\EdgeCommons\camera-adapter\state`, with `%ProgramData%` resolved to an absolute known-folder path rather than by string trust |
| GREENGRASS | No implicit fallback. The recipe/deployment must set an explicit absolute durable host path; omission is a configuration error. |
| KUBERNETES | The chart/manifests set `/var/lib/edgecommons/camera-adapter-state` and mount it on the same declared `ReadWriteOnce` PVC class as the durable output, either as a separate subdirectory or volume. An omitted/unmounted path is a deployment validation error. |

HOST containers must explicitly mount the resolved directory; image-local storage is not a durable
deployment. The resolved state directory is created with the modes in §9.3, locked before SQLite opens,
reported in sanitized startup diagnostics, and probed for durable write/rename behavior before readiness.

If EdgeCommons core gains a reusable platform durable-directory resolver, it is a public cross-language
facility and belongs in the P1 integration map. Its behavior must match the table above or require an
explicit component override; it may return an error but may not silently choose transient storage.

## 8. Closed command schemas

All request bodies are JSON objects with unknown fields rejected. String identifiers use the existing UNS
token limits unless a stricter bound appears below. `limit` defaults to 100 and ranges 1–1000. Cursors are
opaque, maximum 4096 bytes, query-bound, and rejected if used with different filters.

### 8.1 Read-only commands

`sb/list` request:

```json
{
  "includeCapabilities": true,
  "includeUnconfigured": false,
  "limit": 100,
  "cursor": "opaque-or-null"
}
```

The booleans default false. Result is exactly `{ cameras, unconfigured, nextCursor }`; `unconfigured` is
empty unless both discovery reporting and the request flag permit it.

`sb/discover` request:

```json
{
  "backends": ["genicam-aravis", "onvif-rtsp"],
  "timeoutMs": 5000,
  "limit": 100,
  "cursor": "opaque-or-null"
}
```

`backends` defaults to all compiled discovery backends, contains distinct values, and never includes
`sim`. `timeoutMs` ranges 100–300000 and is ignored for a continuation cursor because the cursor reads the
bounded discovery snapshot created by the first call. Result is `{ candidates, nextCursor, completedAt }`.

`sb/status` request is either `{}` for a bounded component summary or
`{ "instance": "camera-id" }` for full per-camera status. No job list is embedded.

### 8.2 Capture commands

`sb/capture` and `sb/capture-submit` accept:

```json
{
  "instance": "camera-id",
  "requestId": "caller-id",
  "captureProfile": "optional-profile",
  "timeoutMs": 30000,
  "metadata": {}
}
```

`captureProfile`, `timeoutMs`, and `metadata` are optional. The profile defaults to the camera's current
default only for a new idempotency key. `metadata` defaults to `{}`. `instance` follows the single-camera
omission rule. Results follow `DESIGN.md` §§13.5–13.6, lines 1168–1245.

Group capture request:

```json
{
  "requestId": "caller-id",
  "instances": ["camera-a", "camera-b"],
  "captureProfile": "optional-common-profile",
  "profileOverrides": { "camera-b": "other-profile" },
  "timeoutMs": 30000,
  "metadata": {}
}
```

`captureProfile` is optional. A member without an override uses the common profile when present, otherwise
that camera's default. `profileOverrides` keys must be exactly a subset of `instances`. Result/member order
matches the request's original `instances` order even though canonical idempotency hashing sorts the set.

### 8.3 Capture status

Exactly one lookup mode is allowed:

```jsonc
{ "captureId": "cap_..." }
{ "captureGroupId": "grp_...", "limit": 100, "cursor": null }
{ "instance": "camera-id", "requestId": "caller-id" }
{ "requestId": "group-caller-id" }
{ "instance": "camera-id", "states": ["FAILED"], "limit": 100, "cursor": null }
{ "states": ["FAILED", "INTERRUPTED"], "limit": 100, "cursor": null }
```

A bare `requestId` searches only component-scoped group-capture ledger entries. It never scans all camera
request IDs. `states` contains distinct public job states. Single-job result is `{ job }`; group result is
`{ group, members, nextCursor }`; list result is `{ jobs, nextCursor }`.

### 8.4 Capture cancellation

Request:

```jsonc
{
  "requestId": "cancel-id",
  "captureId": "cap_...",              // exactly one of captureId/captureGroupId
  "captureGroupId": "grp_...",
  "reason": "optional operator text"
}
```

`reason` is optional, 0–1024 UTF-8 bytes, no controls. Single result is:

```json
{
  "captureId": "cap_...",
  "cancelled": true,
  "state": "CANCELLED",
  "cancellationInProgress": false
}
```

Group result is `{ captureGroupId, cancelledMembers, unchangedMembers, members }`. The ledger key is
`(main, sb/capture-cancel, requestId)` and its immutable hash includes target kind, target ID, and reason.

### 8.5 Reconnect

Request is `{ instance, requestId, reason? }`; reason follows the cancellation bound. Immediate accepted
result is:

```json
{
  "operationId": "op_...",
  "instance": "camera-id",
  "state": "ACCEPTED"
}
```

`sb/status` includes `reconnectOperation` with operation ID, state `ACCEPTED|DRAINING|CONNECTING|SUCCEEDED|FAILED`,
accepted/completed timestamps, and sanitized failure. The command ledger retains the terminal reconnect
result for the normal result-retention window.

### 8.6 PTZ

The request shapes in `DESIGN.md` §13.11, lines 1369–1403, are closed schemas. Successful mutating result:

```json
{
  "operation": "continuous",
  "state": "COMMANDED",
  "acceptedAt": "RFC3339 timestamp",
  "stopDeadline": "RFC3339 timestamp or null"
}
```

`stopDeadline` is non-null only for continuous motion. Status result is:

```json
{
  "position": { "pan": 0.0, "tilt": 0.0, "zoom": 0.0 },
  "moving": false,
  "available": true,
  "observedAt": "RFC3339 timestamp"
}
```

Unsupported position or motion fields are `null`; they are never fabricated. `stopAndSettle` accepts stop
acknowledgement plus `settleMs` when `available` is false because PTZ status is not a capability.

### 8.7 PTZ presets

List result is `{ presets: [{ token, name }], nextCursor }`; tokens are opaque strings and names may be
null. `goto` result is `{ operation: "goto", state: "COMMANDED", token }`. `set` result is
`{ operation: "set", token, name }`; `remove` result is `{ operation: "remove", token, removed: true }`.
Only `set` and `remove` require `allowPresetMutation`; `goto` is actuation but not preset mutation. All
three mutating operations require `requestId` and use the durable ledger.

## 9. Storage, path, and sidecar semantics

### 9.1 Capability-scoped path handling

On Linux, all traversal begins from an already-open handle/capability for the canonical output root. Each
relative component is validated and opened/created without following symbolic links, junctions, mount-point
reparse redirection, or another namespace escape. The implementation revalidates that the final parent is
beneath the root at the time of creation; a prior string canonicalization alone is insufficient.

On Linux, use directory-relative no-follow operations and a no-replace install primitive such as
`renameat2(RENAME_NOREPLACE)` or an equivalently race-safe link/install sequence. On Windows, accepted
portable persistence validates the absolute root and rendered lexical path, creates an exclusive partial,
streams/checksums/flushes it, and finalizes it through a standard-library no-overwrite link/install followed
by partial cleanup. A collision or finalization failure is `PERSISTENCE_FAILED`. Windows does not guarantee
containment against a hostile concurrent local junction/reparse or directory-rename actor after validation.
The selected output filesystem must support same-directory hard links; otherwise finalization is
`PERSISTENCE_FAILED`. Windows output-root ACL restriction is deployment guidance.

Partial image and sidecar names are exclusive, include `captureId`, and are never valid final extensions.
Recovery removes only partials proven to belong to a catalog record or older orphan partials that pass the
component's exact naming and root checks.

### 9.2 Install order and durability

The writer performs:

1. reserve memory and disk;
2. create an exclusive image partial under the final parent;
3. stream source/encoder bytes through SHA-256 into the partial;
4. flush and request durable file storage;
5. build the complete terminal body from the known final path, size, checksum, and frame metadata;
6. if sidecar enabled, write and flush the sidecar before the final image;
7. acquire the catalog `install_started` CAS;
8. install the final image (Linux: no-clobber; Windows: standard-library no-overwrite link/install with collision/error failure);
9. flush the parent directory where supported; and
10. transactionally commit terminal success, then announce it best effort.

If cancellation wins before step 7, remove partials and, if present, the not-yet-consumable installed
sidecar. Once step 7 wins, cancellation cannot win. A crash after sidecar installation but before image
installation is reconciled from the `PERSISTING` record: install the verified image partial when possible;
otherwise remove the orphan sidecar and record failure. A required sidecar is therefore always present
before a final image becomes visible to file-replicator.

### 9.3 Unix modes and ownership

On Unix-like systems:

- output root and created image directories default to mode `0750`;
- final images and sidecars default to `0640`;
- the state directory defaults to `0700`; and
- database, WAL, shared-memory, lock, and state temporary files use `0600`.

Configured output modes may be more restrictive. The process honors a more restrictive umask and never
adds permissions after creation beyond the configured mode. Deployment documentation must explain the
service user/group arrangement needed for file-replicator access. Greengrass and Kubernetes run non-root.
Windows documentation must describe deployment-owned output ACLs and the accepted portable persistence
profile; the adapter does not enforce output DACLs.

## 10. ONVIF, RTSP, and network safety

### 10.1 DNS and URI pinning

For every configured or camera-returned HTTP(S), RTSP(S), or redirect URI:

1. reject user information in the URI;
2. require the scheme allowed by the configured security policy;
3. require the explicit/default port to remain within the approved endpoint tuple;
4. compare the normalized hostname against `allowedUriHosts` before DNS;
5. resolve all addresses with a bounded resolver deadline;
6. reject the request if any selected address is outside the configured endpoint addresses or
   `allowedUriCidrs`, including loopback, link-local metadata, and unrelated private ranges;
7. pin the chosen address for the connection while preserving the validated hostname for TLS SNI and
   certificate verification;
8. repeat validation on every new connection and redirect; and
9. never carry an Authorization header across origin changes.

The configured camera endpoint is an explicit exception only for its own resolved addresses. A DNS answer
change is not trusted merely because the hostname is allowed. Resolution failure or an address-set policy
change fails closed and is visible as a sanitized backend/security error.

Basic authentication over plaintext is forbidden unless both `allowInsecure` and
`allowBasicOverPlaintext` are true. Digest is preferred when offered. TLS verification remains on unless
the existing explicit insecure development setting disables it.

### 10.2 Response bounds

Reject a response before body processing when headers exceed `maxHeaderBytes`. Stream bodies through
`maxSoapBytes`, `maxSnapshotBytes`, profile `maximumFrameBytes`, and decompression-ratio checks. XML DTDs
and external entities remain unconditionally disabled. Content decoding stops as soon as either absolute
or ratio bound would be exceeded.

### 10.3 RTSP readiness and fallback

An RTSP frame is ready only after:

- transport and codec negotiation succeed;
- the decoder produces a complete frame with valid dimensions and a supported pixel format;
- the frame belongs to the selected media profile;
- it is not an initialization/preroll placeholder reported by the pipeline; and
- its stream timestamp is at or after the capture request's stream-ready point when the pipeline exposes
  that relation.

Otherwise the backend continues until the acquisition or job deadline. It never returns an incomplete
frame as success.

Snapshot-to-RTSP fallback is allowed only for:

- snapshot capability absent for the selected profile;
- ONVIF `ActionNotSupported` or equivalent documented snapshot fault;
- HTTP 404, 405, 410, or 501 from the validated snapshot endpoint;
- validated response with an unsupported snapshot content type;
- bounded connection, response, or body timeout; or
- truncated/corrupt snapshot bytes that fail image validation.

It is not allowed for authentication/authorization failure, TLS verification failure, URI policy/SSRF
rejection, oversize/decompression-limit failure, explicit caller `rtsp-frame` mismatch, or general
configuration error. The terminal result records both requested and actual mode plus the fallback reason.

`rtspSessionPolicy = warm` keeps at most one session per camera for 30 seconds after the last capture. The
session consumes observable decoder/native resources, is closed on reload, disconnect, auth change, or
shutdown, and is never shared between cameras. When the RTSP build feature is absent, config that requires
`rtsp-frame` or enables fallback is rejected for that instance with a stable unsupported-build error.

## 11a. Capture thumbnail

A capture profile may opt in to a thumbnail (`thumbnail.size` = `small` | `medium` | `large`). Absent, no
thumbnail is produced and no `thumbnail` key appears on the wire.

The bound is the **longest edge** — 160, 320, 640 px — with the aspect ratio preserved and no upscaling of a
smaller frame. The encoding is always JPEG. It is rendered from the camera's `CaptureFrame`, on the blocking
pool, inside the same permits as encoding and persistence, so no image work reaches the reactor.

The thumbnail is carried in the **announcement only**. It is not written to the metadata sidecar, not stored
in the catalog's `terminal_result`, and not included in a deferred or group reply — those are all made from
the committed body, and a lossy, derived, disposable preview must not be durably stored once per capture
(the reason the outbox was removed). An announcement rebuilt from the durable body after a restart therefore
carries no thumbnail.

The thumbnail carries **no digest**. It is a lossy re-encode; a `sha256` beside the artifact's own would
invite a consumer to believe it is verifiable against the artifact, which it is not and cannot be.

`data` is a binary value and MUST reach the wire as native protobuf bytes, never as base64 inside JSON.

### The transport decides the ceiling

The component resolves its transport at startup (`--platform`/`--transport`, or auto) and derives the
thumbnail policy from it. It never guesses from the config file, and it never learns the limit by failing.

| transport | largest size carried | preview budget |
|---|---|---|
| `IPC` (GREENGRASS) | `small` (160 px) | 6 KiB |
| `MQTT` (HOST, KUBERNETES) | `large` (640 px) | 60 KiB |

The IPC number is not a Greengrass protocol limit and not the Java nucleus's limit. The Greengrass IPC
*client* this component links (`aws-greengrass-component-sdk`) encodes the whole eventstream packet into a
**static 10,000-byte buffer** — `GG_IPC_MAX_MSG_LEN` in `include/gg/ipc/limits.h`, backing
`static uint8_t ipc_send_mem[…]` in `csrc/ipc/client.c` — and `eventstream_encode` answers `NOMEM` above
it, *before a byte reaches the nucleus*. The packet must also carry the envelope, the eventstream headers
and the topic, so the preview gets 6 KiB of it. The define is overridable at SDK build time
(`-D GG_IPC_MAX_MSG_LEN=<N>`); raising it is a core decision, not this component's.

A configured size larger than the transport carries is **clamped down, never rejected** — the same
configuration is deployed to Greengrass and to Kubernetes, and refusing to start on one of them would be
hostile. The clamp is reported once per camera at startup (and again after a reload, which may introduce a
new one), never per capture.

**A preview never outranks the result.** If an announcement carrying a thumbnail cannot be published, the
result is announced again without it. A result nobody was told about is a real loss; a missing preview is
an inconvenience. This is the belt to the transport policy's braces: it covers any transport whose limit
the component has mis-modelled.

### Byte ceiling — a known constraint, not a design

The messaging library caps a binary value at `MAX_BINARY_BODY_BYTES` = 64 KiB and errors above it. If a
thumbnail exceeded that, the announcement itself would fail to build and the capture's message would be
lost — a preview must never be able to do that. The component therefore encodes at JPEG quality 80, then 65,
then 50, accepting the first result at or under **the transport's preview budget** (the table above: 6 KiB
on IPC, 60 KiB on MQTT); if none fits, the thumbnail is dropped and the announcement is published without
it. The MQTT budget sits under the 64 KiB library cap by construction, and a compile-time assertion pins it
there.

That 64 KiB is **not** a transport limit. `core/docs/platform/DESIGN-binary-messaging.md` §3.6 sizes it for
the JSON BinaryValue path, where base64 inflates the payload and it flows through JSON parsers. On the
protobuf wire the value is emitted as native `EcValue.bytes_value` — no base64, no JSON parser — so the cap
guards a cost this path does not pay, and it over-constrains it. AWS IoT Core's 128 KiB ceiling does not
apply either: announcements are local-destination only and never go northbound. Raising the limit is the
`BinaryFrame` work in core (1 MiB default, configurable, all four languages). Until then this component
lives under 64 KiB because that is what the library enforces, not because the number is principled.

### Failure semantics

A thumbnail that cannot be rendered (`thumbnailFailed`) or will not fit (`thumbnailDropped`) is counted on
`camera_captures`, logged at WARN, and omitted. It never fails a capture, never rejects one, and never
changes readiness.

### Decompression bound

The thumbnail is the only code in the component that decodes a camera's JPEG. A JPEG's decoded size is not
its file size — a few hundred bytes can declare a 65500x65500 image and decode to gigabytes. A JPEG frame
whose header-declared decoded size exceeds the capture's own admitted `maximumFrameBytes` is refused before
any pixel buffer is allocated, and counted as a render failure. An out-of-memory death is not a degraded
preview.

## 11. Announcement failure and readiness

A terminal announcement is published once, best effort, after its terminal state is durably committed.
It is not stored for retry and it is not retried. If the broker or IPC transport is unavailable, or the
component stops between the commit and the publish, that announcement is lost. The catalog and the
installed image remain authoritative, and `sb/capture-status` answers for the capture afterwards.

A failed announcement raises the stateful `message-publish-degraded` condition and increments the
`announcementFailed` measure on `camera_captures`. The condition clears on the next successful
announcement. Its context contains the instance, the capture id, and a sanitized error code — never
payloads or paths.

A messaging outage never makes readiness false and never rejects a capture. The component continues to
capture, encode, and persist while the transport reconnects; only the announcements are lost. Readiness
becomes false, and new captures are rejected with `STORAGE_PRESSURE`, when the state filesystem crosses
either configured free-space floor, SQLite cannot commit, integrity is lost, or the adapter cannot
reserve enough state capacity for the next bounded terminal record. Liveness remains true while the
runtime can publish status and alarms.

Durable, acknowledged delivery is a generic messaging concern. It belongs in the EdgeCommons messaging
service as an opt-in augmentation available to any component in all four languages, not reimplemented
inside this one.

## 12. Simulator and validation topology

The required deterministic stack is:

| Service | Selected implementation | Container/network rule | Required evidence |
|---|---|---|---|
| GenICam GigE | Distribution-pinned `arv-fake-gv-camera` matching the Aravis build | Linux host networking or a dedicated L2-capable network; ordinary NAT does not count as discovery proof | Explicit selector, software trigger, payload and reconnect; netem fault evidence |
| RTSP | MediaMTX `1.19.2` (immutable image digest in `simulators/image-lock.json`) fed by deterministic generated H.264/H.265 video; GStreamer `gst-rtsp-server` remains an optional independent compatibility target | Fixed codec/profile fixtures; host or bridge networking as appropriate. The adapter decoder remains the pinned GStreamer Rust/native pipeline. | negotiation, slow first frame, invalid stream, codec change, reconnect |
| ONVIF | In-repository deterministic simulator | Publishes only test XAddrs; credentials and faults configured from fixtures | capabilities, profiles, Digest/TLS, snapshot, PTZ/presets, faults, hostile URI |
| WS-Discovery | In-repository UDP responder | Linux host/L2 network for multicast tests; separate deterministic direct harness for CI | duplicate EPR, malformed XAddr, timeout, multi-interface |
| Snapshot HTTP(S) | In-repository fixture service | Test CA and fixed DNS/IP fixtures | delays, truncation, oversize, redirect, wrong type, auth, DNS pinning |
| Fault proxy | Pinned Toxiproxy plus Linux `tc netem` | Never used to weaken production URI policy | timeout, half-open, disconnect, latency, UDP loss/reorder |

Every image or package is pinned by digest or exact version and recorded in acceptance evidence. Downloaded
third-party software and codecs receive a license/security review before inclusion in build or release
artifacts.

Physical-camera validation is waived for this project because the owner has no hardware, as recorded in
`DESIGN.md` §23.6. The compatibility register must state `WAIVED — NO HARDWARE AVAILABLE` and explicitly
exclude model, firmware, hardware, and device-timing compatibility claims. Simulator evidence cannot
convert that waiver into a hardware pass.

## 13. Traceability and review plan

### 13.1 Requirement-to-implementation matrix

The implementation PR or local review record maintains a live matrix with one row per identifier below.
Each row links source files, tests, evidence artifacts, and status (`not started`, `implemented`,
`validated`, or `blocked`).

| Requirement group | Binding source | Owning modules | Minimum proof |
|---|---|---|---|
| `TR-GOAL` | DESIGN §§1–4, lines 35–132 | all | Signed scope review and negative-scope tests |
| `TR-ARCH` | DESIGN §§5–6, lines 134–272 | registry, supervisor, actor, backend, admission, catalog, storage, messages | Architecture review; bounded-thread/buffer tests |
| `TR-LIFE` | DESIGN §7, lines 274–306 | supervisor, registry, observability | Injected reconnect/auth/capability tests |
| `TR-JOB` | DESIGN §8, lines 308–409; addendum §§6.2–6.6 | jobs, catalog, actor | State/property tests, crash recovery, idempotency and cancellation races |
| `TR-STORAGE` | DESIGN §9, lines 411–502; addendum §§6.4, 9 | storage, encoding, catalog | Path adversarial tests, no-clobber, fsync checkpoints, ENOSPC, sidecar ordering |
| `TR-CONFIG` | DESIGN §10, lines 504–837; addendum §7 | config | Defaults/ranges/unknown fields/redaction/startup/reload matrix |
| `TR-BACKEND` | DESIGN §11; addendum §§7.2–7.3, 10 | backend modules | Simulator/protocol evidence; physical compatibility waiver with excluded claims |
| `TR-CORE-P1` | DESIGN §§10.5, 12.2, 19.4, 20.1, lines 690–698, 960–1014, 1924–1945; addendum §5 | four core language libraries and templates | Unit coverage, reload-veto/readiness/startup races, 4×4 MQTT, lab-5950x IPC |
| `TR-MSG` | DESIGN §§12, 14–15, lines 930–1070 and 1448–1739 | commands, messages | Exact rooted/rootless topic/envelope vectors, correlation, late reply, broker outage |
| `TR-CMD` | DESIGN §13, lines 1071–1446; addendum §8 | commands, jobs, catalog | Every success/error schema with independent client |
| `TR-CAPACITY` | DESIGN §16, lines 1741–1784; addendum §§6.4–6.5 | admission, actor, encoding, storage | 256/32/1,024 tests, priority aging, p95 control latency, RSS/thread graphs |
| `TR-RECOVERY` | DESIGN §17, lines 1786–1839; addendum §§6.1–6.3, 11 | catalog, jobs | Kill-point matrix; a capture that survives a kill is durable and answerable via sb/capture-status |
| `TR-SEC` | DESIGN §18, lines 1841–1884; addendum §§7.1, 9, 10 | config, onvif, storage, logging, packaging | Threat review, SSRF/DNS/XXE/decompression/path/credential tests |
| `TR-OBS` | DESIGN §19, lines 1886–1930; addendum §11 | observability, runtime | Metric schema/cardinality, alarm raise/clear, health transition tests |
| `TR-RUNTIME` | DESIGN §§19.4–20, lines 1924–1955; addendum §§5.4, 7.5–7.6, 9.3 | runtime, config, supervisor, jobs | Initial-not-ready and command-start races, reload compatibility matrix, durable-path resolution, and timed shutdown/forced-stop tests |
| `TR-DEPLOY` | DESIGN §21, lines 1957–2038 | packaging/deployment artifacts | HOST, Greengrass, kind, hardware runner evidence or explicit gaps |
| `TR-INTEGRATION` | DESIGN §22, lines 2040–2108 | system tests, file-replicator docs/config | End-to-end metadata/file/checksum/group/replication evidence |
| `TR-VALIDATION` | DESIGN §23; addendum §12 | all test suites | Portable and native-feature coverage, simulator versions/config, short-capacity artifacts, deferred-soak plan/results after later execution, and the hardware-waiver register |
| `TR-DOCS` | DESIGN §25, lines 2303–2321 | component and core docs | Diátaxis set reviewed against shipped behavior |

### 13.2 Adversarial review gates

At the end of every phase, a reviewer who did not implement that phase must attempt to falsify its claims.
The review must examine at least:

- whether code bypasses a required EdgeCommons facade or guard;
- whether a green test uses mocks where a real transport/protocol is required;
- whether queue, waiter, thread, native resource, buffer, or database growth is actually bounded;
- whether cancellation, reload, timeout, and restart races have one durable winner;
- whether a success can reference a partial, overwritten, escaped, or sidecar-incomplete file;
- whether URI, XML, decompression, path, credentials, or caller metadata crosses a trust boundary unsafely;
- whether a message acknowledgement is a real transport acknowledgement;
- whether duplicates are tolerated without repeated camera/PTZ actuation;
- whether any backend failure changes another camera's readiness or progress;
- whether documentation claims conditional Windows, physical camera, Greengrass, Kubernetes, codec, or
  format support without matching evidence; and
- whether the implementation silently reintroduces one of the exclusions in `DESIGN.md` §2.2,
  lines 77–90.

Any deviation from `DESIGN.md` or this addendum is a blocking review finding until the user accepts a new
documented decision. The accepted decision must update this addendum before the implementation is reported
complete.

## 14. Required acceptance artifacts

The final local handoff contains:

- a completed `TR-*` matrix with source/test/evidence links;
- exact build, lint, test, coverage, audit, simulator, and deployment commands;
- pinned native dependency, container image, codec, and simulator versions;
- sample successful and failing requests, replies, terminal app envelopes, and operator alarm envelopes;
- image and sidecar paths, modes, sizes, SHA-256 values, and crash-recovery evidence;
- 256-camera/32-capture short-capacity artifacts, their write-once run manifest, SHA-256 attestations, and complete 15-minute human-readable report with report attestation, plus the deferred 24-hour soak plan; attach soak results only after that later execution;
- broker-outage evidence: captures continue and persist while announcements are dropped;
- four-language local MQTT and deployed Greengrass IPC interop evidence for P1;
- HOST, Greengrass, kind, and applicable hardware-cluster evidence;
- the physical-camera compatibility register with explicit `WAIVED — NO HARDWARE AVAILABLE` and excluded
  claims for this project, or `PASS`, `FAIL`, or `NOT RUN` per model/capability for a hardware-certified
  release; and
- an adversarial senior review that checks the original `DESIGN.md` and this addendum, not only an
  implementation summary.
