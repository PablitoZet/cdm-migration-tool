# CDM Migration Tool architecture

## Purpose and boundary

The application is a single-host migration controller for a selected OpenText
Content Server source folder or Business Workspace. It reads metadata from
on-premise PostgreSQL, reads versions from Azure Blob Storage, creates supported
objects in OpenText Extended ECM Cloud GX39 and stores local execution state in
SQLite.

PostgreSQL and Azure are read-only. GX39 is the only remote system changed by a
Pilot or Full Cutover. Dry Run performs no target calls. The web application
binds to `127.0.0.1:8110` and is intended for a controlled operator workstation,
not a shared network service.

## Quality objective

After migration, users should work in the cloud scope with equivalent hierarchy,
names, descriptions, binary content, version chains, category values, source
dates and owner provenance, target ownership, effective access and workspace
behavior. Source and target technical IDs will necessarily differ. GX39 system
create/modify dates remain target-generated; the original values are preserved
as explicit provenance.

The tool fails closed when required equivalence cannot be proven. It does not
silently flatten unsupported types or accept a partial verification result.

Historical audit events and personal UI state are outside this workspace tool.
Their exclusion requires an explicit Acceptance decision.

## Components

| Component | Responsibility |
|---|---|
| `app.py` | Local FastAPI control plane, profile persistence and operator UI APIs |
| `static/index.html` | Four-step operator workflow and contextual safety dialogs |
| `engine/config.py` | Profile validation, fixed policy normalization and secret persistence |
| `engine/db.py` | Repeatable-read, read-only recursive source extraction |
| `engine/inventory.py` | Deterministic scope signature |
| `engine/manifest.py` | SQLite inventory, runs, leases, mappings, multipart checkpoints and backup |
| `engine/source.py` | Bounded-memory source Content Server REST, Azure and local binary streams |
| `engine/client.py` | GX39 authentication, rate limiting, REST calls and multipart protocol |
| `engine/pipeline.py` | Phase ordering, worker execution, retry classification and recovery |
| `engine/preflight.py` | Fail-closed readiness checks |
| `engine/reconciler.py` | Inventory, version, hash, category and read-back verification |
| `engine/instance_lock.py` | One controller per profile state database |

## Configuration model

A fresh installation has two editable profiles:

- `test`: corporate qualification against GX39 TEST;
- `production`: final GX39 PROD cutover with production safety gates.

The operator configures both through Migration Setup. Local `config.json` stores
credentials and is excluded from Git and release archives.

Migration Setup can test the current draft values before saving. The draft
connection check creates temporary source and target clients, performs only the
read-only PostgreSQL connection check and GX39 authentication, and returns
sanitized connection results without changing `config.json`, the active runtime
configuration or SQLite state. The UI renders a concise status first and keeps
the full response behind expandable technical details.

The selected Source root DataID may be any supported folder/workspace in the
configured Content Server database. Its complete subtree is in scope. The
Destination parent NodeID is an existing GX39 container; the source root is
created inside it.

The application fixes these policies:

- `source_root_maps_to_target=false`;
- leave `Created By` as the migration service account;
- resolve and assign `Owned By` once per distinct source KUAF owner using exact
  active login/email identity;
- preserve original source dates, owner identity and resolution status in the
  `CDM Migration Provenance` category;
- inherit the approved permissions of the destination;
- TLS verification enabled;
- Azure binary access derived from one container SAS URL.

Tenant ID mappings remain explicit exceptions:

- source category definition/attribute to GX39 category attribute;
- source owner identity to an exact GX39 user identity, with an approved
  run-scoped fallback only for unresolved/deactivated exceptions;
- Business Workspace subtype/type to target workspace type/template.

Every real run performs online target capability checks. Owner assignment for
ordinary folders/documents and every Business Workspace route, provenance
applicability/read-back, creator read-back and ordered version-provenance rows
must be explicitly qualified before a real run. GX39 system dates are not sent
to create/update APIs, and writable `external_*` dates are not silently treated
as equivalent to system dates.

The fixed provenance category uses business labels beginning with
`Original Source ... (Pre-Migration)`, including source DataID, created/modified
dates, owner ID/login/email/display name, source system and owner resolution
status. Documents additionally store ordered rows for each original version's
number, created date, modified date and file date. Values use deterministic
UTC ISO 8601 normalization and are verified by target read-back.

Direct Azure reads require source provider metadata that resolves
deterministically to a blob locator. Archive Center `acprimary`/`ixos`
descriptors are treated as opaque handles; those versions stream through the
read-only source Content Server REST endpoint by DataID and version. Multipart
recovery replays the source from byte zero and does not depend on HTTP Range.

## Inventory and source signature

`engine/db.py` opens one repeatable-read, read-only PostgreSQL snapshot and
extracts:

- the root and descendants from `DTree`;
- primary document version records from `DVersData`, excluding renditions and
  transient rows, with binary locators resolved through `ProviderData`;
- source category values.

The manifest stores the active profile ID and source root. Reusing an inventory
with a different profile or root is blocked.

The deterministic signature covers hierarchy, relevant metadata, owner
identity/status, version identity/size/locator and category values. Before
production Full Cutover, the
operator confirms that the source is read-only and the application re-reads the
scope. A different signature rejects the cutover.

## Run and item state

Each execution has a UUID `run_id` and an independent set of run items.

```text
READY -> CLAIMED -> REMOTE_COMMITTED -> METADATA_APPLIED -> OWNER_PENDING -> VERIFIED
                  \-> RETRY_WAIT -> CLAIMED
                  \-> FAILED_TERMINAL

Dry Run: READY -> CLAIMED -> SIMULATED
```

`OWNER_PENDING` marks an item whose content and provenance were written
successfully but whose final target owner reassignment is deferred to a
dedicated, run-wide `OWNER` phase (see "Phase ordering" below). An item in
this state is claimed again, from `OWNER_PENDING`, once the OWNER phase runs;
on success it transitions to `VERIFIED`.

Claims contain worker ownership and expiration leases. A stopped process can be
resumed only through explicit Run history recovery. Recovery releases incomplete
claims but preserves successful terminal states, target mappings and multipart
checkpoints.
Retryable failures that exhausted their automatic attempt budget are requeued
with a fresh budget only during explicit operator recovery; terminal data,
mapping and contract failures remain terminal.

## Phase ordering

Real migration executes dependency-aware phases:

1. create supported containers top-down, applying provenance (not owner);
2. create/upload documents and all versions, applying provenance (not owner);
3. recreate references after their target mappings exist;
4. reassign the target owner for every created object, once the whole tree
   (containers, documents and references, at every depth) exists;
5. verify target state and record durable evidence.

Children cannot run before their parent is mapped. Shortcuts cannot be created
until the referenced source object has an approved target mapping.

Owner reassignment is intentionally deferred to its own final phase, after
every other object has been created. GX39 containers inherit their ACL from
the parent at the moment a child is created. If a container's owner were
reassigned immediately after the container itself is created, and that owner
has a restrictive or empty permission list, any not-yet-created descendant
would inherit an ACL without the migration service account's edit rights,
causing later writes (categories, provenance, further children) to fail with
HTTP 500 "Insufficient permissions". Running owner reassignment last, across
the whole run, guarantees no container loses the migration account's ACL
entry before all of its descendants exist.

Business Workspace subtype 848 uses the target Business Workspace API and
requires a configured target type/template route. On the source side,
`engine/db.py` also follows each workspace W to its source-only shadow container
at DataID -W, including workspaces encountered below ordinary folders or other
workspaces. The shadow shares the workspace's logical depth/path while both IDs
remain in the recursive cycle guard. Python validates that every workspace has
an active shadow of subtype 849 with ParentID -1, excludes that row from the
inventory, and rewrites its direct children's `parent_source_id` to W. Deeper
ordinary descendants retain their existing parent chain; no shadow ID is
created or mapped on GX39. Missing/invalid pairs, unexpected shadow rows and
repeated DataIDs fail extraction before an inventory can be imported.

The negative-DataID layout is a source database observation recorded in the
task brief, not a contract confirmed by the public OpenText REST reference.
Traversal is implemented and tested with fake cursors; actual PostgreSQL
recursion, completeness and workspace target behavior require corporate TEST
qualification. See `BUSINESS_WORKSPACE_IMPLEMENTATION_REPORT.md` for the
verification checklist. Unknown container subtypes remain terminal errors.

## Idempotency and ambiguous commits

Every real target object receives an indexed GX39 text attribute named for the
migration, with value:

```text
CDM:<namespace>:<source DataID>
```

The configured OpenText attribute key has format
`categoryID_attributeID`. Before creating an object, the client searches the
intended parent and validates this category value. If a create response is lost,
the same lookup reconciles the remote result. Same-name matching is never used.

This makes retries and crash recovery safe without forcing source DataIDs into
GX39 NodeIDs.

## HTTP, authentication and throttling

The GX39 client provides:

- synchronized token acquisition and keep-alive;
- stale-token validation before reauthentication;
- shared request rate limiting;
- bounded exponential retry with jitter;
- `Retry-After` handling;
- explicit safe/unsafe retry classification;
- separate connect/read timeouts;
- TLS verification through the native operating-system certificate store.

Create and multipart completion calls are not blindly retried. Unknown commit
outcomes must be reconciled with the migration attribute and read-back.

## Streaming and multipart

Ordinary versions use streaming multipart/form-data with a deterministic
`Content-Length`; the source stream is read only as the HTTP client sends bytes.

Large versions use GX39 multipart upload. The default threshold is 50 MiB and
part size is 16 MiB. Only a bounded part is materialized at a time. The manifest
stores upload key, next part, size and hashes so recovery continues from a saved
checkpoint. Recovery reopens the source at byte zero, streams the completed
prefix once to reconstruct SHA-256 and continues with the same stream; source
HTTP Range support is therefore an optimization, not a correctness dependency.

Default execution starts with eight document workers and one large-file slot.
These are conservative defaults, not a certified production optimum.

## Verification contract

Production verification requires:

- all run items in successful verified states;
- a durable source-to-target mapping for every migrated object;
- correct target parent and name;
- complete version counts and order;
- non-empty source and target SHA-256 values with equality for every version;
- category values mapped and read back;
- target owner, migration service-account creator and source-date/owner
  provenance read back according to the current metadata contract;
- complete ordered version-provenance rows read back for every document;
- destination permissions and Business Workspace behavior qualified;
- search, lifecycle and legacy-link acceptance evidence.

Dry Run produces `SIMULATED` states only and can never satisfy production
verification.

## Persistence and release boundary

For each profile, state is stored in
`migration_state_v2_<profile>.db`. SQLite WAL files and the instance lock are
runtime artifacts. Online backup is the only supported way to copy active state.

Release archives contain source, static assets, tests, example configuration and
canonical documentation. They exclude `config.json`, state databases, logs,
caches, `.venv`, previous releases and secrets. `RELEASE_SHA256.json` records a
hash for every included file, and a sibling `.sha256` protects the ZIP.

## Integration claims

Unit tests and local Dry Run verify deterministic engine behavior, not tenant
compatibility. GX39 multipart, target schemas, owner-write routes, provenance
category/set payloads, workspace routes, permissions, token behavior,
WAF/rate limits, indexing and throughput remain corporate TEST qualification
obligations described in `DEPLOYMENT_AND_QUALIFICATION.md`.
