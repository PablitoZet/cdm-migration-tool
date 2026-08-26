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
names, descriptions, binary content, version chains, category values, dates,
owners, effective access and workspace behavior. Source and target technical IDs
will necessarily differ.

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
| `engine/source.py` | Bounded-memory Azure/local binary streams |
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

The selected Source root DataID may be any supported folder/workspace in the
configured Content Server database. Its complete subtree is in scope. The
Destination parent NodeID is an existing GX39 container; the source root is
created inside it.

The application fixes these policies:

- `source_root_maps_to_target=false`;
- preserve source dates and owners;
- inherit the approved permissions of the destination;
- TLS verification enabled;
- Azure binary access derived from one container SAS URL.

Tenant ID mappings remain explicit exceptions:

- source category definition/attribute to GX39 category attribute;
- source owner ID to GX39 user ID;
- Business Workspace subtype/type to target workspace type/template.

## Inventory and source signature

`engine/db.py` opens one repeatable-read, read-only PostgreSQL snapshot and
extracts:

- the root and descendants from `DTree`;
- all document version records and binary locators;
- source category values.

The manifest stores the active profile ID and source root. Reusing an inventory
with a different profile or root is blocked.

The deterministic signature covers hierarchy, relevant metadata, version
identity/size/locator and category values. Before production Full Cutover, the
operator confirms that the source is read-only and the application re-reads the
scope. A different signature rejects the cutover.

## Run and item state

Each execution has a UUID `run_id` and an independent set of run items.

```text
READY -> CLAIMED -> REMOTE_COMMITTED -> METADATA_APPLIED -> VERIFIED
                  \-> RETRY_WAIT -> CLAIMED
                  \-> FAILED_TERMINAL

Dry Run: READY -> CLAIMED -> SIMULATED
```

Claims contain worker ownership and expiration leases. A stopped process can be
resumed only through explicit Run history recovery. Recovery releases incomplete
claims but preserves successful terminal states, target mappings and multipart
checkpoints.

## Phase ordering

Real migration executes dependency-aware phases:

1. create supported containers top-down;
2. create/upload documents and all versions;
3. recreate references after their target mappings exist;
4. verify target state and record durable evidence.

Children cannot run before their parent is mapped. Shortcuts cannot be created
until the referenced source object has an approved target mapping.

Business Workspace subtype 848 uses the target Business Workspace API and
requires a configured target type/template route. Unknown container subtypes are
terminal errors.

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
- TLS verification.

Create and multipart completion calls are not blindly retried. Unknown commit
outcomes must be reconciled with the migration attribute and read-back.

## Streaming and multipart

Ordinary versions use streaming multipart/form-data with a deterministic
`Content-Length`; the source stream is read only as the HTTP client sends bytes.

Large versions use GX39 multipart upload. The default threshold is 50 MiB and
part size is 16 MiB. Only a bounded part is materialized at a time. The manifest
stores upload key, next part, size and hashes so recovery continues from a saved
checkpoint.

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
- dates/owners read back according to policy;
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
compatibility. GX39 multipart, target schemas, workspace routes, permissions,
token behavior, WAF/rate limits, indexing and throughput remain corporate TEST
qualification obligations described in `DEPLOYMENT_AND_QUALIFICATION.md`.
