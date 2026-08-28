# CDM Migration Tool — instructions for AI coding agents

This file is the controlling engineering guide for every AI agent modifying this
repository. Read it completely before editing code. It is deliberately explicit
so that a less capable agent can make safe, reviewable changes without weakening
migration integrity.

## 1. Mission and current scope

This is an internal, single-operator migration controller for moving a selected
OpenText Content Server folder or Business Workspace from an on-premise database
and Azure Blob Storage into OpenText Extended ECM Cloud SaaS (GX39).

It is not a commercial migration product and must not become a generic workflow
platform. The primary objective is one approved production workspace cutover,
but operators may use test profiles to migrate other supported source
folders/workspaces from the same Content Server database.

Production reference scale:

- 22,115 document files;
- approximately 151 GB of binary content;
- 543 files larger than 50 MB;
- largest known file approximately 7.44 GB;
- the production source root DataID is configured only at runtime and must not
  be stored in the repository;
- target platform: OpenText GX39 SaaS.

The application is operated by two designated internal operators. The UI must
be simple enough that they do not need to study an engineering manual before
each run.

## 2. Environment boundary

Development can occur on a private/home machine where:

- the corporate PostgreSQL servers are unreachable without the corporate VPN;
- the public GX39 TEST HTTPS endpoint may be reachable;
- SQLite state, unit tests and Dry Run work locally;
- corporate Azure Blob paths, GX39 tenant contracts and production throughput
  cannot be fully qualified.

Never interpret a local test PASS as proof of production compatibility. The
corporate-machine qualification steps in `DEPLOYMENT_AND_QUALIFICATION.md` are
mandatory before a real upload or production cutover.

## 3. Source of truth and document hierarchy

Use documents in this order:

1. `AGENTS.md` — engineering invariants and agent workflow.
2. `CORPORATE_HANDOFF.md` — current corporate-machine checkpoint, known
   unknowns and agreed next work. It cannot override this file's invariants.
3. `ARCHITECTURE.md` — implemented component and state-machine design.
4. `DEPLOYMENT_AND_QUALIFICATION.md` — installation, tenant qualification and
   operator procedure.
5. `MIGRATION_PLAN.md` — production scope, responsibilities and acceptance plan.
6. Tests and current code — executable contract.

If documentation conflicts with code, stop and determine whether the code or
the documentation is stale. Do not silently choose the easier interpretation.
Update both in the same change.

The pre-refactor architecture dossier and legacy operator SOP were intentionally
removed. Do not restore or cite them.

## 4. Non-negotiable safety invariants

The following rules are mandatory:

1. The source PostgreSQL connection is read-only. Do not add source `INSERT`,
   `UPDATE`, `DELETE`, DDL, advisory locks or write-capable transactions.
2. Azure Blob Storage is a read-only binary source.
3. Dry Run must not call or mutate GX39 and must not create durable target ID
   mappings.
4. The app binds only to `127.0.0.1`. It must start without an API key or other
   artificial operator authentication gate.
5. TLS verification is mandatory for production profiles.
6. A source inventory is bound to its profile ID and source root DataID. A
   different profile/root requires a new scan.
7. Only one process may control a profile state database at a time.
8. Production Full Cutover requires a source read-only confirmation and a fresh
   source signature equal to the scanned manifest.
9. Real target creation requires the GX39 duplicate-protection attribute.
10. Unknown remote commit outcomes must be reconciled; never blindly repeat an
    unsafe create request.
11. Verification fails closed. Missing evidence, hashes, mappings or read-back
    values are failures, not warnings to ignore.
12. Never delete, rewrite or copy an active SQLite database with filesystem
    commands. Use the SQLite online backup path exposed by the application/CLI.
13. Never include `config.json`, `.env`, credentials, SAS tokens, SQLite state,
    logs, caches or virtual environments in a release archive.

Do not weaken these invariants to make a test pass or unblock a demo.

## 5. Operator-facing configuration contract

Keep ordinary Migration Setup small and understandable.

- **Source root DataID** is the DataID of a supported folder or workspace. The
  complete subtree is scanned. A document is not a valid migration root.
- **Destination parent NodeID** is an existing GX39 folder/workspace that accepts
  child objects. The selected source root is created inside this destination.
- `source_root_maps_to_target` is fixed to `false` for this tool.
- Source dates and owners are preserved.
- Migrated content inherits the approved permissions of the selected GX39
  destination.
- Azure source access is configured with one container-level SAS URL. The app
  derives account URL, container, token and blob locator template.
- Passwords and tokens are intentionally persisted in local `config.json` for
  this controlled internal tool. The file must be mode `0600` where supported
  and excluded from release archives/version control.
- Category, owner and Business Workspace mappings are exception fields. Do not
  expose extra strategies or tenant internals unless Readiness can name the
  exact required operator action.

Profiles carry an internal `environment_class` (`sandbox`, `test`, or
`production`). Existing production profiles retain production safety gates.
Do not infer production safety only from a profile name.

## 6. Duplicate protection (idempotency)

Every real target object receives a dedicated indexed text attribute in GX39,
normally named `CDM Migration ID`. Its value is:

```text
CDM:<migration namespace>:<source DataID>
```

The OpenText attribute key uses `categoryID_attributeID`, for example
`12345_2`. The category ID is derived from this key.

This is not a Pilot-only feature. It protects Pilot, Full Cutover and recovery.
If GX39 commits a create but the HTTP response is lost, the client scans the
intended parent, reads this attribute, reuses the existing object and continues.
A same-name object is never accepted as proof of identity.

Do not replace this mechanism with filename/path matching, in-memory tracking or
unconditional POST retries.

## 7. State machine and recovery

Each execution has an immutable UUID `run_id` and separate `run_items`.

```text
READY -> CLAIMED -> REMOTE_COMMITTED -> METADATA_APPLIED -> VERIFIED
                  \-> RETRY_WAIT -> CLAIMED
                  \-> FAILED_TERMINAL

Dry Run: READY -> CLAIMED -> SIMULATED
```

Claims use leases. Large multipart uploads persist upload key, next part, part
size, bytes and hashes. `Run history` offers `Resume interrupted run` only for
`STOPPED`, `FAILED` and `COMPLETED_WITH_ERRORS` runs. Completed items are not
uploaded again.

Recovery must reject:

- a completed or currently active run;
- a run whose configuration fingerprint differs from the current profile;
- production recovery without a valid unchanged source freeze;
- recovery while another run is active.

Recovery is not rollback and must never delete GX39 objects.

## 8. Streaming and concurrency rules

Never load a complete file into memory or stage multi-gigabyte files on disk.

- Standard uploads stream a fixed-length multipart/form-data body.
- Large files use numbered OpenText multipart parts.
- Default multipart part size is 16 MiB.
- Default document workers: 8.
- Default large-file concurrency: 1.
- HTTP 429 and retryable 5xx responses use bounded exponential backoff and
  server `Retry-After` when available.
- Authentication uses a shared, synchronized token manager and verifies stale
  tickets before reauthentication.

Do not increase concurrency based on Dry Run. Dry Run does not upload content
and cannot measure GX39 production throughput. Tune only from corporate TEST
measurements, p95 latency, 429 rate and 5xx rate.

## 9. OpenText REST API boundary

Tenant behavior is not guessed. The following contracts must be proven against
GX39 TEST before production:

- multipart availability, part endpoints and completion body;
- first and subsequent document version multipart semantics;
- category and multi-row/set payload shapes;
- Business Workspace type/template creation and role behavior;
- preservation/read-back of system dates and owners;
- service-account rights and target ACL inheritance;
- indexing delay and duplicate-marker read-back;
- throttling/WAF behavior and token lifetime.

Keep tenant-specific adaptations isolated in `engine/client.py` or explicit
mapping data. Never scatter one tenant's response assumptions through the
pipeline.

Unsafe operations (`POST` create, multipart completion) are not generally
retry-safe. Retry only when the operation is proven idempotent or when an
ambiguous result is reconciled by the migration marker and read-back.

## 10. Supported fidelity and explicit exclusions

The quality objective is operational equivalence after unavoidable technical ID
changes: hierarchy, names, descriptions, content, versions, categories, dates,
owners, effective access and workspace behavior.

Current rules:

- Business Workspace subtype 848 requires a target workspace type/template
  route.
- Unknown subtypes fail; never silently convert them to folders/documents.
- Category and attribute IDs are tenant-specific and require mappings.
- Missing parent mappings block children.
- Shortcuts whose targets are outside the migration scope must be resolved by an
  approved policy; do not invent targets.
- Historical audit events and personal UI state (favorites, recents,
  subscriptions, preferences) are not migrated by this tool and require explicit
  acceptance.
- Legacy links require separate qualified routing based on the durable source to
  target ID mapping.

## 11. UX rules for this internal tool

The operators explicitly rejected a highly configurable, commercial-product
style UI. Follow these rules:

- Prefer safe fixed defaults over strategy dropdowns.
- Show a setting only when the operator can act on it now.
- Put one-time prerequisites in a clearly named contextual dialog.
- Use business language; avoid labels such as `Friday-Monday parity`, `freeze
  module`, `marker`, or unexplained OpenText jargon.
- Do not add a permanent dashboard card for a single confirmation. Ask at the
  action boundary instead.
- Keep help text adjacent to an unfamiliar field and state whether it is
  required, optional or requested only by Readiness.
- Do not add an API key requirement, onboarding wizard, support bundle or broad
  mapping assistant without an explicit user request.
- Preserve credentials locally so operators are not driven to desktop notes.
- UI text is currently English; keep terminology consistent across the UI and
  runbooks.

## 12. Repository map

- `app.py` — FastAPI localhost control plane, profile API and UI endpoints.
- `static/index.html` — operator UI and browser-side workflow gates.
- `engine/config.py` — normalization, local secret persistence and profile rules.
- `engine/db.py` — repeatable-read source extraction.
- `engine/inventory.py` — deterministic source signature.
- `engine/manifest.py` — SQLite schema, state transitions, mappings and backup.
- `engine/source.py` — Azure/local bounded-memory streams.
- `engine/client.py` — GX39 HTTP/auth/rate-limit/multipart boundary.
- `engine/pipeline.py` — orchestration, workers, recovery and phase ordering.
- `engine/preflight.py` — fail-closed readiness checks.
- `engine/reconciler.py` — verification and read-back.
- `engine/instance_lock.py` — single-controller lock.
- `cli.py` — headless corporate-machine operations.
- `preflight.py` — lightweight preflight entry point.
- `tests/` — engine and UI contracts.
- `package_release.py` — secret-free checksummed transfer bundle.
- `.github/workflows/quality.yml` — mandatory GitHub quality and release check.

## 13. Required change procedure for every agent

Before editing:

1. Read this file and the relevant canonical documents.
2. Inspect current code and tests; do not implement from an old prompt or memory.
3. Check whether a server/run is active before changing state-sensitive code.
4. Identify whether the change affects source safety, target writes, state,
   idempotency, streaming, verification, configuration or UI gates.

While editing:

1. Make the smallest coherent change.
2. Preserve backward compatibility of existing `config.json` and SQLite schema,
   or implement and test an explicit migration.
3. Do not modify or delete operator state as part of a code change.
4. Keep secrets out of output, fixtures and documentation.
5. Update tests and canonical documentation in the same change.
6. Do not claim an integration was tested if the corporate dependency was not
   available.

Before declaring completion, run from the repository root:

```bash
python3 -m unittest discover -s tests -v
.venv/bin/ruff check .
.venv/bin/mypy app.py engine tests
```

If `.venv` does not exist, run `./bootstrap.sh` (or `bootstrap.ps1` on Windows)
first. Also run a JavaScript syntax check after editing inline UI code:

```bash
sed -n '/<script>/,/<\/script>/p' static/index.html | sed '1d;$d' | node --check
```

For UI changes, start the app and inspect the affected user path in a browser.
Do not rely only on string assertions.

For release:

```bash
python3 package_release.py
```

Inspect the ZIP listing and confirm that no secret/state/cache file is present.
Transfer both ZIP and `.sha256`.

## 14. Forbidden shortcuts

An agent must not:

- disable a readiness/verification gate because corporate integration is
  unavailable;
- treat HTTP 200/201 alone as migration success without required read-back;
- retry target creates blindly;
- use a same-name target object as an idempotency match;
- buffer a complete large file in RAM or on local disk;
- mark a Dry Run as evidence of throughput or production parity;
- assume source and target category, owner, permission or Node IDs are equal;
- flatten unsupported structures to make counts match;
- reset/delete SQLite after a failure instead of diagnosing/recovering it;
- copy a live SQLite database with `cp`/Explorer;
- weaken TLS or production confirmation;
- log passwords, tickets, Authorization headers or SAS query strings;
- add real credentials to `config.example.json`, tests or docs;
- edit generated release archives instead of rebuilding them;
- add UI configuration merely because an engine option exists;
- report corporate PostgreSQL, Azure or GX39 behavior as tested from a private
  machine.

## 15. Corporate-machine qualification checklist

Before enabling production, an agent assisting on the corporate machine must
collect evidence for all of the following:

1. PostgreSQL read-only connection and exact selected root.
2. Container-level Azure SAS resolves representative and largest binaries.
3. GX39 TLS/authentication and destination identity.
4. Duplicate-protection category accepted on every migrated object type.
5. Ordinary, threshold-boundary and large multipart uploads.
6. Interrupted multipart recovery and expired-token recovery.
7. Lost-response create reconciliation without duplicate objects.
8. First and subsequent versions, dates, owners and comments.
9. Categories including multi-row/set fields.
10. Business Workspace templates and roles.
11. Destination ACL inheritance and intended user access.
12. Search/indexing, lifecycle operations and legacy links.
13. Measured throughput and conservative approved concurrency.
14. State backup/restore rehearsal.
15. Production source read-only signature confirmation.
16. Full post-migration reconciliation and business acceptance.

Record unknowns as blockers. Never convert an unknown into an assumption.
