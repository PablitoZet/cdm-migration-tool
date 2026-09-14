# Corporate-machine engineering handoff

Last updated: 2026-09-11

This document is the current checkpoint for continuing development and
qualification on the corporate machine. It records what has already been
implemented, what the operators decided, what remains unverified and the next
recommended work. It does not override the safety invariants in `AGENTS.md`.

An AI agent taking over this repository must first read, in full:

1. `AGENTS.md`;
2. this document;
3. `ARCHITECTURE.md`;
4. the relevant sections of `DEPLOYMENT_AND_QUALIFICATION.md` and
   `MIGRATION_PLAN.md`;
5. the current code and tests.

Do not implement from an earlier chat summary when it conflicts with the
repository.

## 1. Current checkpoint

- Application version: `2.3.0`.
- Canonical branch: `main`.
- The canonical upstream is private; obtain its URL through the approved
  operator channel rather than storing account identifiers in documentation.
- GitHub quality workflow passes on Python 3.11.
- Local clean-install validation passes with the full unit/contract suite (87
  tests in this checkout), Ruff and
  mypy. The Windows bootstrap was also validated on Python 3.14; CI remains
  pinned to the canonical Python 3.11 baseline.
- The private/home development machine cannot reach corporate PostgreSQL or
  fully qualify corporate Azure/GX39 behavior.
- The first corporate GX39 DEV smoke run completed 19/19 objects and 12/12
  versions with hierarchy, hashes, categories, marker read-back and destination
  permission evidence.
- A subsequent Representative Pilot against GX39 TEST (run
  `aabc664c-1886-4523-9e53-996442a6a81d`, 2026-09-10) completed **19/19 nodes
  VERIFIED with zero failures** on a small pre-existing on-premise folder (9
  containers, 10 ordinary documents, up to 3 versions each). This is the first
  fully clean end-to-end run of the current owner/provenance replacement
  contract: hierarchy, content hashes, `CDM Migration ID` duplicate-protection
  read-back, the dedicated final OWNER phase (service account →
  target-resolved owner), and full `CDM Migration Provenance` read-back
  (node-level and per-version dates/owner) all passed.
- This Pilot found and fixed two defects that had blocked every earlier
  attempt, both now resolved and regression-tested (see commit `e6fbc3f`):
  1. Reassigning a container's owner immediately after its own metadata write
     broke GX39's ACL inheritance for children created afterward. Fixed by
     deferring every owner reassignment to a dedicated final run-wide `OWNER`
     phase (see the `OWNER_PENDING` state in `ARCHITECTURE.md`).
  2. GX39 `GET /api/v2/nodes/{id}/categories/{category_id}` returns multi-row/
     set category values as flat keys (`categoryId_setId_row_attrId`) nested
     under a `"categories"` wrapper, not as a nested list. `read_provenance()`
     now reconstructs this recursively.
  Both fixes required no product/config change beyond one GX39 tenant
  correction (see next point) and are safe to carry into further
  qualification.
- Two GX39 TEST category attributes for `CDM Migration Provenance` were found
  configured as `date` (day-only) instead of `datetime`: the node-level
  `source_created_at`/`source_modified_at` fields and the three per-version
  fields (created/modified/file date) inside the version set. GX39 silently
  truncates the time-of-day on write for a `date`-typed attribute, which reads
  back as a mismatch and is easy to misdiagnose as a client bug. The operator
  changed all five attributes to `datetime` in GX39 TEST category admin, after
  which the Pilot passed cleanly. **This is a tenant configuration setting, not
  a code fix — it must be reproduced explicitly for every new GX39
  environment (including production) before a Pilot/Cutover there.** See
  `DEPLOYMENT_AND_QUALIFICATION.md` section 5a, which is now the canonical,
  must-stay-current list of every GX39 tenant-side configuration this tool
  depends on.
- This TEST Pilot still does **not** constitute production approval: it ran
  small ordinary folders/documents only (no Business Workspace route, no file
  above a few KB, no multipart/large-file path, no corporate PostgreSQL/Azure
  source). The wider qualification matrix in section 7 below and in
  `DEPLOYMENT_AND_QUALIFICATION.md` sections 8 and 15 remains required before
  Full Cutover.
- Dry Run executes offline preflight before creating a run and rejects missing
  structural/category/owner/workspace mapping prerequisites without requiring
  post-Pilot operational acceptance.
- The first DEV smoke test confirmed that this Content Server schema stores
  binary locator data in `ProviderData.ProviderData`, referenced by
  `DVersData.ProviderID`, and that `DVersData` also contains `otthumb`
  renditions. Extraction now selects only non-transient primary versions and
  fails closed on duplicate primary version identities.
- The DEV primary provider is Archive Center `acprimary`; its `ProviderData`
  value contains an `ixos://` provider handle, not an Azure blob name. The
  application does not format these handles into false Azure paths. A bounded
  source Content Server REST adapter now streams the requested DataID/version;
  DEV authentication and exact content were qualified against all 12 synthetic
  versions. Measured representative/production throughput remains unqualified.
  DEV REST responses
  use gzip/chunked transfer for some content, so the adapter requests identity
  encoding and never treats an encoded transport length as the source file
  size. DEV ignores HTTP Range; multipart recovery therefore replays the
  completed prefix from byte zero in one bounded-memory stream before resuming
  at the saved GX39 part. The configured
  unexpired container SAS had `r/l` permissions but returned Azure
  `AuthorizationFailure`; container identity/network policy also remains to be
  resolved. Actual Azure binary reads and the production source schema remain
  unqualified.
- Corporate TEST confirmed that user owners are stored as negative
  `DTree.OwnerID` values while the corresponding `KUAF.ID` is positive. Owner
  extraction resolves the absolute KUAF lookup ID while retaining the original
  signed source OwnerID in the manifest and provenance. This KUAF schema uses
  `Name` as the login and `FirstName`/`LastName` as display-name components.
  GX39 CE 25.4 member searches return identity under `data.properties`, with
  `name`, `name_formatted`, `business_email`/`personal_email` and `deleted`;
  the client normalizes that shape before exact login/email resolution.
- The first real GX39 DEV Pilot created and verified all nine folders, then
  GX39/WAF reset every ordinary document create because Requests emitted both
  `Content-Length` and `Transfer-Encoding: chunked` for the custom multipart
  stream. No document marker was found after reconciling all ten ambiguous
  creates. `MultipartStream.tell()` now gives Requests an exact remaining
  length, preventing chunked framing; the interrupted Pilot must be resumed,
  not replaced with a new run.
- Explicit recovery now requeues only exhausted `RetryableMigrationError` and
  `AmbiguousRemoteCommit` items with a fresh attempt budget. Verified folders
  remain untouched and permanent terminal failures are not reopened.
- A controlled document probe confirmed that `roles.categories` applies the
  duplicate marker atomically during multipart-form document create. Earlier
  missing-marker results were caused by parsing the category read-back at the
  wrong nesting level. The pipeline also persists the known target ID
  immediately after the first-version response, applies and reads back the
  marker before later versions, and lists target versions before content GET.
  GX39 returns HTTP 500 rather than 404 when asked for a nonexistent version.
- The same probe showed that an arbitrary `external_identity_type` is rejected
  and `external_identity` alone is not preserved, so external identity is not
  used for idempotency. The probe remains in GX39 DEV for manual administrator
  cleanup; its target ID is recorded only in the local session artifact.
- Recovery now runs the same online target capability checks as a new real run.
  Replacement runs reconcile every version of a pre-existing mapping before
  deciding to upload, preventing duplicate version appends.
- GX39 CE 25.4 returns category values under
  `results.data.categories.<categoryID_attributeID>`; marker read-back uses an
  exact recursive key lookup for this tenant response shape.
- GX39 CE 25.4 standard create/update forms expose system create/modify dates
  and owner as readonly. Create permits writable `external_create_date` and
  `external_modify_date`, but these do not provide system-date fidelity.
- The current checkout implements the agreed replacement contract:
  `Created By` remains the migration service account; `Owned By` is resolved
  from the exact active target e-mail once per distinct source KUAF owner; cloud
  member IDs and logins are tenant-local and are not compared with on-premise
  values; and original source dates, owner identity and resolution status are
  stored in the dedicated `CDM Migration Provenance` category. Documents also
  carry ordered provenance rows for every source version.
- Unresolved, ambiguous, deactivated or status-unknown owners fail Readiness
  unless one exact active fallback such as `CDM Legacy Owner` is configured
  and the operator approves the displayed exception digest and change record
  at the run boundary. There is no per-file owner mapping and no silent
  fallback. Owner assignment and provenance applicability/read-back are now
  qualified against GX39 TEST for the ordinary-folder/document route (see the
  2026-09-10 Pilot above). The Business Workspace route (creation, roles,
  owner assignment and provenance on Business Workspace objects) remains
  unqualified against corporate TEST.

- When the operator pointed the production profile at real production
  PostgreSQL credentials (source only; cloud/target temporarily still GX39
  TEST for safe testing), `Test Connection` failed with `relation
  "public.dtree" does not exist`. Diagnosis found that production `DTree` (and
  the other Content Server tables) live in schema **`cs`**, not `public`,
  while TEST uses `public` — a genuine, previously-unknown environment
  difference; both environments share the same non-helpful
  `search_path = "$user", public"`. `engine/db.py` previously hardcoded
  `public.` everywhere. Added a `db_schema` profile field (default `public`,
  validated against a plain-identifier pattern before use in SQL) that
  qualifies every source query; set to `cs` for the production profile. See
  `DEPLOYMENT_AND_QUALIFICATION.md` section 5b for the operator-facing
  contract and regression tests in `tests/test_engine_v2.py` for the
  schema-qualification behavior itself.

Use `git log -1 --oneline` to identify the exact checked-out revision. Never
assume that a release ZIP and the Git checkout are at the same revision.

## 2. Implemented product decisions

The refactor intentionally favors a small internal-operator workflow over a
commercial migration-product UX.

- The server binds to `127.0.0.1:8110` and starts without an API key.
- Successful profile saves close Migration Setup and render confirmation toasts
  above modal overlays so operators can see the outcome. Save errors also render
  inline inside Migration Setup and do not depend on a transient toast.
- Migration Readiness reports source-owner identity completeness and, online,
  exact target resolution evidence without exposing bulk account lists. Owner
  exceptions are approved contextually for one run; ordinary setup does not
  expose per-owner numeric mappings.
- Verified GX39 and Azure HTTPS connections use the native operating-system
  certificate store, including approved corporate CAs installed on Windows.
- Profiles and credentials are configured in the UI.
- Credentials are intentionally stored in local `config.json`, protected with
  mode `0600` where supported. The file is ignored by Git and release builds.
- Ordinary setup exposes only the source, target, one Azure Container SAS URL
  and exception mappings.
- **Source root DataID** may identify any supported folder or Business
  Workspace in the configured source database. A document is not a valid root.
- **Destination parent NodeID** may identify any approved GX39 container that
  accepts child objects. The selected source root is created beneath it.
- `Created By` remains the migration service account; `Owned By` is assigned
  from immutable exact e-mail owner resolutions. Original source dates and owner
  identity are preserved in `CDM Migration Provenance`.
- Migrated objects inherit the approved permissions of the destination.
- Duplicate protection uses a dedicated indexed GX39 text attribute such as
  `CDM Migration ID`, with value
  `CDM:<migration namespace>:<source DataID>`.
- Real creates are reconciled by that attribute after ambiguous HTTP outcomes;
  same-name matching is not accepted as identity.
- `Run history` resumes interrupted runs and is not presented as rollback.
- The production source read-only confirmation is contextual to Full Cutover,
  not a permanent dashboard widget.
- The former How-to Guide, support-bundle concept, mapping-assistant concept and
  `Friday-Monday parity` wording were deliberately rejected.

The main workflow is:

1. Source Scan;
2. Dry-Run Simulation;
3. Representative Pilot;
4. Full Cutover;
5. live reconciliation and business acceptance.

## 3. Next corporate qualification: authorized scope

The small ordinary-folder/document Representative Pilot described in section 1
is now complete and clean on GX39 TEST. The next run should build on the
current version (commit `e6fbc3f` or later on `main`) and extend qualification
into the areas that small pilot did **not** cover. It must still not be treated
as a production run.

Use only:

- a small-to-medium, non-sensitive folder/workspace in the on-premise DEV/TEST
  database, extended to include: at least one Business Workspace, one document
  with more than one version, and (capacity permitting) one file at or above
  the 50 MiB multipart threshold;
- the matching corporate Azure source storage access;
- an isolated GX39 DEV/TEST destination with `CDM Migration Provenance`
  re-verified per `DEPLOYMENT_AND_QUALIFICATION.md` section 5a (all date
  attributes `datetime`, not `date`);
- a profile classified as `test`, never `production`;
- a dedicated test migration namespace and duplicate-protection attribute.

Priority order for this next qualification pass:

1. Business Workspace type/template creation, roles and owner/provenance on
   that route (unqualified — see section 1). **Source-side traversal for this
   is currently blocked by a confirmed gap**: a real production Business
   Workspace was scanned and returned a scope of 1 node with zero content,
   because Content Server parents a Business Workspace's actual content under
   a separate "shadow container" DataID (the negative of the workspace's own
   DataID), not under the workspace's own DataID directly. This has been
   delegated as a self-contained implementation task to
   `BUSINESS_WORKSPACE_TASK_BRIEF.md` (being implemented on a machine without
   corporate/GX39 access); once that branch is ready, pull it here, qualify
   it against a real Business Workspace on GX39 TEST, then fold the outcome
   into this section and delete the brief file.
2. Multipart upload at the 49/50/51 MiB boundaries, at least one 100+ MiB file,
   and one interrupted-multipart-recovery rehearsal (unqualified).
3. A representative large file approaching the known production maximum
   (~7.44 GB) once capacity/approval allow (unqualified).
4. Measured throughput/latency/429/5xx rates to calibrate concurrency away
   from the conservative defaults (unqualified — do not tune from Dry Run or
   from the small Pilot).

Do **not** use production customer content in GX39 DEV/TEST. Production data may
be used in a lower environment only after explicit data-owner, information
security/privacy and contractual approval and only when the environment has
approved equivalent controls. Treat file names, paths, versions, authors,
comments, categories and ACLs as potentially sensitive even if file bodies are
removed.

### Smoke-test sequence

1. Clone the repository and run `start.ps1` from PowerShell.
2. Confirm that the app starts at `http://127.0.0.1:8110` without an API key.
3. Configure a new TEST profile through the UI. Never paste credentials into
   source files, prompts, screenshots or Git.
4. Inspect the exact source root and target parent before scanning.
5. Run Source Scan and save the inventory summary.
6. Resolve only the specific mapping exceptions reported by Readiness.
7. Run Dry-Run Simulation and confirm that GX39 remains unchanged.
8. Configure and validate duplicate protection.
9. Run a small Representative Pilot.
10. Exercise Pause/Resume and one controlled Stop/Resume interrupted-run path.
11. Run automated live verification and manually inspect the migrated sample in
    Smart View.
12. Stop at the first unexplained API/schema/permission failure. Diagnose it;
    never disable a gate to continue.

The first smoke test is not a throughput test and must not be used to estimate
the 151 GB cutover duration.

## 4. Evidence to collect from the first test

Record a sanitized report outside Git containing:

- checked-out commit SHA and Python version;
- Content Server and GX39 versions/build identifiers when available;
- source and target host names without credentials or SAS query strings;
- selected source root and target parent IDs;
- inventory counts by object subtype and version count;
- Readiness results;
- HTTP status, endpoint shape and sanitized response schema for failures;
- GX39 correlation/request IDs;
- multipart endpoint and completion-body behavior;
- authentication/token lifetime observations;
- p50/p95 request latency, transferred bytes, 429 count and 5xx count;
- target ID mappings and verification summary;
- manual Smart View, role/access, search and lifecycle observations.

Never record passwords, OTDS/OTCSTicket values, Authorization headers, complete
SAS URLs, file content or unredacted corporate metadata in an AI prompt or Git.
SQLite state may itself contain corporate metadata and must remain on the
corporate machine.

## 5. Corporate contracts still unqualified

The 2026-09-10 GX39 TEST Pilot (section 1) qualified, for the ordinary
folder/document route only, small file sizes and short version chains: GX39
REST shapes for container/document creation, first/subsequent version
semantics without multipart, category/set/multi-row attribute payloads
(read and write), exact target owner/creator read-back, source-date/owner
provenance including ordered version rows, ordinary-route owner-assignment,
and permission inheritance from the destination for that small sample.

The following remain explicitly unknown until tested against corporate systems
or at larger scale:

- production PostgreSQL provider data and exact source inventory (the schema
  *name* difference itself is now qualified and configurable — see section 1
  and `DEPLOYMENT_AND_QUALIFICATION.md` section 5b — but the production tables'
  actual contents, `ProviderData` values and inventory counts remain unread);
- Azure locator construction and representative binary access at production
  scale (the DEV Content Server REST adapter path was qualified for small
  synthetic versions only; direct Azure Blob SAS access remains blocked by an
  `AuthorizationFailure` — see section 1 history);
- multipart start/part/complete requests and responses (not exercised by the
  small Pilot — every file in that run was well under the 50 MiB threshold);
- Business Workspace type/template creation, roles and the
  Business-Workspace owner-assignment/provenance route;
- duplicate-attribute indexing delay and ambiguous-create reconciliation at
  realistic concurrency;
- token expiry/renewal behavior under sustained load;
- WAF, 429, retry and atypical-use limits under production-like concurrency;
- indexing/search delay and production throughput/duration.

Tenant adaptations belong in `engine/client.py` with regression tests. Do not
scatter tenant response-shape exceptions through the pipeline.

## 6. Important limitation of the current Dry Run

Current Dry Run validates manifest structure, node rules, version sizes recorded
in the manifest and the selected binary-source configuration. It does **not**
open and stream every Azure Blob or source Content Server REST version and
therefore does not prove that every production binary is readable or
byte-correct.

The online preflight can sample Blob properties, but a sample is not complete
source evidence. Do not report Dry Run as a full binary-integrity test.

## 7. Agreed pre-production hardening backlog

The owner/provenance implementation and its local regression coverage are now in
the checkout. Before production approval, the following corporate evidence and
remaining hardening work is required.

### P0 — qualify the owner/provenance replacement contract

- Ordinary folders/documents: **done** — qualified against GX39 TEST by the
  2026-09-10 Pilot (section 1). Owner read/write, provenance category
  read/write (including per-version date precision), and duplicate-attribute
  reconciliation are confirmed working end-to-end for that route.
- Business Workspace route: still open — reproduce the same owner/provenance
  read/write and category/date-precision checks for every Business Workspace
  type/template used in scope.
- Exercise lost-response and interruption recovery after owner/provenance
  writes at realistic concurrency (the small Pilot ran with default
  conservative concurrency and did not exercise recovery mid-run).
- Keep GX39 contract changes isolated in `engine/client.py`.
- Add regression tests before changing the pipeline.
- Rerun the complete quality suite and the affected corporate test.

### P1 — resumable Source Integrity Validation

Add a target-free operation that:

- opens every version through the configured read-only binary adapter;
- validates actual Blob size;
- streams SHA-256 with bounded memory;
- persists per-version progress and hash evidence in SQLite;
- supports pause, stop and resumable recovery;
- binds its result to profile ID, source root and inventory signature;
- invalidates PASS when the bound source signature changes;
- makes no GX39 calls and performs no source writes;
- becomes a required production gate.

Do not silently redefine the existing fast Dry Run. Keep the operator language
clear about which operation validates the plan and which reads all source data.

### P1 — explicit Production Canary

Add a production-only canary step that:

- is available only after source read-only confirmation and an unchanged source
  signature;
- selects a deterministic risk-stratified sample, normally about 100 documents
  plus required ancestors;
- writes to the final, access-restricted GX39 PROD destination;
- runs full content/metadata/permission verification;
- blocks Full Cutover until PASS;
- uses the same profile, migration namespace and durable mappings as Full
  Cutover so verified objects are not uploaded again.

Current code enforces source freeze for production `FULL` but not production
`PILOT`. Do not use the current Pilot as an improvised production canary. The
pilot-to-full reuse behavior also needs an explicit regression test before it is
relied on operationally.

### P1 — clarify TEST Pilot data policy

Update UI and runbooks so that a GX39 TEST Pilot explicitly requires synthetic,
non-sensitive or formally approved test data. Existing wording must never be
interpreted as authorization to copy production customer data into GX39
DEV/TEST.

### P2 — sanitized qualification blueprint

Add an aggregate export that describes production shape without exporting
content or identifying metadata. Appropriate fields include:

- counts by supported subtype;
- depth histogram;
- file-size and version-count histograms;
- aggregate category field types/multiplicity;
- aggregate permission cardinality;
- counts of duplicate names, Unicode cases, shortcuts and URLs.

Exclude names, paths, DataIDs, user/group names, comments, category values,
content hashes and file bodies. Review the resulting blueprint under corporate
data-classification rules before moving it anywhere.

### P2 — synthetic qualification dataset tooling

Keep fixture creation separate from the migration controller so the migration
source remains read-only. A fixture builder may generate deterministic local
files and an expected manifest, but Content Server DEV objects must be created
through an approved Content Server UI/API/import path. Never write directly to
Content Server PostgreSQL or its Azure Blob container.

Recommended tiers:

- `smoke`: 20–30 objects and less than 1 GB;
- `fidelity`: about 100 risk-oriented documents, including 49/50/51 MiB,
  100+ MiB and a synthetic file near 7.44 GB;
- `scale`: approximately 22,115 documents, 151 GB total and 543 files over
  50 MB when capacity and approval permit.

The fidelity set should include valid synthetic PDF/Office files, approved
non-sensitive engineering-format samples, multiple versions, deep hierarchy,
Unicode, duplicate names, categories/sets, Business Workspaces, shortcuts,
URLs and fake users/groups/roles. Bulk payloads must not be only sparse or
all-zero files because compression/deduplication can distort throughput.

If Content Server DEV uses a different storage provider from production, record
that source-side Azure behavior remains unqualified until Source Integrity
Validation runs against production.

## 8. Recommended qualification sequence after the smoke test

1. Fix and regression-test real corporate contract findings. **Done** for the
   ordinary folder/document route (see section 1, 2026-09-10 Pilot).
2. Build and run the synthetic fidelity dataset in Content Server DEV.
3. Qualify exact owner resolution, `Owned By`, `Created By` and
   `CDM Migration Provenance` on ordinary and Business Workspace routes,
   including an approved fallback. **Done for the ordinary route; Business
   Workspace still open.**
4. Exercise multipart interruption, token expiry and ambiguous-create recovery
   after metadata writes. **Still open — the 2026-09-10 Pilot used only small
   single-part files.**
5. Run a synthetic scale test if resources and OpenText limits allow it.
6. Complete the GX39 TEST acceptance matrix.
7. Add and complete Source Integrity Validation against the intended production
   source without target writes.
8. Rehearse online state backup and recovery.
9. During the approved cutover: stop work, make source read-only, confirm the
   signature, run Production Canary, make a go/no-go decision, then continue
   Full Cutover.
10. Run full automated reconciliation and technical/business acceptance before
   users are enabled on GX39.

No lower environment can provide 100% certainty about production rate limits,
WAF behavior or final configuration. Layered qualification plus a frozen-source
production canary is the agreed risk-reduction strategy.

## 9. Rules for an AI agent continuing on the corporate machine

- Communicate with the operator in their preferred language; keep code and
  canonical UI terminology in English.
- Ask for or inspect real evidence before changing a tenant contract.
- Treat corporate logs, manifests and screenshots as potentially sensitive.
- Never paste secrets or complete sensitive payloads into chat.
- Do not send corporate test data, logs, SQLite state or configuration to the
  personal GitHub repository.
- Do not push corporate-derived code or evidence to any remote unless corporate
  policy and the operator explicitly authorize that remote.
- Preserve the simple operator UX; do not expose every engine option.
- Do not implement source writes inside the migration controller.
- Do not weaken idempotency, source freeze, TLS, verification or recovery gates.
- Do not claim production readiness because a smoke test or synthetic test
  passed.
- Commit coherent source-only changes locally after tests. Keep `config.json`,
  state databases, logs, generated datasets and credentials untracked.

Before declaring a change complete, run the commands required by `AGENTS.md`.
For a UI change, inspect the actual browser path. For a release, inspect the
archive and its checksum.

## 10. Suggested first prompt for the corporate AI agent

Use the following intent, adapted with the sanitized test result:

> Read `AGENTS.md` and `CORPORATE_HANDOFF.md` completely, then inspect the
> canonical architecture/runbook and current tests. We are qualifying the
> owner/provenance replacement contract against a non-sensitive GX39 DEV/TEST
> dataset after the earlier 19/19 smoke. Diagnose sanitized results without
> weakening safety gates or changing unrelated code. Separate an
> environment/configuration problem from an application defect. If code must
> change, add a regression test, update canonical documentation, run the full
> required quality suite and state exactly which corporate contracts remain
> unqualified.

This handoff is deliberately explicit so a faster or less capable agent does
not confuse planned work with implemented functionality.
