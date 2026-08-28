# Corporate-machine engineering handoff

Last updated: 2026-08-28

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
- Local clean-install validation passes with 62 unit/contract tests, Ruff and
  mypy. The Windows bootstrap was also validated on Python 3.14; CI remains
  pinned to the canonical Python 3.11 baseline.
- The private/home development machine cannot reach corporate PostgreSQL or
  fully qualify corporate Azure/GX39 behavior.
- The first corporate GX39 DEV smoke run completed 19/19 objects and 12/12
  versions with hierarchy, hashes, categories, marker read-back and destination
  permission evidence. It is **not yet approved for production execution**
  because system-date/owner fidelity and the wider qualification matrix remain
  blocked.
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
  `external_modify_date`, but these do not satisfy the current system-attribute
  preservation contract. Online preflight now blocks real runs unless system
  dates and owner are writable through qualified node-update and
  document-version-update contracts; create-form writability alone is
  insufficient.
- The agreed fidelity direction, pending implementation and formal acceptance,
  is: `Created By` remains the migration service account; `Owned By` maps to the
  corresponding GX39 business user; original source dates and owner provenance
  are stored in a dedicated `CDM Migration Provenance` category with explicit
  labels such as `Original Source Created Date (Pre-Migration)` and
  `Original Source Modified Date (Pre-Migration)`.
- Owner resolution must run once per distinct source KUAF owner, never once per
  file. Use exact login/email mappings cached for the run. Unresolved or
  deactivated owners fail Readiness unless an explicitly approved fallback
  principal such as `CDM Legacy Owner` is configured; the original source owner
  remains recorded in provenance. GX39 `Owned By` assignment still requires
  qualification for ordinary folders/documents and every Business Workspace
  route.

Use `git log -1 --oneline` to identify the exact checked-out revision. Never
assume that a release ZIP and the Git checkout are at the same revision.

## 2. Implemented product decisions

The refactor intentionally favors a small internal-operator workflow over a
commercial migration-product UX.

- The server binds to `127.0.0.1:8110` and starts without an API key.
- Successful profile saves close Migration Setup and render confirmation toasts
  above modal overlays so operators can see the outcome. Save errors also render
  inline inside Migration Setup and do not depend on a transient toast.
- Migration Readiness lists missing source owner IDs locally so the operator can
  populate the exact GX39 Member ID mapping without exposing account names. The
  Owners field includes the required JSON direction and syntax example.
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
- Source dates and owners are preserved.
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

## 3. First corporate test: authorized scope

The first run should use the current version without adding large new features.
Its purpose is to expose the real corporate integration contract before more
code is written.

Use only:

- a small, non-sensitive folder/workspace in the on-premise DEV database;
- the matching corporate Azure source storage access;
- an isolated GX39 DEV/TEST destination;
- a profile classified as `test`, never `production`;
- a dedicated test migration namespace and duplicate-protection attribute.

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

The following are explicitly unknown until tested against corporate systems:

- production PostgreSQL schema/provider data and exact source inventory;
- Azure locator construction and representative binary access;
- GX39 REST shapes for container/document creation;
- first and subsequent version semantics;
- multipart start/part/complete requests and responses;
- category, set and multi-row attribute payloads;
- Business Workspace type/template creation and roles;
- preservation and read-back of dates and owners;
- permission inheritance and intended-user access;
- duplicate-attribute indexing delay and ambiguous-create reconciliation;
- token expiry/renewal behavior;
- WAF, 429, retry and atypical-use limits;
- indexing/search delay and production throughput.

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

Do not add all items blindly before the initial smoke test. First incorporate
the observed corporate API contract. Before production approval, however, the
following work is required.

### P0 — fix findings from the corporate smoke test

- Reproduce each finding with sanitized fixtures.
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

1. Fix and regression-test real corporate contract findings.
2. Build and run the synthetic fidelity dataset in Content Server DEV.
3. Exercise multipart interruption, token expiry and ambiguous-create recovery.
4. Run a synthetic scale test if resources and OpenText limits allow it.
5. Complete the GX39 TEST acceptance matrix.
6. Add and complete Source Integrity Validation against the intended production
   source without target writes.
7. Rehearse online state backup and recovery.
8. During the approved cutover: stop work, make source read-only, confirm the
   signature, run Production Canary, make a go/no-go decision, then continue
   Full Cutover.
9. Run full automated reconciliation and technical/business acceptance before
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
> canonical architecture/runbook and current tests. We are performing the first
> non-sensitive DEV-to-GX39-DEV smoke test. Diagnose the attached sanitized
> result without weakening safety gates or changing unrelated code. Separate an
> environment/configuration problem from an application defect. If code must
> change, add a regression test, update canonical documentation, run the full
> required quality suite and state exactly which corporate contracts remain
> unqualified.

This handoff is deliberately explicit so a faster or less capable agent does
not confuse planned work with implemented functionality.
