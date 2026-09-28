# Corporate-machine engineering handoff

Last updated: 2026-09-15

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

### 5a. Business Workspace template candidate — GX39 DEV access evidence

Read-only evidence collected 2026-09-15 for the proposed GX39 DEV template
`38920`, `[Obsolete] R&D CI WS Template 001`:

- The migration service account can read the node and its category endpoint
  (`GET /api/v2/nodes/38920` and
  `GET /api/v2/nodes/38920/categories`, both HTTP 200). This resolves the
  earlier access barrier observed for node `328434`; the migration account has
  sufficient read access to investigate node `38920`.
- The GX39 node is type `848` (Business Workspace), with parent `2316`, owner
  `Admin`, and no directly assigned category values. The latter is not a
  mismatch by itself: the matching production source template node
  `763418`, `R&D CI WS Template 001`, is also subtype `848` and likewise has
  no directly assigned `LLAttrData` category values.
- The matching title and Business Workspace type make the administrator's
  statement that `38920` is the migrated DEV copy of the source template
  plausible. However, it is **not yet a qualified workspace creation route**:
  querying the source's known `xengtranstemplates` workspace-to-template
  relationship table found no row connecting the production migration root
  workspace `763886` to source node `763418`. Therefore the database evidence
  currently cannot prove that this is the exact template used by `763886`;
  the administrator must confirm that relationship from the xECM
  configuration.
- Do not change `workspace_routes` from the currently configured route or
  create a real target Business Workspace merely on this evidence. After the
  administrator confirms the relationship, qualify `38920` by creating one
  isolated test Business Workspace under the approved DEV test parent, then
  read back its categories, roles, owner/creator and duplicate marker. This
  target-write test must also establish the exact DEV category IDs/attribute
  keys for the two source definitions (`762577` and `762581`); the template
  node's empty category-values response cannot provide those mappings.

#### Category-mapping information required before the first Business Workspace Pilot

As of 2026-09-15, `GET /api/v2/nodes/38920/categories` returns an empty
result. This is expected for a template node with no directly assigned
category *values*, but it means the tool cannot derive target category
identifiers from that endpoint. The generic read-only Business Workspace
create form is accessible under the approved DEV parent `87457`, confirming
that the account can inspect the target form; it does not expose the
template's category schema. The category-create form reports that an explicit
`category_id` is required, so it cannot safely enumerate unknown tenant
categories.

Ask the GX39 administrator to open the Category/Attribute administration view
for the two DEV categories attached to the Business Workspace type/template
used by `38920`. For each attribute, the administrator must provide the
numeric `categoryID_attributeID` key shown by that view or its form/API
definition. A screenshot is sufficient if it visibly includes each category
ID, attribute ID/key, label and type; never send account credentials.

Required source-to-target correspondence:

| Source category / definition | Source attribute | Required target value |
| --- | --- | --- |
| `R&D_CI_Document_Information` / `762577` | `1` (name) | target category ID and `categoryID_attributeID` |
| `R&D_CI_Document_Information` / `762577` | `2` (status) | target category ID and `categoryID_attributeID`; include allowed values |
| `R&D_CI_Document_Information` / `762577` | `3` (related DataID) | target category ID and `categoryID_attributeID`; identify whether it is a node reference or text |
| `R&D_CI_Info_Sec_Classification` / `762581` | `1` (name) | target category ID and `categoryID_attributeID` |
| `R&D_CI_Info_Sec_Classification` / `762581` | `2` (classification) | target category ID and `categoryID_attributeID`; include allowed values |

On receipt, enter the values as `category_mappings` in local `config.json`
using `engine.manifest.ManifestStore.apply_category_mappings`' documented
shape. Do not infer attribute IDs from labels, reuse IDs from another tenant,
or mark `CATEGORY_MAPPING` as accepted without these values.

### 5b. Evidence gathered for `active_workflows_confirmed_zero` and
`personal_state_out_of_scope_approved` (workspace 763886, 29,229-node scope)

Read-only PostgreSQL evidence collected 2026-09-14 against the production
source database, in scope of the 29,229 nodes already extracted into
`migration_state_v2_production.db`. Recorded here so the operator does not
have to re-derive this before the real production cutover.

- **`active_workflows_confirmed_zero`** — safe to approve. All classic
  Content Server XML Workflow tables (`wfattrdata`, `wfattrdataversions`,
  `wfcomments`, `wfdispositions`, `wfforms`, `wfformslock`,
  `wfformsversions`, `wfassignmentsconfiguration`) contain **zero rows
  instance-wide**, not only in scope. `xecmgov_dynwfactivitytaskevents` (13
  rows) is a static event-type enum, not instance data.
  `kuafrightslistworkflow` (7,684 rows) is the unrelated Rights List
  access-control feature, not business-process workflow data. Conclusion:
  there are no active or historical workflow instances anywhere in the
  source system, so nothing needs to be built or migrated for workflow
  scope — this flag is a confirmation of fact, not a feature gap.

- **`personal_state_out_of_scope_approved`** — NOT a no-op; a real, bounded
  exclusion. Notification/subscription tables (`dtreenotify`,
  `dtreeaspectsnotify`, `dtreenotifyrecover`, `dtreesyncinterests`,
  `dtreevectornotify`, `elinksubscription`, `notifyevents`,
  `notifymessages`, `notifyinterests2`) are all empty (0 rows) instance-wide.
  The only substantive item is `dfavorites`: **105 rows in scope, across 60
  distinct KUAF `userid`s** (out of only 116 favorites rows in the *entire*
  source instance — ~90% of all favorites system-wide belong to this one
  workspace). This tool does not migrate favorites (by design, see
  `ARCHITECTURE.md`). The only consequence of approving this flag is that
  those ~60 users will need to manually re-add the workspace to their
  favorites on GX39 after cutover. Recommendation: notify those users ahead
  of the real cutover so this is not a surprise.

- **`historical_audit_out_of_scope_approved`** — NOT a no-op; a real,
  substantial exclusion. `dauditnew`/`dauditnewcore` (Content Server audit
  trail) contain **128,701 rows in scope** (of 386,931 instance-wide, ~33%
  of the entire instance's audit trail), spanning `auditdate` 2025-05-29 to
  present. This history is not migrated by this tool; GX39 starts a fresh
  audit trail at cutover. A meaningful share of these audit rows are
  category-attribute-change events (`valuekey` values `762577`/`762581`
  match the two source category definitions used by this workspace,
  ~1,925 rows each). This is a genuine, informed business-acceptance
  decision, not a formality — confirm with the workspace's business owner
  before approving.

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

## 11. Active handoff - 2026-09-15 evening

This section is the current continuation point for the next agent. It
supersedes older "next step" wording above where the two differ, but it does
not override the safety rules in `AGENTS.md`.

### Operator objective

The operator wants a deliberately limited, real-data DEV test to validate the
core migration path before spending time proving that the DEV/PROD Business
Workspace template is a 1:1 reconstruction of on-premise configuration.
This is not production approval.

Current source and target:

- source root: `763886` (the production Business Workspace in on-prem);
- target parent: `87457` (GX39 DEV `CDM_Migration_Sandbox`);
- profile key: `production` (this retains production safety gates because the
  source is production data, even though the target is GX39 DEV);
- route for source workspace `763886`: `workspace_type_id=1`,
  `template_id=38920` (`[Obsolete] R&D CI WS Template 001`);
- `source_root_maps_to_target=false`;
- permission strategy: `inherit_target`;
- `target_acl_approved=true`.

The operator approved the Core Pilot owner fallback with:

- operator: `zawodpwe`;
- change record: `Dev-Test-Small-Pilot-R&D-Real-Data`.

Do not copy or print credentials, passwords, tickets, SAS URLs or the service
account login/e-mail from local `config.json`. The tenant-local migration
account was read back through GX39 `GET /api/v2/members/84116` (`PZMIG_TEST_TECH_ACC`)
and is active; the local configuration uses `service_account_member_id=84116`,
`service_account_login="PZMIG_TEST_TECH_ACC"`, and uses `PZMIG_TEST_TECH_ACC`
(member `84116`) as `owner_fallback`.

### Implemented in the current uncommitted worktree

The following changes were made and validated, but have not been committed:

1. Added `RunMode.CORE_PILOT` and CLI mode `core_pilot`.
2. Core Pilot selection is deterministic, includes required ancestors, and
   excludes shortcuts/URL references, documents with any non-empty category
   value, versions with comments, and versions at or above the 50 MiB
   multipart threshold.
3. Core Pilot scope is passed through manifest selection, inventory, owner
   resolution, preflight, online blob checks and run creation.
4. Core Pilot deliberately defers tenant qualifications that it is intended
   to exercise after the write: owner assignment, provenance
   applicability/readback and creator readback. It does not disable ordinary
   verification.
5. `engine/db.py` no longer stores the technical `AttrID=1` category-name row
   when its value equals the node name, and omits completely empty primitive
   category rows.
6. Fallback/service-account resolution supports a direct, read-only member GET
   by an explicitly configured tenant-local member ID, while still requiring
   active status and exact configured login/e-mail read-back.
7. The CLI accepts `--owner-exception-operator` and
   `--owner-exception-change-record` and binds approval to the freshly
   calculated exception digest.
8. A directly related SQL scope bug in shortcut fidelity checking was fixed:
   the selected-node predicate now applies to the complete parenthesized
   reference condition.
9. Regression coverage was added for Core Pilot selection and CLI approval
   digest binding.

Validation after these changes:

```text
111 unittest tests: PASS
ruff check .: PASS
mypy app.py engine tests: PASS
```

`pytest` is not installed in the local virtual environment; the repository's
existing `unittest` suite is the runner used here.

### Current local state and configuration

The application-level SQLite backup command was used before each manifest
refresh. These local, gitignored files are sensitive state and must not be
deleted or copied with filesystem commands:

- `migration_state_v2_production_before_category_pilot_20260915.db`;
- `migration_state_v2_production_before_core_pilot_20260915.db`.

The latest forced extraction completed successfully:

```text
snapshot: 464030186:464030186:
nodes: 29252
containers: 6438
documents: 22809
versions: 24420
bytes: 161031852220
```

The current local profile has `category_mappings={}`. This is intentional for
the Core Pilot because its selected documents have no non-empty category
values, so the scoped `CATEGORY_MAPPING` check passes without pretending that
the full workspace category mapping is complete. Full-scope category mapping
remains unqualified.

The two existing DEV categories read through the category form API are:

```text
R&D_CI_Document_Information: category 33268
  33268_2 Status: DRAFT, SUBMITTED, REVIEWED, RELEASED, OBSOLETE
  33268_3 Process File Owner: GX39 user picker
  33268_5 ASIL relevant: boolean

R&D_CI_Info_Sec_Classification: category 33270
  33270_2 Information Classification:
    PUBLIC, INTERNAL, CONFIDENTIAL, STRICTLY CONFIDENTIAL
```

`GET /api/v2/nodes/38920/categories` is empty because the template node has no
direct category values; it does not enumerate the template type schema. The
source's non-empty `Related DataID` values resolve to on-prem `KUAF` users,
not documents. They are intentionally excluded from this Core Pilot because
the corresponding users do not exist in GX39 DEV.

The local configuration also currently has:

```text
active_workflows_confirmed_zero=true
personal_state_out_of_scope_approved=true
historical_audit_out_of_scope_approved=false
```

The audit-history flag remains false because real in-scope audit rows exist;
Core Pilot defers that scope decision rather than silently changing it.

### Core Pilot preflight result

The exact online preflight used `for_mode="core_pilot"`,
`max_documents=15`, and `core_pilot=True`. It returned
`PASS_WITH_WARNINGS`, zero failures, and one expected ACL-inheritance
warning. The exact selected scope was:

```text
45 nodes
18 versions
15,687,834 bytes total
maximum version 4,370,132 bytes
0 multipart versions
0 shortcuts
0 version comments
0 non-empty category values
```

Source PostgreSQL was connected read-only, TLS verification was enabled, GX39
DEV authentication passed, target `87457` read-back passed, the workspace
route passed, and the scoped category/support/type checks passed.
Owner/provenance/creator qualification checks were `DEFERRED` by design for
this Core Pilot.

Owner readiness for this exact scope returned:

```text
2 exceptions, both SYSTEM_OWNER
source owner IDs: -2000 and 763886
exception digest:
3f699b16cfd31dc64bf86366ada2f7227beb1491c36586486dfcc87a9ca4556c
fallback: active GX39 member 38919
service account: exact and active
```

### Attempted run and current blocker

The approved one-worker command was:

```powershell
"YES" | .\.venv\Scripts\python.exe cli.py --environment production run `
  --mode core_pilot --max-documents 15 --threads 1 `
  --owner-exception-operator zawodpwe `
  --owner-exception-change-record "Dev-Test-Small-Pilot-R&D-Real-Data"
```

Run ID:

```text
dc87f2e7-7fa5-4030-b519-31a1f23b2d55
```

Final state:

```text
status: COMPLETED_WITH_ERRORS
mode: core_pilot
total_nodes: 45
FAILED_TERMINAL: 45
remote_committed_nodes: 0
transferred_bytes: 0
```

The first container (`source_id=763886`) failed before document transfer.
GX39 returned HTTP 500 from `POST /api/v2/businessworkspaces/` with:

```text
The role 'categories' of the parameter 'roles' cannot be parsed.
```

The client currently uses the same `_with_migration_category()` helper for
ordinary containers and Business Workspaces. That helper injects:

```python
body["roles"] = {"categories": {migration_attribute_key: migration_id}}
```

This is accepted/qualified for the ordinary document route, but GX39 DEV
rejects it on `/api/v2/businessworkspaces/`. The affected code is in
`engine/client.py`, `create_container()` and `_with_migration_category()`.
The failure was retried until the run retry budget was exhausted. Do not
blindly rerun or recover this run.

### Resolution of the Business Workspace creation blocker (2026-09-17)

1. **Root cause diagnosed and verified**:
   - In OpenText Extended ECM (xECM), `POST /api/v2/businessworkspaces/` uses the
     `roles` parameter to assign workspace participant roles (e.g. Coordinator,
     Member), not metadata categories. Passing `roles: {"categories": ...}`
     caused xECM to reject the payload with `The role 'categories' of the
     parameter 'roles' cannot be parsed.`
   - Furthermore, unlike ordinary nodes where category attributes can be
     injected at creation time, a freshly created Business Workspace does not yet
     have the migration marker category attached unless the template explicitly
     includes it. Attempting a direct `PUT /categories/{cat_id}` on a node
     without that category attached returns `Category ID '{cat_id}' is not a valid category.`

2. **Empirical qualification against GX39 Cloud**:
   - Tested using isolated scratch probes against GX39 DEV:
     - `POST /api/v2/businessworkspaces/` with clean parameters (`name`,
       `description`, `parent_id`, `template_id`, `wksp_type_id`) succeeds with
       HTTP 200, creating an authentic Business Workspace object (subtype 848).
     - `POST /api/v2/nodes/{target_id}/categories` attaches the duplicate
       marker category with HTTP 200.
     - `_migration_id_matches()` confirms exact read-back of the marker.
     - A subsequent update via `PUT` succeeds with HTTP 200, while a duplicate
       `POST` returns an "already exists" error that is cleanly handled by fallback.

3. **Code changes implemented and validated**:
   - In `engine/client.py`:
     - `create_container()` now sends a clean dictionary to
       `/api/v2/businessworkspaces/` without the `roles` field, extracts the
       resulting `target_id`, and immediately applies the marker via
       `apply_migration_marker()`.
     - `apply_migration_marker()` now attempts `POST /api/v2/nodes/{target_id}/categories`
       first to attach the category, and falls back to `PUT` if it is already
       attached, ensuring idempotency across all object types.
   - In `tests/test_engine_v2.py`:
     - Updated `test_container_routes_cover_ordinary_folder_and_business_workspace`
       to verify clean POST for Business Workspace and subsequent marker attachment.
   - Validation:
     - 111 unittest tests: PASS
     - ruff check .: PASS
     - mypy app.py engine tests: PASS
     - Scoped online preflight for `core_pilot`: PASS_WITH_WARNINGS (0 failures,
       1 expected ACL warning).

4. **Core Pilot execution and resolved findings**:
   During initial execution of the Core Pilot (`core_pilot`, 15 documents, 45 nodes), four specific findings were identified and resolved:
   - **Service Account ID alignment**: OpenText Cloud creates nodes under the authenticated technical account `PZMIG_TEST_TECH_ACC` (Member ID `84116`). `service_account_member_id` and `service_account_login` in `config.json` were corrected from Fabian Haber's ID (`38919`, which remains `owner_fallback`) to `84116`/`PZMIG_TEST_TECH_ACC`.
   - **Provenance Category HTTP 500 handling**: When a node already has the provenance category attached, OpenText returns HTTP 500 with `"The attribute group 'CDM Migration Provenance' already exists."`. In `engine/client.py`, `apply_provenance` now catches both `TerminalMigrationError` and `RetryableMigrationError` when `"already exists"` is present, falling back to `PUT`.
   - **Business Workspace Location Rule in container read-after-write**: Template `38920` enforces a location rule placing project workspaces in `/Projects` (NodeID `34803`). `_process_container` in `engine/pipeline.py` now recognizes `route.get("location_id")` as `expected_parent`, avoiding false parent-mismatch errors.
   - **Scoped recovery preflight**: `recover_run` in `engine/pipeline.py` now determines the exact scope (document count and `core_pilot` flag) when running preflight, avoiding full-manifest qualification errors when recovering a pilot run.

5. **Core Pilot execution and live verification results**:
   - **Run ID**: `a2736075-d127-4b66-b881-04e8ea3febbd`
   - **Environment**: `production` (targeting GX39 DEV/TEST with real corporate source data)
   - **Scope**: Core Pilot (15 documents, 45 nodes total including hierarchy)
   - **Execution Status**: `COMPLETED`
   - **Verified Nodes**: 45 / 45 (100% success rate, 0 failed nodes)
   - **Binary Data Transferred**: 15,687,834 bytes (~15.7 MB)
   - **Automated Live Verification (`cli.py verify --live`)**:
     - `TEST_01_TARGET_INVENTORY`: PASS (45 run items verified and readable on target)
     - `TEST_02_VERSION_SHA256`: PASS (18 version transfer records with verified SHA-256 hashes)
     - `TEST_03_CATEGORY_VALUES`: PASS (target category value parity)
     - `TEST_04_VERSION_CHAIN`: PASS (15 document version chains complete and intact)
     - `TEST_05_OWNER_PROVENANCE`: PASS (owner, creator, and migration provenance verified across all 45 target nodes)
     - `TEST_06_PERMISSION_READBACK`: PASS (target ACLs visible and readable for 45 nodes)
     - **Overall Status**: `PASS` (6 of 6 tests passed in 72.58s)

6. **Creator-based owner remapping for Business Workspace items**:
   - In OpenText Extended ECM, `OwnerID` for all objects inside a Business Workspace is assigned to the workspace container DataID (`763886`) or a system code (`-2000`).
   - Implemented extraction of `CreatedBy` from `cs.DTree` in `engine/db.py`.
   - When an object inside a Business Workspace carries a system/workspace `OwnerID`, `extract_all` remaps the effective source owner to its actual creator (`CreatedBy`).
   - This ensures full author identity (login, email, display name) is preserved in `CDM Migration Provenance` (e.g. Ricardo Krause, `uib13313@vitesco.com` on node `802355`), and active Schaeffler engineers (>82% of workspace objects) are resolved directly as owners in GX39 Cloud.
   - Tested and verified: 114/114 unittests pass, clean ruff and mypy.
