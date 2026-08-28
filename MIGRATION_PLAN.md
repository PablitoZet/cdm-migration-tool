# Production migration plan — on-premise Content Server to GX39

Status: implementation handoff baseline, 2026-08-26.

This plan governs the primary approved production workspace migration. All
inventory figures and tenant behavior must be reconfirmed on the corporate
migration machine before approval. Operating steps are defined in
`DEPLOYMENT_AND_QUALIFICATION.md`.

## 1. Objective

Users finish work on the on-premise system, the selected source is made
read-only, the complete workspace is migrated and verified in GX39, and users
resume work in the cloud with equivalent content and business behavior apart
from unavoidable technical ID changes.

The migration must preserve:

- hierarchy, names and descriptions;
- every supported document and version;
- binary integrity;
- category values;
- create/modify dates and owners;
- approved effective access and Business Workspace roles;
- shortcuts/URLs within the approved scope;
- search, lifecycle operations and legacy-link continuity.

## 2. Primary production baseline

| Item | Current planning value |
|---|---:|
| Source root DataID | Configured at runtime; not stored in the repository |
| Total objects | 28,730 |
| Document files | 22,115 |
| Folders/containers | 6,341 |
| Archived email objects | 268 |
| Shortcuts/aliases | 5 |
| Collections | 1 |
| Binary volume | approximately 151 GB |
| Files above 50 MB | 543 |
| Largest known file | approximately 7.44 GB |
| Source database | PostgreSQL `cs` on the production Content Server platform |
| Binary source | corporate Azure Blob Storage |
| Test target | GX39 TEST |
| Production target | GX39 PROD |

These are planning values, not hard-coded acceptance values. The signed source
scan performed on the corporate machine becomes the authoritative cutover
inventory.

## 3. Migration method

Direct database restoration is outside tenant administrator control in GX39
SaaS. The tool therefore performs controlled API ingestion:

1. extract metadata and hierarchy from a read-only PostgreSQL snapshot;
2. store an immutable local manifest in SQLite;
3. stream binaries from Azure without full-file local staging;
4. create supported structures and versions through OpenText REST APIs;
5. store durable source-to-target mappings;
6. verify target content and business behavior.

GX39 generates new NodeIDs. Source DataIDs are retained in local mappings and a
technical `CDM Migration ID` attribute used for safe retries and future link
resolution.

## 4. Scope boundaries

In scope:

- the selected source root and supported descendants;
- supported folders, Business Workspaces, documents, versions, shortcuts, URLs
  and collections;
- mapped categories, dates and owners;
- approved destination permission inheritance;
- reconciliation evidence and legacy-to-cloud ID mapping.

Not migrated by this tool:

- historical audit event history;
- personal favorites, recents, subscriptions and UI preferences;
- active workflow execution state;
- external systems or proxy/WebReport deployment.

Out-of-scope items require explicit business/technical acceptance or a separate
workstream.

## 5. Current readiness status

Verified locally:

- deterministic SQLite state engine and recovery;
- manifest/profile/root binding;
- source signature logic;
- bounded streaming and multipart checkpoint model;
- duplicate reconciliation logic;
- fail-closed readiness and verification behavior;
- localhost UI and secret-free release packaging;
- unit, lint, type and UI-contract tests.

Pending corporate evidence:

- source PostgreSQL access and final inventory;
- Azure locator correctness for all providers;
- GX39 duplicate attribute and category applicability;
- GX39 multipart API dialect and large-file behavior;
- target category and Business Workspace mappings;
- owner preservation and permission inheritance;
- service-account rights, throttling, WAF and token behavior;
- representative Pilot, operational UAT and measured throughput;
- final PROD destination and legacy-link implementation.

No production date or duration should be committed until the representative
GX39 TEST Pilot provides measured evidence.

## 6. Roles and approvals

| Role | Responsibility |
|---|---|
| Migration operator | Configure profiles, scan, run, monitor, recover and export evidence |
| Content Server administrator | Provide read-only DB access and enforce production source freeze |
| Azure/storage administrator | Provide scoped container SAS and validate provider mapping |
| GX39 administrator | Create destination, service rights, duplicate attribute, schemas and workspace routes |
| OpenText/CSM support | Acknowledge atypical use and approved load characteristics |
| Business owner/key users | Validate content access and normal working behavior |
| Change/rollback owner | Go/no-go authority and recovery coordination |

Named individuals and contact channels belong in the controlled change record,
not in this public-ready repository.

## 7. Qualification sequence

1. Install from a clean Git clone/release on the corporate migration machine.
2. Configure GX39 TEST and PROD profiles entirely through the UI.
3. Validate source root, target parent and Azure container.
4. Configure GX39 duplicate protection.
5. Scan the intended source and resolve all structural Readiness issues.
6. Execute Dry Run.
7. Execute the mandatory GX39 TEST contract matrix.
8. Run a deterministic Representative Pilot.
9. Exercise Pause, Stop and Resume interrupted run.
10. Run live automated verification and operational UAT.
11. Tune concurrency from measurements and obtain OpenText approval.
12. Rehearse state backup/restore and operator procedure.
13. Approve production mappings, destination access and exclusions.

Any unexplained failure returns the plan to the relevant qualification step.

## 8. Production cutover sequence

1. Confirm go/no-go participants and approved change window.
2. Verify the production profile, credentials, source DataID and destination
   parent NodeID.
3. Create an online backup of production profile state.
4. Stop user changes and make the selected on-premise source read-only.
5. Start Full Cutover and record operator/change information.
6. Allow the application to compare the frozen source signature with the
   scanned manifest.
7. If equal, execute container, document/version and reference phases.
8. Monitor telemetry and pause on sustained tenant instability.
9. Recover only after diagnosing a stopped/failed run; do not start a competing
   new run.
10. Execute full live reconciliation and export evidence.
11. Execute technical and business acceptance.
12. Enable users/legacy routing only after formal acceptance.

## 9. Go/no-go criteria

Go requires all of the following:

- exact frozen source signature matches the approved scan;
- GX39 TEST contract suite and Representative Pilot pass;
- all Readiness checks pass;
- zero unresolved category/owner/workspace mappings;
- destination ACL and roles are approved;
- state backup and recovery rehearsal pass;
- approved concurrency and OpenText atypical-use acknowledgement;
- sufficient time remains for upload plus verification;
- business and rollback owners are available.

No-go conditions include an unexpected source change, missing binary, invalid
destination, unknown target API behavior, sustained throttling/errors,
verification mismatch or unavailable acceptance owner.

## 10. Verification and acceptance matrix

### Automated reconciliation

- exact terminal item/mapping coverage;
- target hierarchy and name read-back;
- all version counts/order;
- all-version source/target SHA-256 equality;
- category value read-back;
- dates/owners and permission evidence;
- zero unresolved or terminal failures.

### Technical acceptance

- intended user access and Business Workspace roles;
- SSO and Smart View access;
- search/facets after indexing;
- shortcuts, URLs and legacy links;
- checkout, metadata edit, new version and new document operations;
- representative small and multi-gigabyte content access.

### Business acceptance

- familiar navigation and hierarchy;
- representative engineering documents and versions;
- metadata-based daily work;
- role-based access;
- accepted treatment of historical audit and personal UI state.

## 11. Recovery and rollback

The application provides resumable recovery, not automated rollback.

- Resume uses the original run and checkpoints.
- Verified items are skipped.
- Ambiguous creates are reconciled through `CDM Migration ID`.
- Production resume requires the unchanged frozen source.

If production acceptance fails, keep the source read-only and invoke the
approved change rollback plan. Any quarantine/deletion of partially migrated
GX39 content is a separate administrator-controlled action with explicit target
identification and approval; the migration tool must not perform an inferred
bulk delete.

## 12. Legacy-link workstream

Source DataIDs cannot be preserved as GX39 NodeIDs. Existing links require a
separately qualified resolver using the exported source-to-target mapping or an
indexed migration attribute. Proxy/servlet/WebReport routing must be tested for
browse and download actions before user release. This workstream must not block
safe migration engine recovery, and it must not infer targets by object name.
