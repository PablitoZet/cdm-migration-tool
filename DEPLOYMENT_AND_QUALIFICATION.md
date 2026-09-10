# Deployment, qualification and operator runbook

This is the canonical runbook for installing and operating the CDM Migration
Tool. It separates actions that work on any machine from evidence that can only
be collected on the corporate network.

## 1. Fresh Git clone or transfer package

### From Git

Clone the private repository on the corporate migration machine and enter its
root directory. Do not copy a private-machine `.venv`, `config.json` or SQLite
state database into the clone.

### From a release ZIP

Transfer both the generated ZIP and its `.sha256` file, verify the digest and
extract the archive. The ZIP is built with:

```bash
python3 package_release.py
```

The archive must not contain secrets, `config.json`, state databases, logs,
caches, virtual environments or previous releases.

## 2. Runtime requirements

- Python 3.11 or newer;
- network access to the approved Python package source for first installation,
  or a reviewed internal wheelhouse;
- browser access to `http://127.0.0.1:8110`;
- corporate connectivity to source PostgreSQL and Azure Blob Storage;
- outbound HTTPS to GX39 TEST/PROD.

HTTPS clients use the native operating-system certificate store. On Windows,
the approved corporate root and intermediate CAs must be installed in the
Windows certificate store. A missing issuer is an environment/certificate-chain
failure; never work around it by disabling TLS verification.

If internet package installation is blocked, prepare dependencies through the
corporate artifact process and install with:

```bash
python -m pip install --no-index --find-links <wheelhouse> -r requirements-dev.txt
```

## 3. First start

Linux/macOS:

```bash
./start.sh
```

Windows PowerShell:

```powershell
.\start.ps1
```

The first launch:

1. creates `.venv`;
2. installs runtime dependencies;
3. executes the unit tests;
4. copies `config.example.json` to local `config.json`;
5. starts the application on `127.0.0.1:8110`.

Bootstrap fails closed if virtual-environment creation, dependency installation
or unit tests return a non-zero exit code. Do not start the application or
continue corporate qualification until that failure is diagnosed.

Before configuration, the scope header must show both source and target as
`not configured`. If it displays unexpected identifiers, stop and verify that
the intended local `config.json` and profile state are active.

The application does not require an API key. It must not be exposed on a network
interface or reverse proxy.

If PowerShell blocks local scripts, use the execution policy approved for the
corporate workstation rather than weakening machine-wide policy.

## 4. Configure profiles in the UI

A fresh installation contains two profiles:

- **GX39 TEST qualification** — for scans, Dry Run and real TEST pilots;
- **GX39 PROD cutover** — for the final production target and production gates.

Open **Migration Setup** for each profile and enter:

### Source Content Server

- PostgreSQL host, database, user and password;
- **Source root DataID**: a supported folder or workspace whose entire subtree
  is in scope.

Use a read-only database principal. `Check source workspace` must return the
expected root name/type and plausible inventory before scanning.

### OpenText Cloud

- GX39 Content Server URL;
- migration service account and password;
- **Destination parent NodeID**: an existing GX39 folder/workspace that accepts
  child objects.

The application creates the selected source root inside this destination. It
does not assume source and target IDs are equal. Use `Check Cloud destination`
to confirm the exact destination before any Pilot.

### Source document files

For Archive Center `acprimary` sources, enter the source Content Server REST URL
and a read-only service account. The application streams
`/api/v2/nodes/{DataID}/versions/{version}/content` and does not stage binaries.
On multipart recovery it replays the completed prefix from byte zero to
reconstruct SHA-256 and then continues the same source stream, so HTTP Range is
not required for correctness. Include that replay cost in recovery timing.

For a provider that exposes direct blob paths, paste one complete Azure
**container-level SAS URL** containing Read and List rights. A blob-specific URL
is rejected. Archive Center `ixos://` handles are not Azure blob names and must
never be used as a path template.

### Local credential storage

Passwords and SAS values are persisted in local `config.json` so operators do
not have to retain them in notes. Protect the corporate workstation and file;
where supported, it is set to mode `0600`. The file is ignored by Git and
excluded from releases.

Environment variables are an optional corporate override, not a normal UI
requirement:

```text
CDM_DB_PASSWORD_<PROFILE>
CDM_SOURCE_CS_PASSWORD_<PROFILE>
CDM_OT_PASSWORD_<PROFILE>
CDM_AZURE_SAS_URL_<PROFILE>
```

### Test before saving

Migration Setup provides **Test connections (without saving)**. It tests the
values currently present in the form, including newly entered credentials,
without writing `config.json`, replacing the active runtime profile or changing
SQLite state. The source check uses the read-only PostgreSQL path and the target
check authenticates to GX39; it does not create or update target objects.

The dialog shows a short status for each connection first. Expand **Technical
details** only when diagnosing a failure. Save the setup after the displayed
connection checks pass; an unsaved successful test is not a persisted profile
configuration.

## 5. Configure duplicate protection

Before the first real upload, select **Duplicate protection**.

The GX39 administrator must provide an indexed text attribute, preferably named
`CDM Migration ID`, applicable to all migrated object types. Enter its OpenText
attribute key in `categoryID_attributeID` format, for example `12345_2`.
On GX39 CE 25.4, a `Text: Field` length must be less than 255; use length 254,
one locked row, `Required` and `Show in Search`.

The tool writes `CDM:<namespace>:<source DataID>` and uses it to reconcile a
create that may have succeeded when its HTTP response was lost. Never bypass
this requirement and never substitute same-name matching.

The same target tenant attribute can be reused by profiles when its category is
valid for their migrated types. Namespace values keep profile identities
separate.

## 5a. Required GX39 tenant configuration (must be reproduced 1:1 per environment)

The migration only works if the target GX39 tenant has the following objects
configured *before* Dry Run/Pilot/Cutover. These are tenant-side admin settings,
not tool configuration, and are **not exported by any release ZIP or state
backup**. Every new tenant (a fresh TEST tenant, a tenant reset, or the
production tenant) needs this section re-applied and re-verified before any
Pilot is attempted there. Treat this list as the single source of truth and
update it immediately whenever a category, attribute or attribute type changes
in any qualified tenant — do not let this drift from what is actually deployed.

1. **Duplicate-protection attribute** (see section 5): one indexed text
   attribute (`Text: Field`, length 254, one locked row, `Required`,
   `Show in Search`), applicable to every migrated object type. Record its
   `categoryID_attributeID` key in the profile's Duplicate protection field.

2. **`CDM Migration Provenance` category**: one category applicable to every
   migrated object type (folders, documents, Business Workspaces), containing:
   - Node-level fields: source DataID (text), source created/modified date
     (datetime — see point 4), source owner ID/login/email/display name
     (text), source owner resolution status (text), source system (text).
   - One ordered multi-row/set field for document version provenance,
     containing per-row: version number (number), version created date,
     version modified date, version file date (all datetime — see point 4).
   - Record the category ID and every field/attribute key
     (`categoryID_attributeID`, and for the set the row-column keys) in the
     profile's Configure provenance dialog, including the set's own field key
     (`provenance_versions_field`) — this is a distinct value from the
     individual row-column keys and is easy to forget (see finding #8).
   - GX39 TEST reference values currently qualified (test tenant only, will
     differ per tenant — do not assume they apply to production without
     re-reading the category from the production tenant admin UI):
     category `156923`; node-level fields `156923_2`..`156923_10`; version set
     field key `156923_11`; version row-column keys
     `156923_11_x_12` (version number), `156923_11_x_13` (version created
     date), `156923_11_x_14` (version modified date), `156923_11_x_15`
     (version file date).

3. **GX39 migration service account**: an active account used as `Created By`
   for every migrated object and as the object owner until the final OWNER
   phase reassigns it to the resolved target owner. Its exact login/email must
   be entered in the profile so the client can verify creator read-back.

4. **Attribute type for every date/time provenance field must be `datetime`,
   never `date`.** GX39 silently truncates `date`-typed attributes to midnight
   on write, which is indistinguishable from a code bug during a Pilot (see
   findings #18 and #20 — this cost significant investigation time before the
   category schema was found to be the real cause). When creating or
   reviewing the `CDM Migration Provenance` category (and any multi-row/set
   version fields inside it) in GX39 category admin, confirm every
   created/modified/file-date attribute is `datetime`, not `date`, by checking
   `/api/v1/forms/nodes/categories/create` (or the equivalent update form) for
   `"type"` on that attribute before running a Pilot. Re-verify this after any
   category recreation, tenant refresh, or migration to a new GX39 tenant.

5. **Destination ACL**: the destination parent NodeID must have the intended
   access already configured (approved permissions/role membership) before the
   Pilot, since migrated objects inherit it — the tool does not create ACLs.

6. **Business Workspace types/templates**: every Business Workspace subtype
   (for example 848) present in the migration scope must have its target
   type/template route already created and reachable in the destination
   tenant, with roles behaving as expected.

Corporate-machine qualification (`DEPLOYMENT_AND_QUALIFICATION.md` section 8)
must re-confirm points 1-6 explicitly for the production GX39 tenant before
Full Cutover; do not assume TEST tenant configuration was copied correctly.

## 6. Scan and offline readiness

Select the correct profile and click **1. Scan Source Workspace**. The scan uses
one repeatable-read, read-only PostgreSQL snapshot and replaces the profile's
local inventory after explicit confirmation.

Review:

- root name and DataID;
- total folders, documents, versions and bytes;
- maximum file size and depth;
- detected subtypes, categories, owners and Business Workspaces;
- Azure binary locators.

The inventory is bound to the profile and Source root DataID. After changing
either, scan again.

Headless backup and scan equivalents:

```bash
python cli.py --environment test backup state-before-scan.db
python cli.py --environment test extract --force
python preflight.py --environment test
```

Do not continue while Readiness reports an unexplained failure. Populate mapping
exception fields only when Readiness identifies the specific missing category,
owner or Business Workspace route.

Owner and provenance are configured as one-time prerequisites when Readiness
requests them:

- `Created By` is the GX39 migration service account; enter its exact login and,
  when available, email for online identity read-back.
- `Owned By` is resolved automatically once per distinct source KUAF owner.
  The tool uses exact trimmed, case-insensitive equality between the source
  e-mail and the GX39 Login Name (which is an e-mail in this tenant) and never
  accepts display names, filenames or same-name objects as identity evidence.
- If automatic matching fails, the operator may provide an optional
  `owner_mappings` entry keyed by source owner ID and cloud Login Name.
  Otherwise the default fallback is the configured GX39 migration account.
  The operator explicitly chooses manual mappings or that fallback at the run
  boundary; the choice is recorded in provenance.
- The GX39 administrator must provide the dedicated `CDM Migration Provenance`
  category ID and attribute keys. Its labels must explicitly identify
  `Original Source` and `(Pre-Migration)` values for the source DataID, created
  and modified dates, owner identity, source system and resolution status.
  Documents additionally require ordered multi-row/set fields for every source
  version number and source created/modified/file date.

Original source dates and owner identity remain in provenance. GX39 system
create/modify dates are target-generated and are not overwritten.

## 7. Dry Run

Click **2. Dry-Run Simulation**.

Dry Run validates the complete manifest, hierarchy dependencies, supported
types, mappings and binary source configuration. Direct Azure/file sources
require a locator for every version; source Content Server REST resolves content
by DataID and version. Dry Run makes no source-content or GX39 calls and writes
no target mappings. Missing structural, category, owner or Business Workspace
mapping prerequisites reject the Dry Run before a run record is created;
post-Pilot operational acceptance is not required at this stage. It also checks
source owner identity completeness and deterministic provenance construction,
but online GX39 member resolution and category compatibility remain required
before a real run.

Dry Run does not upload files and does not predict production throughput. Never
use its duration to estimate the cutover window.

## 8. Mandatory GX39 TEST contract qualification

Before a representative Pilot or production approval, run controlled GX39 TEST
cases for:

1. an ordinary folder and an ordinary small document;
2. a 49 MiB ordinary upload;
3. the 50 MiB threshold boundary;
4. at least one 100 MiB multipart document;
5. a representative multi-gigabyte file, preferably the known maximum;
6. multipart interruption and checkpoint recovery;
7. token expiration between multipart parts;
8. a deliberately lost/ambiguous create response and duplicate reconciliation;
9. first version and subsequent versions;
10. exact owner lookup for e-mail, service-account creator read-back and
    `Owned By` assignment on ordinary folders/documents;
11. `CDM Migration Provenance` applicability, date/time normalization and
    read-back for ordinary objects;
12. version dates, comments, owners and complete ordered provenance rows for
    first and subsequent versions;
13. categories, sets and multi-row attributes;
14. every Business Workspace type/template route, owner assignment, provenance
    applicability and role behavior;
15. an approved unresolved/deactivated-owner fallback, including provenance of
    the original owner and exact approval evidence;
16. destination ACL inheritance and role membership;
17. Unicode, duplicate names, deep paths, shortcuts and URLs.

Capture sanitized HTTP schemas, GX39 correlation IDs, latency, 429/5xx rates and
indexing delay. Never capture passwords, tickets, Authorization headers or SAS
query strings.

If GX39's API contract differs, isolate the adaptation in `engine/client.py`, add
a regression test and rerun the whole suite.

## 9. Representative Pilot

After Dry Run, duplicate protection and the owner/provenance qualification are
ready, complete the pre-Pilot items under **Acceptance**, then run
**3. Representative Pilot** against GX39 TEST. If the selected scope contains
owner exceptions, the contextual approval must be supplied for that exact
distinct-owner set; it is not a standing approval.

Pilot selection is deterministic and risk-oriented: large files, deep paths,
multiple versions, categories, providers, duplicate names, Unicode and
references receive priority.

During the Pilot:

1. monitor progress, latency, transfer rate, 429 and 5xx telemetry;
2. test Pause/Resume;
3. stop one controlled run and use **Run history → Resume interrupted run**;
4. execute automated live verification;
5. inspect representative content and metadata in Smart View;
6. test intended user access, roles, search/facets, checkout, metadata edit,
   version creation and legacy links;
7. record operational qualification under **Acceptance**.

Already completed items are skipped during recovery. Recovery is not rollback.

## 10. Concurrency qualification

Start with eight document workers and one large-file worker. Increase only when
GX39 TEST measurements show stable p95 latency and negligible throttling/errors.

Do not exceed the configured maximum or OpenText-approved atypical-use plan.
Record the final approved worker count, large-file concurrency and evidence in
the production change record.

## 11. Production prerequisites

Before selecting **GX39 PROD cutover**, verify:

- OpenText atypical-use notification/approval;
- exact production source DataID and destination parent NodeID;
- separate production credentials and duplicate-protection attribute;
- successful GX39 TEST contract suite and representative Pilot;
- Readiness PASS apart from the contextual source read-only confirmation;
- `CDM Migration Provenance` category and attribute contract qualified;
- exact owner resolution, active fallback policy and Business Workspace owner
  routes qualified;
- destination permissions and intended users approved;
- active workflows/reservations resolved;
- historical audit and personal UI exclusions accepted;
- state backup and restore rehearsal completed;
- adequate cutover plus verification window;
- operator, rollback owner and business acceptance contacts available.

Do not copy TEST SQLite state into PROD. Each profile owns a separate state file.

## 12. Production Full Cutover

1. Ensure users have stopped work on the on-premise source.
2. Administratively make the selected source scope read-only.
3. Click **4. Start Full Cutover**.
4. In the contextual confirmation, verify profile, Source DataID and Destination
   NodeID, enter operator/change record and confirm read-only.
5. The application re-reads the source signature. A difference rejects start.
6. If the signature matches, the Full Cutover starts.

Do not rescan after the approved freeze unless the manifest is deliberately
invalidated and the production plan is restarted.

Monitor the live console and telemetry. Pause for sustained throttling or target
instability. Stop only when required; an interrupted run is resumed from Run
history after the cause is understood.

## 13. Verification and acceptance

Full migration is not complete when uploads stop. Run live verification and
require:

- zero failed/blocked items;
- exact migrated inventory and parent mapping;
- every source version represented in order;
- source/target SHA-256 equality for every version;
- category value read-back;
- target `Created By` service-account read-back and `Owned By` exact
  run-resolution read-back;
- source dates, owner identity and resolution status read back from
  `CDM Migration Provenance`, including every version row;
- intended destination access and Business Workspace roles;
- lifecycle operations, search/facets and legacy-link continuity;
- business navigation and representative large-file access.

Export and retain the reconciliation workbook, sanitized logs, profile/config
fingerprint, source signature, run IDs, operator/change record and acceptance
evidence according to corporate retention rules. Never include secrets.

## 14. Recovery and state backup

**Run history** displays every run. `Resume interrupted run` appears only for
`STOPPED`, `FAILED` and `COMPLETED_WITH_ERRORS` runs.

Recovery:

- retains verified items and mappings;
- retains the immutable per-run owner resolutions and any fallback approval;
- retains multipart upload checkpoints;
- retries incomplete/eligible failed work;
- rejects a changed profile fingerprint;
- rechecks that resolved target members remain active and identity-consistent;
- requires a valid source freeze for production;
- never deletes target content.

Create state backups through the UI/CLI online backup path:

```bash
python cli.py --environment production backup production-state-backup.db
```

Do not use `cp`, Explorer or backup agents against a running SQLite database and
its WAL files.

## 15. Known corporate-only evidence

The following cannot be certified on a private machine:

- real PostgreSQL schema/data access and exact source counts;
- mapping from `DVersData.ProviderID` through `ProviderData.ProviderData` to the
  corporate Azure container;
- GX39 multipart and subsequent-version dialect;
- target provenance category IDs, date/set/multi-row payloads and read-back;
- ordinary and Business Workspace owner-assignment routes;
- Business Workspace creation routes and roles;
- service-account privileges and ACL inheritance;
- token lifetime, rate limits, WAF behavior and indexing delay;
- end-to-end throughput and cutover duration.

The first GX39 DEV Pilot showed that standard node create/update forms expose
`create_date`, `modify_date` and `owner_user_id` as readonly. Create forms allow
`external_create_date` and `external_modify_date`, but those are not silently
accepted as system-date fidelity. The implemented replacement contract keeps
GX39 system dates target-generated, assigns `Owned By` from exact active target
identity evidence, keeps `Created By` as the migration account, and records
original dates/owner in `CDM Migration Provenance`. That replacement contract
still requires the qualification matrix above before another real run.

Treat each unknown as a blocker until corporate TEST evidence exists.
