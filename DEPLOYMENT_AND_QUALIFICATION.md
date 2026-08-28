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

## 7. Dry Run

Click **2. Dry-Run Simulation**.

Dry Run validates the complete manifest, hierarchy dependencies, supported
types, mappings and binary source configuration. Direct Azure/file sources
require a locator for every version; source Content Server REST resolves content
by DataID and version. Dry Run makes no source-content or GX39 calls and writes
no target mappings. Missing structural, category, owner or Business Workspace
mapping prerequisites reject the Dry Run before a run record is created;
post-Pilot operational acceptance is not required at this stage.

Dry Run does not upload files and does not predict production throughput. Never
use its duration to estimate the cutover window.

## 8. Mandatory GX39 TEST contract qualification

Before a representative Pilot or production approval, run controlled GX39 TEST
cases for:

1. an ordinary small document;
2. a 49 MiB ordinary upload;
3. the 50 MiB threshold boundary;
4. at least one 100 MiB multipart document;
5. a representative multi-gigabyte file, preferably the known maximum;
6. multipart interruption and checkpoint recovery;
7. token expiration between multipart parts;
8. a deliberately lost/ambiguous create response and duplicate reconciliation;
9. first version and subsequent versions;
10. version dates, comments and owners;
11. categories, sets and multi-row attributes;
12. every Business Workspace type/template route;
13. destination ACL inheritance and role membership;
14. Unicode, duplicate names, deep paths, shortcuts and URLs.

Capture sanitized HTTP schemas, GX39 correlation IDs, latency, 429/5xx rates and
indexing delay. Never capture passwords, tickets, Authorization headers or SAS
query strings.

If GX39's API contract differs, isolate the adaptation in `engine/client.py`, add
a regression test and rerun the whole suite.

## 9. Representative Pilot

After Dry Run and duplicate protection are ready, complete the pre-Pilot items
under **Acceptance**, then run **3. Representative Pilot** against GX39 TEST.

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
- category, owner and Business Workspace exception mappings approved;
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
- date and owner read-back;
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
- retains multipart upload checkpoints;
- retries incomplete/eligible failed work;
- rejects a changed profile fingerprint;
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
- target category IDs and complex field payloads;
- Business Workspace routes and roles;
- service-account privileges and ACL inheritance;
- token lifetime, rate limits, WAF behavior and indexing delay;
- end-to-end throughput and cutover duration.

The first GX39 DEV Pilot showed that standard node create/update forms expose
`create_date`, `modify_date` and `owner_user_id` as readonly. Create forms allow
`external_create_date` and `external_modify_date`, but those are not silently
accepted as system-date fidelity. A supported OpenText import/bulk contract or
explicitly approved fidelity design is required before another real run.

Treat each unknown as a blocker until corporate TEST evidence exists.
