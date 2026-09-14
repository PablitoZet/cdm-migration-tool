# Business Workspace source traversal — implementation handoff

Date: 2026-09-14. Status: **implemented and locally tested; unqualified against
real Content Server PostgreSQL and GX39 TEST**.

Branch: `feature/business-workspace-source-traversal`.
Base: `37fd682223905f6b11922a9942d631cd25a03888` (main).

## Problem and implementation

The [original task brief](BUSINESS_WORKSPACE_TASK_BRIEF.md) records a source
database observation: Business Workspace W (subtype 848) has its content below
the distinct technical DTree container -W (subtype 849, ParentID -1). The old
ParentID-only scan returned just W. `engine/db.py` now adds that jump at every
workspace encountered in the recursive query, retaining the ordinary ParentID
branch and read-only repeatable-read snapshot. The shadow uses W's logical
depth/path, while both IDs remain in the cycle-protection array. Python validates
the workspace/shadow pairs, removes shadows, and remaps direct children's parent
IDs from -W to W before extracting versions, categories and owners or computing
the source signature. GX39 parent resolution consequently uses the real
workspace mapping. Deeper descendants keep their ordinary parents.

Extraction fails with an identifying RuntimeError for a missing/deleted shadow,
wrong shadow subtype/sentinel, inconsistent shadow depth/path, nonpositive
workspace ID, an unpaired subtype-849 row, or repeated DataIDs. A genuinely empty
workspace is accepted only with a valid shadow. No alternate layout is guessed.

## Exact changed files

| File | Reason |
|---|---|
| `engine/db.py` | Add recursive workspace jumps and Python validation/exclusion/parent remapping, with explanatory comments. |
| `tests/test_engine_v2.py` | Add 11 offline source/manifest tests and node fixtures; extend the existing fake cursor to capture query parameters. |
| `ARCHITECTURE.md` | Explain the source-only shadow, logical hierarchy, fail-closed behavior and qualification boundary. |
| `BUSINESS_WORKSPACE_IMPLEMENTATION_REPORT.md` | Provide this independent implementation and Windows qualification handoff. |

`CORPORATE_HANDOFF.md` and `DEPLOYMENT_AND_QUALIFICATION.md` are intentionally
untouched as required by the brief: the corporate session owns their update
after qualification. The brief is retained until qualification is complete.
No target API, UI, profile schema, SQLite schema, owner phase, streaming,
multipart, concurrency or readiness gate was changed. Current target
`create_container()` and `WORKSPACE_ROUTES` already accept positive workspace
IDs and still require explicit routes and target qualification.

## OpenText documentation investigation

Reviewed these official references on 2026-09-14:

1. [OpenText API catalog](https://developer.opentext.com/apis): the required API
   entry point. The text-only web reader exposed the site's JavaScript fallback;
   the product references below were inspected in a JavaScript-enabled browser.
2. [Content Server 25.4 API reference](https://developer.opentext.com/ce/products/content-management/apis/content-server-25-4-0):
   identifies itself as the Content Server REST reference for v1/v2 calls. The
   visible operations and descriptions did not document the classic DTree
   negative-ID workspace/shadow mechanism.
3. [Business Workspaces 25.4 API reference](https://developer.opentext.com/ce/products/content-management/apis/business-workspaces-25-4-0):
   documents a POST operation at `/v2/businessworkspaces` to create/update a
   workspace. Its request examples contain `parent_id`, `template_id`, `name`
   and `wksp_type_id`. This confirms an explicit parent in the public REST
   creation model; it does **not** establish a classic PostgreSQL storage
   convention or establish that all nested workspace routes are tenant-supported.
   The existing target operation was inspected but not changed or tenant-tested.

Targeted searches for DTree, negative DataIDs, subtypes 848/849 and nested
workspaces found no clear public developer-reference confirmation of the full
W/-W storage contract. Historical forum discussions are not treated as a
versioned database specification.

### Unverified assumptions implemented defensively

- Every supported live subtype-848 source workspace has a distinct active
  subtype-849 row at exactly -W, with ParentID -1. This is the task brief's
  reported database evidence, **not independently verified here**. Every pair is
  checked before an inventory is returned; otherwise extraction stops.
- The complete source content is reachable through the existing ordinary edges
  plus these shadow jumps. Existence of a valid shadow alone cannot prove
  completeness against another, unobserved storage layout. Corporate
  qualification must compare the extracted ID set with independent source
  evidence, not merely check that the count is greater than one.
- Nesting is supported structurally: the current DTree/manifest ParentID model
  can place an 848 row anywhere in a subtree, and no inspected public reference
  establishes a restriction that would justify root-only traversal. Therefore
  the implementation handles every occurrence, including nested workspaces.
  This is a conservative schema-based conclusion, not a claim that every
  Content Server installation permits creating every such nesting. Real nested
  source data and the corresponding GX39 type/template/role behavior remain
  qualification items.
- PostgreSQL execution plans, performance, and actual recursive results on the
  corporate database remain untested. The default statement timeout remains in
  force; collect timing/plan evidence if a scan times out.

## New tests and what they establish

All new tests are in `SourceExtractionTests` in `tests/test_engine_v2.py`:

1. `test_ordinary_node_extraction_preserves_hierarchy_metadata_and_query_branch`:
   ordinary hierarchy, metadata and reference extra values match the prior
   output contract; SQL retains root/deletion filtering, ordinary ParentID
   edges, ordinary depth/path expressions, parameter binding and ordering.
2. `test_workspace_root_excludes_shadow_and_remaps_only_direct_children`:
   a workspace plus direct folder/document and deeper folder/document produces
   no shadow, correct real parent IDs, unchanged root metadata and continuous
   depth/path. Checks custom `cs` schema qualification, array append and shared
   cycle/deletion predicate in SQL text; checks driver fixtures are not mutated.
3. `test_workspace_missing_active_shadow_fails_closed`: incomplete root data
   raises an error naming W and -W rather than returning an empty workspace.
4. `test_empty_workspace_with_valid_shadow_is_allowed`: valid empty workspaces
   retain their real root, without a technical target object.
5. `test_invalid_workspace_shadow_fails_closed`: subcases reject a wrong
   subtype, wrong ParentID sentinel, inconsistent depth or inconsistent path.
6. `test_nested_workspace_remaps_at_its_own_depth`: both shadows are excluded,
   nested children map to their own workspace, deeper paths/parents survive,
   and a missing nested shadow identifies that nested workspace.
7. `test_workspace_inside_ordinary_root_is_normalized`: workspace handling also
   applies when the selected root is an ordinary folder.
8. `test_unpaired_shadow_and_nonpositive_workspace_fail_closed`: no standalone
   technical container or invalid workspace identity is accepted.
9. `test_repeated_tree_identity_fails_closed`: ambiguous repeated tree rows
   cannot overwrite one another in the manifest.
10. `test_extract_all_never_requests_shadow_metadata`: versions/categories
    receive only real IDs, owners are deduplicated and a signature is produced
    from the normalized inventory.
11. `test_workspace_manifest_pilot_keeps_real_ancestors_and_parent_dependencies`:
    imports the normalized data into real temporary SQLite, selects a deep
    document with its real ancestors, exercises top-down claims/mappings and
    verifies parent resolution without any negative mapping. Uses synthetic
    immutable owner identity/resolution evidence to preserve existing gates.

The existing fake cursors return canned rows; **they do not execute the SQL
CTE**. Python normalization and real SQLite dependency handling are exercised.
SQL shape/cycle assertions do not constitute PostgreSQL integration evidence.
No network, real source database or tenant is needed to run these tests.

## Local verification evidence

macOS, Python 3.14.3, isolated checkout with no operator configuration/state.
Ran `./bootstrap.sh` to install `requirements-dev.txt`; its baseline suite
reported 90 tests, OK. Dependency installation required network access to PyPI;
the tests themselves run offline. Then activated the virtual environment with
`. .venv/bin/activate` and ran the required commands from the repository root.

`python3 -m unittest discover -s tests -v` — actual final tail:

```text
----------------------------------------------------------------------
Ran 101 tests in 1.762s

OK
```

101 passed, zero failures/errors (90 existing + 11 new).

`ruff check .` — actual output:

```text
All checks passed!
```

`mypy app.py engine tests` — actual final output:

```text
tests/test_engine_v2.py:2153: note: By default the bodies of untyped functions are not checked, consider using --check-untyped-defs  [annotation-unchecked]
tests/test_engine_v2.py:2154: note: By default the bodies of untyped functions are not checked, consider using --check-untyped-defs  [annotation-unchecked]
Success: no issues found in 21 source files
```

The notes describe the existing mypy configuration; no type-check failure was
suppressed for this change. No UI JavaScript was edited, so UI browser/syntax
qualification is not part of this source-only patch. Windows has not been run
locally. Python 3.11 remains the repository's CI baseline.

## Manual verification checklist — corporate Windows machine

Keep configuration, backups, source identifiers and collected evidence on the
corporate machine, outside Git. Use an approved non-sensitive source sample and
GX39 TEST destination according to the existing runbook/handoff.

1. Stop the application/CLI controllers before updating code. Commit/stash any
   local source changes using the corporate workflow. Fetch and check out
   `feature/business-workspace-source-traversal`, then pull with `--ff-only`.
   Record `git log -1 --oneline`. Read this report first, then the current
   `AGENTS.md`, corporate handoff and deployment qualification requirements.
2. On first installation run `./bootstrap.ps1` in PowerShell. On an existing
   installation retain local `config.json` and state. Run these checks:

   ```powershell
   .\.venv\Scripts\python.exe -m unittest discover -s tests -v
   .\.venv\Scripts\ruff.exe check .
   .\.venv\Scripts\mypy.exe app.py engine tests
   ```

   Expect 101 tests OK, Ruff clean, mypy success for this revision. Stop and
   diagnose failures rather than weakening a gate.
3. Use a new profile classified `test` and an isolated GX39 TEST destination
   with its own approved migration namespace. If reusing a profile, take the
   supported SQLite online backup before rescanning, with all controllers
   stopped, for example:

   ```powershell
   .\.venv\Scripts\python.exe cli.py --environment test backup .\migration_state_v2_test_before_bw.db
   ```

   Substitute the actual profile ID. Do not copy/delete a live database.
   A successful new scan replaces that profile's inventory, runs and mappings
   as in the existing tool; an old one-node scan must be replaced with a fresh
   complete scan, not resumed as if it contained the workspace's content.
4. Start `./start.ps1`; open `http://127.0.0.1:8110`. In Migration Setup configure
   the real approved Business Workspace's positive DataID as Source root DataID,
   the correct `db_schema`, read-only source credentials/binary adapter and GX39
   TEST destination parent. Use connection checks and check the destination.
5. With the source DBA/read-only diagnostic connection, independently confirm W
   is active subtype 848 and -W exists, is active subtype 849, and has ParentID
   -1. Obtain the expected active descendant ID set/counts using the source UI
   and the already-qualified closure-table diagnostic for both W and -W. Record
   evidence locally. Confirm a representative direct child has ParentID -W,
   and include at least two ordinary levels below it. Do not put real IDs into
   fixtures or this report.
6. Click **1. Scan Source Workspace**. Confirm the scan succeeds and inventory
   counts cover the independently confirmed active subtree, rather than one
   node. Compare the full ID set with the independent source result, accounting
   explicitly for deleted rows and the excluded shadow. Do not accept unexplained
   count differences. Confirm versions, categories and owners are now present
   for the content rather than just for W.
7. Inspect the manifest via the supported online backup, not a filesystem copy.
   Stop the app, run the CLI backup to a separate review database, then inspect
   that backup read-only. In a SQLite viewer or Python `sqlite3` read-only
   connection, use parameterized queries with locally supplied IDs:

   ```sql
   SELECT source_id,parent_source_id,subtype,depth,path
     FROM manifest_nodes ORDER BY depth,source_id;
   SELECT COUNT(*) FROM manifest_nodes WHERE source_id=? OR parent_source_id=?;
   SELECT COUNT(*) FROM manifest_nodes WHERE subtype=849;
   ```

   Bind both parameters to -W: expect zero for the second query and zero for the
   third. Former direct shadow children must have parent_source_id W and depth
   W.depth+1; deeper nodes retain ordinary parents. W itself retains its true
   enclosing ParentID. Paths contain W's visible name and no hidden-container
   segment. Confirm `schema_meta` binds this inventory to the selected root and
   profile. Never edit rows to repair an extraction failure.
8. If approved source samples include nested workspaces, repeat the pair,
   full-ID-set, parent, depth and path checks for each nested W/-W. Also scan an
   ordinary-root regression sample and a folder containing a workspace. If no
   nested sample exists, record that qualification limitation explicitly.
9. Restart the app and open Migration Setup's **Business Workspace types and
   templates** exception field. Add `workspace_routes` for every real subtype-848
   ID or its type name using actual tenant-approved values, for example this
   shape (replace placeholder keys/values locally):

   ```json
   {"SOURCE_WORKSPACE_ID": {"workspace_type_id": 12345, "template_id": 67890}}
   ```

   These are illustrative values, not GX39 settings. Missing routes must still
   produce `WORKSPACE_ROUTES` FAIL; after saving approved routes it must pass.
   Confirm duplicate protection, exact owner decisions, provenance and destination
   ACL settings per deployment section 5a. Qualify target workspace creation,
   owner/provenance applicability/read-back and roles through the existing TEST
   qualification workflow before enabling a real run. Do not set qualification
   flags just to dismiss a blocker.
10. Resolve offline Readiness prerequisites and click **2. Dry-Run Simulation**.
    Expect SIMULATED items and no new target objects or durable mappings. This
    proves planning behavior, not actual binary readability, throughput or
    tenant compatibility.
11. After target prerequisites are evidenced, run an approved **Representative
    Pilot** including W, its selected descendants, multiple document versions,
    and nested W if available. Confirm positive workspace objects are created
    through their approved routes, direct content is beneath the corresponding
    workspace target, and deeper content keeps its hierarchy. Confirm no
    subtype-849 target or negative-shadow mapping appears. All selected run items
    must be VERIFIED with zero unexplained failures; inspect read-back/hash,
    version, category, owner, creator and provenance evidence. The existing final
    OWNER phase must remain after all creates.
12. Run live automated verification and inspect Smart View navigation and
    intended-user role/ACL access. Record the commit, scan/expected counts, Pilot
    run ID, verification result, sanitized errors if any and limitations in the
    corporate evidence location. A structurally non-sensitive failed/missing
    shadow sample may be checked if already available; never mutate source DTree
    to manufacture one. On any unexplained failure stop and diagnose.
13. Only after this qualification, fold results and any tenant prerequisites
    into `CORPORATE_HANDOFF.md` and `DEPLOYMENT_AND_QUALIFICATION.md` section 5a
    in the corporate session. Update architecture qualification wording and
    remove the temporary brief/report once their useful content is integrated.
    No production cutover approval follows from this local implementation.

## Limitations and follow-up boundary

Only the observed 848/849 W/-W layout is supported by this addition. Other
shadow conventions, project-specific traversal changes and alternate sentinels
are outside this task. Do not use unconditional absolute IDs to broaden scope.
Existing cycle suppression remains bounded by the complete traversal ID path;
malformed multi-path output additionally fails on duplicate IDs. Fake SQL-text
assertions cannot prove real cycle termination or PostgreSQL typing/performance;
real database qualification remains necessary.

Completeness, GX39 workspace templates/roles and ownership/provenance on that
route remain open until the checklist above passes. Multipart/large-file
qualification, throughput tuning and the approximately 7.44 GB representative
file are unchanged backlog items. This change adds no dependencies or Windows
platform-specific calls, but the Windows checks must still be executed there.

## Addendum: source subtype 749 support (Outlook email messages)

Date: 2026-09-14 (same session). Status: **implemented and locally tested;
GX39 TEST document creation/read-back for this MIME type not yet qualified**.

During read-only qualification of the fixed W/-W traversal against the real
production Business Workspace `763886`, `cli.py --environment production
inspect-source` reported 275 objects of a previously unsupported source
subtype 749 inside the migration scope (304 active instances org-wide), which
would have made `SUPPORTED_SUBTYPES`/`SUPPORTED_OBJECT_TYPES` fail closed for
this workspace.

Read-only PostgreSQL profiling and a bounded (4 KB) read-only binary read
through the Source Content Server REST client (`engine/source.py`,
`ContentServerBinarySource`) established, independently, that subtype 749 is
Outlook `.msg` email messages:

- all sampled rows are direct children of ordinary folders (subtype 0), each
  with a single primary version, provider type `acprimary` (Archive Center),
  sizes from ~11 KB to ~26 MB;
- filename suffix `.msg` and MIME type `application/x-outlook-msg` on every
  sampled row (275/304 also carry category rows);
- the first 4096 bytes of a sampled binary begin with `D0 CF 11 E0 A1 B1 1A
  E1`, the OLE Compound File Binary Format signature used by real Outlook
  `.msg` files, and the read size matched the expected `DVersData` size.

This evidence is based on observed source data and public MIME/format
signatures, not on OpenText subtype documentation (see AGENTS.md section 3a).
No source-system-owner confirmation of subtype 749's business semantics or of
any out-of-band `.msg` metadata beyond `DTree`/`DVersData`/categories has been
obtained; this remains an open corporate-side item.

Subtype 749 was added as a supported document type, handled identically to
the existing subtype 751 (Compound/Email) pattern — no client-side special
case is needed because `engine/client.py` `upload_first_version()` already
creates every document subtype as GX39 type 144 regardless of source subtype.
Changed files (all additive, same list shape as subtype 751 in each file):

- `engine/db.py` — `SUBTYPE_NAMES[749] = "Email Message"`.
- `engine/models.py` — `749` added to `DOCUMENT_TYPES`.
- `engine/inventory.py` — `749` added to the documents-count subtype tuple.
- `engine/manifest.py` — `749` added to the `SUPPORTED_OBJECT_TYPES` check,
  the pilot document-candidate query, the category-documents query, the
  `inventory_summary` `total_docs` sum, and `_phase_for_subtype` (now returns
  `DOCUMENT`).
- `engine/preflight.py` — `749` added to the `SUPPORTED_SUBTYPES` check.
- `engine/reconciler.py` — `749` added to the version-provenance subtype
  check.
- `tests/test_engine_v2.py` — new tests
  `test_email_message_subtype_is_named_and_counted_as_a_document`
  (`SourceExtractionTests`) and
  `test_email_message_subtype_is_accepted_but_genuinely_unknown_subtypes_fail_closed`
  (`ManifestTests`); the latter also proves a genuinely unknown subtype (999)
  still fails `SUPPORTED_OBJECT_TYPES`, so the fail-closed invariant for
  unmapped subtypes is preserved, not weakened.

Local verification after this change: `python -m unittest discover -s tests -v`
→ 103/103 passed; `ruff check .` → clean; `mypy app.py engine tests` → success
(informational notes only).

Remaining qualification before Pilot/Full Cutover for this subtype:

1. Corporate-machine confirmation with the source-system owner that subtype
   749 objects carry no additional business data outside `DTree`,
   `DVersData` and categories.
2. GX39 TEST document creation and content read-back for MIME type
   `application/x-outlook-msg`, including a representative and a
   near-largest sampled `.msg` file, per the existing Pilot qualification
   workflow (no new tenant configuration was identified; type 144 creation
   is reused).
3. `DEPLOYMENT_AND_QUALIFICATION.md` section 5a should be checked after step
   2 in case a category/attribute exception is discovered for this content
   type; none was required for this addition.

## Addendum: Business Workspace ownership without an individual KUAF principal

Real read-only qualification against the actual production Business Workspace
selected for migration (source DataID `763886`, GX39 target parent `763886`
route configured for mechanism testing only, using a non-representative TEST
workspace type/template) surfaced a distinct gap from subtype 749: the
offline `OWNER_IDENTITY_COVERAGE` preflight check failed for essentially every
node in the extracted tree, blocking even Dry Run.

Investigation (read-only, via `SourceDB.snapshot()` against the source
PostgreSQL, never against DTree/KUAF with write intent):

- The workspace node itself (`763886`) has `DTree.OwnerID = -2000`. KUAF has
  no row with `id=2000`, so the classic `-OwnerID -> KUAF.id` convention used
  for ordinary folders/documents does not resolve.
- Extending the check to all 4 Business Workspaces present in the entire
  source database: all 4 show only 2 distinct `OwnerID` sentinel values
  (`-2000` for 3 of them, `-2429` for 1), confirming this is a shared,
  system-level Content Server convention, not something specific to this one
  workspace.
- Nearly all descendants of the inspected workspace (29,225 of 29,226 nodes)
  have `owner_id = 763886` — the workspace's own positive DataID, again not a
  real KUAF identity.
- The workspace's `PermID` is `NULL` (classic ACL unused for subtype 848).
  KUAF contains 4 auto-generated role groups scoped to this workspace via
  `KUAF.type = 763886`: "Confidential", "Editors", "Managers", "Readers", all
  self-referencing `leaderid` (no individual user recorded there either).
- The exact mechanism behind the Content Server UI's "Owned By: Admin"
  display for this workspace was not fully explained by `DTree`/`KUAF` data
  alone; it is very likely resolved through a Business Workspace-specific
  API/metadata layer not covered by this read-only database investigation.
  This remains an open, low-priority item — see below for why it does not
  block the tool.

Per `ARCHITECTURE.md`, migrated-object access/permissions are always
inherited from the approved GX39 destination ACL, never from `Owned By`.
`Owned By` is therefore a provenance/reporting attribute with no functional
access-control impact — but `OWNER_IDENTITY_COVERAGE` is an offline,
fail-closed gate that blocked Dry Run/Pilot readiness regardless, so a
usability fix (not a permissions fix) was implemented:

- `engine/db.py::extract_all`/`_extract_owners` now classify an owner_id as
  `identity_status="SYSTEM_OWNER"` only when it exactly matches an in-scope
  subtype-848 node's own DataID or that same node's own recorded `OwnerID`,
  and only when zero KUAF rows match. Every other unmatched owner_id remains
  `UNKNOWN`, preserving the fail-closed guarantee for genuinely orphaned or
  corrupted owner references anywhere else in the tree.
- `OWNER_IDENTITY_COVERAGE` (`engine/preflight.py`) and its twin
  `OWNER_IDENTITY_PARITY` (`engine/manifest.py::parity_report`) now exempt
  `SYSTEM_OWNER` from their incompleteness check.
- `engine/provenance.py::source_owner_exception_reason` now reports
  `"SYSTEM_OWNER"` distinctly (instead of the generic `"STATUS_UNKNOWN"`), so
  an operator approving a run-scoped fallback can see precisely why these
  owners require it.
- No change was needed in the fallback/approval machinery itself
  (`engine/pipeline.py::owner_readiness`/`_resolve_run_identities`,
  `engine/provenance.py::resolve_fallback_principal`/`fallback_resolve`):
  `SYSTEM_OWNER` owners still have `active=None`, so they still require an
  explicit operator-approved fallback mapping (or a manual OwnerID-to-Login
  mapping) before `Owned By` is actually assigned in a real Pilot/Full run.
  This fix only unblocks the offline readiness gate; it does not decide what
  `Owned By` ends up being for these nodes, and does not weaken the approval
  requirement in any way.

New regression tests (`tests/test_engine_v2.py`):
`test_business_workspace_system_owner_ids_are_classified_not_unknown` proves
the new classification fires only for the narrow in-scope pattern, and
`test_unrelated_unmatched_owner_id_still_fails_closed_as_unknown` proves an
unrelated owner_id with no KUAF match still fails closed as `UNKNOWN`. The
pre-existing `test_extract_all_never_requests_shadow_metadata` assertion was
updated for the new `_extract_owners` parameter.

Local verification after this change against real production data (workspace
`763886`, ~29.2k nodes): `cli.py --environment production preflight` moved
`OWNER_IDENTITY_COVERAGE` from `FAIL` to `PASS` with no change to any other
check's status. `python -m unittest discover -s tests -v` → 105/105 passed;
`ruff check .` → clean; `mypy app.py engine tests` → success (informational
notes only).

Remaining qualification before Pilot/Full Cutover: an approved
`owner_fallback` mapping (or explicit manual OwnerID-to-Login mappings) is
still required at Pilot/Full time for every `SYSTEM_OWNER` (and any other
non-`KNOWN`) owner before a real run assigns `Owned By`; this addendum only
removes an offline-gate blocker for reaching that stage during testing.


