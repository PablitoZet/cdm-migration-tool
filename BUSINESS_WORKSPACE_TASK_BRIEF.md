# Task brief: source-side Business Workspace traversal support

Status: **open, not started**. This document is the complete, self-contained
context for implementing this one feature. It is written for an AI coding
agent that has **no access to this project's corporate on-premise database,
corporate network, or the GX39 cloud tenant**, and therefore **cannot run a
real end-to-end test**. Everything here is designed so the work can be
implemented, unit-tested with fakes, linted and type-checked entirely offline,
and then handed back for a real qualification pass by someone who does have
that access.

If anything in this brief conflicts with the current repository code or with
`AGENTS.md`, stop and flag the conflict instead of guessing — do not silently
pick whichever interpretation is easier to implement.

## 0. Required reading before writing any code

In this exact order:

1. `AGENTS.md` in full — this repository's binding engineering rules. Sections
   3a and 3b (OpenText API verification, tenant configuration bookkeeping) and
   section 4 (non-negotiable safety invariants) apply directly to this task.
2. `ARCHITECTURE.md`, in particular the "Business Workspace subtype 848 uses
   the target Business Workspace API..." note and the container/state-machine
   sections.
3. `DEPLOYMENT_AND_QUALIFICATION.md` section 5a (GX39 tenant configuration,
   including Business Workspace type/template routes).
4. The current code: `engine/db.py`, `engine/models.py`, `engine/client.py`
   (`create_container`), `engine/manifest.py`, `engine/preflight.py`
   (`WORKSPACE_ROUTES` check), `app.py` (`PROFILE_EDITABLE_KEYS`), and
   `static/index.html` (the `workspace_routes` JSON field in Migration Setup).
5. `tests/test_engine_v2.py`, particularly the `SourceExtractionTests` class
   and its fake `_TestSourceDB`/`_VersionConnection`/`_VersionCursor` helper
   classes — this is the established pattern for testing `engine/db.py`
   without a real PostgreSQL connection, and you must follow it.

Do not implement from memory or from a chat summary; read the actual current
files first.

## 1. Mission context (condensed — see AGENTS.md for the full version)

This is an internal, single-operator tool that migrates one selected OpenText
Content Server folder or **Business Workspace** subtree from an on-premise
PostgreSQL-backed Content Server plus Azure Blob Storage into OpenText
Extended ECM Cloud (GX39 SaaS). The source database connection is always
**read-only** (non-negotiable invariant — never add source `INSERT`/`UPDATE`/
`DELETE`/DDL). Dry Run must never call or mutate the target. Every real target
object gets a duplicate-protection marker so a create can be safely retried
after an ambiguous network failure. See `AGENTS.md` section 4 for the full
list; nothing in this task should require touching any of those invariants.

## 2. What already exists and works (do not re-implement)

A representative Pilot against GX39 TEST already completed successfully
end-to-end for **ordinary folders and documents** (subtype 0/144, not Business
Workspace): hierarchy, content hashes, categories, owner assignment, and the
dedicated `CDM Migration Provenance` category all round-trip correctly. That
mechanism must not be broken by this change — see section 6 (regression
safety).

On the **target/write side**, Business Workspace support is already
substantially implemented, just never qualified against a real Business
Workspace because no test source object of that type was available until now:

- `engine/models.py` — `CONTAINER_TYPES` already includes `848` (Business
  Workspace) alongside `0` (folder), `202` (project), `298` (collection), and
  `899` (Business Workspace template).
- `engine/client.py`, `create_container()` — already branches on
  `node.subtype == 848`: it looks up a per-workspace or per-type route from
  `self.workspace_routes` (keyed by source DataID string, falling back to
  `node.type_name`), and if found, POSTs to `/api/v2/businessworkspaces/`
  with `wksp_type_id`/`template_id` from that route instead of the ordinary
  `/api/v2/nodes` container-create path. If no route is configured it raises
  `TerminalMigrationError` (fails closed — correct, do not change this
  default).
- `engine/preflight.py` — the `WORKSPACE_ROUTES` readiness check already scans
  the manifest for every node with `subtype=848` and fails Readiness if any of
  them lacks an entry in `workspace_routes`.
- `app.py` (`PROFILE_EDITABLE_KEYS`) and `static/index.html` (`pf-workspace-map`
  textarea, serialized as JSON) already let an operator configure
  `workspace_routes` per profile through the normal Migration Setup UI.

**None of the above target-side code should need functional changes for this
task**, unless your investigation in section 4 proves otherwise — in which
case explain why in your PR/commit description before changing it.

## 3. The gap this task must close (source-side traversal)

`engine/db.py`'s `_extract_nodes()` builds the manifest tree with a single
recursive SQL CTE that walks `{schema}.DTree` purely by `ParentID`, starting
from the configured `source_workspace_nodeid`:

```sql
WITH RECURSIVE workspace_tree AS (
    SELECT ... FROM {schema}.DTree d WHERE d.DataID=%s AND COALESCE(d.Deleted,0)=0
    UNION ALL
    SELECT ... FROM {schema}.DTree c JOIN workspace_tree w ON c.ParentID=w.DataID
    WHERE COALESCE(c.Deleted,0)=0 AND NOT c.DataID=ANY(w.path_ids)
)
```

This works correctly for an ordinary folder (subtype `0`), where child objects
really do have `ParentID = <folder's own DataID>`.

**Confirmed finding (read-only diagnostic query against a real production
Content Server database, subtype `848` Business Workspace object)**: for a
Business Workspace, `DTree.ParentID` of its actual content (subfolders,
documents — tens of thousands of rows in the case that was inspected) is
**not** the workspace's own `DataID`. Instead:

- The workspace object itself (`DataID = W`, `SubType = 848`) has zero rows in
  `DTree` with `ParentID = W`.
- There exists a second, distinct `DTree` row with `DataID = -W` (the exact
  negation of the workspace's own DataID) and `SubType = 849`, with its own
  `ParentID = -1` (a root-level sentinel, not related to the workspace's real
  parent).
- Every actual child of the workspace (subfolders, documents, etc.) has
  `ParentID = -W`, i.e. they are parented under this second "shadow container"
  node, not under the workspace's own DataID.
- A `dtreeancestors` closure-table query confirmed this is complete: every
  descendant reachable from `AncestorID = W` is also reachable from
  `AncestorID = -W`, and the workspace's real subtree (thousands of rows) is
  entirely rooted under `-W`, not under `W` directly.

This means today's code, given a Business Workspace as
`source_workspace_nodeid`, extracts exactly one node (the workspace itself)
and stops — it never sees any of the workspace's actual content. This is
almost certainly why "Business Workspace route... unqualified" has stayed on
the qualification backlog: nobody had a real Business Workspace to scan until
now, so this traversal gap was invisible.

### 3a. You must verify this, not just trust this document

Per `AGENTS.md` section 3a, do not implement an assumption about Content
Server / OpenText behavior without checking it against the official
documentation first:

- [OpenText APIs](https://developer.opentext.com/apis)
- [Content Server 25.4 APIs](https://developer.opentext.com/ce/products/content-management/apis/content-server-25-4-0)

Look specifically for documentation of how a Business Workspace's contents are
represented in the classic `DTree` schema (sometimes described as the
workspace's "container" or "workspace container" object), and for whether the
negative-DataID convention above is a documented, general Content Server
mechanism or something you should treat as unconfirmed/tenant-specific. You do
not have on-prem database access to re-run the diagnostic query yourself, so:

- If the documentation confirms this is a general, stable Content Server
  mechanism (this is the expected outcome — it is a long-standing classic CS
  convention, not new in any particular release), implement against it
  directly.
- If you cannot find clear public documentation confirming it, implement the
  detection defensively (see section 5) so that if the assumption is wrong for
  a given tenant, the code fails closed with a clear error rather than
  silently migrating an incomplete or wrongly-parented tree, and say so
  explicitly in your PR description as an open item for the corporate
  qualification pass.

## 4. Your investigation checklist before writing code

1. Read `engine/db.py` in full, not just the excerpt above — understand
   `_extract_nodes()`, `_extract_versions()`, `_extract_categories()`,
   `_extract_owners()`, and the `_column_exists`/`_first_existing_column`
   defensive-schema helpers, and the recently added `db_schema` field (used to
   qualify every table reference, e.g. `{self._schema}.DTree`) — your change
   must also go through `self._schema`, not a hardcoded schema name.
2. Read `engine/manifest.py`'s node ingestion (`_selected_nodes`,
   `resolve_parent`, the `manifest_nodes` table schema with its
   `parent_source_id` column) and `engine/pipeline.py`'s use of
   `self.manifest.resolve_parent(node.parent_source_id, target_root)` when
   creating a container/document target. This is the critical constraint:
   **the pipeline resolves a target parent purely from `node.parent_source_id`
   via the `id_mapping` table, which only ever contains mappings for objects
   that were actually created on the target.** The shadow container (`-W`) is
   a source-side-only artifact; it must never be created on the GX39 side and
   must never appear as a `parent_source_id` value in the manifest that the
   pipeline is expected to resolve. Direct children of the shadow container
   must have their `parent_source_id` rewritten to the **real** workspace ID
   (`W`, positive) so the pipeline correctly parents them under the Business
   Workspace object once it exists on the target.
3. Read `tests/test_engine_v2.py`'s `SourceExtractionTests` class fully,
   including `_TestSourceDB`, `_VersionConnection`, `_VersionCursor`. Note
   that these fakes capture the executed SQL text and return **pre-supplied**
   rows — they do not actually execute a recursive CTE. This means your unit
   tests cannot mechanically prove the SQL traversal logic is correct; they
   can only prove (a) the query text is constructed correctly given the
   `db_schema`/business-workspace inputs, and (b) any **Python-side**
   post-processing (e.g. remapping `parent_source_id`, excluding the shadow
   node from the returned rows) behaves correctly given a canned set of rows
   that simulates what PostgreSQL would return. Prefer putting as much of the
   business-workspace-specific logic as possible in testable Python
   post-processing rather than deep inside SQL, specifically so this task can
   be meaningfully unit-tested without a real database.
4. Check whether a Business Workspace can itself be nested inside another
   Business Workspace's subtree, or whether `source_workspace_nodeid` is
   always the top-level scan root. If nested Business Workspaces are
   possible, your traversal must handle the same shadow-container remap at
   every depth where a `SubType=848` node is encountered, not only at the
   scan root. Confirm this from the docs/your reasoning about the schema, not
   by assumption — comment your conclusion in the code.

## 5. Required implementation (functional scope)

Extend `engine/db.py`'s node extraction so that, when the recursive
traversal encounters a node with `SubType = 848` (whether it is the scan root
or a nested Business Workspace found deeper in the tree):

1. It also traverses that workspace's shadow container (`DataID = -<workspace
   DataID>`) to reach its actual content, instead of stopping because the
   workspace has no ordinary `ParentID`-based children.
2. The shadow container node itself (`SubType = 849`, `DataID = -W`) is
   **excluded** from the returned node list — it is not a real object to
   migrate and must never be created on the target.
3. The direct children of the shadow container have `parent_source_id`
   rewritten to `W` (the real, positive workspace DataID) so the pipeline
   parents them under the already-created Business Workspace target object.
   Deeper descendants keep their normal `ParentID`-derived `parent_source_id`
   values (those are ordinary folders/documents once you're below the shadow
   container).
4. `depth`/`path` values for the redirected subtree continue naturally from
   the workspace's own depth/path (do not reset them to 0 or introduce a
   visible gap that would confuse `discovery_summary()` or the preflight
   checks in `engine/preflight.py`).
5. Ordinary (non-848) roots and ordinary folders found anywhere in the tree
   are completely unaffected — this must be a strictly additive change to the
   existing recursive query/post-processing, not a rewrite of the ordinary
   path. Prove this with the regression tests in section 6.
6. Cycle protection (`NOT c.DataID=ANY(w.path_ids)`) must still apply across
   the shadow-container jump — do not introduce a possible infinite loop if a
   tenant's data is malformed (e.g., a workspace whose shadow container
   somehow points back into the workspace's own ordinary ancestry).
7. If a `SubType=848` node is encountered but its shadow container
   (`DataID = -W`) does not exist in `DTree` at all (defensive case — do not
   assume it always exists), fail closed with a clear
   `RuntimeError`/`TerminalMigrationError`-style message identifying the
   workspace DataID, rather than silently treating the workspace as having
   zero content. Do not guess an alternate parent.

You may implement the SQL/Python split however you judge is cleanest and most
testable, but the four items above (correct children, no shadow node in
output, correct `parent_source_id` remap, fail-closed on missing shadow
container) are the acceptance criteria, not suggestions.

### Out of scope for this task — do not touch

- Multipart/large-file upload logic, throughput/concurrency tuning, and the
  `~7.44 GB` representative-file qualification — unrelated backlog items.
- Anything about GX39-side category/provenance/owner behavior on Business
  Workspace objects specifically — the target-side `create_container()` path
  already exists; only touch it if your investigation in section 4 finds it
  is actually broken, and if so, explain exactly why in your PR.
- `db_schema`/PostgreSQL environment differences — already resolved in a
  separate change; just make sure your new queries also go through
  `self._schema` like the rest of the file already does.

## 6. Required tests (must run and pass without any network/DB access)

Add to `tests/test_engine_v2.py`, following the existing `_TestSourceDB` /
`_VersionConnection` fake-cursor pattern:

1. A regression test proving that an ordinary (non-848) root's extraction
   behavior/query shape is unchanged (guard against accidental scope creep
   into the ordinary path).
2. A test simulating a `SubType=848` root: feed a canned row set representing
   what the DB layer would return for the workspace node itself plus its
   shadow-container descendants (including at least one nested ordinary
   folder and one document, at least 2 levels deep below the shadow
   container), and assert that:
   - the returned node list does **not** contain a row for `DataID = -W`;
   - the direct children of the shadow container have
     `parent_source_id == W` in the output, not `-W`;
   - deeper descendants retain their normal parent chain;
   - the real workspace node itself is unchanged (still has its true
     `parent_source_id` pointing at its real container, e.g. its own
     enclosing folder).
3. A test for the fail-closed case: the shadow container row is absent from
   the simulated data — assert a clear, specific exception is raised instead
   of silently returning only the workspace node.
4. If you conclude nested Business Workspaces are possible (see section 4.4),
   add a test covering a `SubType=848` node found *inside* another
   workspace's shadow-container subtree, proving the same remap applies at
   that depth too. If you conclude nested Business Workspaces cannot occur in
   this schema, state that conclusion and its justification in a code comment
   near the relevant logic instead of adding a test for an impossible case.

Then run, from the repository root, and confirm all pass before considering
the task done (per `AGENTS.md` section 13):

```bash
python3 -m unittest discover -s tests -v
ruff check .
mypy app.py engine tests
```

If `.venv`/dependencies are missing, run `./bootstrap.sh` first (this repo
also has a Windows `bootstrap.ps1`, but you are on macOS so use the shell
script). None of this requires corporate network access — it only needs
`pip`-installable dependencies.

## 7. Documentation you must update as part of this change

- `ARCHITECTURE.md`: extend the existing "Business Workspace subtype 848
  uses the target Business Workspace API..." note to also describe the
  source-side shadow-container traversal you implemented, so a future reader
  understands why `engine/db.py` treats `SubType 848`/`849` specially.
- Add a short comment block directly above the relevant code in
  `engine/db.py` explaining the shadow-container mechanism in your own words
  (not just "see BUSINESS_WORKSPACE_TASK_BRIEF.md" — that file may be deleted
  once this task is integrated; the code must be self-explanatory on its
  own).
- Do **not** edit `CORPORATE_HANDOFF.md` or `DEPLOYMENT_AND_QUALIFICATION.md`
  yourself — those are actively maintained on a different machine as part of
  ongoing corporate qualification and editing them here risks a conflicting,
  hard-to-merge change. Instead, write a clear PR/commit description
  (see section 9) summarizing exactly what changed, what you verified against
  OpenText docs, what remains an open assumption, and what a real pilot still
  needs to prove — the corporate-side session will fold that into
  `CORPORATE_HANDOFF.md` and `DEPLOYMENT_AND_QUALIFICATION.md` itself once the
  change is qualified against a real Business Workspace on GX39 TEST.

## 8. Non-negotiable safety reminders for this specific change

- The source PostgreSQL connection stays strictly read-only — this task only
  changes what is *read* and how it is *shaped in Python*, never adds a
  write/DDL/lock.
- Do not invent or guess a target-side mapping for the shadow container. It
  is excluded from the manifest entirely (section 5, item 2) — it must never
  be created, referenced, or migrated as if it were a real object.
- Do not weaken the existing fail-closed behavior in `create_container()`
  (missing `workspace_routes` entry raises `TerminalMigrationError`) or in the
  `WORKSPACE_ROUTES` preflight check.
- Do not claim this change is "qualified" or "production-ready" in any commit
  message, code comment, or PR description. It is implemented and
  unit-tested only. Only a real Pilot run against GX39 TEST with a real
  Business Workspace object (performed by someone with corporate/GX39 access)
  can qualify it. Say this explicitly in your PR description.

## 9. Handoff back for integration and real-world testing

When you are done:

1. Commit your work on a dedicated branch (e.g.
   `feature/business-workspace-source-traversal`), not directly on `main`.
2. Write a PR/commit description that includes: what you implemented, exactly
   what you verified against the OpenText developer documentation (with
   links) versus what remains an open/unverified assumption, the full list of
   new/changed tests and why they prove what they prove, and an explicit
   statement that this has not been tested against a real Content Server
   database or a real GX39 tenant.
3. **Also write a standalone `BUSINESS_WORKSPACE_IMPLEMENTATION_REPORT.md`**
   at the repository root (do not only rely on the PR/commit description).
   Verification will happen in a **fresh chat session, on a different
   machine, with no memory of this implementation session**, so the report
   must be fully self-contained. Include at minimum:
   - A one-paragraph summary of what was implemented and why (link back to
     `BUSINESS_WORKSPACE_TASK_BRIEF.md` for the original problem statement).
   - The exact list of files changed, with a one-line reason for each.
   - What you verified against the OpenText developer documentation, with
     direct links and a quote/paraphrase of the relevant confirming text; and
     separately, a clearly marked list of anything you could **not** verify
     and implemented defensively/fail-closed instead (per section 4/5 above).
   - The conclusion you reached on nested Business Workspaces (section 4,
     item 4) and why.
   - The exact commands you ran locally (`python3 -m unittest discover -s
     tests -v`, `ruff check .`, `mypy app.py engine tests`) and their final
     result (pass/fail counts) — copy the actual tail output, not just "all
     passed".
   - A precise, numbered **manual verification checklist** for the person
     continuing in the new chat, written as concrete steps against the real
     running app, e.g.: "1. Configure a `test` profile with
     `source_workspace_nodeid` set to a real Business Workspace's DataID. 2.
     Add a `workspace_routes` entry for it. 3. Run Scan source and confirm
     `total_nodes` now reflects the full subtree, not 1. 4. Inspect the
     manifest and confirm no node has `source_id` equal to the negative
     shadow DataID. 5. Confirm direct former-shadow-children have
     `parent_source_id` equal to the workspace's own positive DataID. 6. Run
     Dry Run. 7. Run a Representative Pilot and confirm the Business
     Workspace and its content are created and VERIFIED on GX39 TEST." Adjust
     to whatever your actual implementation and test coverage make sensible
     — the point is that whoever picks this up in the new thread should be
     able to follow it step by step without re-reading your diff first.
   - Any known limitations, edge cases deliberately not handled, or follow-up
     work you'd recommend.
4. Hand the branch back. The next step, done together on a machine with
   corporate/GX39 access, will be: pull the branch, read
   `BUSINESS_WORKSPACE_IMPLEMENTATION_REPORT.md` first, then follow its
   manual verification checklist against a real Business Workspace in GX39
   TEST with a configured `workspace_routes` entry (Dry Run, then a
   Representative Pilot), and only then fold the results into
   `CORPORATE_HANDOFF.md` and `DEPLOYMENT_AND_QUALIFICATION.md` per the
   existing pattern used for the ordinary-folder Pilot, and delete both
   `BUSINESS_WORKSPACE_TASK_BRIEF.md` and
   `BUSINESS_WORKSPACE_IMPLEMENTATION_REPORT.md`.

## 10. Quick acceptance checklist

- [ ] Read all required documents/files in section 0.
- [ ] Verified (or explicitly flagged as unverified) the shadow-container
      mechanism against OpenText documentation.
- [ ] `engine/db.py` extraction correctly reaches Business Workspace content,
      excludes the shadow node, and remaps `parent_source_id` correctly.
- [ ] Fails closed if the shadow container is missing.
- [ ] Ordinary (non-848) extraction is provably unchanged.
- [ ] New unit tests added and passing; full suite, ruff, and mypy all clean.
- [ ] `ARCHITECTURE.md` updated; in-code comment explains the mechanism.
- [ ] `CORPORATE_HANDOFF.md`/`DEPLOYMENT_AND_QUALIFICATION.md` left untouched.
- [ ] `BUSINESS_WORKSPACE_IMPLEMENTATION_REPORT.md` written at the repository
      root with a self-contained summary and manual verification checklist
      for a fresh chat session to follow.
- [ ] PR/commit description states clearly this is unqualified pending a real
      Pilot.
