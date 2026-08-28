from __future__ import annotations

import unittest
from pathlib import Path


class OperatorUiContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = (Path(__file__).parents[1] / "static" / "index.html").read_text(encoding="utf-8")

    def test_workflow_actions_have_stable_ids_and_prerequisite_locking(self):
        self.assertIn('id="btn-scan-source"', self.html)
        self.assertIn('id="btn-dry-run" onclick="startMigration(\'dry_run\')" disabled', self.html)
        self.assertIn('id="btn-pilot" onclick="startMigration(\'poc_100\')" disabled', self.html)
        self.assertIn('id="btn-full-cutover" onclick="startMigration(\'full\')" disabled', self.html)
        self.assertIn('id="workflow-next-action"', self.html)

    def test_verification_handles_non_success_responses_before_rendering_tests(self):
        response_guard = "if (!res.ok) throw new Error(data.detail"
        tests_guard = "if (!Array.isArray(data.tests))"
        self.assertIn(response_guard, self.html)
        self.assertIn(tests_guard, self.html)
        self.assertIn('badge.textContent = "NOT AVAILABLE"', self.html)

    def test_missing_link_mapping_has_a_human_readable_fallback(self):
        self.assertIn("This object has not been migrated or is not present in the active manifest.", self.html)
        self.assertNotIn("${data.message}</div>", self.html)

    def test_operator_setup_uses_one_sas_url_and_separates_acceptance(self):
        self.assertIn('id="pf-azure-sas-url"', self.html)
        self.assertIn('id="pf-source-cs-url"', self.html)
        self.assertIn('id="pf-source-cs-user"', self.html)
        self.assertIn('id="pf-source-cs-password"', self.html)
        self.assertIn("exact source OwnerID on the left", self.html)
        self.assertIn("placeholder='Example: {\"-123456\": 789012}'", self.html)
        self.assertNotIn('id="pf-azure-url"', self.html)
        self.assertNotIn('id="pf-locator"', self.html)
        self.assertNotIn('id="pf-sas-token"', self.html)
        self.assertIn('id="modal-acceptance"', self.html)
        self.assertIn('Mappings for exceptions — normally leave empty', self.html)
        self.assertIn('id="modal-duplicate-protection"', self.html)
        self.assertIn('id="duplicate-marker-attr"', self.html)
        self.assertNotIn('id="pf-class"', self.html)
        self.assertNotIn('id="pf-root-maps"', self.html)
        self.assertNotIn('id="pf-system-strategy"', self.html)
        self.assertNotIn('id="pf-permission-strategy"', self.html)
        self.assertIn('Source root DataID', self.html)
        self.assertIn('The complete subtree below this object is included.', self.html)
        self.assertIn('The app creates the selected source root inside it.', self.html)
        self.assertNotIn('R&amp;D workspace DataID', self.html)
        self.assertNotIn('Pilot setup', self.html)

    def test_unconfigured_scope_never_displays_example_identifiers(self):
        self.assertIn('id="scope-node-badge">NodeID: not configured</span>', self.html)
        self.assertIn('id="scope-target-cloud">not configured</strong>', self.html)
        self.assertIn("data.source_workspace_nodeid || 'not configured'", self.html)
        self.assertIn("data.target_workspace_nodeid ? `NodeID: ${data.target_workspace_nodeid}`", self.html)
        self.assertNotIn("NodeID: 1124604", self.html)
        self.assertNotIn(">Cloud DEV</strong>", self.html)

    def test_scan_success_uses_the_manifest_summary_contract(self):
        self.assertIn("Number(data.total_nodes || 0).toLocaleString()", self.html)
        self.assertNotIn("data.nodes_count.toLocaleString()", self.html)

    def test_successful_setup_closes_modal_and_toasts_above_dialogs(self):
        self.assertIn('id="toast-container" class="fixed top-5 right-5', self.html)
        self.assertIn('style="z-index: 100;"', self.html)
        save_success = self.html.index("showToast('Setup Saved'")
        self.assertIn("closeModal('modal-profile');", self.html[save_success - 150:save_success])
        self.assertIn("result.textContent = `Setup could not be saved: ${error.message}`", self.html)

    def test_source_freeze_is_requested_only_inside_full_cutover_confirmation(self):
        self.assertIn('id="modal-full-cutover"', self.html)
        self.assertIn('id="cutover-readonly"', self.html)
        self.assertIn('onclick="confirmFullCutover()" disabled', self.html)
        self.assertNotIn('id="freeze-summary"', self.html)
        self.assertNotIn('id="btn-confirm-freeze"', self.html)
        self.assertIn("blocker !== 'SOURCE_READ_ONLY_FREEZE'", self.html)
        self.assertIn("Target NodeID: ${profile.target_workspace_nodeid || 'not configured'}", self.html)

    def test_run_history_exposes_resume_only_for_interrupted_runs(self):
        self.assertIn('<span>Run history</span>', self.html)
        self.assertIn("['COMPLETED_WITH_ERRORS', 'FAILED', 'STOPPED']", self.html)
        self.assertIn("button.textContent = 'Resume interrupted run'", self.html)
        self.assertIn('Already completed items will not be migrated again.', self.html)
        self.assertNotIn('<span>Recovery</span>', self.html)


if __name__ == "__main__":
    unittest.main()
