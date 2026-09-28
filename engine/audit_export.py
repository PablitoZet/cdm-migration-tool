"""Optional, read-only historical-audit-history export.

This module does NOT migrate the source audit trail into GX39. Audit trails
are system-generated, tamper-evident records of real actions on a given
system; OpenText exposes no supported API to backdate historical events into
a target audit log, and this tool must never forge one (see `AGENTS.md`
section 14, "never claim an integration was tested" and the general
provenance-vs-system-field distinction already used for source dates/owner).

Instead, this module renders the in-scope source audit rows (already fetched
read-only via `engine.db.SourceDB.export_audit_history`) as a plain CSV
document. An operator may optionally upload that CSV as a supplementary
reference document into the migrated workspace so the information is not
lost, without pretending it is GX39's own audit log. This is unrelated to,
and does not change, the `historical_audit_out_of_scope_approved`
qualification flag: GX39's native audit trail still starts fresh at cutover.
"""

from __future__ import annotations

import csv
import io
from typing import Any

AUDIT_EXPORT_FIELDNAMES = [
    "event_id", "audit_date", "data_id", "subtype", "action",
    "user_id", "user_name", "performer_id", "performer_name",
    "value_key", "value1", "value2",
]


def audit_history_csv(rows: list[dict[str, Any]]) -> bytes:
    """Render exported audit rows (as returned by `SourceDB.export_audit_history`)
    into a plain UTF-8 CSV document."""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=AUDIT_EXPORT_FIELDNAMES, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue().encode("utf-8")
