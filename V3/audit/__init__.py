# V3/audit/__init__.py
# Audit subsystem — re-exports key symbols for convenient imports.
#
# Modules:
#   logger  - JSONL audit logger (audit_event)
#   tracker - Unified Excel audit tracker (write_hotfolder_event, etc.)

from V3.audit.logger import audit_event
from V3.audit.tracker import (
    write_hotfolder_event,
    write_edm_event,
    write_batch_event,
    record_hotfolder_start,
    record_hotfolder_end,
    record_hotfolder_needs_review,
    read_dashboard_stats,
    detection_tier,
)
