"""Schema for the feedback Firestore collection.

Written by the backend ``POST /api/feedback`` handler. This is the spine of the
future learning loop (Module 5 — Feedback Triage), so getting its contract right
matters.
"""

from __future__ import annotations

from typing import Any, Dict, Literal, Optional

from ._base import FirestoreDoc

FeedbackCategory = Literal[
    "correction",
    "missing_data",
    "suggestion",
    "bug_report",
    "other",
]

# "new" is the only status the backend writes today; the rest are reserved for
# the triage workflow introduced in Module 5.
FeedbackStatus = Literal["new", "triaged", "actioned", "rejected", "resolved"]


class FeedbackDoc(FirestoreDoc):
    category: FeedbackCategory
    message: str
    status: FeedbackStatus = "new"
    page_url: Optional[str] = None
    entity_context: Optional[Dict[str, Any]] = None
    user_agent: Optional[str] = None
    client_ip: Optional[str] = None
    # created_at is a Firestore SERVER_TIMESTAMP sentinel on write; tolerate any.
    created_at: Optional[Any] = None
