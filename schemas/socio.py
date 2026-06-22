"""Schema for the socio_economics Firestore collection.

Produced by socio_transformer (ASER merge + curated NFHS-5 / education metrics).
Keyed by ``metric_id``.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Union

from ._base import Confidence, FirestoreDoc


class SocioEconomicDoc(FirestoreDoc):
    metric_id: str
    category: Optional[str] = None
    subcategory: Optional[str] = None
    metric_name: Optional[str] = None
    tamil_name: Optional[str] = None
    value: Optional[Union[float, int, str]] = None
    unit: Optional[str] = None
    year: Optional[int] = None
    survey: Optional[str] = None
    trend: Optional[Dict[str, Any]] = None
    context: Optional[str] = None
    source_url: Optional[str] = None
    pdf_checksum: Optional[str] = None
    ground_truth_confidence: Optional[Confidence] = None
