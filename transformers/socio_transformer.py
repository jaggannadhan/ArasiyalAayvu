"""
Socio-Economic & Education Transformer
Merges ASER scraped data, NFHS-5 curated metrics, NITI SDG Index,
and economic indicators into the socio_economics Firestore collection.
"""

import json
from pathlib import Path


def merge_aser_into_socio(aser_data: dict, socio_docs: list[dict]) -> list[dict]:
    """
    Overrides the curated ASER entries in socio_economics with freshly scraped values.
    Only updates the metrics where ASER provides the primary number.
    """
    aser_updates = {
        "aser2024_std3_reading_recovery": {
            "value": aser_data.get("std3_read_std2_2024_pct"),
            "trend": {
                "aser_2018": aser_data.get("std3_read_std2_2018_pct"),
                "aser_2022": aser_data.get("std3_read_std2_2022_pct"),
                "aser_2024": aser_data.get("std3_read_std2_2024_pct"),
                "recovery_2022_to_2024": aser_data.get("std3_reading_recovery_2022_to_2024"),
            },
            "source_url": aser_data.get("source_url"),
            "pdf_checksum": aser_data.get("pdf_checksum"),
        },
        "aser2024_std8_arithmetic": {
            "value": aser_data.get("std8_arith_division_2024_pct"),
            "trend": {
                "aser_2018": aser_data.get("std8_arith_division_2018_pct"),
                "aser_2022": aser_data.get("std8_arith_division_2022_pct"),
                "aser_2024": aser_data.get("std8_arith_division_2024_pct"),
                "recovery_2022_to_2024": aser_data.get("std8_arith_recovery_2022_to_2024"),
            },
            "source_url": aser_data.get("source_url"),
            "pdf_checksum": aser_data.get("pdf_checksum"),
        },
    }

    updated_docs = []
    for doc in socio_docs:
        mid = doc.get("metric_id")
        if mid in aser_updates:
            updates = aser_updates[mid]
            if updates.get("value") is not None:
                doc.update({k: v for k, v in updates.items() if v is not None})
                doc["ground_truth_confidence"] = "HIGH"
        updated_docs.append(doc)

    return updated_docs


def add_aser_enrollment_metrics(aser_data: dict) -> list[dict]:
    """
    Creates additional socio_economics docs from ASER enrollment and dropout data.
    """
    extras = []

    if aser_data.get("not_in_school_2024_pct") is not None:
        extras.append({
            "metric_id": "aser2024_out_of_school_rate",
            "category": "Education",
            "subcategory": "Enrollment",
            "metric_name": "Children Not in School (Age 6-14)",
            "tamil_name": "பள்ளிக்கு வராத குழந்தைகள்",
            "value": aser_data["not_in_school_2024_pct"],
            "unit": "percent",
            "year": 2024,
            "survey": "ASER 2024",
            "trend": {
                "aser_2018": aser_data.get("not_in_school_2018_pct"),
                "aser_2022": aser_data.get("not_in_school_2022_pct"),
                "aser_2024": aser_data.get("not_in_school_2024_pct"),
            },
            "context": (
                f"Only {aser_data['not_in_school_2024_pct']}% of children (6-14) are out of school in TN — "
                "one of the lowest dropout rates in India. Near-universal enrollment achieved."
            ),
            "source_url": aser_data.get("source_url"),
            "ground_truth_confidence": "HIGH",
        })

    if aser_data.get("govt_school_enrollment_2024_pct") is not None:
        extras.append({
            "metric_id": "aser2024_govt_school_enrollment",
            "category": "Education",
            "subcategory": "Enrollment",
            "metric_name": "Government School Enrollment Share",
            "tamil_name": "அரசுப் பள்ளிகளில் சேர்க்கை பங்கு",
            "value": aser_data["govt_school_enrollment_2024_pct"],
            "unit": "percent_of_enrolled_children",
            "year": 2024,
            "survey": "ASER 2024",
            "trend": {
                "aser_2018": aser_data.get("govt_school_enrollment_2018_pct"),
                "aser_2022": aser_data.get("govt_school_enrollment_2022_pct"),
                "aser_2024": aser_data.get("govt_school_enrollment_2024_pct"),
            },
            "context": (
                "Pandemic-era surge in government school enrollment (75.7% in 2022) "
                "normalized to 68.7% by 2024 as private schools reopened."
            ),
            "source_url": aser_data.get("source_url"),
            "ground_truth_confidence": "HIGH",
        })

    return extras


def save_processed(data, path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  [saved] {path}")
