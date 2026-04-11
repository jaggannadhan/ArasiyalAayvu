"""
PLFS (Periodic Labour Force Survey) 2023-24 — ingestor
Source: MoSPI Annual Report PLFS 2023-24
PDF: https://www.mospi.gov.in/publications-reports  (26.9 MB — downloaded manually)

Extracts Tables 16 (LFPR), 17 (WPR), 18 (UR) from Appendix A for focus states.
Status: usual status (ps+ss), all age groups (15-29, 15-59, 15+, all ages)
Dimensions: rural / urban / rural+urban  ×  male / female / person

Outputs: data/processed/plfs_2023_24.json

Run:
    python scrapers/plfs_ingest.py
    python scrapers/plfs_ingest.py --upload   # also writes to Firestore
"""

import json
import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent

sys.path.insert(0, str(Path(__file__).parent))
from ts_utils import load_timeseries, upsert_snapshot, save_timeseries, upload_snapshot_to_firestore

# ---------------------------------------------------------------------------
# Extracted data — Tables 16 (LFPR), 17 (WPR), 18 (UR)
# Source: PLFS 2023-24 Annual Report, Appendix A
# Methodology: pdftotext -layout, rows matched by state-name prefix
# ---------------------------------------------------------------------------

FOCUS_STATES_DATA = {
    "lfpr": {
        "15-29": {
            "Andhra Pradesh": {
                "rural":  {"male": 63.0, "female": 33.6, "person": 48.6},
                "urban":  {"male": 57.2, "female": 22.4, "person": 39.3},
                "total":  {"male": 61.2, "female": 29.9, "person": 45.6},
            },
            "Karnataka": {
                "rural":  {"male": 62.4, "female": 20.0, "person": 42.3},
                "urban":  {"male": 57.7, "female": 25.9, "person": 42.6},
                "total":  {"male": 60.5, "female": 22.4, "person": 42.4},
            },
            "Kerala": {
                "rural":  {"male": 51.5, "female": 30.5, "person": 40.9},
                "urban":  {"male": 51.0, "female": 30.2, "person": 40.3},
                "total":  {"male": 51.2, "female": 30.3, "person": 40.6},
            },
            "Tamil Nadu": {
                "rural":  {"male": 60.2, "female": 24.7, "person": 42.8},
                "urban":  {"male": 57.4, "female": 22.9, "person": 39.6},
                "total":  {"male": 59.0, "female": 23.8, "person": 41.3},
            },
            "Telangana": {
                "rural":  {"male": 67.7, "female": 35.5, "person": 51.7},
                "urban":  {"male": 61.9, "female": 27.4, "person": 45.0},
                "total":  {"male": 65.2, "female": 32.1, "person": 48.8},
            },
        },
        "15-59": {
            "Andhra Pradesh": {
                "rural":  {"male": 84.8, "female": 59.1, "person": 71.8},
                "urban":  {"male": 81.1, "female": 34.1, "person": 56.8},
                "total":  {"male": 83.6, "female": 51.3, "person": 67.1},
            },
            "Karnataka": {
                "rural":  {"male": 83.7, "female": 49.9, "person": 66.9},
                "urban":  {"male": 81.4, "female": 33.5, "person": 58.2},
                "total":  {"male": 82.8, "female": 43.6, "person": 63.5},
            },
            "Kerala": {
                "rural":  {"male": 80.5, "female": 48.4, "person": 63.3},
                "urban":  {"male": 80.3, "female": 43.4, "person": 60.4},
                "total":  {"male": 80.4, "female": 46.0, "person": 61.9},
            },
            "Tamil Nadu": {
                "rural":  {"male": 83.9, "female": 58.5, "person": 70.8},
                "urban":  {"male": 82.5, "female": 33.6, "person": 57.4},
                "total":  {"male": 83.3, "female": 47.0, "person": 64.6},
            },
            "Telangana": {
                "rural":  {"male": 86.8, "female": 64.0, "person": 75.0},
                "urban":  {"male": 83.1, "female": 35.3, "person": 59.5},
                "total":  {"male": 85.3, "female": 52.7, "person": 68.7},
            },
        },
        "15+": {
            "Andhra Pradesh": {
                "rural":  {"male": 77.9, "female": 51.2, "person": 64.2},
                "urban":  {"male": 73.9, "female": 30.2, "person": 51.3},
                "total":  {"male": 76.7, "female": 44.8, "person": 60.2},
            },
            "Karnataka": {
                "rural":  {"male": 76.3, "female": 43.5, "person": 59.8},
                "urban":  {"male": 74.1, "female": 28.8, "person": 51.7},
                "total":  {"male": 75.5, "female": 38.0, "person": 56.8},
            },
            "Kerala": {
                "rural":  {"male": 75.3, "female": 44.2, "person": 58.6},
                "urban":  {"male": 73.5, "female": 37.0, "person": 53.7},
                "total":  {"male": 74.4, "female": 40.8, "person": 56.2},
            },
            "Tamil Nadu": {
                "rural":  {"male": 77.0, "female": 54.2, "person": 65.2},
                "urban":  {"male": 74.2, "female": 30.2, "person": 51.3},
                "total":  {"male": 75.7, "female": 43.2, "person": 58.8},
            },
            "Telangana": {
                "rural":  {"male": 79.5, "female": 55.7, "person": 67.1},
                "urban":  {"male": 76.9, "female": 31.8, "person": 54.4},
                "total":  {"male": 78.5, "female": 46.7, "person": 62.2},
            },
        },
        "all": {
            "Andhra Pradesh": {
                "rural":  {"male": 59.7, "female": 40.7, "person": 50.1},
                "urban":  {"male": 59.3, "female": 24.5, "person": 41.3},
                "total":  {"male": 59.5, "female": 35.8, "person": 47.5},
            },
            "Karnataka": {
                "rural":  {"male": 60.0, "female": 34.6, "person": 47.3},
                "urban":  {"male": 59.8, "female": 23.6, "person": 42.1},
                "total":  {"male": 59.9, "female": 30.5, "person": 45.4},
            },
            "Kerala": {
                "rural":  {"male": 59.7, "female": 36.2, "person": 47.3},
                "urban":  {"male": 58.4, "female": 30.3, "person": 43.3},
                "total":  {"male": 59.1, "female": 33.4, "person": 45.4},
            },
            "Tamil Nadu": {
                "rural":  {"male": 60.4, "female": 44.5, "person": 52.3},
                "urban":  {"male": 59.2, "female": 24.4, "person": 41.3},
                "total":  {"male": 59.8, "female": 35.2, "person": 47.2},
            },
            "Telangana": {
                "rural":  {"male": 61.4, "female": 44.0, "person": 52.4},
                "urban":  {"male": 57.5, "female": 24.5, "person": 41.3},
                "total":  {"male": 59.8, "female": 36.5, "person": 48.0},
            },
        },
    },
    "wpr": {
        "15-29": {
            "Andhra Pradesh": {
                "rural":  {"male": 53.6, "female": 28.0, "person": 41.1},
                "urban":  {"male": 45.7, "female": 15.9, "person": 30.4},
                "total":  {"male": 51.2, "female": 24.0, "person": 37.7},
            },
            "Karnataka": {
                "rural":  {"male": 56.8, "female": 18.7, "person": 38.7},
                "urban":  {"male": 50.6, "female": 22.5, "person": 37.2},
                "total":  {"male": 54.2, "female": 20.2, "person": 38.1},
            },
            "Kerala": {
                "rural":  {"male": 40.0, "female": 13.2, "person": 26.6},
                "urban":  {"male": 42.8, "female": 19.0, "person": 30.6},
                "total":  {"male": 41.3, "female": 16.0, "person": 28.5},
            },
            "Tamil Nadu": {
                "rural":  {"male": 51.7, "female": 19.8, "person": 36.1},
                "urban":  {"male": 51.2, "female": 17.4, "person": 33.7},
                "total":  {"male": 51.5, "female": 18.6, "person": 35.0},
            },
            "Telangana": {
                "rural":  {"male": 59.6, "female": 29.4, "person": 44.5},
                "urban":  {"male": 51.6, "female": 19.0, "person": 35.6},
                "total":  {"male": 56.1, "female": 25.0, "person": 40.7},
            },
        },
        "15-59": {
            "Andhra Pradesh": {
                "rural":  {"male": 81.3, "female": 57.3, "person": 69.1},
                "urban":  {"male": 76.3, "female": 31.6, "person": 53.3},
                "total":  {"male": 79.8, "female": 49.2, "person": 64.2},
            },
            "Karnataka": {
                "rural":  {"male": 81.3, "female": 49.5, "person": 65.5},
                "urban":  {"male": 77.9, "female": 32.0, "person": 55.6},
                "total":  {"male": 80.0, "female": 42.7, "person": 61.6},
            },
            "Kerala": {
                "rural":  {"male": 75.9, "female": 41.1, "person": 57.3},
                "urban":  {"male": 76.3, "female": 37.9, "person": 55.6},
                "total":  {"male": 76.1, "female": 39.6, "person": 56.5},
            },
            "Tamil Nadu": {
                "rural":  {"male": 80.6, "female": 56.6, "person": 68.3},
                "urban":  {"male": 79.7, "female": 31.4, "person": 54.9},
                "total":  {"male": 80.2, "female": 44.9, "person": 62.1},
            },
            "Telangana": {
                "rural":  {"male": 83.3, "female": 61.9, "person": 72.2},
                "urban":  {"male": 77.9, "female": 31.5, "person": 55.0},
                "total":  {"male": 81.0, "female": 49.9, "person": 65.2},
            },
        },
        "15+": {
            "Andhra Pradesh": {
                "rural":  {"male": 75.0, "female": 49.7, "person": 62.0},
                "urban":  {"male": 69.8, "female": 28.1, "person": 48.2},
                "total":  {"male": 73.4, "female": 43.1, "person": 57.8},
            },
            "Karnataka": {
                "rural":  {"male": 74.4, "female": 43.1, "person": 58.7},
                "urban":  {"male": 71.1, "female": 27.5, "person": 49.6},
                "total":  {"male": 73.1, "female": 37.2, "person": 55.2},
            },
            "Kerala": {
                "rural":  {"male": 71.8, "female": 38.8, "person": 54.1},
                "urban":  {"male": 70.4, "female": 33.0, "person": 50.1},
                "total":  {"male": 71.1, "female": 36.0, "person": 52.2},
            },
            "Tamil Nadu": {
                "rural":  {"male": 74.3, "female": 52.7, "person": 63.1},
                "urban":  {"male": 71.8, "female": 28.4, "person": 49.2},
                "total":  {"male": 73.2, "female": 41.5, "person": 56.8},
            },
            "Telangana": {
                "rural":  {"male": 76.6, "female": 54.0, "person": 64.8},
                "urban":  {"male": 72.2, "female": 28.5, "person": 50.4},
                "total":  {"male": 74.8, "female": 44.3, "person": 59.2},
            },
        },
        "all": {
            "Andhra Pradesh": {
                "rural":  {"male": 57.4, "female": 39.6, "person": 48.4},
                "urban":  {"male": 56.0, "female": 22.8, "person": 38.9},
                "total":  {"male": 57.0, "female": 34.5, "person": 45.6},
            },
            "Karnataka": {
                "rural":  {"male": 58.5, "female": 34.3, "person": 46.4},
                "urban":  {"male": 57.4, "female": 22.5, "person": 40.3},
                "total":  {"male": 58.1, "female": 29.9, "person": 44.1},
            },
            "Kerala": {
                "rural":  {"male": 57.0, "female": 31.8, "person": 43.7},
                "urban":  {"male": 55.9, "female": 27.0, "person": 40.4},
                "total":  {"male": 56.5, "female": 29.5, "person": 42.1},
            },
            "Tamil Nadu": {
                "rural":  {"male": 58.3, "female": 43.3, "person": 50.7},
                "urban":  {"male": 57.3, "female": 22.9, "person": 39.6},
                "total":  {"male": 57.8, "female": 33.9, "person": 45.6},
            },
            "Telangana": {
                "rural":  {"male": 59.1, "female": 42.6, "person": 50.6},
                "urban":  {"male": 54.0, "female": 21.9, "person": 38.3},
                "total":  {"male": 57.0, "female": 34.7, "person": 45.7},
            },
        },
    },
    "ur": {
        "15-29": {
            "Andhra Pradesh": {
                "rural":  {"male": 14.9, "female": 16.6, "person": 15.5},
                "urban":  {"male": 20.1, "female": 29.1, "person": 22.7},
                "total":  {"male": 16.4, "female": 19.7, "person": 17.5},
            },
            "Karnataka": {
                "rural":  {"male": 9.0,  "female": 6.7,  "person": 8.5},
                "urban":  {"male": 12.3, "female": 13.1, "person": 12.6},
                "total":  {"male": 10.3, "female": 9.7,  "person": 10.2},
            },
            "Kerala": {
                "rural":  {"male": 22.2, "female": 56.6, "person": 35.1},
                "urban":  {"male": 15.9, "female": 37.0, "person": 24.1},
                "total":  {"male": 19.3, "female": 47.1, "person": 29.9},
            },
            "Tamil Nadu": {
                "rural":  {"male": 14.1, "female": 19.8, "person": 15.7},
                "urban":  {"male": 10.9, "female": 24.2, "person": 14.9},
                "total":  {"male": 12.7, "female": 21.9, "person": 15.3},
            },
            "Telangana": {
                "rural":  {"male": 12.0, "female": 17.2, "person": 13.8},
                "urban":  {"male": 16.7, "female": 30.7, "person": 20.9},
                "total":  {"male": 13.9, "female": 22.1, "person": 16.6},
            },
        },
        "15-59": {
            "Andhra Pradesh": {
                "rural":  {"male": 4.1, "female": 3.1, "person": 3.7},
                "urban":  {"male": 5.8, "female": 7.2, "person": 6.3},
                "total":  {"male": 4.6, "female": 4.0, "person": 4.4},
            },
            "Karnataka": {
                "rural":  {"male": 2.8, "female": 1.0, "person": 2.1},
                "urban":  {"male": 4.3, "female": 4.5, "person": 4.4},
                "total":  {"male": 3.4, "female": 2.0, "person": 2.9},
            },
            "Kerala": {
                "rural":  {"male": 5.8, "female": 15.0, "person": 9.6},
                "urban":  {"male": 5.0, "female": 12.6, "person": 7.9},
                "total":  {"male": 5.4, "female": 13.9, "person": 8.8},
            },
            "Tamil Nadu": {
                "rural":  {"male": 3.9, "female": 3.2, "person": 3.6},
                "urban":  {"male": 3.4, "female": 6.5, "person": 4.3},
                "total":  {"male": 3.7, "female": 4.3, "person": 3.9},
            },
            "Telangana": {
                "rural":  {"male": 4.1, "female": 3.3, "person": 3.8},
                "urban":  {"male": 6.3, "female": 10.7, "person": 7.6},
                "total":  {"male": 5.0, "female": 5.3, "person": 5.1},
            },
        },
        "15+": {
            "Andhra Pradesh": {
                "rural":  {"male": 3.8, "female": 2.9, "person": 3.4},
                "urban":  {"male": 5.5, "female": 6.9, "person": 5.9},
                "total":  {"male": 4.3, "female": 3.7, "person": 4.1},
            },
            "Karnataka": {
                "rural":  {"male": 2.5, "female": 0.9, "person": 1.9},
                "urban":  {"male": 4.1, "female": 4.4, "person": 4.2},
                "total":  {"male": 3.1, "female": 1.9, "person": 2.7},
            },
            "Kerala": {
                "rural":  {"male": 4.6, "female": 12.1, "person": 7.7},
                "urban":  {"male": 4.2, "female": 10.9, "person": 6.7},
                "total":  {"male": 4.4, "female": 11.6, "person": 7.2},
            },
            "Tamil Nadu": {
                "rural":  {"male": 3.5, "female": 2.7, "person": 3.1},
                "urban":  {"male": 3.2, "female": 6.0, "person": 4.1},
                "total":  {"male": 3.4, "female": 3.8, "person": 3.5},
            },
            "Telangana": {
                "rural":  {"male": 3.7, "female": 3.1, "person": 3.5},
                "urban":  {"male": 6.0, "female": 10.4, "person": 7.3},
                "total":  {"male": 4.6, "female": 5.0, "person": 4.8},
            },
        },
        "all": {
            "Andhra Pradesh": {
                "rural":  {"male": 3.8, "female": 2.9, "person": 3.4},
                "urban":  {"male": 5.5, "female": 6.9, "person": 5.9},
                "total":  {"male": 4.3, "female": 3.7, "person": 4.1},
            },
            "Karnataka": {
                "rural":  {"male": 2.5, "female": 0.9, "person": 1.9},
                "urban":  {"male": 4.1, "female": 4.4, "person": 4.2},
                "total":  {"male": 3.1, "female": 1.9, "person": 2.7},
            },
            "Kerala": {
                "rural":  {"male": 4.6, "female": 12.1, "person": 7.6},
                "urban":  {"male": 4.2, "female": 10.9, "person": 6.7},
                "total":  {"male": 4.4, "female": 11.6, "person": 7.2},
            },
            "Tamil Nadu": {
                "rural":  {"male": 3.5, "female": 2.7, "person": 3.1},
                "urban":  {"male": 3.2, "female": 6.0, "person": 4.1},
                "total":  {"male": 3.4, "female": 3.8, "person": 3.5},
            },
            "Telangana": {
                "rural":  {"male": 3.7, "female": 3.1, "person": 3.5},
                "urban":  {"male": 6.0, "female": 10.4, "person": 7.3},
                "total":  {"male": 4.6, "female": 5.0, "person": 4.8},
            },
        },
    },
}

ALL_INDIA_BENCHMARKS = {
    "lfpr": {
        "15-29": {
            "rural":  {"male": 65.1, "female": 30.8, "person": 48.1},
            "urban":  {"male": 59.9, "female": 23.8, "person": 42.6},
            "total":  {"male": 63.5, "female": 28.8, "person": 46.5},
        },
        "15-59": {
            "rural":  {"male": 84.3, "female": 51.2, "person": 67.6},
            "urban":  {"male": 81.9, "female": 31.2, "person": 57.0},
            "total":  {"male": 83.5, "female": 45.2, "person": 64.3},
        },
        "15+": {
            "rural":  {"male": 80.2, "female": 47.6, "person": 63.7},
            "urban":  {"male": 75.6, "female": 28.0, "person": 52.0},
            "total":  {"male": 78.8, "female": 41.7, "person": 60.1},
        },
        "all": {
            "rural":  {"male": 57.9, "female": 35.5, "person": 46.8},
            "urban":  {"male": 59.0, "female": 22.3, "person": 41.0},
            "total":  {"male": 58.2, "female": 31.7, "person": 45.1},
        },
    },
    "wpr": {
        "15-29": {
            "rural":  {"male": 59.5, "female": 28.3, "person": 44.0},
            "urban":  {"male": 52.2, "female": 19.0, "person": 36.3},
            "total":  {"male": 57.3, "female": 25.6, "person": 41.7},
        },
        "15-59": {
            "rural":  {"male": 81.7, "female": 50.0, "person": 65.7},
            "urban":  {"male": 78.1, "female": 28.8, "person": 53.9},
            "total":  {"male": 80.6, "female": 43.7, "person": 62.1},
        },
        "15+": {
            "rural":  {"male": 78.1, "female": 46.5, "person": 62.1},
            "urban":  {"male": 72.3, "female": 26.0, "person": 49.4},
            "total":  {"male": 76.3, "female": 40.3, "person": 58.2},
        },
        "all": {
            "rural":  {"male": 56.3, "female": 34.8, "person": 45.6},
            "urban":  {"male": 56.4, "female": 20.7, "person": 38.9},
            "total":  {"male": 56.4, "female": 30.7, "person": 43.7},
        },
    },
    "ur": {
        "15-29": {
            "rural":  {"male": 8.7,  "female": 8.2,  "person": 8.5},
            "urban":  {"male": 12.8, "female": 20.1, "person": 14.7},
            "total":  {"male": 9.8,  "female": 11.0, "person": 10.2},
        },
        "15-59": {
            "rural":  {"male": 3.0, "female": 2.3, "person": 2.8},
            "urban":  {"male": 4.6, "female": 7.6, "person": 5.4},
            "total":  {"male": 3.5, "female": 3.4, "person": 3.5},
        },
        "15+": {
            "rural":  {"male": 2.7, "female": 2.1, "person": 2.5},
            "urban":  {"male": 4.4, "female": 7.1, "person": 5.1},
            "total":  {"male": 3.2, "female": 3.2, "person": 3.2},
        },
        "all": {
            "rural":  {"male": 2.7, "female": 2.1, "person": 2.5},
            "urban":  {"male": 4.4, "female": 7.1, "person": 5.1},
            "total":  {"male": 3.2, "female": 3.1, "person": 3.2},
        },
    },
}

FOCUS_STATES = ["Andhra Pradesh", "Karnataka", "Kerala", "Tamil Nadu", "Telangana"]
INDICATORS = ["lfpr", "wpr", "ur"]
AGE_GROUPS = ["15-29", "15-59", "15+", "all"]

INDICATOR_LABELS = {
    "lfpr": "Labour Force Participation Rate",
    "wpr":  "Worker Population Ratio",
    "ur":   "Unemployment Rate",
}
AGE_GROUP_LABELS = {
    "15-29": "15 to 29 years",
    "15-59": "15 to 59 years",
    "15+":   "15 years and above",
    "all":   "All ages",
}


def build_by_state() -> dict:
    """Restructure as by_state[state][indicator][age_group] → {rural,urban,total}."""
    by_state: dict[str, dict] = {}
    for state in FOCUS_STATES:
        by_state[state] = {}
        for ind in INDICATORS:
            by_state[state][ind] = {}
            for age in AGE_GROUPS:
                by_state[state][ind][age] = FOCUS_STATES_DATA[ind][age][state]
    return by_state


def compute_gaps_vs_national(by_state: dict) -> dict:
    """
    For each focus state × indicator × age_group: compute gap vs All-India total person.
    Returns {state: {indicator: {age_group: {rural_gap, urban_gap, total_gap}}}}
    Positive = above All-India; negative = below.
    """
    gaps: dict[str, dict] = {}
    for state in FOCUS_STATES:
        gaps[state] = {}
        for ind in INDICATORS:
            gaps[state][ind] = {}
            for age in AGE_GROUPS:
                ai = ALL_INDIA_BENCHMARKS[ind][age]
                st = by_state[state][ind][age]
                gaps[state][ind][age] = {
                    loc: round(st[loc]["person"] - ai[loc]["person"], 1)
                    for loc in ["rural", "urban", "total"]
                }
    return gaps


def build_output(by_state: dict) -> dict:
    gaps = compute_gaps_vs_national(by_state)

    # Key highlights for TN quick reference
    tn_highlights = {
        "ur_15plus_total_person":          by_state["Tamil Nadu"]["ur"]["15+"]["total"]["person"],
        "ur_15plus_rural_person":          by_state["Tamil Nadu"]["ur"]["15+"]["rural"]["person"],
        "ur_15plus_urban_person":          by_state["Tamil Nadu"]["ur"]["15+"]["urban"]["person"],
        "ur_youth_1529_total_person":      by_state["Tamil Nadu"]["ur"]["15-29"]["total"]["person"],
        "ur_youth_1529_female_urban":      by_state["Tamil Nadu"]["ur"]["15-29"]["urban"]["female"],
        "lfpr_15plus_total_person":        by_state["Tamil Nadu"]["lfpr"]["15+"]["total"]["person"],
        "lfpr_female_rural_15plus":        by_state["Tamil Nadu"]["lfpr"]["15+"]["rural"]["female"],
        "wpr_15plus_total_person":         by_state["Tamil Nadu"]["wpr"]["15+"]["total"]["person"],
    }

    kerala_alerts = {
        "ur_15plus_total_person":             by_state["Kerala"]["ur"]["15+"]["total"]["person"],
        "ur_youth_1529_female_rural":         by_state["Kerala"]["ur"]["15-29"]["rural"]["female"],
        "ur_youth_1529_female_total":         by_state["Kerala"]["ur"]["15-29"]["total"]["female"],
        "note": "Kerala has highest UR among focus states; severe female youth structural unemployment",
    }

    return {
        "meta": {
            "source": "MoSPI — Periodic Labour Force Survey (PLFS) Annual Report 2023-24",
            "url": "https://www.mospi.gov.in/publications-reports",
            "survey_year": "2023-24",
            "status": "usual status (ps+ss)",
            "note": (
                "ps = principal status; ss = subsidiary status. "
                "Data covers July 2023 – June 2024."
            ),
            "indicators": INDICATOR_LABELS,
            "age_groups": AGE_GROUP_LABELS,
            "focus_states": FOCUS_STATES,
            "tables_extracted": {
                "Table 16": "LFPR — Appendix A",
                "Table 17": "WPR — Appendix A",
                "Table 18": "UR — Appendix A",
            },
        },
        "all_india_benchmarks": ALL_INDIA_BENCHMARKS,
        "by_state": by_state,
        "gaps_vs_all_india": gaps,
        "highlights": {
            "Tamil Nadu": tn_highlights,
            "Kerala": kerala_alerts,
        },
    }


def upload_to_firestore(ts: dict) -> None:
    """Upload all PLFS snapshots to Firestore sub-collection pattern: plfs/{entity_id}/snapshots/{data_period}."""
    import firebase_admin
    from firebase_admin import credentials, firestore as fs

    cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not firebase_admin._apps:
        cred = credentials.Certificate(cred_path) if cred_path else credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred)
    db = fs.client()

    count = 0
    for display_name, entity in ts["entities"].items():
        for data_period, snapshot in entity["snapshots"].items():
            upload_snapshot_to_firestore(db, "plfs", display_name, data_period, snapshot)
            count += 1
    print(f"  Uploaded {count} PLFS snapshots to Firestore.")


def main():
    upload = "--upload" in sys.argv
    out_path = BASE_DIR / "data" / "processed" / "plfs_ts.json"

    ts = load_timeseries(out_path)
    meta = {
        "dataset": "plfs",
        "source": "MoSPI — Periodic Labour Force Survey (PLFS) Annual Report 2023-24",
        "url": "https://www.mospi.gov.in/publications-reports",
        "status": "usual status (ps+ss)",
        "note": "ps = principal status; ss = subsidiary status. Data covers July 2023 – June 2024.",
        "indicators": INDICATOR_LABELS,
        "age_groups": AGE_GROUP_LABELS,
    }

    by_state = build_by_state()
    gaps = compute_gaps_vs_national(by_state)

    for i, state in enumerate(FOCUS_STATES):
        snapshot = {
            "lfpr": by_state[state]["lfpr"],
            "wpr":  by_state[state]["wpr"],
            "ur":   by_state[state]["ur"],
            "gaps_vs_all_india": gaps[state],
        }
        upsert_snapshot(ts, state, "2023-24", snapshot, meta=meta if i == 0 else None)

    upsert_snapshot(ts, "All India", "2023-24", {
        "lfpr": ALL_INDIA_BENCHMARKS["lfpr"],
        "wpr":  ALL_INDIA_BENCHMARKS["wpr"],
        "ur":   ALL_INDIA_BENCHMARKS["ur"],
    })

    save_timeseries(ts, out_path)
    print(f"Wrote {out_path}  ({out_path.stat().st_size // 1024} KB)")

    # Summary table
    print("\n── UR (15+, total person) — focus states vs All-India ────")
    ai_ur = ALL_INDIA_BENCHMARKS["ur"]["15+"]["total"]["person"]
    print(f"{'State':<20} {'UR 15+':>8} {'vs India':>10} {'UR youth (15-29)':>17}")
    print("─" * 60)
    for state in FOCUS_STATES:
        ur15 = by_state[state]["ur"]["15+"]["total"]["person"]
        ur29 = by_state[state]["ur"]["15-29"]["total"]["person"]
        gap = round(ur15 - ai_ur, 1)
        sign = "+" if gap >= 0 else ""
        print(f"{state:<20} {ur15:>8.1f} {sign+str(gap):>10} {ur29:>17.1f}")
    print(f"{'All India':20} {ai_ur:>8.1f} {'—':>10} {ALL_INDIA_BENCHMARKS['ur']['15-29']['total']['person']:>17.1f}")

    print("\n── Kerala female youth UR (structural alert) ─────────────")
    kl29 = by_state["Kerala"]["ur"]["15-29"]
    print(f"  Rural female: {kl29['rural']['female']}%  |  Urban female: {kl29['urban']['female']}%  |  Total female: {kl29['total']['female']}%")

    if upload:
        print("\nUploading to Firestore...")
        upload_to_firestore(ts)

    return ts


if __name__ == "__main__":
    main()
