"""
Applore Technologies — Lead Export Script
==========================================
Reads leads_raw.json and exports a timestamped CSV.
Run manually or scheduled via cron.

Usage:
    python3 export_leads.py
"""

import csv
import json
import os
from datetime import datetime
from pathlib import Path

BASE_DIR   = Path(__file__).parent
INPUT_FILE = BASE_DIR / "leads_raw.json"
OUTPUT_DIR = BASE_DIR / "exports"

FIELDS = [
    "company_name", "contact_name", "title",
    "email_guess", "email_confidence", "email_verified", "email_reason",
    "website", "sector", "country", "employees",
    "description", "last_round", "linkedin_url", "scraped_at",
]


def export():
    if not INPUT_FILE.exists():
        print(f"[ERROR] {INPUT_FILE} not found.")
        return

    OUTPUT_DIR.mkdir(exist_ok=True)

    with open(INPUT_FILE, encoding="utf-8") as f:
        data = json.load(f)

    if not data:
        print("[WARN] leads_raw.json is empty. Nothing to export.")
        return

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename  = OUTPUT_DIR / f"leads_export_{timestamp}.csv"

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in data:
            writer.writerow({k: row.get(k, "") for k in FIELDS})

    size_kb = os.path.getsize(filename) / 1024
    print(f"[{timestamp}] Exported {len(data)} records → {filename} ({size_kb:.1f} KB)")


if __name__ == "__main__":
    export()
