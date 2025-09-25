#!/usr/bin/env python3
import csv
import os
from pathlib import Path

RAW_ROOT = Path(os.environ.get("EBISU_RAW_ROOT", "data/raw")).expanduser().resolve()
PROCESSED_ROOT = Path(os.environ.get("EBISU_PROCESSED_ROOT", RAW_ROOT)).expanduser().resolve()
REFERENCE_OUT = PROCESSED_ROOT / "reference"
REFERENCE_OUT.mkdir(parents=True, exist_ok=True)

INPUT = RAW_ROOT / "MSC_fishery_2025-06-17.csv"
OUTPUT = REFERENCE_OUT / "cleaned_msc_fishery.csv"

def collapse_whitespace(value: str) -> str:
    return " ".join(value.split())

def main():
    if not INPUT.exists():
        raise FileNotFoundError(f"Missing raw MSC file: {INPUT}")

    with INPUT.open("r", newline="", encoding="utf-8-sig") as infile:
        reader = csv.DictReader(infile)
        fieldnames = [name.strip().replace(" ", "_") for name in reader.fieldnames]

        OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        with OUTPUT.open("w", newline="", encoding="utf-8") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                cleaned = {}
                for original_name, cleaned_name in zip(reader.fieldnames, fieldnames):
                    value = row.get(original_name, "")
                    value = collapse_whitespace(value.strip()) if isinstance(value, str) else value
                    cleaned[cleaned_name] = value
                writer.writerow(cleaned)

    print(f"✅ Cleaned MSC fishery file written to {OUTPUT}")

if __name__ == "__main__":
    main()
