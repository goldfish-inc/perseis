#!/usr/bin/env python3
import csv
import os
from pathlib import Path

RAW_ROOT = Path(os.environ.get("EBISU_RAW_ROOT", "data/raw")).expanduser().resolve()
PROCESSED_ROOT = Path(os.environ.get("EBISU_PROCESSED_ROOT", RAW_ROOT)).expanduser().resolve()
RAW_DIR = RAW_ROOT / "vessels" / "vessel_data" / "RFMO" / "raw"
OUT_DIR = PROCESSED_ROOT / "vessels" / "RFMO" / "cleaned"
OUT_DIR.mkdir(parents=True, exist_ok=True)

def collapse_whitespace(value: str) -> str:
    return " ".join(value.split())

def clean_csv(raw_file: Path):
    prefix = raw_file.name.split('_')[0].lower()
    output_name = f"{prefix}_vessels_cleaned.csv"
    output_path = OUT_DIR / output_name

    with raw_file.open('r', newline='', encoding='utf-8-sig') as infile:
        reader = csv.DictReader(infile)
        fieldnames = [name.strip().replace(' ', '_') for name in reader.fieldnames]

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open('w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                cleaned = {}
                for original, cleaned_name in zip(reader.fieldnames, fieldnames):
                    value = row.get(original, '')
                    if isinstance(value, str):
                        value = collapse_whitespace(value.strip())
                    cleaned[cleaned_name] = value
                writer.writerow(cleaned)

    print(f"✅ Cleaned {raw_file.name} → {output_name}")

def main():
    if not RAW_DIR.exists():
        raise FileNotFoundError(f"Missing raw RFMO directory: {RAW_DIR}")

    csv_files = sorted(RAW_DIR.glob('*.csv'))
    if not csv_files:
        print("⚠️ No RFMO raw CSV files found to clean.")
        return

    for raw_file in csv_files:
        clean_csv(raw_file)

if __name__ == "__main__":
    main()
