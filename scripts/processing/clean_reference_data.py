#!/usr/bin/env python3
"""Lightweight reference data cleaner that works without pandas/pandera.

Reads raw CSV files from EBISU_RAW_ROOT (defaults to ./data/raw) and writes
cleaned derivatives into EBISU_PROCESSED_ROOT/reference (defaults to raw root).
"""

import csv
import os
import re
from pathlib import Path
from typing import List, Dict

RAW_ROOT = Path(os.environ.get("EBISU_RAW_ROOT", "data/raw")).expanduser().resolve()
PROCESSED_ROOT = Path(os.environ.get("EBISU_PROCESSED_ROOT", RAW_ROOT)).expanduser().resolve()
REFERENCE_OUT = PROCESSED_ROOT / "reference"
REFERENCE_OUT.mkdir(parents=True, exist_ok=True)


def read_csv(path: Path) -> (List[str], List[Dict[str, str]]):
    with path.open("r", newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        rows = []
        for row in reader:
            cleaned = {k: (v.strip() if v is not None else "") for k, v in row.items()}
            rows.append(cleaned)
        return reader.fieldnames, rows


def write_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def clean_country_iso():
    fields, rows = read_csv(RAW_ROOT / "country_iso.csv")
    for row in rows:
        numeric = row.get("numeric_code", "").replace(".0", "").strip()
        row["numeric_code"] = numeric.zfill(3) if numeric else ""
    write_csv(REFERENCE_OUT / "country_iso_cleaned.csv", fields, rows)


def clean_country_iso_foc():
    fields, rows = read_csv(RAW_ROOT / "country_iso_foc.csv")
    write_csv(REFERENCE_OUT / "country_iso_foc_cleaned.csv", fields, rows)


def clean_country_iso_ilo():
    fields, rows = read_csv(RAW_ROOT / "country_iso_ILO_c188.csv")
    write_csv(REFERENCE_OUT / "country_iso_ILO_c188_cleaned.csv", fields, rows)

def clean_country_iso_eu():
    fields, rows = read_csv(RAW_ROOT / "country_iso_EU.csv")
    write_csv(REFERENCE_OUT / "country_iso_EU_cleaned.csv", fields, rows)


def clean_fao_major_areas():
    fields, rows = read_csv(RAW_ROOT / "fao_major_areas.csv")
    for row in rows:
        code = row.get("fao_major_area", "").replace(".0", "").strip()
        row["fao_major_area"] = code.zfill(2) if code else ""
    write_csv(REFERENCE_OUT / "fao_major_areas_cleaned.csv", fields, rows)


def clean_gear_types_fao():
    fields, rows = read_csv(RAW_ROOT / "gearTypes_fao.csv")
    cleaned_rows = []
    for row in rows:
        code = row.get("fao_isscfg_code", "").replace(".0", "").strip()
        name = row.get("fao_isscfg_name", "").strip()
        row["fao_isscfg_code"] = code.zfill(2) if code else ""
        row["fao_isscfg_alpha"] = row.get("fao_isscfg_alpha", "").strip()
        if name:
            row["fao_isscfg_name"] = name
            cleaned_rows.append(row)
    write_csv(REFERENCE_OUT / "gearTypes_fao_cleaned.csv", fields, cleaned_rows)


def clean_gear_types_cbp():
    fields, rows = read_csv(RAW_ROOT / "gearTypes_cbp.csv")
    write_csv(REFERENCE_OUT / "gearTypes_cbp_cleaned.csv", fields, rows)


def clean_gear_types_msc():
    fields, rows = read_csv(RAW_ROOT / "gearTypes_msc.csv")
    write_csv(REFERENCE_OUT / "cleaned_gear_types_msc.csv", fields, rows)


def clean_gear_relationship():
    _, rows = read_csv(RAW_ROOT / "gearTypes_fao_cbp_relationship.csv")
    relationships = []
    for row in rows:
        fao_code = row.get("fao_isscfg_code", "").replace(".0", "").strip()
        cbp_codes = row.get("cbp_gear_code", "")
        codes = [code.strip() for code in cbp_codes.split(';') if code.strip()]
        for code in codes:
            relationships.append({
                "fao_isscfg_code": fao_code.zfill(2) if fao_code else "",
                "cbp_gear_code": code
            })
    write_csv(REFERENCE_OUT / "gearTypes_relationship_fao_cbp_cleaned.csv",
              ["fao_isscfg_code", "cbp_gear_code"],
              relationships)


def clean_msc_relationship():
    fields, rows = read_csv(RAW_ROOT / "gearTypes_msc_fao_relationship.csv")
    write_csv(REFERENCE_OUT / "cleaned_gear_types_fao_msc_relationship.csv", fields, rows)


def clean_vessel_hull_material():
    fields, rows = read_csv(RAW_ROOT / "vessel_hullMaterial.csv")
    new_fields = ["hull_material" if f == "hullMaterial" else f for f in fields]
    cleaned = []
    for row in rows:
        entry = {}
        for field, value in row.items():
            key = "hull_material" if field == "hullMaterial" else field
            entry[key] = value
        cleaned.append(entry)
    write_csv(REFERENCE_OUT / "vessel_hullMaterial_cleaned.csv", new_fields, cleaned)


def clean_vessel_types():
    fields, rows = read_csv(RAW_ROOT / "vesselTypes.csv")
    mapping = {
        'vesselType_cat': 'vessel_type_cat',
        'vesselType_subcat': 'vessel_type_subcat',
        'vesselType_isscfv_code': 'vessel_type_isscfv_code',
        'vesselType_isscfv_alpha': 'vessel_type_isscfv_alpha'
    }
    new_fields = [mapping.get(f, f) for f in fields]
    cleaned = []
    for row in rows:
        entry = {}
        for field, value in row.items():
            key = mapping.get(field, field)
            if key == 'vessel_type_isscfv_code':
                val = value.replace('.0', '').strip()
                entry[key] = val.zfill(2) if val else ''
            else:
                entry[key] = value
        cleaned.append(entry)
    write_csv(REFERENCE_OUT / "vesselTypes_cleaned.csv", new_fields, cleaned)


def clean_rfmos():
    fields, rows = read_csv(RAW_ROOT / "rfmos.csv")
    write_csv(REFERENCE_OUT / "rfmos_cleaned.csv", fields, rows)


def clean_original_sources():
    fields, rows = read_csv(RAW_ROOT / "original_sources.csv")
    cleaned = []
    for row in rows:
        row['source_type'] = row.get('source_type', '').replace(';', '; ').strip()
        cleaned.append(row)
    write_csv(REFERENCE_OUT / "cleaned_original_sources.csv", fields, cleaned)


def main():
    print("🚀 Cleaning reference datasets (lightweight mode)...")
    clean_country_iso()
    clean_country_iso_foc()
    clean_country_iso_ilo()
    clean_country_iso_eu()
    clean_fao_major_areas()
    clean_gear_types_fao()
    clean_gear_types_cbp()
    clean_gear_types_msc()
    clean_gear_relationship()
    clean_msc_relationship()
    clean_vessel_hull_material()
    clean_vessel_types()
    clean_rfmos()
    clean_original_sources()
    print(f"✅ Outputs written to {REFERENCE_OUT}")


if __name__ == "__main__":
    main()
