#!/usr/bin/env python3
"""
Vessel PDF Processor using Granite-Docling
Processes vessel registry PDFs using docling-granite for high-quality extraction
and converts them to standardized CSV format for the Ebisu intelligence platform.
"""
import csv
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Any

RAW_ROOT = Path(os.environ.get("EBISU_RAW_ROOT", "data/raw")).expanduser().resolve()
PROCESSED_ROOT = Path(os.environ.get("EBISU_PROCESSED_ROOT", RAW_ROOT)).expanduser().resolve()

def extract_with_docling(pdf_path: Path, output_dir: Path) -> Dict[str, Path]:
    """Extract PDF content using docling-granite with multiple formats"""
    print(f"🔍 Extracting {pdf_path.name} using docling-granite...")

    # Create temporary output directory
    temp_dir = output_dir / "temp_docling"
    temp_dir.mkdir(exist_ok=True)

    try:
        # Extract with JSON and Markdown formats for structured data
        cmd = ["doc-extract", str(pdf_path), "--json"]
        result = subprocess.run(cmd, cwd=temp_dir, capture_output=True, text=True, timeout=120)

        if result.returncode != 0:
            print(f"   ❌ docling extraction failed: {result.stderr}")
            return {}

        print(f"   ✅ docling extraction successful")

        # Find generated files (docling outputs to ~/Documents/extracted/ by default)
        extracted_dir = Path.home() / "Documents" / "extracted"
        base_name = pdf_path.stem

        files = {}
        for suffix in [".json", ".md"]:
            potential_file = extracted_dir / f"{base_name}{suffix}"
            if potential_file.exists():
                # Move to our temp directory
                target = temp_dir / f"{base_name}{suffix}"
                potential_file.rename(target)
                files[suffix[1:]] = target

        return files

    except subprocess.TimeoutExpired:
        print(f"   ⏱️ docling extraction timed out for {pdf_path.name}")
        return {}
    except Exception as e:
        print(f"   ❌ docling extraction error: {e}")
        return {}

def extract_tables_with_docling(pdf_path: Path) -> Optional[str]:
    """Extract tables specifically using doc-query tables"""
    print(f"🔍 Extracting tables from {pdf_path.name}...")

    try:
        cmd = ["doc-query", str(pdf_path), "tables"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)

        if result.returncode == 0 and result.stdout.strip():
            print(f"   ✅ Tables extracted successfully")
            return result.stdout
        else:
            print(f"   ⚠️ No tables found or extraction failed")
            return None

    except Exception as e:
        print(f"   ❌ Table extraction error: {e}")
        return None

def parse_vessel_data_from_json(json_path: Path, pdf_source: str) -> List[Dict[str, str]]:
    """Parse vessel data from docling JSON output"""
    try:
        with json_path.open('r', encoding='utf-8') as f:
            data = json.load(f)

        vessels = []

        # Docling JSON structure varies, try to find tabular data
        if 'tables' in data:
            for table in data['tables']:
                vessels.extend(parse_table_data(table, pdf_source))
        elif 'content' in data:
            vessels.extend(parse_content_for_vessels(data['content'], pdf_source))
        else:
            # Fallback: look for any structured data
            vessels.extend(extract_vessels_from_text(str(data), pdf_source))

        return vessels

    except Exception as e:
        print(f"   ❌ JSON parsing error: {e}")
        return []

def parse_vessel_data_from_markdown(md_path: Path, pdf_source: str) -> List[Dict[str, str]]:
    """Parse vessel data from docling Markdown output"""
    try:
        with md_path.open('r', encoding='utf-8') as f:
            content = f.read()

        vessels = []

        # Look for markdown tables
        table_pattern = r'\|.*\|.*\n\|[-\s\|]*\n((?:\|.*\|.*\n)*)'
        tables = re.findall(table_pattern, content)

        for table_content in tables:
            vessels.extend(parse_markdown_table(table_content, pdf_source))

        # If no tables found, try to extract from text
        if not vessels:
            vessels.extend(extract_vessels_from_text(content, pdf_source))

        return vessels

    except Exception as e:
        print(f"   ❌ Markdown parsing error: {e}")
        return []

def parse_table_data(table_data: Any, pdf_source: str) -> List[Dict[str, str]]:
    """Parse tabular data structure from docling"""
    vessels = []

    try:
        if isinstance(table_data, dict):
            # Try to find headers and rows
            headers = table_data.get('headers', [])
            rows = table_data.get('rows', [])

            if headers and rows:
                for row in rows:
                    vessel = {'pdf_source': pdf_source}
                    for i, value in enumerate(row):
                        if i < len(headers):
                            header = str(headers[i]).lower().replace(' ', '_')
                            vessel[header] = str(value).strip() if value else ""

                    if any(vessel.get(field, '') for field in ['name', 'vessel_name', 'ship_name']):
                        vessels.append(vessel)

    except Exception as e:
        print(f"   ⚠️ Table parsing error: {e}")

    return vessels

def parse_content_for_vessels(content: Any, pdf_source: str) -> List[Dict[str, str]]:
    """Parse general content structure for vessel information"""
    vessels = []

    # This is a flexible parser that looks for vessel-like patterns
    text = str(content)
    vessels.extend(extract_vessels_from_text(text, pdf_source))

    return vessels

def parse_markdown_table(table_content: str, pdf_source: str) -> List[Dict[str, str]]:
    """Parse markdown table format"""
    vessels = []

    lines = table_content.strip().split('\n')
    if not lines:
        return vessels

    # First line might be headers
    header_line = lines[0] if lines else ""
    headers = [h.strip() for h in header_line.split('|') if h.strip()]

    for line in lines[1:]:  # Skip potential separator line
        if '|' in line:
            values = [v.strip() for v in line.split('|') if v.strip()]

            if len(values) >= len(headers) * 0.5:  # At least half the headers filled
                vessel = {'pdf_source': pdf_source}
                for i, value in enumerate(values):
                    if i < len(headers):
                        header = headers[i].lower().replace(' ', '_')
                        vessel[header] = value

                if any(vessel.get(field, '') for field in ['name', 'vessel_name', 'ship_name']):
                    vessels.append(vessel)

    return vessels

def extract_vessels_from_text(text: str, pdf_source: str) -> List[Dict[str, str]]:
    """Extract vessel information from free text using patterns"""
    vessels = []

    # Common vessel data patterns
    patterns = {
        'vessel_name': r'(?:vessel|ship|boat)\s+name[:\s]+([^\n,]+)',
        'flag': r'flag[:\s]+([^\n,]+)',
        'imo': r'imo[:\s]+(\d+)',
        'call_sign': r'call[_\s]sign[:\s]+([A-Z0-9]+)',
        'registration': r'registration[:\s]+([^\n,]+)',
    }

    # Try to find structured vessel entries
    text_lower = text.lower()

    for pattern_name, regex in patterns.items():
        matches = re.findall(regex, text_lower, re.IGNORECASE)
        if matches:
            # Create minimal vessel records
            for match in matches[:50]:  # Limit to reasonable number
                vessel = {
                    'pdf_source': pdf_source,
                    pattern_name: match.strip(),
                    'extraction_method': 'text_pattern'
                }
                vessels.append(vessel)

    return vessels

def determine_vessel_type_and_source(pdf_path: Path) -> tuple[str, str]:
    """Determine vessel type and source based on file path and name"""
    path_parts = pdf_path.parts
    filename = pdf_path.stem.upper()

    # Determine vessel type from path
    if 'RFMO' in path_parts:
        vessel_type = 'RFMO'
    elif 'COUNTRY' in path_parts:
        vessel_type = 'COUNTRY'
    elif 'INTERGOV' in path_parts:
        vessel_type = 'INTERGOV'
    else:
        vessel_type = 'MISC'

    # Determine source from filename
    if 'SEAFO' in filename:
        source = 'SEAFO'
    elif 'TWN' in filename:
        if 'SIOFA' in filename:
            source = 'TWN_SIOFA'
        elif 'PACIFIC' in filename:
            source = 'TWN_PACIFIC'
        else:
            source = 'TWN'
    elif 'FRO' in filename:
        source = 'FAROE_ISLANDS'
    else:
        source = filename.split('_')[0]

    return vessel_type, source

def process_pdf(pdf_path: Path) -> bool:
    """Process a single PDF file"""
    print(f"\n📄 Processing: {pdf_path.name}")

    try:
        vessel_type, source = determine_vessel_type_and_source(pdf_path)

        # Create output directory
        output_base = PROCESSED_ROOT / "vessels" / vessel_type / "cleaned"
        output_base.mkdir(parents=True, exist_ok=True)

        temp_dir = output_base / "temp_pdf_processing"
        temp_dir.mkdir(exist_ok=True)

        # Extract content using docling
        extracted_files = extract_with_docling(pdf_path, temp_dir)

        if not extracted_files:
            print(f"   ❌ No content extracted from {pdf_path.name}")
            return False

        # Parse vessel data from extracted files
        all_vessels = []

        if 'json' in extracted_files:
            vessels = parse_vessel_data_from_json(extracted_files['json'], pdf_path.name)
            all_vessels.extend(vessels)

        if 'md' in extracted_files:
            vessels = parse_vessel_data_from_markdown(extracted_files['md'], pdf_path.name)
            all_vessels.extend(vessels)

        # Also try table-specific extraction
        table_text = extract_tables_with_docling(pdf_path)
        if table_text:
            table_vessels = extract_vessels_from_text(table_text, pdf_path.name)
            all_vessels.extend(table_vessels)

        # Clean up duplicates and standardize
        cleaned_vessels = []
        seen_vessels = set()

        for vessel in all_vessels:
            # Create a simple key for deduplication
            key = (vessel.get('vessel_name', ''), vessel.get('imo', ''), vessel.get('call_sign', ''))
            if key not in seen_vessels and any(key):
                seen_vessels.add(key)

                # Standardize column names
                cleaned_vessel = {'pdf_source': vessel.get('pdf_source', '')}
                for k, v in vessel.items():
                    if k != 'pdf_source':
                        clean_key = k.lower().replace(' ', '_').replace('(', '').replace(')', '')
                        cleaned_vessel[clean_key] = str(v).strip() if v else ""

                cleaned_vessels.append(cleaned_vessel)

        if not cleaned_vessels:
            print(f"   ⚠️ No vessel data found in {pdf_path.name}")
            return False

        # Write to CSV
        output_file = output_base / f"{source.lower()}_vessels_cleaned.csv"

        if cleaned_vessels:
            fieldnames = set()
            for vessel in cleaned_vessels:
                fieldnames.update(vessel.keys())
            fieldnames = sorted(list(fieldnames))

            with output_file.open('w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(cleaned_vessels)

        # Clean up temp files
        for temp_file in extracted_files.values():
            if temp_file.exists():
                temp_file.unlink()
        if temp_dir.exists():
            temp_dir.rmdir()

        print(f"✅ Processed {len(cleaned_vessels)} vessels → {output_file.name}")
        return True

    except Exception as e:
        print(f"❌ Failed to process {pdf_path.name}: {e}")
        return False

def main():
    """Process all vessel PDFs"""
    print("📄 Processing vessel PDFs with docling-granite...")

    # Find all PDF files
    pdf_files = []
    pdf_files.extend(RAW_ROOT.rglob("*.pdf"))

    if not pdf_files:
        print("⚠️ No PDF files found")
        return

    print(f"Found {len(pdf_files)} PDF files to process")

    successful = 0
    failed = 0

    for pdf_file in sorted(pdf_files):
        if process_pdf(pdf_file):
            successful += 1
        else:
            failed += 1

    print(f"\n📊 PDF Processing Complete:")
    print(f"   ✅ Successful: {successful}")
    print(f"   ❌ Failed: {failed}")
    print(f"   📁 Output: {PROCESSED_ROOT}/vessels/[TYPE]/cleaned/")

if __name__ == "__main__":
    main()