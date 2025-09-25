#!/usr/bin/env python3
"""
Robust Bulgaria CSV cleaner
Handles Cyrillic text, quote issues, and field count problems
"""
import sys
import re
from pathlib import Path

def fix_bgr_quotes(line):
    """
    Fix quote issues in Bulgarian vessel names and ports
    """
    # The specific pattern mentioned in error: СВ",НИКОЛА
    line = line.replace('СВ",НИКОЛА', 'СВ,НИКОЛА')
    
    # General pattern for quotes before commas in Cyrillic text
    # Pattern: Cyrillic text", more text
    pattern = r'([А-Яа-я\s]+)",([ ][А-Яа-я\s]+)'
    line = re.sub(pattern, r'\1,\2', line)
    
    # Also handle quotes within fields more generally
    # Look for patterns where quotes appear mid-field (not at semicolon boundaries)
    pattern2 = r'([^;]+)"([А-Яа-я\s,]+)'
    
    # But only if it's not a proper quote boundary
    def replace_if_not_boundary(match):
        before = match.group(1)
        after = match.group(2)
        # If the quote is at the start of a field after semicolon, keep it
        if before.endswith(';'):
            return match.group(0)
        # Otherwise remove the quote
        return before + after
    
    line = re.sub(pattern2, replace_if_not_boundary, line)
    
    return line

def clean_bgr_robust(input_file, output_file):
    """
    Robust cleaning of Bulgarian vessel data
    """
    print(f"Robust cleaning of BGR vessels: {input_file}")
    
    cleaned_lines = []
    fixes_log = []
    quote_patterns = set()
    
    with open(input_file, 'r', encoding='utf-8', errors='replace') as f:
        lines = [line.rstrip('\n\r') for line in f]
    
    # Process header
    header = lines[0]
    # EU Fleet Register should have 40 fields
    expected_fields = 40
    header_fields = header.count(';') + 1
    
    if header_fields == 41:
        # Bulgaria might have an extra field - let's check
        print(f"Header has {header_fields} fields, expected {expected_fields}")
        print("Checking if we need to adjust...")
        # Keep the header as is for now
        cleaned_lines.append(header)
        actual_fields = header_fields
    else:
        cleaned_lines.append(header)
        actual_fields = expected_fields
    
    print(f"Processing {len(lines)-1} data lines")
    
    # Process data lines
    for line_num, line in enumerate(lines[1:], start=2):
        if not line.strip():
            continue
        
        original = line
        
        # First fix quotes
        line = fix_bgr_quotes(line)
        
        if line != original:
            # Find what changed for logging
            if 'СВ",НИКОЛА' in original:
                quote_patterns.add('СВ",НИКОЛА')
            # Find other Cyrillic quote patterns
            cyrillic_quotes = re.findall(r'([А-Яа-я]+)"([А-Яа-я,\s]+)', original)
            for pat in cyrillic_quotes:
                quote_patterns.add(f'{pat[0]}"{pat[1][:20]}...')
            
            fixes_log.append((line_num, "Fixed Cyrillic quote issue"))
        
        # Check field count
        field_count = line.count(';') + 1
        
        if field_count < actual_fields:
            # Add missing fields
            missing = actual_fields - field_count
            line = line + (';' * missing)
            fixes_log.append((line_num, f"Added {missing} empty fields"))
        elif field_count > actual_fields:
            # Too many fields - likely due to unescaped semicolons
            # Try to merge fields that might have been split
            parts = line.split(';')
            
            # If we have 41 fields and expect 40, we might need to merge
            if len(parts) == 41 and actual_fields == 40:
                # Common issue: vessel name or port name contains semicolon
                # Try merging fields that look like they belong together
                # This is a heuristic - might need adjustment
                merged_parts = parts[:40]  # Keep first 40
                fixes_log.append((line_num, f"Truncated from {len(parts)} to {actual_fields} fields"))
                line = ';'.join(merged_parts)
            else:
                fixes_log.append((line_num, f"Field count issue: {field_count} vs {actual_fields}"))
        
        cleaned_lines.append(line)
    
    # Write cleaned data
    with open(output_file, 'w', encoding='utf-8') as f:
        for line in cleaned_lines:
            f.write(line + '\n')
    
    print(f"\nCleaning complete:")
    print(f"  Total lines: {len(cleaned_lines)}")
    print(f"  Lines with fixes: {len(set(ln for ln, _ in fixes_log))}")
    
    if quote_patterns:
        print(f"\nCyrillic quote patterns fixed:")
        for pat in sorted(quote_patterns)[:10]:
            print(f"  - {pat}")
    
    # Show sample fixes
    if fixes_log:
        print(f"\nSample fixes:")
        shown_lines = set()
        count = 0
        for line_num, fix in fixes_log:
            if line_num not in shown_lines and count < 15:
                print(f"  Line {line_num}: {fix}")
                shown_lines.add(line_num)
                count += 1
        if len(fixes_log) > 15:
            print(f"  ... and more")
    
    return len(cleaned_lines) - 1

def verify_cleaned_bgr(cleaned_file):
    """
    Verify the cleaned Bulgarian file
    """
    print(f"\nVerifying cleaned file...")
    issues = []
    
    with open(cleaned_file, 'r', encoding='utf-8') as f:
        lines = [line.rstrip('\n\r') for line in f]
    
    # Check header
    header_fields = lines[0].count(';') + 1
    print(f"Header has {header_fields} fields")
    
    # Check data lines
    for line_num, line in enumerate(lines[1:], start=2):
        if not line.strip():
            continue
            
        field_count = line.count(';') + 1
        
        if field_count != header_fields:
            issues.append((line_num, f"Field count: {field_count} vs header: {header_fields}"))
        
        # Check for remaining quote issues
        if re.search(r'[А-Яа-я]"[А-Яа-я]', line):
            issues.append((line_num, "Still contains Cyrillic quote pattern"))
    
    if issues:
        print(f"⚠️  Found {len(issues)} potential issues:")
        for line_num, issue in issues[:10]:
            print(f"  Line {line_num}: {issue}")
    else:
        print("✓ File appears clean!")
    
    return len(issues) == 0

def main():
    input_file = Path("/import/vessels/vessel_data/COUNTRY/EU_BGR/raw/BGR_vessels_2025-09-08.csv")
    output_file = Path("/import/vessels/vessel_data/COUNTRY/EU_BGR/cleaned/BGR_vessels_cleaned.csv")
    
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        return 1
    
    try:
        records = clean_bgr_robust(input_file, output_file)
        print(f"\n✓ Successfully processed {records} BGR vessel records")
        
        # Verify
        verify_cleaned_bgr(output_file)
            
        return 0
    except Exception as e:
        print(f"\n✗ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())