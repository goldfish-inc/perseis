#!/usr/bin/env python3
"""
Final Denmark (DNK) vessel CSV cleaner
Handles quote issues AND missing fields
"""
import sys
import re
from pathlib import Path

def clean_dnk_final(input_file, output_file):
    """
    Final comprehensive cleaning of DNK vessels
    """
    print(f"Final cleaning of DNK vessels: {input_file}")
    
    cleaned_lines = []
    fixes_log = []
    
    with open(input_file, 'r', encoding='utf-8', errors='replace') as f:
        lines = [line.rstrip('\n\r') for line in f]
    
    # Process header
    header = lines[0]
    expected_fields = header.count(';') + 1
    cleaned_lines.append(header)
    
    print(f"Expected {expected_fields} fields per line")
    
    # Known problematic patterns
    quote_fixes = [
        ('Korshavn", V. Fyns Hoved', 'Korshavn, V. Fyns Hoved'),
        ('Østerby", Læsø', 'Østerby, Læsø'),
        ('Hadsund", Øster Hurup', 'Hadsund, Øster Hurup'),
        ('Nykøbing", Mors', 'Nykøbing, Mors'),
        ('Rønne", Bornholm', 'Rønne, Bornholm'),
        ('Thyborøn", Lemvig', 'Thyborøn, Lemvig'),
        ('Nexø", Bornholm', 'Nexø, Bornholm'),
        ('Nørre", Nissum', 'Nørre, Nissum'),
        ('Hvide", Sande', 'Hvide, Sande'),
        ('Strib", Middelfart', 'Strib, Middelfart'),
    ]
    
    # Process each data line
    for line_num, line in enumerate(lines[1:], start=2):
        if not line.strip():
            continue
        
        # Apply quote fixes
        for old_pat, new_pat in quote_fixes:
            if old_pat in line:
                line = line.replace(old_pat, new_pat)
                fixes_log.append((line_num, f"Fixed quote: {old_pat}"))
        
        # Count fields
        field_count = line.count(';') + 1
        
        # If missing fields, add empty fields at the end
        if field_count < expected_fields:
            missing = expected_fields - field_count
            line = line + (';' * missing)
            fixes_log.append((line_num, f"Added {missing} empty fields"))
        
        # If too many fields (shouldn't happen but just in case)
        elif field_count > expected_fields:
            # Try to fix by removing quotes that might be splitting fields
            parts = line.split(';')
            if len(parts) > expected_fields:
                # Truncate to expected number
                parts = parts[:expected_fields]
                line = ';'.join(parts)
                fixes_log.append((line_num, f"Truncated to {expected_fields} fields"))
        
        cleaned_lines.append(line)
    
    # Write cleaned data
    with open(output_file, 'w', encoding='utf-8') as f:
        for line in cleaned_lines:
            f.write(line + '\n')
    
    print(f"\nCleaning complete:")
    print(f"  Total lines: {len(cleaned_lines)}")
    print(f"  Lines fixed: {len(set(line_num for line_num, _ in fixes_log))}")
    
    # Show sample fixes
    if fixes_log:
        print(f"\nSample fixes (up to 20):")
        shown = set()
        count = 0
        for line_num, fix in fixes_log:
            if line_num not in shown and count < 20:
                print(f"  Line {line_num}: {fix}")
                shown.add(line_num)
                count += 1
        if len(fixes_log) > 20:
            print(f"  ... and more fixes")
    
    return len(cleaned_lines) - 1

def main():
    input_file = Path("/import/vessels/vessel_data/COUNTRY/EU_DNK/raw/DNK_vessels_2025-09-08.csv")
    output_file = Path("/import/vessels/vessel_data/COUNTRY/EU_DNK/cleaned/DNK_vessels_cleaned.csv")
    
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        return 1
    
    try:
        records = clean_dnk_final(input_file, output_file)
        print(f"\n✓ Successfully cleaned {records} DNK vessel records")
        return 0
    except Exception as e:
        print(f"\n✗ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())