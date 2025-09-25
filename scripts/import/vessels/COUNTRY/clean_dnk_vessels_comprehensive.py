#!/usr/bin/env python3
"""
Comprehensive Denmark (DNK) vessel CSV cleaner
Handles all quote issues within fields
"""
import sys
import re
import csv
from pathlib import Path

def fix_dnk_line(line, line_num, expected_fields=41):
    """
    Fix a single line of DNK data
    """
    fixes = []
    
    # First, handle specific known patterns
    patterns_to_fix = [
        ('Korshavn", V. Fyns Hoved', 'Korshavn, V. Fyns Hoved'),
        ('Østerby", Læsø', 'Østerby, Læsø'),
        ('Hadsund", Øster Hurup', 'Hadsund, Øster Hurup'),
        ('Nykøbing", Mors', 'Nykøbing, Mors'),
        ('Rønne", Bornholm', 'Rønne, Bornholm'),
        ('Thyborøn", Lemvig', 'Thyborøn, Lemvig'),
        ('Nexø", Bornholm', 'Nexø, Bornholm'),
    ]
    
    for old_pattern, new_pattern in patterns_to_fix:
        if old_pattern in line:
            line = line.replace(old_pattern, new_pattern)
            fixes.append(f"Fixed: {old_pattern}")
    
    # General pattern: find any quote before comma within fields
    # This regex looks for patterns like: ;sometext", moretext;
    # But we need to be careful not to break actual CSV structure
    
    # Split by semicolon but preserve quoted fields
    parts = []
    current = ""
    in_quotes = False
    i = 0
    
    while i < len(line):
        char = line[i]
        
        if char == '"':
            # Check if this is a field boundary or embedded quote
            if not in_quotes:
                # Starting a quoted field
                in_quotes = True
                current += char
            else:
                # Could be ending quote or embedded quote
                if i + 1 < len(line) and line[i + 1] == ';':
                    # This is end of quoted field
                    in_quotes = False
                    current += char
                elif i + 1 < len(line) and line[i + 1] == ',':
                    # This is likely an embedded quote before comma - skip it
                    fixes.append(f"Removed embedded quote at position {i}")
                else:
                    current += char
        elif char == ';' and not in_quotes:
            parts.append(current)
            current = ""
        else:
            current += char
        
        i += 1
    
    if current:
        parts.append(current)
    
    # Verify field count
    if len(parts) != expected_fields:
        fixes.append(f"Field count: {len(parts)} vs expected {expected_fields}")
    
    # Reconstruct line
    cleaned_line = ';'.join(parts)
    
    return cleaned_line, fixes

def clean_dnk_vessels_comprehensive(input_file, output_file):
    """
    Comprehensive cleaning of DNK vessels
    """
    print(f"Comprehensive cleaning of DNK vessels: {input_file}")
    
    all_fixes = []
    cleaned_lines = []
    
    with open(input_file, 'r', encoding='utf-8', errors='replace') as f:
        lines = f.readlines()
    
    # Process header to get field count
    header = lines[0].strip()
    expected_fields = header.count(';') + 1
    cleaned_lines.append(header)
    
    print(f"Expected fields per line: {expected_fields}")
    
    # Process each data line
    for line_num, line in enumerate(lines[1:], start=2):
        line = line.strip()
        if not line:
            continue
        
        cleaned_line, fixes = fix_dnk_line(line, line_num, expected_fields)
        
        if fixes:
            all_fixes.append((line_num, fixes))
        
        cleaned_lines.append(cleaned_line)
    
    # Write cleaned data
    with open(output_file, 'w', encoding='utf-8') as f:
        for line in cleaned_lines:
            f.write(line + '\n')
    
    print(f"\nCleaning complete:")
    print(f"  Total lines: {len(cleaned_lines)}")
    print(f"  Lines with fixes: {len(all_fixes)}")
    
    if all_fixes:
        print(f"\nSample fixes (first 20):")
        for line_num, fixes in all_fixes[:20]:
            print(f"  Line {line_num}: {', '.join(fixes)}")
        if len(all_fixes) > 20:
            print(f"  ... and {len(all_fixes) - 20} more")
    
    # Also write a summary of all unique patterns fixed
    unique_patterns = set()
    for _, fixes in all_fixes:
        for fix in fixes:
            if fix.startswith("Fixed:"):
                unique_patterns.add(fix)
    
    if unique_patterns:
        print(f"\nUnique patterns fixed:")
        for pattern in sorted(unique_patterns):
            print(f"  {pattern}")
    
    return len(cleaned_lines) - 1

def main():
    input_file = Path("/import/vessels/vessel_data/COUNTRY/EU_DNK/raw/DNK_vessels_2025-09-08.csv")
    output_file = Path("/import/vessels/vessel_data/COUNTRY/EU_DNK/cleaned/DNK_vessels_cleaned.csv")
    
    # Create output directory
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        return 1
    
    try:
        records = clean_dnk_vessels_comprehensive(input_file, output_file)
        print(f"\n✓ Successfully cleaned {records} DNK vessel records")
        return 0
    except Exception as e:
        print(f"\n✗ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())