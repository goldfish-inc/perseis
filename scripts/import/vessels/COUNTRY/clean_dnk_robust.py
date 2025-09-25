#!/usr/bin/env python3
"""
Robust Denmark CSV cleaner - finds ALL quote issues automatically
"""
import sys
import re
from pathlib import Path

def find_and_fix_quotes(line):
    """
    Find and fix ALL instances of quotes before commas in Danish place names
    """
    # Pattern: find any text", text pattern within semicolon-delimited fields
    # This regex captures: ;"text", text  or  ;text", text
    pattern = r'(;[^;]*)",([ ][^;]*)'
    
    # Replace all matches
    fixed_line = re.sub(pattern, r'\1,\2', line)
    
    # Count how many replacements were made
    replacements = len(re.findall(pattern, line))
    
    return fixed_line, replacements

def clean_dnk_robust(input_file, output_file):
    """
    Robust cleaning that automatically finds all quote issues
    """
    print(f"Robust cleaning of DNK vessels: {input_file}")
    
    cleaned_lines = []
    total_fixes = 0
    lines_with_fixes = 0
    quote_patterns_found = set()
    
    with open(input_file, 'r', encoding='utf-8', errors='replace') as f:
        lines = [line.rstrip('\n\r') for line in f]
    
    # Process header
    header = lines[0]
    expected_fields = header.count(';') + 1
    cleaned_lines.append(header)
    
    print(f"Processing {len(lines)-1} data lines with {expected_fields} expected fields")
    
    # Process each data line
    for line_num, line in enumerate(lines[1:], start=2):
        if not line.strip():
            continue
        
        # Find and fix quotes
        original_line = line
        line, replacements = find_and_fix_quotes(line)
        
        if replacements > 0:
            total_fixes += replacements
            lines_with_fixes += 1
            
            # Extract what patterns we found for reporting
            matches = re.findall(r'([^;]+)",[ ]([^;]+)', original_line)
            for match in matches:
                quote_patterns_found.add(f'{match[0]}", {match[1]}')
        
        # Ensure correct number of fields
        field_count = line.count(';') + 1
        if field_count < expected_fields:
            missing = expected_fields - field_count
            line = line + (';' * missing)
            total_fixes += 1
        
        cleaned_lines.append(line)
    
    # Write cleaned data
    with open(output_file, 'w', encoding='utf-8') as f:
        for line in cleaned_lines:
            f.write(line + '\n')
    
    print(f"\nCleaning complete:")
    print(f"  Total lines: {len(cleaned_lines)}")
    print(f"  Lines with quote fixes: {lines_with_fixes}")
    print(f"  Total fixes applied: {total_fixes}")
    
    if quote_patterns_found:
        print(f"\nUnique quote patterns found and fixed ({len(quote_patterns_found)}):")
        for i, pattern in enumerate(sorted(quote_patterns_found), 1):
            if i <= 20:  # Show first 20
                print(f"  {i}. {pattern}")
            elif i == 21:
                print(f"  ... and {len(quote_patterns_found) - 20} more patterns")
    
    return len(cleaned_lines) - 1

def verify_cleaned_file(cleaned_file, expected_fields=40):
    """
    Verify the cleaned file is valid
    """
    print(f"\nVerifying cleaned file...")
    issues = []
    
    with open(cleaned_file, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.rstrip('\n\r')
            field_count = line.count(';') + 1
            
            # Check field count
            if field_count != expected_fields:
                issues.append((line_num, f"Wrong field count: {field_count}"))
            
            # Check for remaining quote issues
            if '", ' in line:
                issues.append((line_num, "Still contains quote-comma pattern"))
    
    if issues:
        print(f"⚠️  Found {len(issues)} issues:")
        for line_num, issue in issues[:10]:
            print(f"  Line {line_num}: {issue}")
    else:
        print("✓ File appears clean!")
    
    return len(issues) == 0

def main():
    input_file = Path("/import/vessels/vessel_data/COUNTRY/EU_DNK/raw/DNK_vessels_2025-09-08.csv")
    output_file = Path("/import/vessels/vessel_data/COUNTRY/EU_DNK/cleaned/DNK_vessels_cleaned.csv")
    
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        return 1
    
    try:
        records = clean_dnk_robust(input_file, output_file)
        print(f"\n✓ Successfully processed {records} DNK vessel records")
        
        # Verify the output
        if verify_cleaned_file(output_file):
            print("\n✅ DNK vessels ready for import!")
        else:
            print("\n⚠️  There may still be issues with the file")
            
        return 0
    except Exception as e:
        print(f"\n✗ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())