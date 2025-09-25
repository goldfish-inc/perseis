#!/usr/bin/env python3
"""
Clean Denmark (DNK) vessel CSV file - Version 2
More robust handling of the specific Korshavn issue
"""
import sys
import re
from pathlib import Path

def clean_dnk_vessels(input_file, output_file):
    """
    Clean DNK vessels CSV file with specific fixes
    """
    print(f"Cleaning DNK vessels file: {input_file}")
    
    cleaned_lines = []
    fixes_applied = []
    
    with open(input_file, 'r', encoding='utf-8', errors='replace') as f:
        for line_num, line in enumerate(f, 1):
            original_line = line.rstrip('\n\r')
            cleaned_line = original_line
            
            # Fix the specific Korshavn pattern
            # Pattern: ;Korshavn", V. Fyns Hoved;
            if 'Korshavn", V. Fyns Hoved' in cleaned_line:
                cleaned_line = cleaned_line.replace('Korshavn", V. Fyns Hoved', 'Korshavn, V. Fyns Hoved')
                fixes_applied.append((line_num, "Fixed Korshavn quote issue"))
            
            # Fix other similar patterns where a quote appears before a comma
            # Pattern: sometext", moretext within a field
            # This is tricky because we need to identify it's within a field, not at field boundary
            
            # Count semicolons to check if we have the right number of fields
            semicolon_count = cleaned_line.count(';')
            if line_num == 1:
                expected_semicolons = semicolon_count  # Header determines field count
            elif semicolon_count != expected_semicolons:
                # Try to fix by looking for patterns like: ;"text", text;
                # Replace with: ;"text, text;
                pattern = r';([^;]+)", ([^;]+);'
                matches = re.findall(pattern, cleaned_line)
                for match in matches:
                    old_text = f';{match[0]}", {match[1]};'
                    new_text = f';{match[0]}, {match[1]};'
                    cleaned_line = cleaned_line.replace(old_text, new_text)
                    fixes_applied.append((line_num, f"Fixed embedded quote in: {match[0]}"))
            
            cleaned_lines.append(cleaned_line)
    
    # Write cleaned data
    with open(output_file, 'w', encoding='utf-8') as f:
        for line in cleaned_lines:
            f.write(line + '\n')
    
    print(f"\nCleaning complete:")
    print(f"  Total lines: {len(cleaned_lines)}")
    print(f"  Fixes applied: {len(fixes_applied)}")
    
    if fixes_applied:
        print(f"\nFixes applied:")
        for line_num, fix in fixes_applied[:10]:
            print(f"  Line {line_num}: {fix}")
        if len(fixes_applied) > 10:
            print(f"  ... and {len(fixes_applied) - 10} more")
    
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
        records = clean_dnk_vessels(input_file, output_file)
        print(f"\n✓ Successfully cleaned {records} DNK vessel records")
        return 0
    except Exception as e:
        print(f"\n✗ Error cleaning DNK vessels: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())