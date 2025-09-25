#!/usr/bin/env python3
"""
Clean Denmark (DNK) vessel CSV file
Handles specific formatting issues found in Danish vessel registry data
"""
import csv
import sys
import re
from pathlib import Path

def clean_dnk_vessels(input_file, output_file):
    """
    Clean DNK vessels CSV file
    
    Known issues:
    - Embedded quotes in vessel/port names
    - Commas within quoted fields causing parsing errors
    - Line 1710: "Korshavn", V. Fyns Hoved needs proper escaping
    """
    
    print(f"Cleaning DNK vessels file: {input_file}")
    
    cleaned_rows = []
    problem_lines = []
    
    with open(input_file, 'r', encoding='utf-8', errors='replace') as f:
        # Read raw lines to handle problematic CSV
        lines = f.readlines()
        
    # Process header
    header_line = lines[0].strip()
    cleaned_rows.append(header_line)
    
    # Process data lines
    for line_num, line in enumerate(lines[1:], start=2):
        try:
            line = line.strip()
            if not line:
                continue
                
            # Fix specific known issues
            # Issue 1: Quote within quoted field like "Korshavn", V. Fyns Hoved
            if '", ' in line and line.count('"') % 2 != 0:
                # Find pattern like "sometext", moretext;
                pattern = r'"([^"]*)", ([^;]*);'
                def fix_quote(match):
                    return f'"{match.group(1)}, {match.group(2)}";'
                line = re.sub(pattern, fix_quote, line)
                problem_lines.append((line_num, "Fixed embedded quote in field"))
            
            # Issue 2: Unescaped quotes within fields
            # Count semicolons to ensure we have the right number of fields
            expected_fields = 41  # EU Fleet Register has 41 fields
            
            # Split carefully handling quoted fields
            # Use custom parsing for problematic lines
            if line.count(';') != expected_fields - 1:
                # Try to fix by properly handling quoted fields
                fields = []
                current_field = ""
                in_quotes = False
                
                for char in line:
                    if char == '"':
                        if in_quotes and len(current_field) > 0 and current_field[-1] == '"':
                            # Double quote within field - escape it
                            current_field += '"'
                        else:
                            in_quotes = not in_quotes
                            current_field += char
                    elif char == ';' and not in_quotes:
                        fields.append(current_field)
                        current_field = ""
                    else:
                        current_field += char
                
                if current_field:
                    fields.append(current_field)
                
                if len(fields) == expected_fields:
                    line = ';'.join(fields)
                    problem_lines.append((line_num, f"Reparsed line with {len(fields)} fields"))
            
            cleaned_rows.append(line)
            
        except Exception as e:
            print(f"Error on line {line_num}: {str(e)}")
            print(f"  Line content: {line[:100]}...")
            problem_lines.append((line_num, f"Skipped due to error: {str(e)}"))
            continue
    
    # Write cleaned data
    with open(output_file, 'w', encoding='utf-8') as f:
        for row in cleaned_rows:
            f.write(row + '\n')
    
    print(f"\nCleaning complete:")
    print(f"  Input lines: {len(lines)}")
    print(f"  Output lines: {len(cleaned_rows)}")
    print(f"  Problem lines fixed/skipped: {len(problem_lines)}")
    
    if problem_lines:
        print(f"\nProblematic lines addressed:")
        for line_num, issue in problem_lines[:10]:  # Show first 10
            print(f"  Line {line_num}: {issue}")
        if len(problem_lines) > 10:
            print(f"  ... and {len(problem_lines) - 10} more")
    
    return len(cleaned_rows) - 1  # Subtract header

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
        return 1

if __name__ == "__main__":
    sys.exit(main())