#!/usr/bin/env python3
"""
Clean Bulgaria (BGR) vessel CSV file
Handles specific formatting issues found in Bulgarian vessel registry data
"""
import csv
import sys
import re
from pathlib import Path

def clean_bgr_vessels(input_file, output_file):
    """
    Clean BGR vessels CSV file
    
    Known issues:
    - Line 1767: СВ",НИКОЛА 79 - embedded quote in Cyrillic vessel name
    - Mixed encoding issues with Cyrillic characters
    - Unescaped quotes in vessel names
    """
    
    print(f"Cleaning BGR vessels file: {input_file}")
    
    cleaned_rows = []
    problem_lines = []
    
    # Read with UTF-8 encoding to handle Cyrillic
    with open(input_file, 'r', encoding='utf-8', errors='replace') as f:
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
            
            # Fix specific Bulgarian vessel name issues
            # Issue 1: СВ",НИКОЛА pattern - quote within vessel name
            if 'СВ",НИКОЛА' in line:
                line = line.replace('СВ",НИКОЛА', 'СВ,НИКОЛА')
                problem_lines.append((line_num, "Fixed СВ\",НИКОЛА quote issue"))
            
            # Issue 2: General pattern of quotes within Cyrillic names
            # Look for pattern like ABC"DEF where both are Cyrillic
            cyrillic_quote_pattern = r'([А-Яа-я]+)"([А-Яа-я]+)'
            if re.search(cyrillic_quote_pattern, line):
                line = re.sub(cyrillic_quote_pattern, r'\1\2', line)
                problem_lines.append((line_num, "Removed embedded quote in Cyrillic text"))
            
            # Issue 3: Check field count
            expected_fields = 41  # EU Fleet Register standard
            field_count = line.count(';') + 1
            
            if field_count != expected_fields:
                # Try to parse with custom logic
                fields = []
                current_field = ""
                in_quotes = False
                i = 0
                
                while i < len(line):
                    char = line[i]
                    
                    if char == '"':
                        if in_quotes:
                            # Check if it's a closing quote
                            if i + 1 < len(line) and line[i + 1] == ';':
                                in_quotes = False
                                current_field += char
                            elif i + 1 < len(line) and line[i + 1] == '"':
                                # Escaped quote
                                current_field += '""'
                                i += 1  # Skip next quote
                            else:
                                # Likely an embedded quote - remove it
                                problem_lines.append((line_num, "Removed unexpected quote"))
                        else:
                            in_quotes = True
                            current_field += char
                    elif char == ';' and not in_quotes:
                        fields.append(current_field)
                        current_field = ""
                    else:
                        current_field += char
                    
                    i += 1
                
                if current_field:
                    fields.append(current_field)
                
                if len(fields) == expected_fields:
                    line = ';'.join(fields)
                    problem_lines.append((line_num, f"Reparsed into {len(fields)} fields"))
                else:
                    problem_lines.append((line_num, f"Field count mismatch: {len(fields)} vs {expected_fields}"))
            
            cleaned_rows.append(line)
            
        except Exception as e:
            print(f"Error on line {line_num}: {str(e)}")
            print(f"  Line preview: {line[:100]}...")
            problem_lines.append((line_num, f"Skipped: {str(e)}"))
            continue
    
    # Write cleaned data
    with open(output_file, 'w', encoding='utf-8') as f:
        for row in cleaned_rows:
            f.write(row + '\n')
    
    print(f"\nCleaning complete:")
    print(f"  Input lines: {len(lines)}")
    print(f"  Output lines: {len(cleaned_rows)}")
    print(f"  Problem lines addressed: {len(problem_lines)}")
    
    if problem_lines:
        print(f"\nProblematic lines:")
        for line_num, issue in problem_lines[:10]:
            print(f"  Line {line_num}: {issue}")
        if len(problem_lines) > 10:
            print(f"  ... and {len(problem_lines) - 10} more")
    
    return len(cleaned_rows) - 1

def main():
    input_file = Path("/import/vessels/vessel_data/COUNTRY/EU_BGR/raw/BGR_vessels_2025-09-08.csv")
    output_file = Path("/import/vessels/vessel_data/COUNTRY/EU_BGR/cleaned/BGR_vessels_cleaned.csv")
    
    # Create output directory
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        return 1
    
    try:
        records = clean_bgr_vessels(input_file, output_file)
        print(f"\n✓ Successfully cleaned {records} BGR vessel records")
        return 0
    except Exception as e:
        print(f"\n✗ Error cleaning BGR vessels: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())