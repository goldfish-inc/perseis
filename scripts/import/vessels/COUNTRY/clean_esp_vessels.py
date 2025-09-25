#!/usr/bin/env python3
"""
Clean Spain (ESP) vessel CSV file
Handles specific formatting issues found in Spanish vessel registry data
"""
import csv
import sys
import re
from pathlib import Path

def clean_esp_vessels(input_file, output_file):
    """
    Clean ESP vessels CSV file
    
    Known issues:
    - Line 8359: Caleta Del Sebo", La G... - location name with embedded quotes
    - Spanish place names with quotes and commas
    - Special characters in Spanish text (ñ, á, é, etc.)
    """
    
    print(f"Cleaning ESP vessels file: {input_file}")
    
    cleaned_rows = []
    problem_lines = []
    
    # Read with UTF-8 to handle Spanish characters
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
            
            # Fix specific Spanish location patterns
            # Issue 1: "Caleta Del Sebo", La G... pattern
            if 'Caleta Del Sebo", La' in line:
                line = line.replace('Caleta Del Sebo", La', 'Caleta Del Sebo, La')
                problem_lines.append((line_num, "Fixed Caleta Del Sebo quote issue"))
            
            # Issue 2: General pattern for Spanish locations with articles
            # Pattern: SomeName", La/El/Los/Las
            spanish_article_pattern = r'([^;"]+)", (La|El|Los|Las|L\')\s'
            matches = re.findall(spanish_article_pattern, line)
            if matches:
                for place, article in matches:
                    old_pattern = f'{place}", {article}'
                    new_pattern = f'{place}, {article}'
                    line = line.replace(old_pattern, new_pattern)
                problem_lines.append((line_num, f"Fixed quote before Spanish article: {article}"))
            
            # Issue 3: Port names with embedded commas
            # Common pattern in Spanish ports: "Puerto de XXX", Bahía de YYY
            port_pattern = r'"(Puerto[^"]*)", (Bahía|Isla|Costa|Playa)'
            if re.search(port_pattern, line):
                line = re.sub(port_pattern, r'"\1, \2', line)
                problem_lines.append((line_num, "Fixed port name with embedded comma"))
            
            # Issue 4: Field count validation
            expected_fields = 41
            
            # Custom parsing for problematic lines
            fields = []
            current_field = ""
            in_quotes = False
            i = 0
            
            while i < len(line):
                char = line[i]
                
                if char == '"':
                    if in_quotes:
                        # Check if next char is semicolon (end of field)
                        if i + 1 < len(line) and line[i + 1] == ';':
                            in_quotes = False
                            current_field += char
                        elif i + 1 < len(line) and line[i + 1] == '"':
                            # Escaped quote
                            current_field += '""'
                            i += 1
                        else:
                            # Check if it's before a Spanish article or comma
                            if i + 2 < len(line) and line[i+1:i+3] == ', ':
                                # Remove the quote, it's likely misplaced
                                pass
                            else:
                                current_field += char
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
            
            # Reconstruct line if we got the right field count
            if len(fields) == expected_fields:
                line = ';'.join(fields)
            elif len(fields) != expected_fields:
                problem_lines.append((line_num, f"Field count: {len(fields)} vs expected {expected_fields}"))
            
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
        # Show more for Spain as it has many place name issues
        for line_num, issue in problem_lines[:20]:
            print(f"  Line {line_num}: {issue}")
        if len(problem_lines) > 20:
            print(f"  ... and {len(problem_lines) - 20} more")
    
    return len(cleaned_rows) - 1

def main():
    input_file = Path("/import/vessels/vessel_data/COUNTRY/EU_ESP/raw/ESP_vessels_2025-09-08.csv")
    output_file = Path("/import/vessels/vessel_data/COUNTRY/EU_ESP/cleaned/ESP_vessels_cleaned.csv")
    
    # Create output directory
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        return 1
    
    try:
        records = clean_esp_vessels(input_file, output_file)
        print(f"\n✓ Successfully cleaned {records} ESP vessel records")
        return 0
    except Exception as e:
        print(f"\n✗ Error cleaning ESP vessels: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())