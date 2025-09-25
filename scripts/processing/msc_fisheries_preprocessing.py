#!/usr/bin/env python3
"""
Fixed MSC fishery preprocessing - removes source_id handling and normalizes enum values
SQL script will handle source_id lookup directly
"""

import pandas as pd
import re
import sys
from typing import List

# EXACT column mapping for your CSV structure
COLUMN_MAPPING = {
    'Fishery Name': 'msc_fishery_name',
    'MSC Status': 'msc_fishery_status', 
    'Status (Unit of Certification)': 'msc_fishery_status_uoc',
    'Species': 'scientific_names',
    'Gear Type': 'msc_gear',
    'Ocean Area': 'fao_areas',
    'Certificate Code': 'msc_fishery_cert_codes'
}

# Enum value normalization mappings
MSC_FISHERY_STATUS_MAPPING = {
    'certified': 'CERTIFIED',
    'certified with unit(s) in assessment': 'CERTIFIED WITH UNIT(S) IN ASSESSMENT',
    'combined with another assessment': 'COMBINED WITH ANOTHER ASSESSMENT',
    'improvement program': 'IMPROVEMENT PROGRAM',
    'in assessment': 'IN ASSESSMENT',
    'not certified': 'NOT CERTIFIED',
    'suspended': 'SUSPENDED',
    'withdrawn': 'WITHDRAWN'
}

MSC_FISHERY_STATUS_UOC_MAPPING = {
    'certified': 'CERTIFIED',
    'improvement program': 'IMPROVEMENT PROGRAM',
    'in assessment': 'IN ASSESSMENT',
    'not certified': 'NOT CERTIFIED',
    'suspended': 'SUSPENDED',
    'withdrawn': 'WITHDRAWN'
}

def normalize_enum_value(value: str, mapping: dict) -> str:
    """Normalize enum values to match PostgreSQL enum definitions"""
    if not value or pd.isna(value):
        return None
    
    # Convert to lowercase for comparison
    value_lower = str(value).strip().lower()
    
    # Try exact match first
    if value_lower in mapping:
        return mapping[value_lower]
    
    # Try partial matches for variations
    for key, mapped_value in mapping.items():
        if value_lower in key or key in value_lower:
            return mapped_value
    
    # If no match found, return original value (uppercase)
    return str(value).strip().upper()

def clean_scientific_names(species_text: str) -> List[str]:
    """Clean and extract scientific names from MSC species text"""
    if not species_text or pd.isna(species_text):
        return []
    
    cleaned_names = []
    species_text = str(species_text).strip()
    
    # Handle multiple species separated by various delimiters
    separators = [';', '|', ',', ' and ', ' & ']
    species_list = [species_text]
    
    for sep in separators:
        temp_list = []
        for item in species_list:
            temp_list.extend(item.split(sep))
        species_list = temp_list
    
    for species in species_list:
        species = species.strip()
        if not species:
            continue
            
        # Remove common prefixes like "Longfin squid" before scientific name
        scientific_part_match = re.search(r'\(([^)]+)\)$', species)
        if scientific_part_match:
            species = scientific_part_match.group(1)
        
        # Handle complex nested parentheses like "Penaeus (Melicertus) latisulcatus"
        if '(' in species and ')' in species:
            nested_match = re.match(r'^([A-Z][a-z]+)\s*\(([A-Z][a-z]+)\)\s*(.+)$', species)
            if nested_match:
                genus1 = nested_match.group(1)
                genus2 = nested_match.group(2) 
                species_part = nested_match.group(3).strip()
                cleaned_names.extend([f"{genus1} {species_part}", f"{genus2} {species_part}"])
            else:
                cleaned_name = re.sub(r'\s*\([^)]+\)\s*', ' ', species)
                cleaned_names.append(cleaned_name.strip())
        else:
            cleaned_names.append(species)
    
    # Final cleaning
    final_names = []
    for name in cleaned_names:
        if not name:
            continue
            
        words = name.strip().split()
        if len(words) < 1:
            continue
            
        # Remove "spp" or "sp" if it's the second word
        if len(words) >= 2 and words[1].lower() in ['spp', 'sp', 'spp.', 'sp.']:
            cleaned_name = words[0]
        else:
            cleaned_name = ' '.join(words)
        
        cleaned_name = re.sub(r'\s+', ' ', cleaned_name).strip()
        
        # Only keep names that look like valid scientific names
        if cleaned_name and re.match(r'^[A-Z][a-z]+', cleaned_name):
            final_names.append(cleaned_name)
    
    return list(set(final_names))

def clean_fao_areas(fao_text: str) -> List[str]:
    """Clean and normalize FAO area codes from Ocean Area field"""
    if not fao_text or pd.isna(fao_text):
        return []
    
    # Handle various separators and extract numeric FAO codes
    fao_text = str(fao_text)
    
    # Extract FAO area numbers using regex
    fao_numbers = re.findall(r'\b\d{1,2}\b', fao_text)
    
    cleaned_areas = []
    for area in fao_numbers:
        # Zero-pad single digits
        if len(area) == 1:
            area = f"0{area}"
        cleaned_areas.append(area)
    
    # If no numbers found, try to extract from common area names
    if not cleaned_areas:
        area_mappings = {
            'atlantic': ['21', '27', '31', '34', '37', '41', '47'],
            'pacific': ['61', '67', '71', '77', '81', '87'],
            'indian': ['51', '57'],
            'mediterranean': ['37'],
            'north sea': ['27'],
            'baltic': ['27']
        }
        
        fao_lower = fao_text.lower()
        for area_name, codes in area_mappings.items():
            if area_name in fao_lower:
                cleaned_areas.extend(codes)
                break
    
    return list(set(cleaned_areas))

def clean_cert_codes(cert_text: str) -> List[str]:
    """Clean and normalize MSC certification codes - extract all codes from parentheses and commas"""
    if not cert_text or pd.isna(cert_text):
        return []
    
    cert_text = str(cert_text).strip()
    if not cert_text:
        return []
    
    all_codes = []
    
    # First, split by common separators like semicolons, pipes, 'and', '&'
    separators = [';', '|', ' and ', ' & ']
    code_groups = [cert_text]
    
    for sep in separators:
        temp_groups = []
        for group in code_groups:
            temp_groups.extend(group.split(sep))
        code_groups = temp_groups
    
    # Now process each group to extract codes from parentheses and clean them
    for group in code_groups:
        group = group.strip()
        if not group:
            continue
            
        # Extract codes from parentheses (both inside and outside)
        # Examples: "MSC-F-31213 (MRAG-F-0022)" -> ["MSC-F-31213", "MRAG-F-0022"]
        
        # Find all text in parentheses
        import re
        parentheses_matches = re.findall(r'\(([^)]+)\)', group)
        
        # Get text outside parentheses (remove everything in parentheses)
        outside_parens = re.sub(r'\s*\([^)]+\)\s*', '', group).strip()
        
        # Add the main code (outside parentheses) if it exists
        if outside_parens:
            # Split by commas in case there are multiple codes outside parens
            for code in outside_parens.split(','):
                code = code.strip()
                if code and re.match(r'^[A-Z0-9\-]+', code):  # Basic validation
                    all_codes.append(code)
        
        # Add codes found inside parentheses
        for paren_content in parentheses_matches:
            # Split by commas in case there are multiple codes in one parenthesis
            for code in paren_content.split(','):
                code = code.strip()
                if code and re.match(r'^[A-Z0-9\-]+', code):  # Basic validation
                    all_codes.append(code)
    
    # Remove duplicates while preserving order
    seen = set()
    unique_codes = []
    for code in all_codes:
        if code not in seen:
            seen.add(code)
            unique_codes.append(code)
    
    return unique_codes

def truncate_long_text(text: str, max_length: int = 500) -> str:
    """Truncate very long fishery names to fit database constraints"""
    if not text or pd.isna(text):
        return ""
    
    text = str(text).strip()
    if len(text) <= max_length:
        return text
    
    # Truncate at word boundary
    truncated = text[:max_length-3]
    last_space = truncated.rfind(' ')
    
    if last_space > max_length * 0.8:
        return truncated[:last_space] + "..."
    else:
        return truncated + "..."

def process_msc_fishery_data(input_file: str, output_file: str):
    """Process MSC fishery data with exact column mapping and enum normalization"""
    print(f"Processing MSC fishery data: {input_file}")
    
    # Read the data
    df = pd.read_csv(input_file, encoding='utf-8')
    print(f"Loaded {len(df)} records")
    
    # Verify expected columns exist
    missing_columns = []
    for original_col in COLUMN_MAPPING.keys():
        if original_col not in df.columns:
            missing_columns.append(original_col)
    
    if missing_columns:
        print(f"ERROR: Missing expected columns: {missing_columns}")
        print(f"Available columns: {list(df.columns)}")
        sys.exit(1)
    
    print("Column mapping:")
    for orig, new in COLUMN_MAPPING.items():
        print(f"  '{orig}' -> '{new}'")
    
    processed_rows = []
    long_names = 0
    enum_normalizations = 0
    
    for idx, row in df.iterrows():
        new_row = {}
        
        # Process each column with the exact mapping
        
        # Fishery Name (handle very long names)
        fishery_name = row['Fishery Name']
        if pd.notna(fishery_name):
            original_length = len(str(fishery_name))
            fishery_name = truncate_long_text(str(fishery_name), 500)
            if original_length > 500:
                long_names += 1
            new_row['msc_fishery_name'] = fishery_name
        
        # MSC Status (normalize enum values)
        status = row['MSC Status']
        if pd.notna(status):
            original_status = str(status).strip()
            normalized_status = normalize_enum_value(original_status, MSC_FISHERY_STATUS_MAPPING)
            if normalized_status != original_status.upper():
                enum_normalizations += 1
            new_row['msc_fishery_status'] = normalized_status
        
        # Status (Unit of Certification) (normalize enum values)
        uoc_status = row['Status (Unit of Certification)']
        if pd.notna(uoc_status):
            original_uoc = str(uoc_status).strip()
            normalized_uoc = normalize_enum_value(original_uoc, MSC_FISHERY_STATUS_UOC_MAPPING)
            if normalized_uoc != original_uoc.upper():
                enum_normalizations += 1
            new_row['msc_fishery_status_uoc'] = normalized_uoc
        
        # Species
        species = row['Species']
        if pd.notna(species):
            cleaned_names = clean_scientific_names(str(species))
            if cleaned_names:
                new_row['scientific_names'] = '|'.join(cleaned_names)
        
        # Gear Type
        gear = row['Gear Type']
        if pd.notna(gear):
            new_row['msc_gear'] = str(gear).strip()
        
        # Ocean Area -> FAO Areas
        ocean_area = row['Ocean Area']
        if pd.notna(ocean_area):
            cleaned_areas = clean_fao_areas(str(ocean_area))
            if cleaned_areas:
                new_row['fao_areas'] = '|'.join(cleaned_areas)
        
        # Certificate Code
        cert_code = row['Certificate Code']
        if pd.notna(cert_code):
            cleaned_codes = clean_cert_codes(str(cert_code))
            if cleaned_codes:
                new_row['msc_fishery_cert_codes'] = '|'.join(cleaned_codes)
        
        # Add timestamps (REMOVED source_id - SQL will handle it)
        new_row['created_at'] = pd.Timestamp.now()
        new_row['updated_at'] = pd.Timestamp.now()
        
        processed_rows.append(new_row)
    
    # Create output dataframe
    output_df = pd.DataFrame(processed_rows)
    
    # Remove rows with no fishery name
    initial_count = len(output_df)
    output_df = output_df[output_df['msc_fishery_name'].notna() & (output_df['msc_fishery_name'] != '')]
    final_count = len(output_df)
    
    if initial_count != final_count:
        print(f"Removed {initial_count - final_count} records with missing fishery names")
    
    if long_names > 0:
        print(f"Truncated {long_names} fishery names that exceeded 500 characters")
    
    if enum_normalizations > 0:
        print(f"Normalized {enum_normalizations} enum values to uppercase")
    
    # Save processed data
    output_df.to_csv(output_file, index=False)
    
    # Print processing statistics
    print(f"\nProcessing Results:")
    print(f"  Total records processed: {len(output_df)}")
    print(f"  Records with species: {len(output_df[output_df['scientific_names'].notna()])}")
    print(f"  Records with FAO areas: {len(output_df[output_df['fao_areas'].notna()])}")
    print(f"  Records with gear types: {len(output_df[output_df['msc_gear'].notna()])}")
    print(f"  Records with cert codes: {len(output_df[output_df['msc_fishery_cert_codes'].notna()])}")
    
    # Show sample enum values for debugging
    status_samples = output_df['msc_fishery_status'].dropna().unique()[:5]
    uoc_samples = output_df['msc_fishery_status_uoc'].dropna().unique()[:5]
    print(f"  Sample status values: {list(status_samples)}")
    print(f"  Sample UOC status values: {list(uoc_samples)}")
    
    return output_df

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 msc_fisheries_preprocessing.py input.csv output.csv")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    try:
        process_msc_fishery_data(input_file, output_file)
        print("MSC fishery preprocessing completed successfully!")
    except Exception as e:
        print(f"Error processing MSC fishery data: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)