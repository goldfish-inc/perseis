#!/bin/bash
# /app/scripts/import/vessels/data/RFMO/clean_neafc_vessels.sh
# NEAFC Vessel Data Cleaning (Modular Pattern with Authorization Support)
set -euo pipefail

source /app/scripts/core/logging.sh

log_step "🌊 NEAFC Vessel Data Cleaning (Modular Pattern with Authorization Support)"

RAW_DATA_DIR="/import/vessels/vessel_data/RFMO/raw"
CLEANED_DATA_DIR="/import/vessels/vessel_data/RFMO/cleaned"
mkdir -p "$CLEANED_DATA_DIR" /import/logs

# Find latest NEAFC file using multiple patterns
INPUT_FILE=""
for pattern in "*NEAFC*vessel*.csv" "*neafc*vessel*.csv" "*NEAFC*.csv" "*neafc*.csv"; do
    if ls "$RAW_DATA_DIR"/$pattern 1> /dev/null 2>&1; then
        INPUT_FILE=$(ls "$RAW_DATA_DIR"/$pattern | head -1)
        break
    fi
done

if [[ -z "$INPUT_FILE" ]]; then
    log_error "No NEAFC vessel file found in $RAW_DATA_DIR"
    log_error "Expected patterns: *NEAFC*vessel*.csv, *neafc*vessel*.csv, *NEAFC*.csv, *neafc*.csv"
    exit 1
fi

OUTPUT_FILE="$CLEANED_DATA_DIR/neafc_vessels_cleaned.csv"
log_success "Processing: $(basename "$INPUT_FILE") → $(basename "$OUTPUT_FILE")"

# Extract source date from filename or use current date
SOURCE_DATE=$(echo "$(basename "$INPUT_FILE")" | grep -oE '[0-9]{4}-?[0-9]{2}-?[0-9]{2}' | head -1 | sed 's/-//g' | sed 's/\([0-9]{4}\)\([0-9]{2}\)\([0-9]{2}\)/\1-\2-\3/' || date '+%Y-%m-%d')
log_success "Source date: $SOURCE_DATE"

# Create modular cleaning script with NEAFC-specific schema handling
cat > /tmp/clean_neafc_modular.py << 'EOF'
import pandas as pd
import sys
import re
from datetime import datetime
import numpy as np

def main():
    try:
        input_file = sys.argv[1]
        output_file = sys.argv[2]
        source_date = sys.argv[3] if len(sys.argv) > 3 else datetime.now().strftime('%Y-%m-%d')
        
        # Read NEAFC CSV
        df = pd.read_csv(input_file)
        print(f"Loaded {len(df)} NEAFC records")
        
        # Add metadata columns
        df['source_date'] = source_date
        df['original_source'] = 'NEAFC'
        
        # Clean text helper
        def clean_text(x):
            if pd.isna(x): return None
            s = str(x).strip()
            return s if s and s.upper() not in ['N/A', 'UNKNOWN', '', 'NAN'] else None
        
        # Clean IMO
        def clean_imo(x):
            if pd.isna(x): return None
            s = str(x).strip().replace('IMO', '').strip()
            if s and s.isdigit() and len(s) == 7:
                return s
            return None
            
        # Clean numeric values from NEAFC format (e.g., "LC 109" -> 109, "OA 32" -> 32, "KW 350" -> 350)
        def extract_numeric(x):
            if pd.isna(x): return None
            s = str(x).strip()
            # Extract numeric part from format like "LC 109", "OA 32", "KW 350"
            match = re.search(r'(\d+\.?\d*)', s)
            if match:
                return float(match.group(1))
            return None
            
        # Map NEAFC columns to standardized schema
        # NEAFC CSV columns: SENDER,FLAG,IRCS,NAME,IMO_NUMBER,EXT_MARK,TYPE,TONNAGE,LENGTH,POWER,AUTHORISED,SPECIES,STARTS,ENDS
        
        # Basic vessel identifiers
        df['vessel_name'] = df['NAME'].apply(clean_text)
        df['imo'] = df['IMO_NUMBER'].apply(clean_imo) 
        df['ircs'] = df['IRCS'].apply(clean_text)
        df['vessel_flag_alpha3'] = df['FLAG'].apply(lambda x: clean_text(x) if clean_text(x) else None)
        df['external_marking'] = df['EXT_MARK'].apply(clean_text)
        
        # NEAFC-specific SENDER field
        df['sender'] = df['SENDER'].apply(clean_text)
        
        # Vessel type
        df['vessel_type_code'] = df['TYPE'].apply(clean_text)
        
        # Metrics - Extract numeric values from NEAFC format
        df['gross_tonnage'] = df['TONNAGE'].apply(extract_numeric)
        df['length_value'] = df['LENGTH'].apply(extract_numeric)
        df['engine_power'] = df['POWER'].apply(extract_numeric)
        
        # Default units based on NEAFC format
        df['length_type'] = 'LOA'  # Overall length
        df['length_unit'] = 'METER'
        df['engine_power_unit'] = 'KILOWATT'
        
        # Process flag/country codes
        def clean_country(x):
            if pd.isna(x): return None
            s = str(x).strip().upper()
            # Handle NEAFC-specific codes
            if s in ['EU27', 'EU', 'EUR']:
                return 'EUR'  # European Union
            elif s == 'FRO':
                return 'FRO'  # Faroe Islands
            elif s == 'GRL':
                return 'GRL'  # Greenland
            return s if len(s) >= 2 else None
            
        df['vessel_flag_alpha3'] = df['vessel_flag_alpha3'].apply(clean_country)
        df['sender'] = df['sender'].apply(clean_country)
        
        # Authorization fields
        df['authorization_status'] = df['AUTHORISED'].apply(lambda x: 'ACTIVE' if str(x).upper() in ['YES', 'Y', '1', 'TRUE'] else 'INACTIVE')
        
        # Process dates
        def parse_neafc_date(x):
            if pd.isna(x): return None
            s = str(x).strip()
            try:
                # Handle formats like "27-MAY-25", "19-MAR-25"
                if re.match(r'\d{1,2}-[A-Z]{3}-\d{2}', s):
                    return pd.to_datetime(s, format='%d-%b-%y').date()
                # Handle other potential formats
                return pd.to_datetime(s, errors='coerce').date() if pd.to_datetime(s, errors='coerce') else None
            except:
                return None
                
        df['auth_start_date'] = df['STARTS'].apply(parse_neafc_date)
        df['auth_end_date'] = df['ENDS'].apply(parse_neafc_date)
        
        # Authorization country (same as flag for NEAFC)
        df['authorizing_country_alpha3'] = df['vessel_flag_alpha3']
        df['authorizing_country_group'] = df['vessel_flag_alpha3'].apply(
            lambda x: 'EU' if x == 'EUR' else 'Individual' if x else None
        )
        
        # Species processing - NEAFC uses specific codes
        def process_species(code):
            if pd.isna(code) or not code: 
                return None, None
            s = str(code).strip().upper()
            
            # NEAFC species mappings
            species_map = {
                'XDS': ('All deep-sea species', None),
                'CAP': ('Capelin', 'Mallotus villosus'),
                'COD': ('Atlantic cod', 'Gadus morhua'),
                'DGS': ('Spiny dogfish', 'Squalus acanthias'),
                'GHL': ('Greenland halibut', 'Reinhardtius hippoglossoides'),
                'HAD': ('Haddock', 'Melanogrammus aeglefinus'),
                'HER': ('Atlantic herring', 'Clupea harengus'),
                'HOM': ('Horse mackerel', 'Trachurus trachurus'),
                'MAC': ('Atlantic mackerel', 'Scomber scombrus'),
                'PRA': ('Northern prawn', 'Pandalus borealis'),
                'REB': ('Beaked redfish', 'Sebastes mentella'),
                'RED': ('Redfish', 'Sebastes spp.'),
                'WBH': ('Blue whiting', 'Micromesistius poutassou'),
                'WHB': ('Blue whiting', 'Micromesistius poutassou')
            }
            
            if s in species_map:
                return species_map[s]
            return s, None
        
        if 'SPECIES' in df.columns:
            df[['species_description', 'scientific_name']] = df['SPECIES'].apply(
                lambda x: pd.Series(process_species(x))
            )
        else:
            df['species_description'] = None
            df['scientific_name'] = None
            
        # Filter records with valid identifiers (following NPFC pattern)
        df_clean = df[
            df['imo'].notna() | 
            df['ircs'].notna() | 
            (df['vessel_name'].notna() & df['vessel_flag_alpha3'].notna())
        ].copy()
        
        print(f"Cleaned to {len(df_clean)} NEAFC vessel-authorization records with valid identifiers")
        
        # Select final columns for output (comprehensive schema support)
        output_columns = [
            'source_date', 'original_source',
            'vessel_name', 'imo', 'ircs', 'vessel_flag_alpha3',
            'vessel_type_code', 'external_marking',
            'gross_tonnage', 'length_type', 'length_value', 'length_unit',
            'engine_power', 'engine_power_unit',
            'authorization_status', 'auth_start_date', 'auth_end_date',
            'authorizing_country_alpha3', 'authorizing_country_group',
            'species_description', 'scientific_name',
            'sender'  # Include SENDER field for reporting country
        ]
        
        # Only include columns that exist
        available_columns = [col for col in output_columns if col in df_clean.columns]
        df_final = df_clean[available_columns].copy()
        
        # Save cleaned data
        df_final.to_csv(output_file, index=False)
        print(f"NEAFC modular cleaning complete: {len(df_final)} records saved")
        print(f"Columns included: {', '.join(available_columns)}")
        
        # Report SENDER field processing
        if 'sender' in available_columns:
            sender_count = df_final['sender'].notna().sum()
            sender_unique = df_final['sender'].nunique()
            print(f"SENDER data found in {sender_count} records ({sender_unique} unique senders)")
            
            # Show unique senders
            if sender_unique > 0:
                unique_senders = df_final['sender'].dropna().unique()
                print(f"Unique SENDER values: {', '.join(unique_senders)}")
        
        # Summary statistics
        print(f"\nNEAFC Cleaning Summary:")
        print(f"  - Vessels with IMO: {df_final['imo'].notna().sum()}")
        print(f"  - Vessels with IRCS: {df_final['ircs'].notna().sum()}")
        print(f"  - Authorization records: {len(df_final)}")
        print(f"  - Unique species codes: {df_final['species_description'].nunique() if 'species_description' in df_final else 0}")
        
    except Exception as e:
        print(f"Error in NEAFC modular cleaning: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
EOF

# Run modular cleaning
if python3 /tmp/clean_neafc_modular.py "$INPUT_FILE" "$OUTPUT_FILE" "$SOURCE_DATE"; then
    RECORD_COUNT=$(tail -n +2 "$OUTPUT_FILE" | wc -l)
    log_success "✅ Cleaned $RECORD_COUNT NEAFC vessel records (modular pattern with authorization support)"
    
    # Check if SENDER column exists and report
    if head -1 "$OUTPUT_FILE" | grep -q "sender"; then
        SENDER_COUNT=$(tail -n +2 "$OUTPUT_FILE" | cut -d',' -f$(head -1 "$OUTPUT_FILE" | tr ',' '\n' | nl | grep "sender" | cut -f1) | grep -v '^$' | wc -l)
        log_success "SENDER field processed successfully: $SENDER_COUNT records with sender information"
    fi
    
    # Verify cleaned data structure
    log_success "Cleaned data structure:"
    head -1 "$OUTPUT_FILE" | tr ',' '\n' | nl | head -10
    
    rm -f /tmp/clean_neafc_modular.py
else
    log_error "NEAFC modular cleaning failed"
    exit 1
fi