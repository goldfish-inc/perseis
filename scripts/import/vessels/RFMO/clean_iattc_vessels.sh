#!/bin/bash
# /app/scripts/import/vessels/data/RFMO/clean_iattc_vessels.sh
# IATTC Vessel Data Cleaning Script
set -euo pipefail

source /app/scripts/core/logging.sh

log_step "🐟 IATTC Vessel Data Cleaning"

RAW_DATA_DIR="/import/vessels/vessel_data/RFMO/raw"
CLEANED_DATA_DIR="/import/vessels/vessel_data/RFMO/cleaned"
mkdir -p "$CLEANED_DATA_DIR" /import/logs

# Find latest IATTC file
INPUT_FILE=$(find "$RAW_DATA_DIR" -name "*IATTC*vessel*.csv" -o -name "*iattc*vessel*.csv" | head -1)
if [[ -z "$INPUT_FILE" ]]; then
    log_error "No IATTC vessel file found in $RAW_DATA_DIR"
    exit 1
fi

OUTPUT_FILE="$CLEANED_DATA_DIR/iattc_vessels_cleaned.csv"
log_success "Processing: $(basename "$INPUT_FILE") → $(basename "$OUTPUT_FILE")"

# Extract source date from filename or use current date
SOURCE_DATE=$(echo "$(basename "$INPUT_FILE")" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' || date '+%Y-%m-%d')
log_success "Source date: $SOURCE_DATE"

# Create Python cleaning script for IATTC data
cat > /tmp/clean_iattc_vessels.py << 'EOF'
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
        
        # Read IATTC CSV with UTF-8 BOM
        df = pd.read_csv(input_file, encoding='utf-8-sig')
        print(f"Loaded {len(df)} IATTC records with {len(df.columns)} columns")
        print(f"Columns: {list(df.columns)}")
        
        # Add metadata for source tracking
        df['source_date'] = source_date
        df['original_source'] = 'IATTC'
        
        # Clean text helper function
        def clean_text(x):
            if pd.isna(x): return None
            s = str(x).strip()
            return s if s and s.upper() not in ['N/A', '(N/A)', 'NONE', 'UNKNOWN', '', 'NAN', 'NIL'] else None
        
        # Clean numeric helper function
        def clean_numeric(x):
            if pd.isna(x): return None
            try:
                val = str(x).strip().replace(',', '')
                if val and val.replace('.', '').replace('-', '').isdigit():
                    return float(val)
            except:
                pass
            return None
        
        # Country code standardization for IATTC (uses full names mostly)
        def standardize_country(country_name):
            """Convert IATTC country names to ISO alpha3"""
            if pd.isna(country_name) or not country_name: 
                return None
                
            country_name = str(country_name).strip().upper()
            
            # IATTC country mappings
            country_map = {
                # Americas
                'UNITED STATES': 'USA',
                'USA': 'USA',
                'US': 'USA',
                'MEXICO': 'MEX',
                'CANADA': 'CAN',
                'ECUADOR': 'ECU',
                'PANAMA': 'PAN',
                'COLOMBIA': 'COL',
                'COSTA RICA': 'CRI',
                'NICARAGUA': 'NIC',
                'HONDURAS': 'HND',
                'EL SALVADOR': 'SLV',
                'GUATEMALA': 'GTM',
                'PERU': 'PER',
                'CHILE': 'CHL',
                'VENEZUELA': 'VEN',
                # Asia
                'JAPAN': 'JPN',
                'KOREA': 'KOR',
                'KOREA, REPUBLIC OF': 'KOR',
                'SOUTH KOREA': 'KOR',
                'CHINA': 'CHN',
                'CHINESE TAIPEI': 'TWN',
                'TAIWAN': 'TWN',
                # Pacific
                'VANUATU': 'VUT',
                'KIRIBATI': 'KIR',
                'COOK ISLANDS': 'COK',
                # Europe
                'SPAIN': 'ESP',
                'FRANCE': 'FRA',
                # Other
                'BELIZE': 'BLZ',
                'LIBERIA': 'LBR',
                'BOLIVIA': 'BOL'
            }
            
            mapped = country_map.get(country_name)
            if mapped:
                return mapped
            elif len(country_name) == 3:
                return country_name
            else:
                return None
        
        # Gear type mapping for IATTC
        def map_gear_type(gear):
            """Convert IATTC gear descriptions to ISSCFG codes"""
            if pd.isna(gear) or not gear:
                return None
            
            gear = str(gear).strip().upper()
            
            # IATTC gear types
            gear_map = {
                'PURSE SEINE': '01.1.0',
                'PS': '01.1.0',
                'LONGLINE': '09.4.0',
                'LL': '09.4.0',
                'POLE AND LINE': '09.3.0',
                'PL': '09.3.0',
                'TRAWL': '03.1.0',
                'GILLNET': '07.1.0',
                'TROLL': '09.6.0',
                'OTHER': '20.0.0',
                'UNKNOWN': '20.0.0'
            }
            
            # Check exact match
            if gear in gear_map:
                return gear_map[gear]
            
            # Check partial match
            for key, value in gear_map.items():
                if key in gear:
                    return value
            
            return '20.0.0'  # Default to other
        
        # Process vessel identifiers
        df['vessel_name_clean'] = df.get('Vessel', pd.Series()).apply(clean_text)
        
        # IMO processing
        df['imo_clean'] = df.get('IMO number', pd.Series()).apply(lambda x: 
            str(int(float(x))) if pd.notna(x) and str(x).replace('.0', '').isdigit() 
            and len(str(int(float(x)))) == 7 
            and str(int(float(x))) not in ['0000000', '1111111', '9999999'] 
            else None
        )
        
        df['ircs_clean'] = df.get('Call sign', pd.Series()).apply(clean_text)
        
        # IATTC doesn't have MMSI in standard fields
        df['mmsi_clean'] = None
        
        # National registry number
        df['national_registry_clean'] = df.get('Registration number', pd.Series()).apply(clean_text)
        
        # Vessel flag
        df['vessel_flag_alpha3'] = df.get('Flag', pd.Series()).apply(standardize_country)
        
        # Previous flag
        df['previous_flag_alpha3'] = df.get('Previous flag', pd.Series()).apply(standardize_country)
        
        # IATTC external identifier
        df['iattc_vessel_number'] = df.get('IATTC Vessel number', pd.Series()).apply(lambda x: 
            str(int(float(x))) if pd.notna(x) and str(x).replace('.0', '').isdigit() else clean_text(x)
        )
        
        # Registration status
        df['register_status'] = df.get('Register Status', pd.Series()).apply(clean_text)
        
        # Gear type
        df['gear_type_code'] = df.get('Gear', pd.Series()).apply(map_gear_type)
        
        # Port information
        df['port_of_registry_clean'] = df.get('Port of registration', pd.Series()).apply(clean_text)
        
        # Build information
        df['year_built'] = df.get('Year built', pd.Series()).apply(lambda x: 
            int(clean_numeric(x)) if clean_numeric(x) and 1900 <= clean_numeric(x) <= 2030 else None
        )
        df['shipyard'] = df.get('Shipyard', pd.Series()).apply(clean_text)
        
        # Previous name
        df['previous_name'] = df.get('Previous name', pd.Series()).apply(clean_text)
        
        # Measurements
        df['length_value'] = df.get('Length (m)', pd.Series()).apply(clean_numeric)
        df['length_type'] = df.get('Length type', pd.Series()).apply(clean_text)
        
        # Map length type to standard
        df['length_metric_type'] = df['length_type'].map({
            'LOA': 'length_loa',
            'LBP': 'length_between_perpendiculars',
            'OTHER': 'other_length'
        })
        df['length_unit_enum'] = 'METER'  # Length (m) indicates meters
        
        # Other measurements
        df['beam_value'] = df.get('Beam (m)', pd.Series()).apply(clean_numeric)
        df['beam_unit_enum'] = 'METER'
        
        df['depth_value'] = df.get('Depth (m)', pd.Series()).apply(clean_numeric)
        df['depth_unit_enum'] = 'METER'
        
        # Tonnage
        df['gross_tonnage'] = df.get('Gross tonnage (t)', pd.Series()).apply(clean_numeric)
        df['tonnage_metric_type'] = 'gross_tonnage'  # Assuming GT
        
        # Engine power
        df['engine_power'] = df.get('Engine power (HP)', pd.Series()).apply(clean_numeric)
        df['engine_power_unit_enum'] = 'HP'
        
        # Capacity
        df['fish_hold_volume'] = df.get('Fish hold volume (m3)', pd.Series()).apply(clean_numeric)
        df['fish_hold_volume_unit_enum'] = 'CUBIC_METER'
        
        df['carrying_capacity'] = df.get('Carrying capacity (t)', pd.Series()).apply(clean_numeric)
        df['carrying_capacity_unit_enum'] = 'MT'
        
        # Company information
        df['company_name'] = df.get('Company name', pd.Series()).apply(clean_text)
        df['company_address'] = df.get('Business Address', pd.Series()).apply(clean_text)
        
        # Other fields
        df['notes'] = df.get('Notes', pd.Series()).apply(clean_text)
        df['image_available'] = df.get('Image available?', pd.Series()).apply(clean_text)
        
        # Dates
        df['confirmation_date'] = pd.to_datetime(df.get('Confirmation date'), errors='coerce')
        df['last_modification'] = pd.to_datetime(df.get('Last modification'), errors='coerce')
        
        # Data quality filtering
        valid_records = df[
            df['vessel_name_clean'].notna() |
            df['imo_clean'].notna() |
            df['ircs_clean'].notna() |
            df['iattc_vessel_number'].notna()
        ].copy()
        
        # Filter by active status if needed
        if 'register_status' in valid_records.columns:
            # Keep active and some inactive for historical tracking
            status_filter = valid_records['register_status'].str.upper().isin(['ACTIVE', 'AUTHORIZED', 'INACTIVE'])
            valid_records = valid_records[status_filter | valid_records['register_status'].isna()]
        
        print(f"Filtered to {len(valid_records)} valid IATTC vessel records")
        
        # Show statistics
        print(f"Register status: {valid_records['register_status'].value_counts().head().to_dict()}")
        print(f"Gear types: {valid_records['gear_type_code'].value_counts().head().to_dict()}")
        print(f"Flags: {valid_records['vessel_flag_alpha3'].value_counts().head(10).to_dict()}")
        
        # Create output dataframe
        output_df = pd.DataFrame({
            # Source metadata
            'source_date': valid_records['source_date'],
            'original_source': valid_records['original_source'],
            
            # Core vessel identifiers
            'vessel_name': valid_records['vessel_name_clean'],
            'imo': valid_records['imo_clean'],
            'ircs': valid_records['ircs_clean'],
            'mmsi': valid_records['mmsi_clean'],
            'national_registry': valid_records['national_registry_clean'],
            'vessel_flag_alpha3': valid_records['vessel_flag_alpha3'],
            
            # IATTC external identifiers
            'iattc_vessel_number': valid_records['iattc_vessel_number'],
            'register_status': valid_records['register_status'],
            
            # Vessel characteristics
            'gear_type_code': valid_records['gear_type_code'],
            'port_of_registry': valid_records['port_of_registry_clean'],
            'year_built': valid_records['year_built'],
            'shipyard': valid_records['shipyard'],
            'previous_name': valid_records['previous_name'],
            'previous_flag': valid_records['previous_flag_alpha3'],
            
            # Measurements
            'length_value': valid_records['length_value'],
            'length_metric_type': valid_records['length_metric_type'],
            'length_unit_enum': valid_records['length_unit_enum'],
            
            'beam_value': valid_records['beam_value'],
            'beam_unit_enum': valid_records['beam_unit_enum'],
            
            'depth_value': valid_records['depth_value'],
            'depth_unit_enum': valid_records['depth_unit_enum'],
            
            'gross_tonnage': valid_records['gross_tonnage'],
            'tonnage_metric_type': valid_records['tonnage_metric_type'],
            
            'engine_power': valid_records['engine_power'],
            'engine_power_unit_enum': valid_records['engine_power_unit_enum'],
            
            # Capacity
            'fish_hold_volume': valid_records['fish_hold_volume'],
            'fish_hold_volume_unit_enum': valid_records['fish_hold_volume_unit_enum'],
            
            'carrying_capacity': valid_records['carrying_capacity'],
            'carrying_capacity_unit_enum': valid_records['carrying_capacity_unit_enum'],
            
            # Company
            'company_name': valid_records['company_name'],
            'company_address': valid_records['company_address'],
            
            # Metadata
            'notes': valid_records['notes'],
            'image_available': valid_records['image_available'],
            'confirmation_date': valid_records['confirmation_date'],
            'last_modification': valid_records['last_modification']
        })
        
        # Convert dates to strings
        for col in ['confirmation_date', 'last_modification']:
            if col in output_df.columns:
                output_df[col] = output_df[col].astype(str).replace('NaT', '')
        
        # Convert year_built to int
        if 'year_built' in output_df.columns:
            output_df['year_built'] = output_df['year_built'].astype('Int64')
        
        # Save cleaned data
        output_df.to_csv(output_file, index=False, na_rep='')
        print(f"IATTC cleaning complete: {len(output_df)} records saved")
        
        # Summary stats
        print(f"IMO coverage: {(output_df['imo'].notna().sum() / len(output_df) * 100):.1f}%")
        print(f"IRCS coverage: {(output_df['ircs'].notna().sum() / len(output_df) * 100):.1f}%")
        print(f"Active vessels: {(output_df['register_status'] == 'Active').sum()}")
        print(f"Countries: {output_df['vessel_flag_alpha3'].nunique()}")
        
    except Exception as e:
        print(f"Error in IATTC cleaning: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
EOF

# Run cleaning
if python3 /tmp/clean_iattc_vessels.py "$INPUT_FILE" "$OUTPUT_FILE" "$SOURCE_DATE"; then
    RECORD_COUNT=$(tail -n +2 "$OUTPUT_FILE" | wc -l)
    COLUMN_COUNT=$(head -1 "$OUTPUT_FILE" | tr ',' '\n' | wc -l)
    log_success "✅ IATTC cleaning complete:"
    log_success "   - $RECORD_COUNT vessel records processed"
    log_success "   - $COLUMN_COUNT output columns"
    log_success "   - Register status tracked"
    log_success "   - Previous names and flags preserved"
    log_success "   - Carrying capacity captured"
    
    rm -f /tmp/clean_iattc_vessels.py
else
    log_error "IATTC cleaning failed"
    exit 1
fi