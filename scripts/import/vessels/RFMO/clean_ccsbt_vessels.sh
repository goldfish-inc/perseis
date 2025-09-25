#!/bin/bash
# /app/scripts/import/vessels/data/RFMO/clean_ccsbt_vessels.sh
# CCSBT Vessel Data Cleaning Script
set -euo pipefail

source /app/scripts/core/logging.sh

log_step "🐟 CCSBT Vessel Data Cleaning"

RAW_DATA_DIR="/import/vessels/vessel_data/RFMO/raw"
CLEANED_DATA_DIR="/import/vessels/vessel_data/RFMO/cleaned"
mkdir -p "$CLEANED_DATA_DIR" /import/logs

# Find latest CCSBT file
INPUT_FILE=$(find "$RAW_DATA_DIR" -name "*CCSBT*vessel*.csv" -o -name "*ccsbt*vessel*.csv" | head -1)
if [[ -z "$INPUT_FILE" ]]; then
    log_error "No CCSBT vessel file found in $RAW_DATA_DIR"
    exit 1
fi

OUTPUT_FILE="$CLEANED_DATA_DIR/ccsbt_vessels_cleaned.csv"
log_success "Processing: $(basename "$INPUT_FILE") → $(basename "$OUTPUT_FILE")"

# Extract source date from filename or use current date
SOURCE_DATE=$(echo "$(basename "$INPUT_FILE")" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' || date '+%Y-%m-%d')
log_success "Source date: $SOURCE_DATE"

# Create Python cleaning script for CCSBT data
cat > /tmp/clean_ccsbt_vessels.py << 'EOF'
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
        
        # Read CCSBT CSV with UTF-8 BOM
        df = pd.read_csv(input_file, encoding='utf-8-sig')
        print(f"Loaded {len(df)} CCSBT records with {len(df.columns)} columns")
        print(f"Columns: {list(df.columns)}")
        
        # Add metadata for source tracking
        df['source_date'] = source_date
        df['original_source'] = 'CCSBT'
        
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
        
        # Country code standardization for CCSBT
        def standardize_country(country_name):
            """Convert CCSBT country names to ISO alpha3"""
            if pd.isna(country_name) or not country_name: 
                return None
                
            country_name = str(country_name).strip().upper()
            
            # CCSBT member country mappings
            country_map = {
                # CCSBT Members
                'AUSTRALIA': 'AUS',
                'AUS': 'AUS',
                'JAPAN': 'JPN',
                'JPN': 'JPN',
                'NEW ZEALAND': 'NZL',
                'NZL': 'NZL',
                'KOREA': 'KOR',
                'KOREA, REPUBLIC OF': 'KOR',
                'REPUBLIC OF KOREA': 'KOR',
                'KOR': 'KOR',
                'INDONESIA': 'IDN',
                'IDN': 'IDN',
                'SOUTH AFRICA': 'ZAF',
                'ZAF': 'ZAF',
                'TAIWAN': 'TWN',
                'CHINESE TAIPEI': 'TWN',
                'TWN': 'TWN',
                'EUROPEAN UNION': 'EU',
                'EU': 'EU',
                # Cooperating non-members
                'PHILIPPINES': 'PHL',
                'PHL': 'PHL',
                # Common flags
                'CHINA': 'CHN',
                'CHN': 'CHN',
                'VANUATU': 'VUT',
                'VUT': 'VUT',
                'PANAMA': 'PAN',
                'PAN': 'PAN',
                'MARSHALL ISLANDS': 'MHL',
                'MHL': 'MHL',
                'LIBERIA': 'LBR',
                'LBR': 'LBR'
            }
            
            mapped = country_map.get(country_name)
            if mapped:
                return mapped
            elif len(country_name) == 3:
                return country_name
            else:
                # Try to extract ISO-3 code from parentheses
                match = re.search(r'\\(([A-Z]{3})\\)', country_name)
                if match:
                    return match.group(1)
                return None
        
        # Vessel type mapping for CCSBT
        def map_vessel_type(vessel_type):
            """Convert CCSBT vessel types to ISSCFV codes"""
            if pd.isna(vessel_type) or not vessel_type:
                return None
            
            vessel_type = str(vessel_type).strip().upper()
            
            # CCSBT vessel type descriptions
            type_map = {
                'LONGLINE': 'LL',
                'LONGLINER': 'LL',
                'LL': 'LL',
                'PURSE SEINE': 'PS',
                'PURSE SEINER': 'PS',
                'PS': 'PS',
                'POLE AND LINE': 'BB',
                'POLE & LINE': 'BB',
                'BAITBOAT': 'BB',
                'BB': 'BB',
                'TRAWL': 'TO',
                'TRAWLER': 'TO',
                'TO': 'TO',
                'HANDLINE': 'LX',
                'LINE': 'LX',
                'GILLNET': 'GO',
                'DRIFTNET': 'GD',
                'CARRIER': 'SP',
                'FISH CARRIER': 'SP',
                'REEFER': 'SP',
                'TRANSPORT': 'SP',
                'OTHER': 'NO',
                'MULTIPURPOSE': 'MP'
            }
            
            # Check exact match
            if vessel_type in type_map:
                return type_map[vessel_type]
            
            # Check partial match
            for key, value in type_map.items():
                if key in vessel_type:
                    return value
            
            return 'NO'  # Default to other
        
        # Gear type mapping for CCSBT
        def map_gear_type(gear):
            """Convert CCSBT gear descriptions to ISSCFG codes"""
            if pd.isna(gear) or not gear:
                return None
            
            gear = str(gear).strip().upper()
            
            # CCSBT gear types
            gear_map = {
                'LONGLINE': '09.4.0',
                'LL': '09.4.0',
                'PURSE SEINE': '01.1.0',
                'PS': '01.1.0',
                'POLE AND LINE': '09.3.0',
                'BAITBOAT': '09.3.0',
                'TRAWL': '03.1.0',
                'GILLNET': '07.1.0',
                'DRIFTNET': '07.2.1',
                'HANDLINE': '09.5.0',
                'TROLL': '09.6.0',
                'OTHER': '20.0.0'
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
        df['vessel_name_clean'] = df.get('Vessel Name', pd.Series()).apply(clean_text)
        if 'Vessel name' in df.columns:
            df['vessel_name_clean'] = df['vessel_name_clean'].fillna(df.get('Vessel name', pd.Series()).apply(clean_text))
        
        # IMO processing
        df['imo_clean'] = df.get('IMO', pd.Series()).apply(lambda x: 
            str(int(float(x))) if pd.notna(x) and str(x).replace('.0', '').isdigit() 
            and len(str(int(float(x)))) == 7 
            and str(int(float(x))) not in ['0000000', '1111111', '9999999'] 
            else None
        )
        
        # Also check 'IMO Number' column
        if 'IMO Number' in df.columns:
            df['imo_clean'] = df['imo_clean'].fillna(
                df.get('IMO Number', pd.Series()).apply(lambda x: 
                    str(int(float(x))) if pd.notna(x) and str(x).replace('.0', '').isdigit() 
                    and len(str(int(float(x)))) == 7 
                    and str(int(float(x))) not in ['0000000', '1111111', '9999999'] 
                    else None
                )
            )
        
        # IRCS
        df['ircs_clean'] = df.get('Call Sign', pd.Series()).apply(clean_text)
        if 'IRCS' in df.columns:
            df['ircs_clean'] = df['ircs_clean'].fillna(df.get('IRCS', pd.Series()).apply(clean_text))
        
        # CCSBT doesn't typically have MMSI
        df['mmsi_clean'] = None
        
        # National registry number
        df['national_registry_clean'] = df.get('Registration Number', pd.Series()).apply(clean_text)
        if 'Registry Number' in df.columns:
            df['national_registry_clean'] = df['national_registry_clean'].fillna(
                df.get('Registry Number', pd.Series()).apply(clean_text)
            )
        
        # Vessel flag
        df['vessel_flag_alpha3'] = df.get('Flag', pd.Series()).apply(standardize_country)
        if 'Flag State' in df.columns:
            df['vessel_flag_alpha3'] = df['vessel_flag_alpha3'].fillna(
                df.get('Flag State', pd.Series()).apply(standardize_country)
            )
        
        # CCSBT external identifier
        df['ccsbt_vessel_number'] = df.get('CCSBT Record Number', pd.Series()).apply(clean_text)
        if 'CCSBT Number' in df.columns:
            df['ccsbt_vessel_number'] = df['ccsbt_vessel_number'].fillna(
                df.get('CCSBT Number', pd.Series()).apply(clean_text)
            )
        
        # Authorization status
        df['authorization_status'] = df.get('Authorization Status', pd.Series()).apply(clean_text)
        if 'Status' in df.columns:
            df['authorization_status'] = df['authorization_status'].fillna(
                df.get('Status', pd.Series()).apply(clean_text)
            )
        
        # Vessel type
        df['vessel_type_code'] = df.get('Vessel Type', pd.Series()).apply(map_vessel_type)
        if 'Type' in df.columns:
            df['vessel_type_code'] = df['vessel_type_code'].fillna(
                df.get('Type', pd.Series()).apply(map_vessel_type)
            )
        
        # Gear type
        df['gear_type_code'] = df.get('Gear Type', pd.Series()).apply(map_gear_type)
        if 'Gear' in df.columns:
            df['gear_type_code'] = df['gear_type_code'].fillna(
                df.get('Gear', pd.Series()).apply(map_gear_type)
            )
        
        # Port information
        df['port_of_registry_clean'] = df.get('Port of Registry', pd.Series()).apply(clean_text)
        if 'Home Port' in df.columns:
            df['port_of_registry_clean'] = df['port_of_registry_clean'].fillna(
                df.get('Home Port', pd.Series()).apply(clean_text)
            )
        
        # Build information
        df['year_built'] = df.get('Year Built', pd.Series()).apply(lambda x: 
            int(clean_numeric(x)) if clean_numeric(x) and 1900 <= clean_numeric(x) <= 2030 else None
        )
        
        # Measurements
        df['length_value'] = df.get('Length', pd.Series()).apply(clean_numeric)
        if 'LOA' in df.columns:
            df['length_value'] = df['length_value'].fillna(df.get('LOA', pd.Series()).apply(clean_numeric))
        df['length_metric_type'] = 'length_loa'  # Assume LOA unless specified
        df['length_unit_enum'] = 'METER'  # CCSBT typically uses meters
        
        # Check for length unit column
        if 'Length Unit' in df.columns:
            df['length_unit_enum'] = df['Length Unit'].map({
                'm': 'METER',
                'meters': 'METER',
                'ft': 'FEET',
                'feet': 'FEET'
            }).fillna('METER')
        
        # Tonnage
        df['gross_tonnage'] = df.get('GRT', pd.Series()).apply(clean_numeric)
        if 'GT' in df.columns:
            df['gross_tonnage'] = df['gross_tonnage'].fillna(df.get('GT', pd.Series()).apply(clean_numeric))
        if 'Gross Tonnage' in df.columns:
            df['gross_tonnage'] = df['gross_tonnage'].fillna(df.get('Gross Tonnage', pd.Series()).apply(clean_numeric))
        
        df['tonnage_metric_type'] = 'gross_tonnage'  # Assuming GT
        
        # Ownership information
        df['owner_name'] = df.get('Owner', pd.Series()).apply(clean_text)
        if 'Owner Name' in df.columns:
            df['owner_name'] = df['owner_name'].fillna(df.get('Owner Name', pd.Series()).apply(clean_text))
        
        df['operator_name'] = df.get('Operator', pd.Series()).apply(clean_text)
        if 'Operator Name' in df.columns:
            df['operator_name'] = df['operator_name'].fillna(df.get('Operator Name', pd.Series()).apply(clean_text))
        
        # Authorization dates
        df['auth_from_date'] = pd.to_datetime(df.get('Authorization From', pd.Series()), errors='coerce')
        if 'Auth Start Date' in df.columns:
            df['auth_from_date'] = df['auth_from_date'].fillna(
                pd.to_datetime(df.get('Auth Start Date', pd.Series()), errors='coerce')
            )
        
        df['auth_to_date'] = pd.to_datetime(df.get('Authorization To', pd.Series()), errors='coerce')
        if 'Auth End Date' in df.columns:
            df['auth_to_date'] = df['auth_to_date'].fillna(
                pd.to_datetime(df.get('Auth End Date', pd.Series()), errors='coerce')
            )
        
        # Target species (SBT - Southern Bluefin Tuna)
        df['target_species'] = df.get('Target Species', pd.Series()).apply(clean_text)
        if 'Species' in df.columns:
            df['target_species'] = df['target_species'].fillna(df.get('Species', pd.Series()).apply(clean_text))
        
        # Area of operation
        df['area_of_operation'] = df.get('Area of Operation', pd.Series()).apply(clean_text)
        if 'Fishing Area' in df.columns:
            df['area_of_operation'] = df['area_of_operation'].fillna(
                df.get('Fishing Area', pd.Series()).apply(clean_text)
            )
        
        # Data quality filtering
        valid_records = df[
            df['vessel_name_clean'].notna() |
            df['imo_clean'].notna() |
            df['ircs_clean'].notna() |
            df['ccsbt_vessel_number'].notna()
        ].copy()
        
        # Filter by authorization status if available
        if 'authorization_status' in valid_records.columns:
            # Keep authorized and recently expired vessels
            status_filter = valid_records['authorization_status'].str.upper().isin([
                'AUTHORIZED', 'ACTIVE', 'CURRENT', 'VALID'
            ])
            # Also keep recently expired (within last year)
            recently_expired = (valid_records['auth_to_date'] >= (pd.Timestamp.now() - pd.Timedelta(days=365)))
            valid_records = valid_records[status_filter | recently_expired | valid_records['authorization_status'].isna()]
        
        print(f"Filtered to {len(valid_records)} valid CCSBT vessel records")
        
        # Show statistics
        print(f"Authorization status: {valid_records['authorization_status'].value_counts().head().to_dict()}")
        print(f"Vessel types: {valid_records['vessel_type_code'].value_counts().head().to_dict()}")
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
            
            # CCSBT external identifiers
            'ccsbt_vessel_number': valid_records['ccsbt_vessel_number'],
            'authorization_status': valid_records['authorization_status'],
            
            # Vessel characteristics
            'vessel_type_code': valid_records['vessel_type_code'],
            'gear_type_code': valid_records['gear_type_code'],
            'port_of_registry': valid_records['port_of_registry_clean'],
            'year_built': valid_records['year_built'],
            
            # Measurements
            'length_value': valid_records['length_value'],
            'length_metric_type': valid_records['length_metric_type'],
            'length_unit_enum': valid_records['length_unit_enum'],
            
            'gross_tonnage': valid_records['gross_tonnage'],
            'tonnage_metric_type': valid_records['tonnage_metric_type'],
            
            # Ownership
            'owner_name': valid_records['owner_name'],
            'operator_name': valid_records['operator_name'],
            
            # Authorization
            'auth_from_date': valid_records['auth_from_date'],
            'auth_to_date': valid_records['auth_to_date'],
            
            # Target species and area
            'target_species': valid_records['target_species'],
            'area_of_operation': valid_records['area_of_operation']
        })
        
        # Convert dates to strings
        for col in ['auth_from_date', 'auth_to_date']:
            if col in output_df.columns:
                output_df[col] = output_df[col].astype(str).replace('NaT', '')
        
        # Convert year_built to int
        if 'year_built' in output_df.columns:
            output_df['year_built'] = output_df['year_built'].astype('Int64')
        
        # Save cleaned data
        output_df.to_csv(output_file, index=False, na_rep='')
        print(f"CCSBT cleaning complete: {len(output_df)} records saved")
        
        # Summary stats
        print(f"IMO coverage: {(output_df['imo'].notna().sum() / len(output_df) * 100):.1f}%")
        print(f"IRCS coverage: {(output_df['ircs'].notna().sum() / len(output_df) * 100):.1f}%")
        print(f"Active authorizations: {(output_df['auth_to_date'] >= str(datetime.now().date())).sum()}")
        print(f"Countries: {output_df['vessel_flag_alpha3'].nunique()}")
        
    except Exception as e:
        print(f"Error in CCSBT cleaning: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
EOF

# Run cleaning
if python3 /tmp/clean_ccsbt_vessels.py "$INPUT_FILE" "$OUTPUT_FILE" "$SOURCE_DATE"; then
    RECORD_COUNT=$(tail -n +2 "$OUTPUT_FILE" | wc -l)
    COLUMN_COUNT=$(head -1 "$OUTPUT_FILE" | tr ',' '\n' | wc -l)
    log_success "✅ CCSBT cleaning complete:"
    log_success "   - $RECORD_COUNT vessel records processed"
    log_success "   - $COLUMN_COUNT output columns"
    log_success "   - Authorization status tracked"
    log_success "   - Target species (SBT) preserved"
    log_success "   - Operation areas captured"
    
    rm -f /tmp/clean_ccsbt_vessels.py
else
    log_error "CCSBT cleaning failed"
    exit 1
fi