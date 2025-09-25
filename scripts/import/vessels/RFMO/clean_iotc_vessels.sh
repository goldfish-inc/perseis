#!/bin/bash
# ./scripts./import/vessels/data/RFMO/clean_iotc_vessels.sh
# IOTC Vessel Data Cleaning Script
set -euo pipefail

source /app/scripts/core/logging.sh

log_step "🐟 IOTC Vessel Data Cleaning"

RAW_DATA_DIR="/import/vessels/vessel_data/RFMO/raw"
CLEANED_DATA_DIR="/import/vessels/vessel_data/RFMO/cleaned"
mkdir -p "$CLEANED_DATA_DIR" /import/logs

# Find latest IOTC file
INPUT_FILE=$(find "$RAW_DATA_DIR" -name "*IOTC*vessel*.csv" -o -name "*iotc*vessel*.csv" | head -1)
if [[ -z "$INPUT_FILE" ]]; then
    log_error "No IOTC vessel file found in $RAW_DATA_DIR"
    exit 1
fi

OUTPUT_FILE="$CLEANED_DATA_DIR/iotc_vessels_cleaned.csv"
log_success "Processing: $(basename "$INPUT_FILE") → $(basename "$OUTPUT_FILE")"

# Extract source date from filename or use current date
SOURCE_DATE=$(echo "$(basename "$INPUT_FILE")" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' || date '+%Y-%m-%d')
log_success "Source date: $SOURCE_DATE"

# Create Python cleaning script for IOTC data
cat > /tmp/clean_iotc_vessels.py << 'EOF'
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
        
        # Read IOTC CSV with UTF-8 BOM
        df = pd.read_csv(input_file, encoding='utf-8-sig')
        print(f"Loaded {len(df)} IOTC records with {len(df.columns)} columns")
        print(f"Columns: {list(df.columns[:20])}...")
        
        # Add metadata for source tracking
        df['source_date'] = source_date
        df['original_source'] = 'IOTC'
        
        # Clean text helper function
        def clean_text(x):
            if pd.isna(x): return None
            s = str(x).strip()
            # IOTC uses 'NE' for not entered/no data
            return s if s and s.upper() not in ['N/A', '(N/A)', 'NONE', 'UNKNOWN', '', 'NAN', 'NE', 'NIL'] else None
        
        # Clean numeric helper function
        def clean_numeric(x):
            if pd.isna(x): return None
            try:
                val = str(x).strip()
                if val and val.replace('.', '').replace('-', '').isdigit():
                    return float(val)
            except:
                pass
            return None
        
        # Country code standardization for IOTC
        def standardize_country(country_code):
            """Convert IOTC country codes to ISO alpha3"""
            if pd.isna(country_code) or not country_code: 
                return None
                
            country_code = str(country_code).strip().upper()
            
            # IOTC uses ISO3 codes directly but has some variations
            country_map = {
                # Special territories
                'CHN-TWN': 'TWN',  # Taiwan
                'EU': 'EUE',  # European Union entity
                'UK': 'GBR',  # United Kingdom
                'TANZANIA': 'TZA',
                'KOREA': 'KOR',
                'S KOREA': 'KOR',
                'N KOREA': 'PRK',
                'IRAN': 'IRN',
                'RUSSIA': 'RUS',
                'COMOROS': 'COM'
            }
            
            # Check mapping first
            if country_code in country_map:
                return country_map[country_code]
            # Return if already 3 chars
            elif len(country_code) == 3:
                return country_code
            else:
                return None
        
        # Vessel type mapping for IOTC
        def map_vessel_type(type_code):
            """Convert IOTC vessel type codes to standard vessel types"""
            if pd.isna(type_code) or not type_code:
                return None
            
            type_code = str(type_code).strip().upper()
            
            # IOTC vessel type codes
            type_map = {
                'BB': 'BB',     # Pole & Line vessel
                'LI': 'LL',     # Line vessel (longline)
                'LL': 'LL',     # Longline vessel
                'PS': 'PS',     # Purse seine vessel
                'GN': 'GO',     # Gillnet vessel
                'SU': 'SP',     # Support vessel
                'MP': 'MP',     # Multipurpose vessel
                'LN': 'LN',     # Lining vessel
                'TR': 'TO',     # Trawler
                'OT': 'NO'      # Other
            }
            
            return type_map.get(type_code, type_code if len(type_code) <= 3 else None)
        
        # Gear type mapping for IOTC
        def map_gear_types(gear_codes):
            """Convert IOTC gear codes to ISSCFG codes"""
            if pd.isna(gear_codes) or not gear_codes:
                return None
            
            gear_codes = str(gear_codes).strip().upper()
            
            # IOTC uses semicolon-separated gear codes
            gear_map = {
                'BB': '09.3.0',    # Pole and line
                'GILL': '07.1.0',  # Gillnets
                'GN': '07.1.0',    # Gillnets
                'HAND': '09.1.0',  # Hand lines
                'HL': '09.1.0',    # Hand lines
                'LIN': '09.4.0',   # Lines
                'LL': '09.4.0',    # Longlines
                'PL': '09.3.0',    # Pole and line
                'PS': '01.1.0',    # Purse seines
                'TROL': '03.1.0',  # Trawls
                'OTHER': '20.0.0', # Other gear
                'NA': '25.9.0'     # No gear (support vessel)
            }
            
            # Handle multiple gears
            gears = []
            for gear in gear_codes.split(';'):
                mapped = gear_map.get(gear.strip(), gear.strip() if len(gear.strip()) <= 10 else None)
                if mapped:
                    gears.append(mapped)
            
            return ';'.join(gears) if gears else None
        
        # Process authorization dates (IOTC uses From/To columns)
        def parse_auth_date(date_str):
            """Parse IOTC authorization dates"""
            if pd.isna(date_str) or not date_str:
                return None
            try:
                # IOTC uses various date formats
                date_str = str(date_str).strip()
                for fmt in ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y']:
                    try:
                        return datetime.strptime(date_str, fmt).date()
                    except:
                        continue
            except:
                pass
            return None
        
        # Process vessel identifiers
        df['vessel_name_clean'] = df.get('Name', pd.Series()).apply(clean_text)
        
        # IMO processing
        df['imo_clean'] = df.get('IMO', pd.Series()).apply(lambda x: 
            str(x).strip() if pd.notna(x) and str(x).strip() != 'NE' and len(str(x).strip()) == 7 
            and str(x).strip().isdigit() and str(x).strip() not in ['0000000', '1111111', '9999999'] 
            else None
        )
        
        # Handle IRCS - store special values in processing_notes
        def process_ircs(value):
            if pd.isna(value):
                return None
            value_str = str(value).strip()
            # Check for special statuses but keep original
            if value_str.upper() in ['NIL', 'N/A', 'NA', 'NONE', 'NULL', '']:
                return None
            else:
                return value_str  # Keep original including "APPLIED NOT YET RECEIVED"
        
        df['ircs_clean'] = df.get('IRCS', pd.Series()).apply(process_ircs)
        
        # Track special IRCS statuses for later processing
        df['has_ircs_status'] = df.get('IRCS', pd.Series()).apply(
            lambda x: str(x).upper() if pd.notna(x) and 'APPLIED' in str(x).upper() else None
        )
        
        # IOTC doesn't have MMSI in standard export
        df['mmsi_clean'] = None
        
        # National registry number
        df['national_registry_clean'] = df.get('REGNO', pd.Series()).apply(clean_text)
        
        # Vessel flag
        df['vessel_flag_alpha3'] = df.get('Flag State Code', pd.Series()).apply(standardize_country)
        
        # IOTC external identifier
        df['iotc_number'] = df.get('IOTC Number', pd.Series()).apply(clean_text)
        
        # Vessel type and gear
        df['vessel_type_code'] = df.get('Type Code', pd.Series()).apply(map_vessel_type)
        df['gear_type_code'] = df.get('Gears Code', pd.Series()).apply(map_gear_types)
        
        # Port information
        df['port_code'] = df.get('Port Code', pd.Series()).apply(clean_text)
        df['port_name'] = df.get('Port Name', pd.Series()).apply(clean_text)
        
        # Vessel characteristics
        df['vessel_kind'] = df.get('Vessel Kind Code', pd.Series()).apply(clean_text)
        df['range_code'] = df.get('Range Code', pd.Series()).apply(clean_text)
        
        # Measurements
        df['length_value'] = df.get('LOA (m)', pd.Series()).apply(clean_numeric)
        df['length_unit_enum'] = 'METER'  # LOA (m) indicates meters
        df['length_metric_type'] = 'length_loa'
        
        # Tonnage - IOTC provides both GT and GRT
        df['gt_value'] = df.get('GT', pd.Series()).apply(clean_numeric)
        df['grt_value'] = df.get('GRT', pd.Series()).apply(clean_numeric)
        
        # Total volume
        df['volume_value'] = df.get('Total Volume (m3)', pd.Series()).apply(clean_numeric)
        df['volume_unit_enum'] = 'CUBIC_METER'
        
        # Cold storage capacity
        df['cc_value'] = df.get('CC (MT)', pd.Series()).apply(clean_numeric)
        df['cc_unit_enum'] = 'MT'  # Metric tons
        
        # Complex ownership structure (IOTC tracks multiple entities)
        df['owner_name'] = df.get('Owner', pd.Series()).apply(clean_text)
        df['owner_address'] = df.get('Owner Address', pd.Series()).apply(clean_text)
        
        df['operator_name'] = df.get('Operator', pd.Series()).apply(clean_text)
        df['operator_address'] = df.get('Operator Address', pd.Series()).apply(clean_text)
        
        df['operating_company'] = df.get('Operating Company', pd.Series()).apply(clean_text)
        df['operating_company_address'] = df.get('Operating Company Address', pd.Series()).apply(clean_text)
        df['operating_company_reg_num'] = df.get('Operating Company Reg Num', pd.Series()).apply(clean_text)
        
        df['beneficial_owner'] = df.get('Beneficial Owner', pd.Series()).apply(clean_text)
        df['beneficial_owner_address'] = df.get('Beneficial Owner Address', pd.Series()).apply(clean_text)
        
        # Authorization dates
        df['auth_from_date'] = df.get('From', pd.Series()).apply(parse_auth_date)
        df['auth_to_date'] = df.get('To', pd.Series()).apply(parse_auth_date)
        
        # Last updated
        df['last_updated'] = df.get('Last Updated', pd.Series()).apply(parse_auth_date)
        
        # Photo URLs (for future use)
        df['starboard_photo'] = df.get('Starboard Photo', pd.Series()).apply(clean_text)
        df['portside_photo'] = df.get('Portside Photo', pd.Series()).apply(clean_text)
        df['bow_photo'] = df.get('Bow Photo', pd.Series()).apply(clean_text)
        
        # Data quality filtering
        valid_records = df[
            df['vessel_name_clean'].notna() |
            df['imo_clean'].notna() |
            df['ircs_clean'].notna() |
            df['iotc_number'].notna()
        ].copy()
        
        print(f"Filtered to {len(valid_records)} valid IOTC vessel records")
        
        # Show vessel types and flags
        print(f"Vessel types: {valid_records['vessel_type_code'].value_counts().head(10).to_dict()}")
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
            
            # IOTC external identifiers
            'iotc_number': valid_records['iotc_number'],
            
            # Vessel characteristics
            'vessel_type_code': valid_records['vessel_type_code'],
            'gear_type_code': valid_records['gear_type_code'],
            'vessel_kind': valid_records['vessel_kind'],
            'range_code': valid_records['range_code'],
            'port_code': valid_records['port_code'],
            'port_name': valid_records['port_name'],
            
            # Measurements
            'length_value': valid_records['length_value'],
            'length_metric_type': valid_records['length_metric_type'],
            'length_unit_enum': valid_records['length_unit_enum'],
            
            'gt_value': valid_records['gt_value'],
            'grt_value': valid_records['grt_value'],
            
            'volume_value': valid_records['volume_value'],
            'volume_unit_enum': valid_records['volume_unit_enum'],
            
            'cc_value': valid_records['cc_value'],
            'cc_unit_enum': valid_records['cc_unit_enum'],
            
            # Ownership structure
            'owner_name': valid_records['owner_name'],
            'owner_address': valid_records['owner_address'],
            
            'operator_name': valid_records['operator_name'],
            'operator_address': valid_records['operator_address'],
            
            'operating_company': valid_records['operating_company'],
            'operating_company_address': valid_records['operating_company_address'],
            'operating_company_reg_num': valid_records['operating_company_reg_num'],
            
            'beneficial_owner': valid_records['beneficial_owner'],
            'beneficial_owner_address': valid_records['beneficial_owner_address'],
            
            # Authorization
            'auth_from_date': valid_records['auth_from_date'],
            'auth_to_date': valid_records['auth_to_date'],
            
            # Metadata
            'last_updated': valid_records['last_updated'],
            'starboard_photo': valid_records['starboard_photo'],
            'portside_photo': valid_records['portside_photo'],
            'bow_photo': valid_records['bow_photo']
        })
        
        # Convert dates to strings for CSV, handling None and NaT properly
        for col in ['auth_from_date', 'auth_to_date', 'last_updated']:
            if col in output_df.columns:
                output_df[col] = output_df[col].astype(str).replace({'NaT': '', 'None': '', 'nan': ''})
        
        # Save cleaned data
        output_df.to_csv(output_file, index=False, na_rep='')
        print(f"IOTC cleaning complete: {len(output_df)} records saved")
        
        # Summary stats
        print(f"IMO coverage: {(output_df['imo'].notna().sum() / len(output_df) * 100):.1f}%")
        print(f"IRCS coverage: {(output_df['ircs'].notna().sum() / len(output_df) * 100):.1f}%")
        print(f"Active authorizations: {(output_df['auth_to_date'] >= str(datetime.now().date())).sum()}")
        print(f"Countries: {output_df['vessel_flag_alpha3'].nunique()}")
        
    except Exception as e:
        print(f"Error in IOTC cleaning: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
EOF

# Run cleaning
if python3 /tmp/clean_iotc_vessels.py "$INPUT_FILE" "$OUTPUT_FILE" "$SOURCE_DATE"; then
    RECORD_COUNT=$(tail -n +2 "$OUTPUT_FILE" | wc -l)
    COLUMN_COUNT=$(head -1 "$OUTPUT_FILE" | tr ',' '\n' | wc -l)
    log_success "✅ IOTC cleaning complete:"
    log_success "   - $RECORD_COUNT vessel records processed"
    log_success "   - $COLUMN_COUNT output columns"
    log_success "   - Complex ownership structure captured"
    log_success "   - Authorization dates preserved"
    log_success "   - Photo URLs retained for future use"
    
    rm -f /tmp/clean_iotc_vessels.py
else
    log_error "IOTC cleaning failed"
    exit 1
fi