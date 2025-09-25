#!/bin/bash
# /app/scripts/import/vessels/data/RFMO/clean_nafo_vessels.sh
# NAFO Vessel Data Cleaning Script
set -euo pipefail

source /app/scripts/core/logging.sh

log_step "🐟 NAFO Vessel Data Cleaning"

RAW_DATA_DIR="/import/vessels/vessel_data/RFMO/raw"
CLEANED_DATA_DIR="/import/vessels/vessel_data/RFMO/cleaned"
mkdir -p "$CLEANED_DATA_DIR" /import/logs

# Find latest NAFO file
INPUT_FILE=$(find "$RAW_DATA_DIR" -name "*NAFO*vessel*.csv" -o -name "*nafo*vessel*.csv" | head -1)
if [[ -z "$INPUT_FILE" ]]; then
    log_error "No NAFO vessel file found in $RAW_DATA_DIR"
    exit 1
fi

OUTPUT_FILE="$CLEANED_DATA_DIR/nafo_vessels_cleaned.csv"
log_success "Processing: $(basename "$INPUT_FILE") → $(basename "$OUTPUT_FILE")"

# Extract source date from filename or use current date
SOURCE_DATE=$(echo "$(basename "$INPUT_FILE")" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' || date '+%Y-%m-%d')
log_success "Source date: $SOURCE_DATE"

# Create Python cleaning script for NAFO data
cat > /tmp/clean_nafo_vessels.py << 'EOF'
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
        
        # Read NAFO CSV with UTF-8 BOM
        df = pd.read_csv(input_file, encoding='utf-8-sig')
        print(f"Loaded {len(df)} NAFO records with {len(df.columns)} columns")
        print(f"Columns: {list(df.columns)}")
        
        # Add metadata for source tracking
        df['source_date'] = source_date
        df['original_source'] = 'NAFO'
        
        # Clean text helper function
        def clean_text(x):
            if pd.isna(x): return None
            s = str(x).strip()
            return s if s and s.upper() not in ['N/A', '(N/A)', 'NONE', 'UNKNOWN', '', 'NAN', 'NIL', 'NULL'] else None
        
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
        
        # Country code standardization for NAFO
        def standardize_country(country_name):
            """Convert NAFO country names to ISO alpha3"""
            if pd.isna(country_name) or not country_name: 
                return None
                
            country_name = str(country_name).strip().upper()
            
            # NAFO contracting party mappings
            country_map = {
                # North America
                'CANADA': 'CAN',
                'CAN': 'CAN',
                'UNITED STATES': 'USA',
                'UNITED STATES OF AMERICA': 'USA',
                'USA': 'USA',
                'US': 'USA',
                # Europe
                'EUROPEAN UNION': 'EU',
                'EU': 'EU',
                'DENMARK': 'DNK',
                'DNK': 'DNK',
                'FRANCE': 'FRA',
                'FRA': 'FRA',
                'ICELAND': 'ISL',
                'ISL': 'ISL',
                'NORWAY': 'NOR',
                'NOR': 'NOR',
                'RUSSIA': 'RUS',
                'RUSSIAN FEDERATION': 'RUS',
                'RUS': 'RUS',
                'UNITED KINGDOM': 'GBR',
                'UK': 'GBR',
                'GBR': 'GBR',
                'SPAIN': 'ESP',
                'ESP': 'ESP',
                'PORTUGAL': 'PRT',
                'PRT': 'PRT',
                'GERMANY': 'DEU',
                'DEU': 'DEU',
                'NETHERLANDS': 'NLD',
                'NLD': 'NLD',
                'ESTONIA': 'EST',
                'EST': 'EST',
                'LATVIA': 'LVA',
                'LVA': 'LVA',
                'LITHUANIA': 'LTU',
                'LTU': 'LTU',
                'POLAND': 'POL',
                'POL': 'POL',
                # Asia
                'JAPAN': 'JPN',
                'JPN': 'JPN',
                'KOREA': 'KOR',
                'REPUBLIC OF KOREA': 'KOR',
                'KOR': 'KOR',
                'CHINA': 'CHN',
                'CHN': 'CHN',
                # Other
                'CUBA': 'CUB',
                'CUB': 'CUB',
                'UKRAINE': 'UKR',
                'UKR': 'UKR',
                'ST. PIERRE AND MIQUELON': 'SPM',
                'SPM': 'SPM',
                'FAROE ISLANDS': 'FRO',
                'FRO': 'FRO',
                'GREENLAND': 'GRL',
                'GRL': 'GRL'
            }
            
            mapped = country_map.get(country_name)
            if mapped:
                return mapped
            elif len(country_name) == 3:
                return country_name
            else:
                # Try to extract from parentheses
                match = re.search(r'\\(([A-Z]{3})\\)', country_name)
                if match:
                    return match.group(1)
                return None
        
        # Vessel type mapping for NAFO
        def map_vessel_type(vessel_type):
            """Convert NAFO vessel types to ISSCFV codes"""
            if pd.isna(vessel_type) or not vessel_type:
                return None
            
            vessel_type = str(vessel_type).strip().upper()
            
            # NAFO vessel type descriptions
            type_map = {
                'STERN TRAWLER': 'TSS',
                'SIDE TRAWLER': 'TSC',
                'TRAWLER': 'TO',
                'FREEZER TRAWLER': 'TTP',
                'FACTORY TRAWLER': 'TTP',
                'OTTER TRAWLER': 'TO',
                'SHRIMP TRAWLER': 'TS',
                'BOTTOM TRAWLER': 'TB',
                'MIDWATER TRAWLER': 'TM',
                'LONGLINER': 'LL',
                'LONGLINE': 'LL',
                'GILLNETTER': 'GO',
                'GILLNET': 'GO',
                'SEINER': 'SN',
                'PURSE SEINE': 'PS',
                'DRAGGER': 'DRB',
                'DREDGER': 'DO',
                'TRAP': 'FPO',
                'POT': 'FPO',
                'RESEARCH': 'ZRS',
                'FACTORY': 'FF',
                'PROCESSOR': 'FF',
                'TRANSPORT': 'SP',
                'SUPPORT': 'SP',
                'OTHER': 'NO'
            }
            
            # Check exact match
            if vessel_type in type_map:
                return type_map[vessel_type]
            
            # Check partial match
            for key, value in type_map.items():
                if key in vessel_type:
                    return value
            
            return 'NO'  # Default to other
        
        # Gear type mapping for NAFO
        def map_gear_type(gear):
            """Convert NAFO gear descriptions to ISSCFG codes"""
            if pd.isna(gear) or not gear:
                return None
            
            gear = str(gear).strip().upper()
            
            # NAFO gear types
            gear_map = {
                'OTTER TRAWL': '03.1.1',
                'BOTTOM TRAWL': '03.1.1',
                'MIDWATER TRAWL': '03.1.2',
                'SHRIMP TRAWL': '03.1.1',
                'TRAWL': '03.1.0',
                'LONGLINE': '09.4.0',
                'LL': '09.4.0',
                'GILLNET': '07.1.0',
                'SEINE': '01.2.0',
                'PURSE SEINE': '01.1.0',
                'DANISH SEINE': '02.2.0',
                'DREDGE': '04.1.0',
                'TRAP': '08.2.0',
                'POT': '08.2.0',
                'HANDLINE': '09.5.0',
                'JIG': '09.2.0',
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
        if 'Name' in df.columns:
            df['vessel_name_clean'] = df['vessel_name_clean'].fillna(df.get('Name', pd.Series()).apply(clean_text))
        
        # IMO processing
        df['imo_clean'] = df.get('IMO', pd.Series()).apply(lambda x: 
            str(int(float(x))) if pd.notna(x) and str(x).replace('.0', '').isdigit() 
            and len(str(int(float(x)))) == 7 
            and str(int(float(x))) not in ['0000000', '1111111', '9999999'] 
            else None
        )
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
        if 'Radio Call Sign' in df.columns:
            df['ircs_clean'] = df['ircs_clean'].fillna(df.get('Radio Call Sign', pd.Series()).apply(clean_text))
        
        # MMSI
        df['mmsi_clean'] = df.get('MMSI', pd.Series()).apply(lambda x: 
            str(int(float(x))) if pd.notna(x) and str(x).replace('.0', '').isdigit() 
            and len(str(int(float(x)))) == 9 
            else None
        )
        
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
        if 'Contracting Party' in df.columns:
            df['vessel_flag_alpha3'] = df['vessel_flag_alpha3'].fillna(
                df.get('Contracting Party', pd.Series()).apply(standardize_country)
            )
        
        # NAFO external identifier
        df['nafo_id'] = df.get('NAFO ID', pd.Series()).apply(clean_text)
        if 'NAFO Number' in df.columns:
            df['nafo_id'] = df['nafo_id'].fillna(df.get('NAFO Number', pd.Series()).apply(clean_text))
        
        # Authorization status
        df['authorization_status'] = df.get('Status', pd.Series()).apply(clean_text)
        if 'Authorization Status' in df.columns:
            df['authorization_status'] = df['authorization_status'].fillna(
                df.get('Authorization Status', pd.Series()).apply(clean_text)
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
        if 'Main Gear' in df.columns:
            df['gear_type_code'] = df['gear_type_code'].fillna(
                df.get('Main Gear', pd.Series()).apply(map_gear_type)
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
        if 'Length (m)' in df.columns:
            df['length_value'] = df['length_value'].fillna(df.get('Length (m)', pd.Series()).apply(clean_numeric))
        
        df['length_metric_type'] = 'length_loa'  # Assume LOA unless specified
        df['length_unit_enum'] = 'METER'  # NAFO typically uses meters
        
        # Tonnage
        df['gross_tonnage'] = df.get('GT', pd.Series()).apply(clean_numeric)
        if 'GRT' in df.columns:
            df['gross_tonnage'] = df['gross_tonnage'].fillna(df.get('GRT', pd.Series()).apply(clean_numeric))
        if 'Gross Tonnage' in df.columns:
            df['gross_tonnage'] = df['gross_tonnage'].fillna(df.get('Gross Tonnage', pd.Series()).apply(clean_numeric))
        
        df['tonnage_metric_type'] = 'gross_tonnage'
        
        # Engine power
        df['engine_power'] = df.get('Engine Power', pd.Series()).apply(clean_numeric)
        if 'Power (KW)' in df.columns:
            df['engine_power'] = df['engine_power'].fillna(df.get('Power (KW)', pd.Series()).apply(clean_numeric))
            df['engine_power_unit_enum'] = 'KW'
        elif 'Power (HP)' in df.columns:
            df['engine_power'] = df['engine_power'].fillna(df.get('Power (HP)', pd.Series()).apply(clean_numeric))
            df['engine_power_unit_enum'] = 'HP'
        else:
            df['engine_power_unit_enum'] = 'KW'  # Default to KW
        
        # Ownership information
        df['owner_name'] = df.get('Owner', pd.Series()).apply(clean_text)
        if 'Owner Name' in df.columns:
            df['owner_name'] = df['owner_name'].fillna(df.get('Owner Name', pd.Series()).apply(clean_text))
        
        # Manager/operator
        df['manager_name'] = df.get('Manager', pd.Series()).apply(clean_text)
        if 'Operator' in df.columns:
            df['manager_name'] = df['manager_name'].fillna(df.get('Operator', pd.Series()).apply(clean_text))
        
        # NAFO divisions authorized
        df['nafo_divisions'] = df.get('NAFO Divisions', pd.Series()).apply(clean_text)
        if 'Authorized Divisions' in df.columns:
            df['nafo_divisions'] = df['nafo_divisions'].fillna(
                df.get('Authorized Divisions', pd.Series()).apply(clean_text)
            )
        
        # Species quotas
        df['species_quotas'] = df.get('Species Quotas', pd.Series()).apply(clean_text)
        if 'Quota Species' in df.columns:
            df['species_quotas'] = df['species_quotas'].fillna(
                df.get('Quota Species', pd.Series()).apply(clean_text)
            )
        
        # Notification date
        df['notification_date'] = pd.to_datetime(df.get('Notification Date', pd.Series()), errors='coerce')
        
        # Data quality filtering
        valid_records = df[
            df['vessel_name_clean'].notna() |
            df['imo_clean'].notna() |
            df['ircs_clean'].notna() |
            df['nafo_id'].notna()
        ].copy()
        
        # Filter by authorization status if available
        if 'authorization_status' in valid_records.columns:
            # Keep authorized vessels
            status_filter = valid_records['authorization_status'].str.upper().isin([
                'AUTHORIZED', 'ACTIVE', 'NOTIFIED', 'VALID'
            ])
            valid_records = valid_records[status_filter | valid_records['authorization_status'].isna()]
        
        print(f"Filtered to {len(valid_records)} valid NAFO vessel records")
        
        # Show statistics
        print(f"Vessel types: {valid_records['vessel_type_code'].value_counts().head(10).to_dict()}")
        print(f"Flags: {valid_records['vessel_flag_alpha3'].value_counts().head(10).to_dict()}")
        print(f"Gear types: {valid_records['gear_type_code'].value_counts().head().to_dict()}")
        
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
            
            # NAFO external identifiers
            'nafo_id': valid_records['nafo_id'],
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
            
            'engine_power': valid_records['engine_power'],
            'engine_power_unit_enum': valid_records['engine_power_unit_enum'],
            
            # Ownership
            'owner_name': valid_records['owner_name'],
            'manager_name': valid_records['manager_name'],
            
            # NAFO specific
            'nafo_divisions': valid_records['nafo_divisions'],
            'species_quotas': valid_records['species_quotas'],
            'notification_date': valid_records['notification_date']
        })
        
        # Convert dates to strings
        for col in ['notification_date']:
            if col in output_df.columns:
                output_df[col] = output_df[col].astype(str).replace('NaT', '')
        
        # Convert year_built to int
        if 'year_built' in output_df.columns:
            output_df['year_built'] = output_df['year_built'].astype('Int64')
        
        # Save cleaned data
        output_df.to_csv(output_file, index=False, na_rep='')
        print(f"NAFO cleaning complete: {len(output_df)} records saved")
        
        # Summary stats
        print(f"IMO coverage: {(output_df['imo'].notna().sum() / len(output_df) * 100):.1f}%")
        print(f"IRCS coverage: {(output_df['ircs'].notna().sum() / len(output_df) * 100):.1f}%")
        print(f"MMSI coverage: {(output_df['mmsi'].notna().sum() / len(output_df) * 100):.1f}%")
        print(f"Countries: {output_df['vessel_flag_alpha3'].nunique()}")
        
    except Exception as e:
        print(f"Error in NAFO cleaning: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
EOF

# Run cleaning
if python3 /tmp/clean_nafo_vessels.py "$INPUT_FILE" "$OUTPUT_FILE" "$SOURCE_DATE"; then
    RECORD_COUNT=$(tail -n +2 "$OUTPUT_FILE" | wc -l)
    COLUMN_COUNT=$(head -1 "$OUTPUT_FILE" | tr ',' '\n' | wc -l)
    log_success "✅ NAFO cleaning complete:"
    log_success "   - $RECORD_COUNT vessel records processed"
    log_success "   - $COLUMN_COUNT output columns"
    log_success "   - NAFO divisions captured"
    log_success "   - Species quotas preserved"
    log_success "   - Notification dates tracked"
    
    rm -f /tmp/clean_nafo_vessels.py
else
    log_error "NAFO cleaning failed"
    exit 1
fi