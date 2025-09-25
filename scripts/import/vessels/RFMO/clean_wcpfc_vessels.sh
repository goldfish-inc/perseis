#!/bin/bash
# /app/scripts/import/vessels/data/RFMO/clean_wcpfc_vessels.sh
# WCPFC Vessel Data Cleaning Script
set -euo pipefail

source /app/scripts/core/logging.sh

log_step "🐟 WCPFC Vessel Data Cleaning"

RAW_DATA_DIR="/import/vessels/vessel_data/RFMO/raw"
CLEANED_DATA_DIR="/import/vessels/vessel_data/RFMO/cleaned"
mkdir -p "$CLEANED_DATA_DIR" /import/logs

# Find latest WCPFC file
INPUT_FILE=$(find "$RAW_DATA_DIR" -name "*WCPFC*vessel*.csv" -o -name "*wcpfc*vessel*.csv" | head -1)
if [[ -z "$INPUT_FILE" ]]; then
    log_error "No WCPFC vessel file found in $RAW_DATA_DIR"
    exit 1
fi

OUTPUT_FILE="$CLEANED_DATA_DIR/wcpfc_vessels_cleaned.csv"
log_success "Processing: $(basename "$INPUT_FILE") → $(basename "$OUTPUT_FILE")"

# Extract source date from filename or use current date
SOURCE_DATE=$(echo "$(basename "$INPUT_FILE")" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' || date '+%Y-%m-%d')
log_success "Source date: $SOURCE_DATE"

# Create Python cleaning script for WCPFC data
cat > /tmp/clean_wcpfc_vessels.py << 'EOF'
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
        
        # Read WCPFC CSV (note: uses quoted column headers)
        df = pd.read_csv(input_file)
        print(f"Loaded {len(df)} WCPFC records with {len(df.columns)} columns")
        print(f"First few columns: {list(df.columns[:10])}")
        
        # Add metadata for source tracking
        df['source_date'] = source_date
        df['original_source'] = 'WCPFC'
        
        # Clean text helper function
        def clean_text(x):
            if pd.isna(x): return None
            s = str(x).strip()
            return s if s and s.upper() not in ['N/A', '(N/A)', 'NONE', 'UNKNOWN', '', 'NAN', 'NIL', 'NA'] else None
        
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
        
        # Country code standardization for WCPFC
        def standardize_country(country_name):
            """Convert WCPFC country names to ISO alpha3"""
            if pd.isna(country_name) or not country_name: 
                return None
                
            country_name = str(country_name).strip().upper()
            
            # WCPFC uses 2-letter codes mostly
            country_map = {
                # 2-letter to 3-letter mappings
                'CN': 'CHN',
                'JP': 'JPN',
                'KR': 'KOR',
                'US': 'USA',
                'TW': 'TWN',
                'PH': 'PHL',
                'ID': 'IDN',
                'VU': 'VUT',
                'SB': 'SLB',
                'PG': 'PNG',
                'MH': 'MHL',
                'FJ': 'FJI',
                'KI': 'KIR',
                'TV': 'TUV',
                'NR': 'NRU',
                'FM': 'FSM',
                'PW': 'PLW',
                'NZ': 'NZL',
                'AU': 'AUS',
                'FR': 'FRA',
                'ES': 'ESP',
                'EC': 'ECU',
                'SV': 'SLV',
                'PA': 'PAN',
                'LR': 'LBR',
                'BZ': 'BLZ',
                'CK': 'COK',
                'WS': 'WSM',
                'AS': 'ASM',
                'TO': 'TON',
                'NU': 'NIU',
                'TK': 'TKL',
                'NC': 'NCL',
                'PF': 'PYF',
                # Also handle full names
                'UNITED STATES': 'USA',
                'UNITED STATES OF AMERICA': 'USA',
                'USA': 'USA',
                'CHINA': 'CHN',
                'PEOPLE\'S REPUBLIC OF CHINA': 'CHN',
                'TAIWAN': 'TWN',
                'CHINESE TAIPEI': 'TWN',
                'JAPAN': 'JPN',
                'KOREA': 'KOR',
                'KOREA, REPUBLIC OF': 'KOR',
                'REPUBLIC OF KOREA': 'KOR',
                'PHILIPPINES': 'PHL',
                'INDONESIA': 'IDN',
                'VANUATU': 'VUT',
                'SOLOMON ISLANDS': 'SLB',
                'PAPUA NEW GUINEA': 'PNG',
                'MARSHALL ISLANDS': 'MHL',
                'FIJI': 'FJI',
                'KIRIBATI': 'KIR',
                'TUVALU': 'TUV',
                'NAURU': 'NRU',
                'MICRONESIA': 'FSM',
                'FSM': 'FSM',
                'FEDERATED STATES OF MICRONESIA': 'FSM',
                'PALAU': 'PLW',
                'NEW ZEALAND': 'NZL',
                'AUSTRALIA': 'AUS',
                'FRANCE': 'FRA',
                'SPAIN': 'ESP',
                'ECUADOR': 'ECU',
                'EL SALVADOR': 'SLV',
                'PANAMA': 'PAN',
                'LIBERIA': 'LBR',
                'BELIZE': 'BLZ',
                'COOK ISLANDS': 'COK',
                'SAMOA': 'WSM',
                'AMERICAN SAMOA': 'ASM',
                'TONGA': 'TON',
                'NIUE': 'NIU',
                'TOKELAU': 'TKL',
                'NEW CALEDONIA': 'NCL',
                'FRENCH POLYNESIA': 'PYF'
            }
            
            mapped = country_map.get(country_name)
            if mapped:
                return mapped
            # Check if it's already a 3-letter code
            elif len(country_name) == 3:
                return country_name
            else:
                # Try to extract from parentheses (e.g., "Country (XXX)")
                match = re.search(r'\(([A-Z]{3})\)', country_name)
                if match:
                    return match.group(1)
                return None
        
        # Vessel type mapping for WCPFC
        def map_vessel_type(vessel_type):
            """Convert WCPFC vessel types to standard codes"""
            if pd.isna(vessel_type) or not vessel_type:
                return None
            
            vessel_type = str(vessel_type).strip().upper()
            
            # WCPFC vessel type descriptions
            type_map = {
                'PURSE SEINER': 'PS',
                'PURSE SEINE': 'PS',
                'PS': 'PS',
                'LONGLINE': 'LL',
                'LONGLINER': 'LL',
                'LL': 'LL',
                'POLE AND LINE': 'BB',
                'POLE & LINE': 'BB',
                'BAITBOAT': 'BB',
                'TRAWLER': 'TO',
                'TRAWL': 'TO',
                'HANDLINE': 'LX',
                'GILLNET': 'GO',
                'CARRIER': 'SP',
                'FISH CARRIER': 'SP',
                'REFRIGERATED CARGO': 'SP',
                'REEFER': 'SP',
                'SUPPORT VESSEL': 'SP',
                'TENDER': 'SP',
                'BUNKER': 'SP',
                'MOTHERSHIP': 'MP',
                'MULTIPURPOSE': 'MP',
                'OTHER': 'NO'
            }
            
            # Check for exact matches first
            if vessel_type in type_map:
                return type_map[vessel_type]
            
            # Check for partial matches
            for key, value in type_map.items():
                if key in vessel_type:
                    return value
            
            return None
        
        # Parse authorization dates
        def parse_auth_date(date_str):
            """Parse WCPFC authorization dates"""
            if pd.isna(date_str) or not date_str:
                return None
            try:
                date_str = str(date_str).strip()
                # WCPFC uses various formats
                for fmt in ['%d/%m/%Y', '%Y-%m-%d', '%d-%b-%Y', '%m/%d/%Y', '%Y/%m/%d']:
                    try:
                        return datetime.strptime(date_str, fmt).date()
                    except:
                        continue
            except:
                pass
            return None
        
        # Process vessel identifiers
        df['vessel_name_clean'] = df.get('Name of fishing vessel', pd.Series()).apply(clean_text)
        
        # IMO processing (at the end of columns)
        df['imo_clean'] = df.get('IMO or LR number', pd.Series()).apply(lambda x: 
            str(x).strip() if pd.notna(x) and len(str(x).strip()) >= 6 
            and str(x).strip()[0:7].isdigit() 
            and str(x).strip() not in ['0', '0000000', '1111111', '9999999'] 
            else None
        )
        
        df['ircs_clean'] = df.get('International Radio Call Sign', pd.Series()).apply(clean_text)
        
        # WCPFC has vessel communication info that might include MMSI
        comm_info = df.get('Vessel communication types and numbers', pd.Series()).fillna('')
        df['mmsi_clean'] = comm_info.apply(lambda x: 
            re.search(r'MMSI[:\s]*(\d{9})', str(x)).group(1) 
            if re.search(r'MMSI[:\s]*(\d{9})', str(x)) else None
        )
        
        # National registry number
        df['national_registry_clean'] = df.get('Registration number', pd.Series()).apply(clean_text)
        
        # Vessel flag
        df['vessel_flag_alpha3'] = df.get('Flag of fishing vessel', pd.Series()).apply(standardize_country)
        
        # Previous flag
        df['previous_flag_alpha3'] = df.get('Previous flag (if any)', pd.Series()).apply(standardize_country)
        
        # WCPFC external identifiers
        df['wcpfc_vid'] = df.get('VID', pd.Series()).apply(clean_text)
        df['wcpfc_win'] = df.get('WCPFC Identification Number (WIN)', pd.Series()).apply(clean_text)
        
        # Vessel type and fishing methods
        df['vessel_type_code'] = df.get('Type of vessel', pd.Series()).apply(map_vessel_type)
        df['fishing_methods'] = df.get('Type of fishing method or methods', pd.Series()).apply(clean_text)
        
        # Port and build information
        df['port_of_registry_clean'] = df.get('Port of registry', pd.Series()).apply(clean_text)
        df['build_location'] = df.get('Where the vessel was built', pd.Series()).apply(clean_text)
        df['build_year'] = df.get('When the vessel was built', pd.Series()).apply(lambda x: 
            int(clean_numeric(x)) if clean_numeric(x) and 1900 <= clean_numeric(x) <= 2030 else None
        )
        
        # Previous names
        df['previous_names'] = df.get('Previous names (if known)', pd.Series()).apply(clean_text)
        
        # Measurements - WCPFC provides type and unit separately
        df['length_value'] = df.get('Length', pd.Series()).apply(clean_numeric)
        df['length_type'] = df.get('Type of length', pd.Series()).apply(clean_text)
        df['length_unit'] = df.get('Unit of length', pd.Series()).apply(clean_text)
        
        # Standardize length type
        df['length_metric_type'] = df['length_type'].map({
            'LOA': 'length_loa',
            'LBP': 'length_between_perpendiculars',
            'LWL': 'waterline_length',
            'REGISTERED': 'registered_length'
        })
        
        # Standardize length unit
        df['length_unit_enum'] = df['length_unit'].map({
            'METERS': 'METER',
            'METRES': 'METER',
            'M': 'METER',
            'FEET': 'FEET',
            'FT': 'FEET'
        })
        
        # Depth
        df['depth_value'] = df.get('Moulded depth', pd.Series()).apply(clean_numeric)
        df['depth_unit'] = df.get('Unit of depth', pd.Series()).apply(clean_text)
        df['depth_unit_enum'] = df['depth_unit'].map({
            'METERS': 'METER',
            'METRES': 'METER',
            'M': 'METER',
            'FEET': 'FEET',
            'FT': 'FEET'
        })
        
        # Beam
        df['beam_value'] = df.get('Beam', pd.Series()).apply(clean_numeric)
        df['beam_unit'] = df.get('Unit of Beam', pd.Series()).apply(clean_text)
        df['beam_unit_enum'] = df['beam_unit'].map({
            'METERS': 'METER',
            'METRES': 'METER',
            'M': 'METER',
            'FEET': 'FEET',
            'FT': 'FEET'
        })
        
        # Tonnage
        df['tonnage_value'] = df.get('Gross registered tonnage (GRT) or gross tonnage (GT)', pd.Series()).apply(clean_numeric)
        df['tonnage_type'] = df.get('Type of tonnage', pd.Series()).apply(clean_text)
        df['tonnage_metric_type'] = df['tonnage_type'].map({
            'GT': 'gross_tonnage',
            'GRT': 'gross_register_tonnage'
        })
        
        # Engine power
        df['engine_power'] = df.get('Power of main engine or engines', pd.Series()).apply(clean_numeric)
        df['engine_power_unit'] = df.get('Unit of power of main engine or engines', pd.Series()).apply(clean_text)
        df['engine_power_unit_enum'] = df['engine_power_unit'].map({
            'HP': 'HP',
            'HORSEPOWER': 'HP',
            'KW': 'KW',
            'KILOWATTS': 'KW'
        })
        
        # Freezing and hold capacity
        df['freezer_types'] = df.get('Freezer type(s)', pd.Series()).apply(clean_text)
        df['freezing_capacity'] = df.get('Freezing capacity', pd.Series()).apply(clean_numeric)
        df['freezing_capacity_unit'] = df.get('Units of freezing capacity', pd.Series()).apply(clean_text)
        df['freezer_units'] = df.get('Number of freezer units', pd.Series()).apply(clean_numeric)
        df['fish_hold_capacity'] = df.get('Fish hold capacity', pd.Series()).apply(clean_numeric)
        df['fish_hold_capacity_unit'] = df.get('Units of fish hold capacity', pd.Series()).apply(clean_text)
        
        # Crew information
        df['crew_complement'] = df.get('Normal crew complement', pd.Series()).apply(clean_numeric)
        df['master_name'] = df.get('Name of the master', pd.Series()).apply(clean_text)
        df['master_nationality'] = df.get('Nationality of the master', pd.Series()).apply(standardize_country)
        
        # Ownership information
        df['owner_name'] = df.get('Name of the owner or owners', pd.Series()).apply(clean_text)
        df['owner_address'] = df.get('Address of the owner or owners', pd.Series()).apply(clean_text)
        
        # Charter information (WCPFC tracks charter arrangements)
        df['charterer_name'] = df.get('Name of charterer', pd.Series()).apply(clean_text)
        df['charterer_address'] = df.get('Address of charterer', pd.Series()).apply(clean_text)
        df['charter_start'] = df.get('Start date of charter', pd.Series()).apply(parse_auth_date)
        df['charter_end'] = df.get('Expiration date of charter', pd.Series()).apply(parse_auth_date)
        
        # Authorization details
        df['auth_form'] = df.get('Form of the authorization granted by the flag State', pd.Series()).apply(clean_text)
        df['auth_number'] = df.get('Authorization number granted by the flag State', pd.Series()).apply(clean_text)
        df['auth_areas'] = df.get('Any specific areas in which authorized to fish', pd.Series()).apply(clean_text)
        df['auth_species'] = df.get('Any specific species for which authorized to fish', pd.Series()).apply(clean_text)
        df['auth_from_date'] = df.get('Start of period of validity of authorization', pd.Series()).apply(parse_auth_date)
        df['auth_to_date'] = df.get('End of period of validity of authorization', pd.Series()).apply(parse_auth_date)
        
        # Transshipment authorization
        df['tranship_high_seas'] = df.get('Authorized to tranship on the high seas', pd.Series()).apply(clean_text)
        df['tranship_at_sea'] = df.get('Purse seine vessel authorized to tranship at sea', pd.Series()).apply(clean_text)
        
        # CCM information (Cooperating Commission Members)
        df['submitted_by_ccm'] = df.get('Submitted by CCM', pd.Series()).apply(clean_text)
        df['host_ccm'] = df.get('Host CCM', pd.Series()).apply(clean_text)
        
        # Photo URL
        df['vessel_photo'] = df.get('Colour photograph of the vessel (link)', pd.Series()).apply(clean_text)
        
        # Deletion reason (if any)
        df['deletion_reason'] = df.get('Reason for deletion', pd.Series()).apply(clean_text)
        
        # Data quality filtering
        valid_records = df[
            df['vessel_name_clean'].notna() |
            df['imo_clean'].notna() |
            df['ircs_clean'].notna() |
            df['wcpfc_vid'].notna()
        ].copy()
        
        print(f"Filtered to {len(valid_records)} valid WCPFC vessel records")
        
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
            
            # WCPFC external identifiers
            'wcpfc_vid': valid_records['wcpfc_vid'],
            'wcpfc_win': valid_records['wcpfc_win'],
            
            # Vessel characteristics
            'vessel_type_code': valid_records['vessel_type_code'],
            'fishing_methods': valid_records['fishing_methods'],
            'port_of_registry': valid_records['port_of_registry_clean'],
            'build_location': valid_records['build_location'],
            'build_year': valid_records['build_year'],
            'previous_names': valid_records['previous_names'],
            'previous_flag': valid_records['previous_flag_alpha3'],
            
            # Measurements
            'length_value': valid_records['length_value'],
            'length_metric_type': valid_records['length_metric_type'],
            'length_unit_enum': valid_records['length_unit_enum'],
            
            'depth_value': valid_records['depth_value'],
            'depth_unit_enum': valid_records['depth_unit_enum'],
            
            'beam_value': valid_records['beam_value'],
            'beam_unit_enum': valid_records['beam_unit_enum'],
            
            'tonnage_value': valid_records['tonnage_value'],
            'tonnage_metric_type': valid_records['tonnage_metric_type'],
            
            'engine_power': valid_records['engine_power'],
            'engine_power_unit_enum': valid_records['engine_power_unit_enum'],
            
            # Capacity
            'freezer_types': valid_records['freezer_types'],
            'freezing_capacity': valid_records['freezing_capacity'],
            'freezing_capacity_unit': valid_records['freezing_capacity_unit'],
            'freezer_units': valid_records['freezer_units'],
            'fish_hold_capacity': valid_records['fish_hold_capacity'],
            'fish_hold_capacity_unit': valid_records['fish_hold_capacity_unit'],
            
            # Crew
            'crew_complement': valid_records['crew_complement'],
            'master_name': valid_records['master_name'],
            'master_nationality': valid_records['master_nationality'],
            
            # Ownership
            'owner_name': valid_records['owner_name'],
            'owner_address': valid_records['owner_address'],
            
            # Charter
            'charterer_name': valid_records['charterer_name'],
            'charterer_address': valid_records['charterer_address'],
            'charter_start': valid_records['charter_start'],
            'charter_end': valid_records['charter_end'],
            
            # Authorization
            'auth_form': valid_records['auth_form'],
            'auth_number': valid_records['auth_number'],
            'auth_areas': valid_records['auth_areas'],
            'auth_species': valid_records['auth_species'],
            'auth_from_date': valid_records['auth_from_date'],
            'auth_to_date': valid_records['auth_to_date'],
            
            # Transshipment
            'tranship_high_seas': valid_records['tranship_high_seas'],
            'tranship_at_sea': valid_records['tranship_at_sea'],
            
            # CCM
            'submitted_by_ccm': valid_records['submitted_by_ccm'],
            'host_ccm': valid_records['host_ccm'],
            
            # Metadata
            'vessel_photo': valid_records['vessel_photo'],
            'deletion_reason': valid_records['deletion_reason']
        })
        
        # Convert dates to strings for CSV, handling None and NaT properly
        for col in ['charter_start', 'charter_end', 'auth_from_date', 'auth_to_date']:
            if col in output_df.columns:
                output_df[col] = output_df[col].astype(str).replace({'NaT': '', 'None': '', 'nan': ''})
        
        # Convert numeric fields
        for col in ['build_year', 'freezer_units', 'crew_complement']:
            if col in output_df.columns:
                output_df[col] = output_df[col].astype('Int64')
        
        # Save cleaned data
        output_df.to_csv(output_file, index=False, na_rep='')
        print(f"WCPFC cleaning complete: {len(output_df)} records saved")
        
        # Summary stats
        print(f"IMO coverage: {(output_df['imo'].notna().sum() / len(output_df) * 100):.1f}%")
        print(f"IRCS coverage: {(output_df['ircs'].notna().sum() / len(output_df) * 100):.1f}%")
        print(f"Active authorizations: {(output_df['auth_to_date'] >= str(datetime.now().date())).sum()}")
        print(f"Countries: {output_df['vessel_flag_alpha3'].nunique()}")
        print(f"Chartered vessels: {output_df['charterer_name'].notna().sum()}")
        
    except Exception as e:
        print(f"Error in WCPFC cleaning: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
EOF

# Run cleaning
if python3 /tmp/clean_wcpfc_vessels.py "$INPUT_FILE" "$OUTPUT_FILE" "$SOURCE_DATE"; then
    RECORD_COUNT=$(tail -n +2 "$OUTPUT_FILE" | wc -l)
    COLUMN_COUNT=$(head -1 "$OUTPUT_FILE" | tr ',' '\n' | wc -l)
    log_success "✅ WCPFC cleaning complete:"
    log_success "   - $RECORD_COUNT vessel records processed"
    log_success "   - $COLUMN_COUNT output columns"
    log_success "   - Charter arrangements captured"
    log_success "   - Detailed authorization data preserved"
    log_success "   - Crew and master information retained"
    
    rm -f /tmp/clean_wcpfc_vessels.py
else
    log_error "WCPFC cleaning failed"
    exit 1
fi