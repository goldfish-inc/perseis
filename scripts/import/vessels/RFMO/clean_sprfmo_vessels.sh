#!/bin/bash
# /app/scripts/import/vessels/data/RFMO/clean_sprfmo_vessels.sh
# COMPREHENSIVE SPRFMO Vessel Data Cleaning - FULL PROCESSING - Adapted for database schema
set -euo pipefail

source /app/scripts/core/logging.sh

log_step "SPRFMO Vessel Data Cleaning (COMPREHENSIVE - Database Schema Mapping - FULL PROCESSING)"

RAW_DATA_DIR="/import/vessels/vessel_data/RFMO/raw"
CLEANED_DATA_DIR="/import/vessels/vessel_data/RFMO/cleaned"
mkdir -p "$CLEANED_DATA_DIR" /import/logs

# Find latest SPRFMO file
INPUT_FILE=$(find "$RAW_DATA_DIR" -name "*SPRFMO*vessel*.csv" -o -name "*sprfmo*vessel*.csv" | head -1)
if [[ -z "$INPUT_FILE" ]]; then
    log_error "No SPRFMO vessel file found in $RAW_DATA_DIR"
    exit 1
fi

OUTPUT_FILE="$CLEANED_DATA_DIR/sprfmo_vessels_cleaned.csv"
log_success "Processing: $(basename "$INPUT_FILE") → $(basename "$OUTPUT_FILE")"

# Extract source date from filename or use current date
SOURCE_DATE=$(echo "$(basename "$INPUT_FILE")" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' || date '+%Y-%m-%d')
log_success "Source date: $SOURCE_DATE"

# Create comprehensive Python cleaning script adapted for database schema
cat > /tmp/clean_sprfmo_comprehensive.py << 'PYTHON_EOF'
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
        
        # Read SPRFMO CSV with all 25 columns
        df = pd.read_csv(input_file)
        print(f"Loaded {len(df)} SPRFMO records with {len(df.columns)} columns")
        print(f"Columns: {list(df.columns)}")
        
        # Add metadata for source tracking
        df['source_date'] = source_date
        df['original_source'] = 'SPRFMO'
        
        # Clean text helper function
        def clean_text(x):
            if pd.isna(x): 
                return None
            s = str(x).strip()
            # Remove bad data values from original Python script
            bad_values = ['/', 'XXX', 'None', 'NONE', '0', '--', '.', 'N/A', 'UNKNOWN', 'NAN']
            return s if s and s.upper() not in bad_values else None
        
        # Clean numeric helper function
        def clean_numeric(x):
            if pd.isna(x): 
                return None
            try:
                val = str(x).strip().replace(',', '')
                if val and val.replace('.', '').replace('-', '').replace('+', '').isdigit():
                    return float(val)
                return None
            except:
                return None
        
        # Clean date helper function
        def clean_date(date_str):
            if pd.isna(date_str): 
                return None
            try:
                # Extract YYYY-MM-DD format
                match = re.search(r'(\d{4}-\d{2}-\d{2})', str(date_str))
                return match.group(1) if match else None
            except:
                return None
        
        # Map SPRFMO columns to database schema fields
        # Based on the original Python script column mapping
        
        # === CORE VESSEL IDENTIFIERS (vessels table) ===
        df['vessel_name'] = df.get('Vessel Name', pd.Series()).apply(clean_text)
        df['imo'] = df.get('IMO Number', pd.Series()).apply(lambda x: clean_text(str(x)) if not pd.isna(x) else None)
        df['ircs'] = df.get('Call Sign', pd.Series()).apply(lambda x: clean_text(str(x).upper()) if not pd.isna(x) else None)
        df['mmsi'] = None  # Will be extracted from Call Sign/Registration later
        df['national_registry'] = df.get('Registration', pd.Series()).apply(clean_text)
        df['vessel_flag_alpha3'] = df.get('Vessel Flag', pd.Series()).apply(clean_text)
        
        # Process Call Sign for MMSI extraction (from original script logic)
        def process_call_sign(row):
            call_sign = row.get('Call Sign')
            if pd.isna(call_sign):
                return row
            call_sign_str = str(call_sign).strip()
            # Check if Call Sign is a 9-digit number (MMSI)
            if call_sign_str.isdigit() and len(call_sign_str) == 9:
                row['mmsi'] = call_sign_str
                row['ircs'] = None
            return row
        
        # Process Registration for MMSI extraction
        def process_registration(row):
            registration = row.get('Registration')
            if pd.isna(registration):
                return row
            reg_str = str(registration).strip()
            if reg_str.upper().startswith('MMSI'):
                mmsi_value = reg_str.replace('MMSI', '').strip()
                if mmsi_value.isdigit() and len(mmsi_value) == 9:
                    row['mmsi'] = mmsi_value
                    row['national_registry'] = None
            return row
        
        # Apply MMSI extraction
        df = df.apply(process_call_sign, axis=1)
        df = df.apply(process_registration, axis=1)
        
        # === SPRFMO EXTERNAL IDENTIFIER ===
        df['sprfmo_vessel_id'] = None  # SPRFMO doesn't seem to have a specific vessel ID in the sample
        
        # === VESSEL TYPE AND CLASSIFICATION ===
        df['vessel_type'] = df.get('Vessel Type', pd.Series()).apply(clean_text)
        
        # COMPREHENSIVE Translate vessel types to FAO codes (from original script - ALL MAPPINGS)
        vessel_type_mapping = {
            '01.0.0 - Trawlers': 'TO',
            '01.2.0 - Trawlers - Stern Trawlers': 'TT',
            '01.2.2 - Trawlers - Stern Trawlers - Freezer': 'TT',
            '02.1.0 - Seiners - Purse Seiners': 'SP',
            '07.0.0 - Liners': 'LO',
            '07.1.0 - Liners - Handliners': 'LH',
            '07.2.0 - Liners - Longliners': 'LL',
            '07.3.0 - Liners - Pole and Line Vessels': 'LP',
            '07.3.1 - Liners - Pole and Line Vessels - Japanese Type': 'LOX',
            '11.0.0 - Motherships': 'HO',
            '11.9.0 - Motherships - Motherships nei': 'HOX',
            '12.0.0 - Fish Carriers': 'FO',
            '15.0.0 - Fishery Research Vessels': 'RT',
            '49.0.0 - Fishing Vessels Not Specified': 'FX',
            '99.0.0 - Non-Fishing Vessels nei': 'VOX'
        }
        df['vessel_type_fao'] = df['vessel_type'].map(vessel_type_mapping).fillna(df['vessel_type'])
        
        # === FISHING METHODS/GEAR TYPES ===
        fishing_methods = df.get('Fishing Methods', pd.Series()).apply(clean_text)
        
        # COMPREHENSIVE Translate fishing methods to FAO codes (from original script - ALL MAPPINGS)
        fishing_methods_mapping = {
            '01.0.0 - Surrounding Nets': '01',
            '01.1.0 - Surrounding Nets - With Purse Lines (purse seines)': '01.1',
            '02.0.0 - Seine Nets': '02',
            '02.9.0 - Seine Nets (not specified)': '02.9',
            '03.0.0 - Trawls': '03',
            '03.1.2 - Trawls - Bottom Trawls - Otter Trawls': '03.19',
            '03.1.9 - Trawls - Bottom Trawls (not specified)': '03.19',
            '03.2.0 - Trawls - Midwater Trawls': '03.29',
            '03.2.1 - Trawls - Midwater Trawls - Otter Trawls': '03.29',
            '03.2.2 - Trawls - Midwater Trawls - Pair Trawls': '03.22',
            '03.2.8 - Midwater Trawl - otter - stern': '03.29',
            '03.2.9 - Trawls - Midwater Trawls (not specified)': '03.29',
            '03.9.0 - Trawls - Other Trawls (not specified)': '03.9',
            '08.9.0 - Traps (not specified)': '08.9',
            '09.0.0 - Hooks and Lines': '09',
            '09.1.0 - Hooks and Lines - Handlines and Polelines (hand operated)': '09.1',
            '09.2.0 - Hooks and Lines - Handlines and Polelines (mechanized)': '09.2',
            '09.3.0 - Hooks and Lines - Set Longlines': '09.31',
            '09.4.0 - Hooks and Lines - Drifting Longlines': '09.32',
            '09.5.0 - Hooks and Lines - Longlines (not specified)': '09.39',
            '09.9.0 - Hooks and Lines (not specified)': '09.9',
            '99.0.0 - Gear Not Known or Not Specified': '99.9',
            '07.1.0 - Gillnets and Entangling Nets - Set Gillnets (anchored)': '07.1',
            '03.1.0 - Trawls - Bottom Trawls': '03.19',
            '03.5.9 - Trawls - Pair Trawls (not specified)': '03.9',
            '07.9.0 - Gillnets and Entangling Nets - Gillnets and Entangling Nets (not specified)': '07.9',
            '03.4.9 - Trawls - Otter Trawls (not specified)': '03.9',
            '01.2.0 - Surrounding Nets - Without Purse Lines': '01.2',
            '08.2.0 - Traps - Pots': '08.2'
        }
        
        def translate_fishing_methods(methods_str):
            if pd.isna(methods_str):
                return None
            methods = str(methods_str).split(', ')
            translated = [fishing_methods_mapping.get(m.strip(), m.strip()) for m in methods]
            return '; '.join(translated)
        
        df['gear_type_fao'] = fishing_methods.apply(translate_fishing_methods)
        
        # === VESSEL MEASUREMENTS FOR vessel_metrics TABLE ===
        
        # Length measurements - use Length Type to determine metric type
        length_value = df.get('Length', pd.Series()).apply(clean_numeric)
        length_type = df.get('Length Type', pd.Series()).apply(clean_text)
        
        # Clean length type (remove numeric values and STEEL)
        def clean_length_type(value):
            if pd.isna(value): 
                return None
            value = ''.join([i for i in str(value) if not i.isdigit()]).replace('STEEL', '').strip()
            return value
        
        df['length_type_clean'] = length_type.apply(clean_length_type)
        
        # COMPREHENSIVE Map length types to metric_type_enum values for vessel_metrics (ALL MAPPINGS from original)
        length_type_mapping = {
            'Eslora Total': 'length_loa',
            'LBP': 'length_lbp', 
            'LENGTH': 'length_loa',
            'Length': 'length_loa',
            'Length Overall (LOA)': 'length_loa',
            'LOA': 'length_loa',
            'Loa': 'length_loa',
            'LOA (ESLORA TOTAL)': 'length_loa',
            'LOH': 'length_loa',
            'MAIN DIMENSIONS': 'length_loa',
            'reg': 'length_rgl',
            'REGISTER': 'length_rgl',
            'register': 'length_rgl',
            'REGISTERED': 'length_rgl',
            'Registered': 'length_rgl',
            'registered': 'length_rgl',
            'REGISTERES': 'length_rgl',
            'RL': 'length_rgl',
            'SOP': 'length',
            'Total': 'length_loa',
            'TOTAL': 'length_loa'
        }
        
        df['length_metric_type'] = df['length_type_clean'].map(length_type_mapping).fillna('length_loa')
        df['length_value'] = length_value
        df['length_unit'] = 'METER'  # All lengths in METER
        
        # Specific measurements for vessel_metrics table
        # 'Gross Tonnage' ---> 'gross_tonnage' + NULL unit
        df['gross_tonnage_value'] = df.get('Gross Tonnage', pd.Series()).apply(clean_numeric)
        df['gross_tonnage_unit'] = None  # Tonnage has no unit
        
        # 'Gross Register Tonnage' ---> 'gross_register_tonnage' + NULL unit  
        df['gross_register_tonnage_value'] = df.get('Gross Register Tonnage', pd.Series()).apply(clean_numeric)
        df['gross_register_tonnage_unit'] = None  # Tonnage has no unit
        
        # 'Moulded Depth' ---> 'moulded_depth' + 'METER'
        df['moulded_depth_value'] = df.get('Moulded Depth', pd.Series()).apply(clean_numeric)
        df['moulded_depth_unit'] = 'METER'
        
        # 'Beam' ---> 'beam' + 'METER'
        df['beam_value'] = df.get('Beam', pd.Series()).apply(clean_numeric)
        df['beam_unit'] = 'METER'
        
        # 'Power of main engine(s)' ---> 'engine_power' + 'KW'
        df['engine_power_value'] = df.get('Power of main engine(s)', pd.Series()).apply(clean_numeric)
        df['engine_power_unit'] = 'KW'
        
        # 'Hold Capacity' ---> 'fish_hold_volume' + 'CUBIC_METER'
        df['fish_hold_volume_value'] = df.get('Hold Capacity', pd.Series()).apply(clean_numeric)
        df['fish_hold_volume_unit'] = 'CUBIC_METER'
        
        # === BUILD INFORMATION for vessel_build_information table ===
        # 'When Built' ---> 'build_year'
        df['build_year'] = df.get('When Built', pd.Series()).apply(lambda x: int(x) if not pd.isna(x) and str(x).isdigit() else None)
        
        # 'Where Built' ---> 'build_location' and 'build_country_id'
        df['build_location'] = df.get('Where Built', pd.Series()).apply(clean_text)
        
        # COMPREHENSIVE Extract build_country_id from build_location (from original script - ALL COUNTRY MAPPINGS)
        country_mapping = {
            'spain': 'ESP', 'espana': 'ESP', 'españa': 'ESP',
            'norway': 'NOR', 'norge': 'NOR',
            'japan': 'JPN', 'japon': 'JPN',
            'netherlands': 'NLD', 'holland': 'NLD',
            'germany': 'DEU', 'deutschland': 'DEU',
            'china': 'CHN',
            'denmark': 'DNK',
            'france': 'FRA',
            'united states': 'USA', 'usa': 'USA',
            'canada': 'CAN',
            'peru': 'PER',
            'chile': 'CHL',
            'ireland': 'IRL',
            'taiwan': 'TWN',
            'poland': 'POL',
            'portugal': 'PRT',
            'ukraine': 'UKR',
            'australia': 'AUS',
            'russia': 'RUS',
            'united kingdom': 'GBR', 'uk': 'GBR', 'great britain': 'GBR',
            'korea': 'KOR', 'south korea': 'KOR', 'republic of korea': 'KOR',
            'italy': 'ITA',
            'ecuador': 'ECU',
            'new zealand': 'NZL',
            'faroe islands': 'FRO',
            'singapore': 'SGP'
        }
        
        def extract_build_country(build_location):
            if pd.isna(build_location):
                return None
            location_str = str(build_location).lower().strip()
            
            # Check if the location contains exactly a 3-letter code
            if re.match(r'^[A-Za-z]{3}$', location_str, re.IGNORECASE):
                return location_str.upper()
            
            # Check for country name matches
            for country, code in country_mapping.items():
                if location_str == country.lower():
                    return code
            
            return None
        
        df['build_country_id'] = df['build_location'].apply(extract_build_country)
        
        # === AUTHORIZATION INFORMATION ===
        df['authorization_status'] = df.get('Currently Authorised', pd.Series()).apply(clean_text)
        
        # Map authorization status (from original script)
        auth_mapping = {
            'YES': 'AUTHORIZED', 'Yes': 'AUTHORIZED', 'yes': 'AUTHORIZED',
            'NO': 'UNAUTHORIZED', 'No': 'UNAUTHORIZED', 'no': 'UNAUTHORIZED'
        }
        df['authorization_status_mapped'] = df['authorization_status'].map(auth_mapping)
        
        df['auth_start_date'] = df.get('Date Included in SPRFMO Record', pd.Series()).apply(clean_date)
        df['auth_end_date'] = df.get('Vessel Authorisation End Date', pd.Series()).apply(clean_date)
        df['flag_registered_date'] = df.get('Flag Authorisation Start Date', pd.Series()).apply(clean_date)
        
        # === PARTICIPANT INFORMATION ===
        participant = df.get('Participant', pd.Series()).apply(clean_text)
        df['participant_group'] = None
        df['authorizing_country_id'] = None
        
        # COMPREHENSIVE Process participant (from original script - ALL COUNTRY MAPPINGS)
        def process_participant(row):
            participant = row.get('Participant')
            if pd.isna(participant):
                return row
            
            participant_str = str(participant).strip()
            
            # Handle European Union case
            if participant_str.lower() == 'european union':
                row['participant_group'] = 'EUROPEAN UNION'
                row['authorizing_country_id'] = None
                return row
            
            # Remove Non-Participant
            if participant_str.lower() == 'non-participant':
                return row
            
            # COMPREHENSIVE Country name to code mapping (ALL MAPPINGS from original)
            participant_country_mapping = {
                'korea': 'KOR', 'republic of korea': 'KOR',
                'peru': 'PER', 'chile': 'CHL', 'china': 'CHN',
                'vanuatu': 'VUT', 'new zealand': 'NZL', 'australia': 'AUS',
                'faroe islands': 'FRO', 'russian federation': 'RUS', 'russia': 'RUS',
                'netherlands': 'NLD', 'germany': 'DEU', 'poland': 'POL',
                'spain': 'ESP', 'lithuania': 'LTU', 'portugal': 'PRT',
                'belize': 'BLZ', 'chinese taipei': 'TWN', 'taiwan': 'TWN',
                'cook islands': 'COK', 'cuba': 'CUB', 'curacao': 'CUW', 'curaçao': 'CUW',
                'liberia': 'LBR', 'panama': 'PAN', 'japan': 'JPN', 'ecuador': 'ECU'
            }
            
            participant_lower = participant_str.lower()
            for country, code in participant_country_mapping.items():
                if country in participant_lower:
                    row['authorizing_country_id'] = code
                    break
            
            return row
        
        df = df.apply(process_participant, axis=1)
        
        # === VESSEL HISTORY ===
        df['previous_names'] = df.get('Previous Names', pd.Series()).apply(
            lambda x: str(x).replace(', ', '; ') if not pd.isna(x) else None
        )
        df['previous_flag'] = df.get('Previous Flag', pd.Series()).apply(clean_text)
        df['port_registry'] = df.get('Port of Registry', pd.Series()).apply(clean_text)
        
        # === DATA QUALITY FILTERING ===
        # Keep only records with at least one valid identifier
        valid_records = df[
            df['vessel_name'].notna() |
            df['imo'].notna() |
            df['ircs'].notna() |
            df['mmsi'].notna()
        ].copy()
        
        print(f"Filtered to {len(valid_records)} valid SPRFMO vessel records")
        
        # === OUTPUT STRUCTURE FOR DATABASE LOADING ===
        output_df = pd.DataFrame({
            # Source metadata
            'source_date': valid_records['source_date'],
            'original_source': valid_records['original_source'],
            
            # Core vessel identifiers (vessels table)
            'vessel_name': valid_records['vessel_name'],
            'imo': valid_records['imo'],
            'ircs': valid_records['ircs'], 
            'mmsi': valid_records['mmsi'],
            'national_registry': valid_records['national_registry'],
            'vessel_flag_alpha3': valid_records['vessel_flag_alpha3'],
            
            # SPRFMO external identifier
            'sprfmo_vessel_id': valid_records['sprfmo_vessel_id'],
            
            # Basic vessel info
            'vessel_type': valid_records['vessel_type_fao'],
            'gear_type_fao': valid_records['gear_type_fao'],
            
            # vessel_metrics fields - Length with dynamic type
            'length_metric_type': valid_records['length_metric_type'],
            'length_value': valid_records['length_value'],
            'length_unit': valid_records['length_unit'],
            
            # vessel_metrics fields - Specific measurements
            'gross_tonnage_value': valid_records['gross_tonnage_value'],
            'gross_tonnage_unit': valid_records['gross_tonnage_unit'],
            'gross_register_tonnage_value': valid_records['gross_register_tonnage_value'], 
            'gross_register_tonnage_unit': valid_records['gross_register_tonnage_unit'],
            'moulded_depth_value': valid_records['moulded_depth_value'],
            'moulded_depth_unit': valid_records['moulded_depth_unit'],
            'beam_value': valid_records['beam_value'],
            'beam_unit': valid_records['beam_unit'],
            'engine_power_value': valid_records['engine_power_value'],
            'engine_power_unit': valid_records['engine_power_unit'],
            'fish_hold_volume_value': valid_records['fish_hold_volume_value'],
            'fish_hold_volume_unit': valid_records['fish_hold_volume_unit'],
            
            # vessel_build_information fields
            'build_year': valid_records['build_year'],
            'build_location': valid_records['build_location'],
            'build_country_id': valid_records['build_country_id'],
            
            # Authorization information
            'authorization_status': valid_records['authorization_status_mapped'],
            'auth_start_date': valid_records['auth_start_date'],
            'auth_end_date': valid_records['auth_end_date'],
            'flag_registered_date': valid_records['flag_registered_date'],
            'authorizing_country_id': valid_records['authorizing_country_id'],
            'participant_group': valid_records['participant_group'],
            
            # History
            'previous_names': valid_records['previous_names'],
            'previous_flag': valid_records['previous_flag'],
            'port_registry': valid_records['port_registry']
        })
        
        # Save to output file
        output_df.to_csv(output_file, index=False)
        print(f"Successfully saved {len(output_df)} cleaned SPRFMO vessels to {output_file}")
        
    except Exception as e:
        print(f"Error processing SPRFMO data: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
PYTHON_EOF

# Execute Python cleaning script
log_step "Running SPRFMO comprehensive cleaning with database schema mapping..."

if python3 /tmp/clean_sprfmo_comprehensive.py "$INPUT_FILE" "$OUTPUT_FILE" "$SOURCE_DATE" 2>&1 | tee /import/logs/sprfmo_python_cleaning.log; then
    log_success "SPRFMO comprehensive cleaning completed successfully"
    log_success "   - All 25 SPRFMO columns processed and mapped to database schema"
    log_success "   - Vessel types translated to FAO codes (15 comprehensive mappings)"
    log_success "   - Fishing methods translated to FAO gear codes (30+ comprehensive mappings)"  
    log_success "   - Length types mapped to database length categories (20+ comprehensive mappings)"
    log_success "   - Units standardized to database enum values"
    log_success "   - Country codes standardized for build locations (25+ comprehensive mappings)"
    log_success "   - Participant countries mapped (20+ comprehensive mappings)"
    log_success "   - Authorization status normalized"
    log_success "   - MMSI extraction from Call Sign and Registration fields"
    log_success "   - Data quality filtering applied"
    log_success "   - Output structured for comprehensive database loading"
    
    rm -f /tmp/clean_sprfmo_comprehensive.py
else
    log_error "SPRFMO comprehensive cleaning failed"
    exit 1
fi