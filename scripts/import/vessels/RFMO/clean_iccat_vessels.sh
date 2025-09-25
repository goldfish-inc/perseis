#!/bin/bash
# /app/scripts/import/vessels/data/RFMO/clean_iccat_vessels.sh
# ICCAT Vessel Data Cleaning Script
set -euo pipefail

source /app/scripts/core/logging.sh

log_step "🐟 ICCAT Vessel Data Cleaning"

RAW_DATA_DIR="/import/vessels/vessel_data/RFMO/raw"
CLEANED_DATA_DIR="/import/vessels/vessel_data/RFMO/cleaned"
mkdir -p "$CLEANED_DATA_DIR" /import/logs

# Find latest ICCAT file
INPUT_FILE=$(find "$RAW_DATA_DIR" -name "*ICCAT*vessel*.csv" -o -name "*iccat*vessel*.csv" | head -1)
if [[ -z "$INPUT_FILE" ]]; then
    log_error "No ICCAT vessel file found in $RAW_DATA_DIR"
    exit 1
fi

OUTPUT_FILE="$CLEANED_DATA_DIR/iccat_vessels_cleaned.csv"
log_success "Processing: $(basename "$INPUT_FILE") → $(basename "$OUTPUT_FILE")"

# Extract source date from filename or use current date
SOURCE_DATE=$(echo "$(basename "$INPUT_FILE")" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' || date '+%Y-%m-%d')
log_success "Source date: $SOURCE_DATE"

# Create Python cleaning script for ICCAT data
cat > /tmp/clean_iccat_vessels.py << 'EOF'
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
        
        # Read ICCAT CSV 
        df = pd.read_csv(input_file)
        print(f"Loaded {len(df)} ICCAT records with {len(df.columns)} columns")
        print(f"Columns: {list(df.columns)[:20]}...")  # First 20 columns
        
        # Add metadata for source tracking
        df['source_date'] = source_date
        df['original_source'] = 'ICCAT'
        
        # Clean text helper function
        def clean_text(x):
            if pd.isna(x): return None
            s = str(x).strip()
            return s if s and s.upper() not in ['N/A', '(N/A)', 'NONE', 'UNKNOWN', '', 'NAN'] else None
        
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
        
        # Standardize units to match database unit_enum
        def standardize_unit(unit_text, measurement_type='length'):
            """Convert unit names to unit_enum values"""
            if pd.isna(unit_text) or not unit_text:
                return 'METER' if measurement_type == 'length' else 'GT' if measurement_type == 'tonnage' else 'HP'
                
            unit_text = str(unit_text).strip().upper()
            
            # Unit mapping to match database enum values
            unit_map = {
                # Length units
                'M': 'METER', 'METER': 'METER', 'METERS': 'METER',
                'FT': 'FEET', 'FEET': 'FEET',
                
                # Tonnage units  
                'GT': 'GT', 'GRT': 'GRT', 'NT': 'NT', 'NRT': 'NRT',
                
                # Power units
                'HP': 'HP', 'HORSEPOWER': 'HP',
                'KW': 'KW', 'KILOWATTS': 'KW',
                
                # Volume units
                'M3': 'CUBIC_METER', 'CUBIC METERS': 'CUBIC_METER',
                'CUBIC FEET': 'CUBIC_FEET'
            }
            
            return unit_map.get(unit_text, 'METER' if measurement_type == 'length' else 'GT')
        
        # Country code standardization for ICCAT
        def standardize_country(country_code):
            """Convert ICCAT country codes to ISO alpha3"""
            if pd.isna(country_code) or not country_code: 
                return None
                
            country_code = str(country_code).strip().upper()
            
            # ICCAT-specific country mappings
            country_map = {
                # ICCAT uses some non-standard codes
                'EU-ESP': 'ESP',
                'EU-FRA': 'FRA',
                'EU-ITA': 'ITA',
                'EU-PRT': 'PRT',
                'EU-GRC': 'GRC',
                'EU-MLT': 'MLT',
                'EU-CYP': 'CYP',
                'EU-HRV': 'HRV',
                'EU-IRL': 'IRL',
                'EU-NLD': 'NLD',
                'EU': 'EUE',  # European Union as entity
                'UK': 'GBR',
                'CHINESE TAIPEI': 'TWN',
                'KOREA REP': 'KOR',
                'S VINCENT': 'VCT',
                'S TOME PRN': 'STP',
                'T AND T': 'TTO',
                'ICCAT-RMA': None  # Regional management entity
            }
            
            # Check if it's an EU country code
            if country_code.startswith('EU-'):
                mapped = country_map.get(country_code)
                if mapped:
                    return mapped
                # Try to extract country code after EU-
                parts = country_code.split('-')
                if len(parts) > 1:
                    return parts[1]
            
            # Direct mapping or return original if 3 chars
            mapped = country_map.get(country_code)
            if mapped:
                return mapped
            elif len(country_code) == 3:
                return country_code
            else:
                return None
        
        # Vessel type standardization for ICCAT
        def standardize_vessel_type(vessel_type_code):
            """Convert ICCAT vessel type codes"""
            if pd.isna(vessel_type_code) or not vessel_type_code:
                return None
                
            vessel_type_code = str(vessel_type_code).strip().upper()
            
            # ICCAT vessel type codes (IsscfvCode column)
            # These are already standardized ISSCFV codes, so mostly pass through
            # but validate against known codes
            valid_types = {
                'AU', 'BB', 'BO', 'DB', 'DD', 'DO', 'FO', 'FX', 'GO', 
                'HL', 'HO', 'KB', 'LL', 'LN', 'LO', 'LP', 'LT', 'LX',
                'MO', 'MP', 'MS', 'MT', 'NB', 'NO', 'NX', 'PS', 'PT',
                'PU', 'RT', 'SA', 'SB', 'SO', 'SP', 'SS', 'SV', 'SX',
                'TO', 'TP', 'TS', 'TT', 'TU', 'TX', 'VO', 'WO', 'WX'
            }
            
            if vessel_type_code in valid_types:
                return vessel_type_code
            else:
                return None
        
        # Gear type standardization for ICCAT
        def standardize_gear_type(gear_code):
            """Convert ICCAT gear codes"""
            if pd.isna(gear_code) or not gear_code:
                return None
                
            gear_code = str(gear_code).strip().upper()
            
            # ICCAT gear codes (IsscfgCode column)
            # These are FAO ISSCFG codes
            # Common ICCAT gear types
            gear_map = {
                'BB': '09.9.0',    # Traps
                'GILL': '07.1.0',  # Gillnets
                'HAND': '09.1.0',  # Handlines
                'HARP': '10.1.0',  # Harpoons
                'LL': '09.4.0',    # Longlines
                'LLD': '09.4.1',   # Drifting longlines
                'LLP': '09.4.2',   # Set longlines
                'LL?': '09.4.0',   # Longlines (unspecified)
                'MWT': '03.1.2',   # Midwater trawls
                'OTHER': '20.0.0', # Other gear
                'PS': '01.1.0',    # Purse seines
                'RR': '09.3.0',    # Pole and line
                'SURF': '09.9.0',  # Surface
                'TROL': '03.1.0',  # Trawls
                'TROP': '09.9.0',  # Tropical
                'UNCL': '20.0.0',  # Unclassified
                'NAP': '25.9.0'    # No gear (support vessel)
            }
            
            return gear_map.get(gear_code, gear_code if len(gear_code) <= 10 else None)
        
        # Process vessel identifiers
        df['vessel_name_clean'] = df.get('VesselName', pd.Series()).apply(clean_text)
        
        # ICCAT uses IntRegNo for IMO
        df['imo_clean'] = df.get('IntRegNo', pd.Series()).apply(lambda x: 
            str(x).strip() if pd.notna(x) and len(str(x).strip()) == 7 and str(x).strip().isdigit() 
            and str(x).strip() not in ['0000001', '0000000', '1111111', '9999999'] else None
        )
        
        df['ircs_clean'] = df.get('IRCS', pd.Series()).apply(clean_text)
        
        # ICCAT doesn't have MMSI in standard fields
        df['mmsi_clean'] = None
        
        # National registry number
        df['national_registry_clean'] = df.get('NatRegNo', pd.Series()).apply(clean_text)
        
        # Vessel flag - multiple flag fields
        df['vessel_flag_alpha3'] = df.get('FlagVesCode', pd.Series()).apply(standardize_country)
        if df['vessel_flag_alpha3'].isna().all():
            df['vessel_flag_alpha3'] = df.get('FlagRepCode', pd.Series()).apply(standardize_country)
        if df['vessel_flag_alpha3'].isna().all():
            df['vessel_flag_alpha3'] = df.get('FlagChartCode', pd.Series()).apply(standardize_country)
        
        # ICCAT external identifier
        df['iccat_serial_no'] = df.get('ICCATSerialNo', pd.Series()).apply(clean_text)
        df['old_iccat_serial_no'] = df.get('OLDICCATSerialNo', pd.Series()).apply(clean_text)
        
        # Vessel type and gear
        df['vessel_type_code'] = df.get('IsscfvCode', pd.Series()).apply(standardize_vessel_type)
        df['gear_type_code'] = df.get('IsscfgCode', pd.Series()).apply(standardize_gear_type)
        
        # Port and marking
        df['port_of_registry_clean'] = df.get('HomePort', pd.Series()).apply(clean_text)
        df['external_marking'] = df.get('ExternalMark', pd.Series()).apply(clean_text)
        
        # Vessel characteristics
        df['year_built'] = df.get('YearBuilt', pd.Series()).apply(lambda x: 
            int(clean_numeric(x)) if clean_numeric(x) and 1900 <= clean_numeric(x) <= 2030 else None
        )
        df['shipyard_country'] = df.get('ShipyardNatID', pd.Series()).apply(standardize_country)
        
        # Measurements
        df['length_value'] = df.get('LOAm', pd.Series()).apply(clean_numeric)
        df['length_unit_enum'] = 'METER'  # LOAm indicates meters
        df['length_metric_type'] = 'length_loa'
        
        df['depth_value'] = df.get('DepthM', pd.Series()).apply(clean_numeric)
        df['depth_unit_enum'] = 'METER'  # DepthM indicates meters
        df['depth_metric_type'] = 'moulded_depth'
        
        # Tonnage
        df['tonnage_value'] = df.get('Tonnage', pd.Series()).apply(clean_numeric)
        df['tonnage_type_raw'] = df.get('TonTypeCode', pd.Series()).apply(clean_text)
        df['tonnage_metric_type'] = df['tonnage_type_raw'].map({
            'GT': 'gross_tonnage',
            'GRT': 'gross_register_tonnage',
            'NT': 'net_tonnage',
            'NRT': 'net_register_tonnage'
        })
        
        # Engine power
        df['engine_power'] = df.get('EnginePowerHP', pd.Series()).apply(clean_numeric)
        df['engine_power_unit_enum'] = 'HP'  # EnginePowerHP indicates horsepower
        
        # Capacity
        df['car_capacity_value'] = df.get('CarCapacity', pd.Series()).apply(clean_numeric)
        df['car_capacity_unit'] = df.get('CarCapUnitCode', pd.Series()).apply(clean_text)
        df['car_capacity_unit_enum'] = df['car_capacity_unit'].apply(lambda x: standardize_unit(x, 'volume'))
        
        # Communication system
        df['vms_com_sys_code'] = df.get('VmsComSysCode', pd.Series()).apply(clean_text)
        
        # Owner and operator information
        df['operator_name'] = df.get('OpName', pd.Series()).apply(clean_text)
        df['operator_address'] = df.get('OpAddress', pd.Series()).apply(clean_text)
        df['operator_city'] = df.get('OpCity', pd.Series()).apply(clean_text)
        df['operator_zipcode'] = df.get('OpZipCd', pd.Series()).apply(clean_text)
        df['operator_country'] = df.get('OpCountry', pd.Series()).apply(standardize_country)
        
        df['owner_name'] = df.get('OwName', pd.Series()).apply(clean_text)
        df['owner_address'] = df.get('OwAddress', pd.Series()).apply(clean_text)
        df['owner_city'] = df.get('OwCity', pd.Series()).apply(clean_text)
        df['owner_zipcode'] = df.get('OwZipCd', pd.Series()).apply(clean_text)
        df['owner_country'] = df.get('OwCountry', pd.Series()).apply(standardize_country)
        
        # Authorization dates (multiple fishing areas/species)
        # Process these into vessel_authorizations table later
        auth_columns = [col for col in df.columns if col.endswith('_dtFrom') or col.endswith('_dtTo')]
        print(f"Found {len(auth_columns)} authorization date columns")
        
        # Data quality filtering
        valid_records = df[
            df['vessel_name_clean'].notna() |
            df['imo_clean'].notna() |
            df['ircs_clean'].notna() |
            df['iccat_serial_no'].notna()
        ].copy()
        
        print(f"Filtered to {len(valid_records)} valid ICCAT vessel records")
        
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
            
            # ICCAT external identifiers
            'iccat_serial_no': valid_records['iccat_serial_no'],
            'old_iccat_serial_no': valid_records['old_iccat_serial_no'],
            
            # Vessel characteristics
            'vessel_type_code': valid_records['vessel_type_code'],
            'gear_type_code': valid_records['gear_type_code'],
            'port_of_registry': valid_records['port_of_registry_clean'],
            'external_marking': valid_records['external_marking'],
            'year_built': valid_records['year_built'],
            'shipyard_country': valid_records['shipyard_country'],
            
            # Measurements
            'length_value': valid_records['length_value'],
            'length_metric_type': valid_records['length_metric_type'],
            'length_unit_enum': valid_records['length_unit_enum'],
            
            'depth_value': valid_records['depth_value'],
            'depth_metric_type': valid_records['depth_metric_type'],
            'depth_unit_enum': valid_records['depth_unit_enum'],
            
            'tonnage_value': valid_records['tonnage_value'],
            'tonnage_metric_type': valid_records['tonnage_metric_type'],
            
            'engine_power': valid_records['engine_power'],
            'engine_power_unit_enum': valid_records['engine_power_unit_enum'],
            
            'car_capacity_value': valid_records['car_capacity_value'],
            'car_capacity_unit_enum': valid_records['car_capacity_unit_enum'],
            
            # Equipment
            'vms_com_sys_code': valid_records['vms_com_sys_code'],
            
            # Associates
            'operator_name': valid_records['operator_name'],
            'operator_address': valid_records['operator_address'],
            'operator_city': valid_records['operator_city'],
            'operator_zipcode': valid_records['operator_zipcode'],
            'operator_country': valid_records['operator_country'],
            
            'owner_name': valid_records['owner_name'],
            'owner_address': valid_records['owner_address'],
            'owner_city': valid_records['owner_city'],
            'owner_zipcode': valid_records['owner_zipcode'],
            'owner_country': valid_records['owner_country']
        })
        
        # Convert year_built to nullable integer to avoid .0 decimals
        if 'year_built' in output_df.columns:
            output_df['year_built'] = output_df['year_built'].astype('Int64')
        
        # Save cleaned data
        output_df.to_csv(output_file, index=False, na_rep='')
        print(f"ICCAT cleaning complete: {len(output_df)} records saved")
        print(f"Vessel types found: {output_df['vessel_type_code'].value_counts().head(10).to_dict()}")
        print(f"Gear types found: {output_df['gear_type_code'].value_counts().head(10).to_dict()}")
        print(f"Flags found: {output_df['vessel_flag_alpha3'].value_counts().head(10).to_dict()}")
        
    except Exception as e:
        print(f"Error in ICCAT cleaning: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
EOF

# Run cleaning
if python3 /tmp/clean_iccat_vessels.py "$INPUT_FILE" "$OUTPUT_FILE" "$SOURCE_DATE"; then
    RECORD_COUNT=$(tail -n +2 "$OUTPUT_FILE" | wc -l)
    COLUMN_COUNT=$(head -1 "$OUTPUT_FILE" | tr ',' '\n' | wc -l)
    log_success "✅ ICCAT cleaning complete:"
    log_success "   - $RECORD_COUNT vessel records processed"
    log_success "   - $COLUMN_COUNT output columns"
    log_success "   - Country codes standardized (EU-XXX → XXX)"
    log_success "   - Vessel and gear types validated"
    log_success "   - Owner/operator data preserved"
    log_success "   - Multiple authorization areas identified"
    
    rm -f /tmp/clean_iccat_vessels.py
else
    log_error "ICCAT cleaning failed"
    exit 1
fi