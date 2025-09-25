#!/bin/bash
# /app/scripts/import/vessels/data/RFMO/clean_ffa_vessels.sh
# FFA (Forum Fisheries Agency) Vessel Data Cleaning Script
set -euo pipefail

source /app/scripts/core/logging.sh

log_step "🐟 FFA Vessel Data Cleaning"

RAW_DATA_DIR="/import/vessels/vessel_data/RFMO/raw"
CLEANED_DATA_DIR="/import/vessels/vessel_data/RFMO/cleaned"
mkdir -p "$CLEANED_DATA_DIR" /import/logs

# Find latest FFA file
INPUT_FILE=$(find "$RAW_DATA_DIR" -name "*FFA*vessel*.csv" -o -name "*ffa*vessel*.csv" | head -1)
if [[ -z "$INPUT_FILE" ]]; then
    log_error "No FFA vessel file found in $RAW_DATA_DIR"
    exit 1
fi

OUTPUT_FILE="$CLEANED_DATA_DIR/ffa_vessels_cleaned.csv"
log_success "Processing: $(basename "$INPUT_FILE") → $(basename "$OUTPUT_FILE")"

# Extract source date from filename or use current date
SOURCE_DATE=$(echo "$(basename "$INPUT_FILE")" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' || date '+%Y-%m-%d')
log_success "Source date: $SOURCE_DATE"

# Create Python cleaning script for FFA data
cat > /tmp/clean_ffa_vessels.py << 'EOF'
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
        
        # Read FFA CSV
        df = pd.read_csv(input_file, encoding='utf-8')
        print(f"Loaded {len(df)} FFA records with {len(df.columns)} columns")
        print(f"Columns: {list(df.columns)}")
        
        # Add metadata for source tracking
        df['source_date'] = source_date
        df['original_source'] = 'FFA'
        
        # Clean text helper function
        def clean_text(x):
            if pd.isna(x): return None
            s = str(x).strip()
            return s if s and s.upper() not in ['N/A', 'NA', 'NONE', 'UNKNOWN', '', 'NAN', 'NIL', 'NULL'] else None
        
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
        
        # Country code standardization
        def standardize_country(country_name):
            """Convert country names to ISO alpha3"""
            if pd.isna(country_name) or not country_name: 
                return None
                
            country_name = str(country_name).strip().upper()
            
            # FFA member country mappings
            country_map = {
                'FIJI': 'FJI',
                'FJI': 'FJI',
                'KIRIBATI': 'KIR',
                'KIR': 'KIR',
                'MARSHALL ISLANDS': 'MHL',
                'MHL': 'MHL',
                'MICRONESIA': 'FSM',
                'FSM': 'FSM',
                'NAURU': 'NRU',
                'NRU': 'NRU',
                'PALAU': 'PLW',
                'PLW': 'PLW',
                'PAPUA NEW GUINEA': 'PNG',
                'PNG': 'PNG',
                'SAMOA': 'WSM',
                'WSM': 'WSM',
                'SOLOMON ISLANDS': 'SLB',
                'SLB': 'SLB',
                'TONGA': 'TON',
                'TON': 'TON',
                'TUVALU': 'TUV',
                'TUV': 'TUV',
                'VANUATU': 'VUT',
                'VUT': 'VUT',
                'COOK ISLANDS': 'COK',
                'COK': 'COK',
                'NIUE': 'NIU',
                'NIU': 'NIU',
                'TOKELAU': 'TKL',
                'TKL': 'TKL',
                # Other common flags in FFA data
                'CHINA': 'CHN',
                'CHN': 'CHN',
                'TAIWAN': 'TWN',
                'TWN': 'TWN',
                'JAPAN': 'JPN',
                'JPN': 'JPN',
                'KOREA': 'KOR',
                'SOUTH KOREA': 'KOR',
                'KOR': 'KOR',
                'USA': 'USA',
                'UNITED STATES': 'USA',
                'US': 'USA'
            }
            
            return country_map.get(country_name, None)
        
        # Process vessel identifiers
        df['vessel_name_clean'] = df.get('Name', pd.Series()).apply(clean_text)
        
        # IMO processing
        df['imo_clean'] = df.get('IMO Number', pd.Series()).apply(lambda x: 
            str(int(float(x))) if pd.notna(x) and str(x).replace('.0', '').isdigit() 
            and len(str(int(float(x)))) == 7 
            and str(int(float(x))) not in ['0000000', '1111111', '9999999'] 
            else None
        )
        
        # IRCS
        df['ircs_clean'] = df.get('International Radio Call Sign (IRCS)', pd.Series()).apply(clean_text)
        
        # MMSI
        df['mmsi_clean'] = df.get('MMSI', pd.Series()).apply(lambda x: 
            str(int(float(x))) if pd.notna(x) and str(x).replace('.0', '').isdigit() 
            and len(str(int(float(x)))) == 9 
            else None
        )
        
        # National registry
        df['national_registry_clean'] = df.get('Registration Number', pd.Series()).apply(clean_text)
        
        # Vessel flag
        df['vessel_flag_alpha3'] = df.get('Flag', pd.Series()).apply(standardize_country)
        
        # External identifiers
        df['wcpfc_id'] = df.get('WCPFC ID', pd.Series()).apply(clean_text)
        df['ffa_id'] = df.get('FFA ID', pd.Series()).apply(clean_text)
        
        # Vessel type
        df['vessel_type_raw'] = df.get('Type', pd.Series()).apply(clean_text)
        
        # Registration dates
        df['registration_start'] = pd.to_datetime(df.get('Registration Start', pd.Series()), errors='coerce')
        df['registration_period'] = pd.to_datetime(df.get('Registration Period', pd.Series()), errors='coerce')
        
        # Status
        df['current_status'] = df.get('Current Status', pd.Series()).apply(clean_text)
        df['application_type'] = df.get('Application Type', pd.Series()).apply(clean_text)
        
        # Port information
        df['port_of_registry'] = df.get('Port of Registry', pd.Series()).apply(clean_text)
        
        # Build information
        df['build_location'] = df.get('Built in Country', pd.Series()).apply(clean_text)
        df['build_year'] = df.get('Built in Year', pd.Series()).apply(lambda x: 
            int(clean_numeric(x)) if clean_numeric(x) and 1900 <= clean_numeric(x) <= 2030 else None
        )
        
        # Measurements
        df['length_value'] = df.get('Vessel Length (LOA)', pd.Series()).apply(lambda x:
            clean_numeric(re.search(r'([\d.]+)', str(x)).group(1)) if pd.notna(x) and re.search(r'[\d.]+', str(x)) else None
        )
        df['length_metric_type'] = 'length_loa'
        df['length_unit'] = 'METER'  # FFA typically uses meters
        
        # Tonnage
        df['gross_tonnage'] = df.get('Vessel Tonnage (GRT)', pd.Series()).apply(clean_numeric)
        df['tonnage_metric_type'] = 'gross_tonnage'
        
        # Engine
        df['engine_power'] = df.get('Total Power of Main Engine or Engines', pd.Series()).apply(clean_numeric)
        df['engine_power_unit'] = df.get('Power Unit', pd.Series()).apply(clean_text)
        
        # Capacity
        df['fish_hold_capacity'] = df.get('Fish Hold Capacity', pd.Series()).apply(clean_numeric)
        df['fish_hold_capacity_unit'] = df.get('Fish Hold Capacity Unit', pd.Series()).apply(clean_text)
        
        # Ownership
        df['owner_name'] = df.get('Owner Company Name', pd.Series()).apply(clean_text)
        df['owner_address'] = df.get('Owner Mailing Address', pd.Series()).apply(clean_text)
        df['operator_name'] = df.get('Operator Name', pd.Series()).apply(clean_text)
        df['operator_address'] = df.get('Operator Mailing Address', pd.Series()).apply(clean_text)
        
        # Registrant info
        df['registrant'] = df.get('Registrant', pd.Series()).apply(clean_text)
        df['registrant_role'] = df.get('Registrant Acting As', pd.Series()).apply(clean_text)
        
        # Crew information
        df['vessel_master_name'] = df.get('Vessel Master Name', pd.Series()).apply(clean_text)
        df['vessel_master_nationality'] = df.get('Vessel Master Nationality', pd.Series()).apply(clean_text)
        df['fishing_master_name'] = df.get('Fishing Master Name', pd.Series()).apply(clean_text)
        df['fishing_master_nationality'] = df.get('Fishing Master Nationality', pd.Series()).apply(clean_text)
        df['crew_complement'] = df.get('Normal Crew Complement', pd.Series()).apply(clean_numeric)
        
        # Additional details
        df['hull_material'] = df.get('Hull Material', pd.Series()).apply(clean_text)
        df['rated_speed'] = df.get('Rated Speed', pd.Series()).apply(lambda x:
            clean_numeric(re.search(r'([\d.]+)', str(x)).group(1)) if pd.notna(x) and re.search(r'[\d.]+', str(x)) else None
        )
        df['fuel_capacity'] = df.get('Total Fuel Carrying Capacity', pd.Series()).apply(clean_numeric)
        
        # Historical data
        df['previous_names'] = df.get('Previous Names', pd.Series()).apply(clean_text)
        df['previous_flag'] = df.get('Previous Flag', pd.Series()).apply(clean_text)
        df['previous_ircs'] = df.get('Last Radio Call Sign (IRCS)', pd.Series()).apply(clean_text)
        df['previous_registry'] = df.get('Last Flag State Registration Number', pd.Series()).apply(clean_text)
        
        # Photos
        df['side_photo_url'] = df.get('Recent side-view vessel photo', pd.Series()).apply(clean_text)
        df['plan_photo_url'] = df.get('Recent plan-view vessel photo', pd.Series()).apply(clean_text)
        
        # Data quality filtering
        valid_records = df[
            df['vessel_name_clean'].notna() |
            df['imo_clean'].notna() |
            df['ircs_clean'].notna() |
            df['ffa_id'].notna()
        ].copy()
        
        print(f"Filtered to {len(valid_records)} valid FFA vessel records")
        
        # Show statistics
        print(f"Status types: {valid_records['current_status'].value_counts().head().to_dict()}")
        print(f"Vessel types: {valid_records['vessel_type_raw'].value_counts().head().to_dict()}")
        print(f"Flags: {valid_records['vessel_flag_alpha3'].value_counts().head().to_dict()}")
        
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
            
            # External identifiers
            'wcpfc_id': valid_records['wcpfc_id'],
            'ffa_id': valid_records['ffa_id'],
            
            # Vessel characteristics
            'vessel_type': valid_records['vessel_type_raw'],
            'port_of_registry': valid_records['port_of_registry'],
            'hull_material': valid_records['hull_material'],
            
            # Build information
            'build_location': valid_records['build_location'],
            'build_year': valid_records['build_year'],
            
            # Measurements
            'length_value': valid_records['length_value'],
            'length_metric_type': valid_records['length_metric_type'],
            'length_unit': valid_records['length_unit'],
            'gross_tonnage': valid_records['gross_tonnage'],
            'tonnage_metric_type': valid_records['tonnage_metric_type'],
            'rated_speed': valid_records['rated_speed'],
            
            # Engine & capacity
            'engine_power': valid_records['engine_power'],
            'engine_power_unit': valid_records['engine_power_unit'],
            'fish_hold_capacity': valid_records['fish_hold_capacity'],
            'fish_hold_capacity_unit': valid_records['fish_hold_capacity_unit'],
            'fuel_capacity': valid_records['fuel_capacity'],
            
            # Ownership
            'owner_name': valid_records['owner_name'],
            'owner_address': valid_records['owner_address'],
            'operator_name': valid_records['operator_name'],
            'operator_address': valid_records['operator_address'],
            
            # Registration
            'registrant': valid_records['registrant'],
            'registrant_role': valid_records['registrant_role'],
            'registration_start': valid_records['registration_start'],
            'registration_period': valid_records['registration_period'],
            'current_status': valid_records['current_status'],
            'application_type': valid_records['application_type'],
            
            # Crew
            'vessel_master_name': valid_records['vessel_master_name'],
            'vessel_master_nationality': valid_records['vessel_master_nationality'],
            'fishing_master_name': valid_records['fishing_master_name'],
            'fishing_master_nationality': valid_records['fishing_master_nationality'],
            'crew_complement': valid_records['crew_complement'],
            
            # Historical
            'previous_names': valid_records['previous_names'],
            'previous_flag': valid_records['previous_flag'],
            'previous_ircs': valid_records['previous_ircs'],
            'previous_registry': valid_records['previous_registry'],
            
            # Photos
            'side_photo_url': valid_records['side_photo_url'],
            'plan_photo_url': valid_records['plan_photo_url']
        })
        
        # Convert dates to strings
        for col in ['registration_start', 'registration_period']:
            if col in output_df.columns:
                output_df[col] = output_df[col].astype(str).replace('NaT', '')
        
        # Convert year_built to int
        if 'build_year' in output_df.columns:
            output_df['build_year'] = output_df['build_year'].astype('Int64')
        
        # Save cleaned data
        output_df.to_csv(output_file, index=False, na_rep='')
        print(f"FFA cleaning complete: {len(output_df)} records saved")
        
        # Summary stats
        print(f"IMO coverage: {(output_df['imo'].notna().sum() / len(output_df) * 100):.1f}%")
        print(f"IRCS coverage: {(output_df['ircs'].notna().sum() / len(output_df) * 100):.1f}%")
        print(f"FFA ID coverage: {(output_df['ffa_id'].notna().sum() / len(output_df) * 100):.1f}%")
        print(f"WCPFC ID coverage: {(output_df['wcpfc_id'].notna().sum() / len(output_df) * 100):.1f}%")
        print(f"Countries: {output_df['vessel_flag_alpha3'].nunique()}")
        
    except Exception as e:
        print(f"Error in FFA cleaning: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
EOF

# Run cleaning
if python3 /tmp/clean_ffa_vessels.py "$INPUT_FILE" "$OUTPUT_FILE" "$SOURCE_DATE"; then
    RECORD_COUNT=$(tail -n +2 "$OUTPUT_FILE" | wc -l)
    COLUMN_COUNT=$(head -1 "$OUTPUT_FILE" | tr ',' '\n' | wc -l)
    log_success "✅ FFA cleaning complete:"
    log_success "   - $RECORD_COUNT vessel records processed"
    log_success "   - $COLUMN_COUNT output columns"
    log_success "   - FFA and WCPFC IDs captured"
    log_success "   - Registration history preserved"
    log_success "   - Crew nationality tracked"
    
    rm -f /tmp/clean_ffa_vessels.py
else
    log_error "FFA cleaning failed"
    exit 1
fi