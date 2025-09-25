#!/bin/bash
# /app/scripts/import/vessels/data/RFMO/clean_npfc_vessels.sh
# COMPREHENSIVE NPFC Vessel Data Cleaning - CORRECTED: Fixed unit enums, EXACT user translations
set -euo pipefail

source /app/scripts/core/logging.sh

log_step "🇯🇵 NPFC Vessel Data Cleaning (COMPREHENSIVE - CORRECTED Unit Enums + EXACT User Translations)"

RAW_DATA_DIR="/import/vessels/vessel_data/RFMO/raw"
CLEANED_DATA_DIR="/import/vessels/vessel_data/RFMO/cleaned"
mkdir -p "$CLEANED_DATA_DIR" /import/logs

# Find latest NPFC file
INPUT_FILE=$(find "$RAW_DATA_DIR" -name "*NPFC*vessel*.csv" -o -name "*npfc*vessel*.csv" | head -1)
if [[ -z "$INPUT_FILE" ]]; then
    log_error "No NPFC vessel file found in $RAW_DATA_DIR"
    exit 1
fi

OUTPUT_FILE="$CLEANED_DATA_DIR/npfc_vessels_cleaned.csv"
log_success "Processing: $(basename "$INPUT_FILE") → $(basename "$OUTPUT_FILE")"

# Extract source date from filename or use current date
SOURCE_DATE=$(echo "$(basename "$INPUT_FILE")" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' || date '+%Y-%m-%d')
log_success "Source date: $SOURCE_DATE"

# Create comprehensive Python cleaning script with ALL original functionality + CORRECTED enums + EXACT translations
cat > /tmp/clean_npfc_comprehensive_corrected.py << 'EOF'
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
        
        # Read NPFC CSV with all 33 columns
        df = pd.read_csv(input_file)
        print(f"Loaded {len(df)} NPFC records with {len(df.columns)} columns")
        print(f"Columns: {list(df.columns)}")
        
        # Add metadata for source tracking
        df['source_date'] = source_date
        df['original_source'] = 'NPFC'
        
        # Clean text helper function
        def clean_text(x):
            if pd.isna(x): return None
            s = str(x).strip()
            return s if s and s.upper() not in ['N/A', 'NONE KNOWN', 'UNKNOWN', '', 'NAN'] else None
        
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
        
        # === CORRECTED: Fixed unit standardization to match database unit_enum exactly ===
        def standardize_unit(unit_text, measurement_type='length'):
            """Convert unit names to unit_enum values - CORRECTED to match database schema"""
            if pd.isna(unit_text) or not unit_text:
                return 'METER' if measurement_type == 'length' else 'CUBIC_METER' if measurement_type == 'volume' else 'KW'
                
            unit_text = str(unit_text).strip()
            
            # CORRECTED: Fixed unit mapping to match exact database enum values (SINGULAR forms)
            unit_map = {
                # Length units - CORRECTED to singular forms to match database
                'meters': 'METER', 'm': 'METER', 'metre': 'METER', 'metres': 'METER',
                'feet': 'FEET', 'ft': 'FEET', 'foot': 'FEET',
                
                # Volume units - CORRECTED to singular forms to match database  
                'cubic feet': 'CUBIC_FEET', 'Cubic Feet': 'CUBIC_FEET', 'CUBIC FEET': 'CUBIC_FEET',
                'cubic meters': 'CUBIC_METER', 'Cubic Metres': 'CUBIC_METER', 'cubic metres': 'CUBIC_METER', 
                'm3': 'CUBIC_METER', 'm³': 'CUBIC_METER',
                'liter': 'LITER', 'litre': 'LITER', 'l': 'LITER',
                'gallon': 'GALLON', 'gal': 'GALLON',
                
                # Power units
                'Kilowatts (kW)': 'KW', 'kW': 'KW', 'kilowatt': 'KW', 'KW': 'KW',
                'Horse Power (hp)': 'HP', 'hp': 'HP', 'horsepower': 'HP', 'HP': 'HP',
                'Pferdestärke (ps)': 'PS', 'ps': 'PS', 'PS': 'PS',
                
                # Speed units
                'knots': 'KNOTS', 'kt': 'KNOTS', 'kn': 'KNOTS',
                'mph': 'MPH', 'MPH': 'MPH',
                'kmh': 'KMH', 'km/h': 'KMH', 'KMH': 'KMH',
                
                # CORRECTED: Freezer capacity units mapped to standard units available in database
                'Metric Tons / Day': 'METRIC_TONS / DAY',
                'metric tons / day': 'METRIC_TONS / DAY', 
                'METRIC TONS / DAY': 'METRIC_TONS / DAY',
                'Tons / Day': 'TONS / DAY',
                'tons / day': 'TONS / DAY',
                'TONS / DAY': 'TONS / DAY',
                'Tons / Day, Tons / Day': 'TONS / DAY',  # Handle duplicates
                'tons / day, tons / day': 'TONS / DAY'
            }
            
            return unit_map.get(unit_text, 'METER')  # Default to METER if not found
        
        # Comprehensive freezer type standardization - keep all original functionality
        def standardize_freezer_type(freezer_type_text):
            """Convert NPFC freezer types to standardized enum values - ALL ORIGINAL MAPPINGS"""
            if pd.isna(freezer_type_text) or not freezer_type_text:
                return None
                
            freezer_type_text = str(freezer_type_text).strip()
            
            # Comprehensive freezer type mapping - ALL ORIGINAL FUNCTIONALITY MAINTAINED
            freezer_map = {
                'Air Blast': 'AIR_BLAST',
                'air blast': 'AIR_BLAST',
                'AIR BLAST': 'AIR_BLAST',
                
                'Air Coil': 'AIR_COIL', 
                'air coil': 'AIR_COIL',
                'AIR COIL': 'AIR_COIL',
                
                'Bait Freezer': 'BAIT_FREEZER',
                'bait freezer': 'BAIT_FREEZER',
                
                'Blast': 'BLAST',
                'blast': 'BLAST',
                
                'Brine': 'BRINE',
                'brine': 'BRINE',
                'BRINE': 'BRINE',
                
                'Chilled': 'CHILLED',
                'chilled': 'CHILLED',
                
                'Coil': 'COIL',
                'coil': 'COIL',
                
                'Direct Expansion': 'DIRECT_EXPANSION',
                'direct expansion': 'DIRECT_EXPANSION',
                
                'Dry': 'DRY',
                'dry': 'DRY',
                
                'Freon Refrigeration System': 'FREON_REFRIGERATION_SYSTEM',
                'freon refrigeration system': 'FREON_REFRIGERATION_SYSTEM',
                'FREON REFRIGERATION SYSTEM': 'FREON_REFRIGERATION_SYSTEM',
                
                'Grid Coil': 'GRID_COIL',
                'grid coil': 'GRID_COIL',
                
                'Ice': 'ICE',
                'ice': 'ICE',
                'ICE': 'ICE',
                
                'Mykom': 'MYKOM',
                'mykom': 'MYKOM',
                'MYKOM': 'MYKOM',
                
                'Other': 'OTHER',
                'other': 'OTHER',
                'OTHER': 'OTHER',
                
                'Pipe': 'PIPE',
                'pipe': 'PIPE',
                
                'Plate Freezer': 'PLATE_FREEZER',
                'plate freezer': 'PLATE_FREEZER',
                'PLATE FREEZER': 'PLATE_FREEZER',
                'Plate Freezer, Plate Freezer': 'PLATE_FREEZER',  # Handle duplicates
                'plate freezer, plate freezer': 'PLATE_FREEZER',
                
                'RSW': 'RSW',
                'rsw': 'RSW',
                
                'Semi Air Blast': 'SEMI_AIR_BLAST',
                'semi air blast': 'SEMI_AIR_BLAST',
                
                'Tunnel': 'TUNNEL',
                'tunnel': 'TUNNEL'
            }
            
            return freezer_map.get(freezer_type_text, 'OTHER')
        
        # === COMPREHENSIVE COUNTRY CODE STANDARDIZATION - ALL ORIGINAL FUNCTIONALITY ===
        def standardize_country(country_name):
            """Convert NPFC country names to ISO alpha3 codes - ALL ORIGINAL MAPPINGS"""
            if pd.isna(country_name) or not country_name: 
                return None
                
            country_name = str(country_name).strip()
            
            # NPFC-specific country mappings - ALL ORIGINAL FUNCTIONALITY
            country_map = {
                'Chinese Taipei': 'TWN',
                'South Korea': 'KOR', 
                'Korea': 'KOR',
                'Beliz': 'BLZ',
                'RUSSIA': 'RUS',
                'Russia': 'RUS',
                'Japan': 'JPN',
                'China': 'CHN',
                'United States': 'USA',
                'Canada': 'CAN',
                'Norway': 'NOR'
            }
            
            # Handle multiple countries with semicolon - ORIGINAL FUNCTIONALITY
            if ';' in country_name:
                countries = [c.strip() for c in country_name.split(';')]
                mapped = [country_map.get(c, c) for c in countries]
                return '; '.join([c for c in mapped if c])
            
            return country_map.get(country_name, country_name)
        
        # === CORRECTED: VESSEL TYPE STANDARDIZATION using EXACT user-provided mapping ===
        def standardize_vessel_type(vessel_type):
            """Convert NPFC vessel types using EXACT user-provided translations"""
            if pd.isna(vessel_type) or not vessel_type:
                return None
                
            vessel_type = str(vessel_type).strip()
            
            # CORRECTED: Use EXACT vessel type mapping provided by user
            type_map = {
                'BUNKERING TANKER VESSELS (SB)': 'SB',
                'FISH CARRIERS AND REEFERS (FO)': 'FO', 
                'FISHERY RESEARCH VESSELS (RT)': 'RT',
                'FISHERY RESEARCH VESSELS (ZO)': 'RT',
                'Fishery research vessels nei (RTX)': 'RTX',
                'GILLNETTERS (GO)': 'GO',
                'Hand liner vessels (LH)': 'LH',
                'Japanese type liners (LPJ)': 'LO',
                'Japanese type  liners (LPJ)': 'LOX',  # Note: double space
                'Japanese type   liners (LPJ)': 'LOX', # Note: triple space
                'Jigger vessels (LJ)': 'LJ',
                'LIFT NETTERS - using boat operated net (NB)': 'NO',
                'LIFT NETTERS (NO)': 'NO',
                'Lift netters nei (NOX)': 'NOX',
                'Line vessels nei (LOX)': 'LOX',
                'Other fishing vessels [FISHING VESSELS NOT SPECIFIED] (FX)': 'FXX',
                'PROTECTION AND SURVEY VESSELS (BO)': 'SA',
                'Purse seiners (SP)': 'SP',
                'Purse seiners nei (SPX)': 'SPX',
                'Stern trawlers (TT)': 'TT',
                'Stern trawlers freezer (TTF)': 'TT',
                'Stern trawlers factory (TTP)': 'TT',
                'Stick-held dip netters (NS)': 'SA',
                'SUPPORT vessels (SA)': 'SA',
                'TRAWLERS (TO)': 'TO',
                'Trap setters nei (WOX)': 'WOX',
                'Vessels supporting fishing related activities [NON-FISHING VESSELS] (VO)': 'VO',
                'TRAP SETTERS (WO)': 'WO',
                'Side trawlers freezer (TSF)': 'TS',
                'Factory mothership (HSF)': 'HOX',
                'Multipurpose non-fishing vessels (NF)': 'VOM',
                'Refrigerated transport vessels (FR)': 'FR'
            }
            
            return type_map.get(vessel_type, vessel_type)
        
        # === CORRECTED: FISHING METHOD STANDARDIZATION using EXACT user-provided mapping ===
        def standardize_fishing_method(method):
            """Convert NPFC fishing methods using EXACT user-provided translations"""
            if pd.isna(method) or not method:
                return None
                
            method = str(method).strip()
            
            # CORRECTED: Use EXACT fishing method mapping provided by user
            method_map = {
                'Boat-operated lift nets (LNB)': 'LNB',
                'Gear not known (NK)': 'NKX',
                'Gillnets and entangling nets (nei) (GEN)': 'GEN',
                'Handlines and hand-operated pole-and-lines (LHP)': 'LHP',
                'Hooks and lines (nei) (LX)': 'LX',
                'Mechanized lines and pole-and-lines (LHM)': 'LHM',
                'Purse seines (PS)': 'PS',
                'SEINE NETS': 'SX',
                'Single boat midwater otter trawls (OTM)': 'OTM',
                'Stick-held dip net (SHDN)': 'MIS',
                'Pots (FPO)': 'FPO',
                'Aerial traps (FAR)': 'FAR',
                'Bottom trawls (nei) (TB)': 'TB',
                'Midwater trawls (nei) (TM)': 'TM',
                'Seine nets (nei) (SX)': 'SX',
                'Traps (nei) (FIX)': 'FIX',
                'Trawls (nei) (TX)': 'TX',
                'Semipelagic trawls (TSP)': 'TSP',
                'TRAWLS': 'TX'
            }
            
            return method_map.get(method, method)
        
        # === COMPREHENSIVE PROCESSING OF ALL COLUMNS FOR SCHEMA MAPPING - ALL ORIGINAL FUNCTIONALITY ===
        
        # Core vessel identifiers (for vessels table)
        df['vessel_name_clean'] = df.get('vessel_name', pd.Series()).apply(clean_text)
        df['imo_clean'] = df.get('imo', pd.Series()).apply(lambda x: str(x).strip() if pd.notna(x) and len(str(x).strip()) == 7 and str(x).strip().isdigit() else None)
        df['ircs_clean'] = df.get('call_sign', pd.Series()).apply(clean_text)
        df['mmsi_clean'] = df.get('mmsi', pd.Series()).apply(lambda x: str(x).strip() if pd.notna(x) and len(str(x).strip()) == 9 and str(x).strip().isdigit() else None)
        df['national_registry_clean'] = df.get('registration_no', pd.Series()).apply(clean_text)
        df['vessel_flag_alpha3'] = df.get('flag_state', pd.Series()).apply(standardize_country)
        
        # NPFC external identifier (for vessel_external_identifiers table)
        df['npfc_vessel_id'] = df.get('NPFC Vessel_id', pd.Series()).apply(clean_text)
        
        # CORRECTED: Basic vessel info using EXACT user-provided translations
        df['vessel_type_code'] = df.get('vessel_type', pd.Series()).apply(standardize_vessel_type)
        df['port_of_registry_clean'] = df.get('port_of_registry', pd.Series()).apply(clean_text)
        df['fishing_method_code'] = df.get('type_of_fishing_method', pd.Series()).apply(standardize_fishing_method)
        df['external_marking'] = df.get('external_marking', pd.Series()).apply(clean_text) if 'external_marking' in df.columns else None
        
        # Previous vessel information (for vessel_reported_history table) - ALL ORIGINAL PROCESSING
        df['previous_name'] = df.get('previous_name', pd.Series()).apply(clean_text)
        df['previous_flag_alpha3'] = df.get('previous_flag', pd.Series()).apply(standardize_country) 
        df['previous_registry'] = df.get('Previous Registration', pd.Series()).apply(clean_text)
        df['previous_port_registry'] = df.get('Previous Port(s) of Registry', pd.Series()).apply(clean_text)
        
        # Equipment data (for vessel_equipment table) - ALL ORIGINAL FUNCTIONALITY
        df['communication_details'] = df.get('communication_details', pd.Series()).apply(clean_text)
        
        # Comprehensive freezer processing - ALL ORIGINAL FUNCTIONALITY
        df['freezer_type_raw'] = df.get('freezer_type', pd.Series()).apply(clean_text)
        df['freezer_type_enum'] = df['freezer_type_raw'].apply(standardize_freezer_type)
        
        # Comprehensive freezer unit processing - ALL ORIGINAL FUNCTIONALITY + CORRECTED ENUMS
        df['freezer_unit_raw'] = df.get('unit_freezer', pd.Series()).apply(clean_text)
        df['freezer_unit_enum'] = df['freezer_unit_raw'].apply(lambda x: standardize_unit(x, 'freezer'))
        
        # Operational attributes (for vessel_attributes table) - ALL ORIGINAL FUNCTIONALITY
        df['crew_size'] = df.get('crew', pd.Series()).apply(clean_numeric)
        
        # === COMPREHENSIVE MEASUREMENT DATA PROCESSING - ALL ORIGINAL FUNCTIONALITY + CORRECTED ENUMS ===
        # All measurements go to vessel_metrics table with proper metric_type_enum values
        
        # Length measurements with type differentiation - ALL ORIGINAL FUNCTIONALITY
        df['length_value'] = df.get('length', pd.Series()).apply(clean_numeric)
        df['length_type'] = df.get('type_of_length', pd.Series()).apply(clean_text)
        df['length_unit'] = df.get('unit_length', pd.Series()).apply(clean_text)
        
        # Map length types to metric_type_enum values - ALL ORIGINAL MAPPINGS
        df['length_metric_type'] = df['length_type'].map({
            'Length Overall (LOA)': 'length_loa',
            'Registered Length': 'length_rgl', 
            'Length Between Perpendiculars (LPP)': 'length_lbp'
        })
        
        # Depth measurements - ALL ORIGINAL FUNCTIONALITY
        df['depth_value'] = df.get('depth', pd.Series()).apply(clean_numeric)
        df['depth_type'] = df.get('type_of_depth', pd.Series()).apply(clean_text)
        df['depth_unit'] = df.get('unit_depth', pd.Series()).apply(clean_text)
        
        # Map depth types to metric_type_enum values - ALL ORIGINAL MAPPINGS
        df['depth_metric_type'] = df['depth_type'].map({
            'Draft / Draught': 'draft_depth',
            'Moulded Depth': 'moulded_depth'
        })
        
        # Beam measurements - ALL ORIGINAL FUNCTIONALITY
        df['beam_value'] = df.get('beam', pd.Series()).apply(clean_numeric)
        df['beam_type'] = df.get('type_of_beam', pd.Series()).apply(clean_text) 
        df['beam_unit'] = df.get('unit_beam_depth', pd.Series()).apply(clean_text)
        
        # Map beam types to metric_type_enum values - ALL ORIGINAL MAPPINGS
        df['beam_metric_type'] = df['beam_type'].map({
            'Extreme Breadth': 'extreme_beam',
            'Moulded Breadth': 'moulded_beam'
        })
        
        # Tonnage measurements - ALL ORIGINAL FUNCTIONALITY
        df['tonnage_value'] = df.get('tonnage', pd.Series()).apply(clean_numeric)
        df['tonnage_type'] = df.get('tonnage_type', pd.Series()).apply(clean_text)
        
        # Map tonnage types to metric_type_enum values - ALL ORIGINAL MAPPINGS
        df['tonnage_metric_type'] = df['tonnage_type'].map({
            'Gross Tonnage (GT)': 'gross_tonnage',
            'Gross Register Tonnage (GRT)': 'gross_register_tonnage'
        })
        
        # Engine power with comprehensive unit standardization - CORRECTED ENUMS
        df['engine_power'] = df.get('power_of_engine', pd.Series()).apply(clean_numeric)
        df['engine_power_unit'] = df.get('unit_engine', pd.Series()).apply(clean_text)
        df['engine_power_unit_enum'] = df['engine_power_unit'].apply(lambda x: standardize_unit(x, 'power'))
        
        # Capacity measurements with comprehensive unit support - CORRECTED ENUMS
        df['fish_hold_capacity'] = df.get('fish_hold_capacity', pd.Series()).apply(lambda x: clean_numeric(str(x).replace('m³', '').strip()) if pd.notna(x) else None)
        df['freezer_capacity'] = df.get('freezer_capacity', pd.Series()).apply(clean_numeric)
        
        # === COMPREHENSIVE UNITS STANDARDIZATION - ALL ORIGINAL FUNCTIONALITY + CORRECTED ENUMS ===
        df['length_unit_enum'] = df['length_unit'].apply(lambda x: standardize_unit(x, 'length'))
        df['depth_unit_enum'] = df['depth_unit'].apply(lambda x: standardize_unit(x, 'length')) 
        df['beam_unit_enum'] = df['beam_unit'].apply(lambda x: standardize_unit(x, 'length'))
        
        # === DATA QUALITY FILTERING - ALL ORIGINAL FUNCTIONALITY ===
        # Keep only records with at least one valid identifier
        valid_records = df[
            df['vessel_name_clean'].notna() |
            df['imo_clean'].notna() |
            df['ircs_clean'].notna() |
            df['mmsi_clean'].notna()
        ].copy()
        
        print(f"Filtered to {len(valid_records)} valid NPFC vessel records")
        
        # === OUTPUT STRUCTURE FOR COMPREHENSIVE DATABASE LOADING - ALL ORIGINAL FUNCTIONALITY ===
        # Create output columns matching comprehensive loading script expectations
        output_df = pd.DataFrame({
            # Source metadata
            'source_date': valid_records['source_date'],
            'original_source': valid_records['original_source'],
            
            # Core vessel identifiers (vessels table)
            'vessel_name': valid_records['vessel_name_clean'],
            'imo': valid_records['imo_clean'],
            'ircs': valid_records['ircs_clean'],
            'mmsi': valid_records['mmsi_clean'], 
            'national_registry': valid_records['national_registry_clean'],
            'vessel_flag_alpha3': valid_records['vessel_flag_alpha3'],
            
            # NPFC external identifier
            'npfc_vessel_id': valid_records['npfc_vessel_id'],
            
            # CORRECTED: Basic vessel info with EXACT user translations
            'vessel_type_code': valid_records['vessel_type_code'],
            'port_of_registry': valid_records['port_of_registry_clean'],
            'fishing_method_code': valid_records['fishing_method_code'],
            'external_marking': valid_records['external_marking'],
            
            # Previous vessel info (vessel_reported_history table)
            'previous_name': valid_records['previous_name'],
            'previous_flag_alpha3': valid_records['previous_flag_alpha3'],
            'previous_registry': valid_records['previous_registry'],
            'previous_port_registry': valid_records['previous_port_registry'],
            
            # Equipment data (vessel_equipment table) - ALL ORIGINAL FUNCTIONALITY
            'communication_details': valid_records['communication_details'],
            'freezer_type_enum': valid_records['freezer_type_enum'],
            'freezer_unit_enum': valid_records['freezer_unit_enum'],
            'freezer_type_raw': valid_records['freezer_type_raw'],
            'freezer_unit_raw': valid_records['freezer_unit_raw'],
            
            # Operational attributes (vessel_attributes table)
            'crew_size': valid_records['crew_size'],
            
            # Measurement data (vessel_metrics table) - ALL TYPES WITH CORRECTED UNITS
            'length_value': valid_records['length_value'],
            'length_metric_type': valid_records['length_metric_type'],
            'length_unit_enum': valid_records['length_unit_enum'],
            
            'depth_value': valid_records['depth_value'], 
            'depth_metric_type': valid_records['depth_metric_type'],
            'depth_unit_enum': valid_records['depth_unit_enum'],
            
            'beam_value': valid_records['beam_value'],
            'beam_metric_type': valid_records['beam_metric_type'], 
            'beam_unit_enum': valid_records['beam_unit_enum'],
            
            'tonnage_value': valid_records['tonnage_value'],
            'tonnage_metric_type': valid_records['tonnage_metric_type'],
            
            'engine_power': valid_records['engine_power'],
            'engine_power_unit_enum': valid_records['engine_power_unit_enum'],
            
            'fish_hold_capacity': valid_records['fish_hold_capacity'],
            'freezer_capacity': valid_records['freezer_capacity']
        })
        
        # Save comprehensive cleaned data
        output_df.to_csv(output_file, index=False)
        print(f"NPFC comprehensive cleaning complete: {len(output_df)} records saved")
        print(f"CORRECTED: Unit enums fixed (METER not METERS, CUBIC_METER not CUBIC_METERS)")
        print(f"CORRECTED: Vessel type translations using EXACT user mapping: {output_df['vessel_type_code'].value_counts().to_dict()}")
        print(f"CORRECTED: Fishing method translations using EXACT user mapping: {output_df['fishing_method_code'].value_counts().to_dict()}")
        print(f"Freezer type enums processed: {output_df['freezer_type_enum'].value_counts().to_dict()}")
        print(f"CORRECTED freezer unit enums: {output_df['freezer_unit_enum'].value_counts().to_dict()}")
        print(f"All 33 columns processed and mapped to comprehensive vessel database schema")
        print(f"ALL ORIGINAL COMPREHENSIVE FUNCTIONALITY MAINTAINED + EXACT USER TRANSLATIONS + ENUM CORRECTIONS APPLIED")
        
    except Exception as e:
        print(f"Error in NPFC comprehensive cleaning: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
EOF

# Run comprehensive cleaning with corrected enum support
if python3 /tmp/clean_npfc_comprehensive_corrected.py "$INPUT_FILE" "$OUTPUT_FILE" "$SOURCE_DATE"; then
    RECORD_COUNT=$(tail -n +2 "$OUTPUT_FILE" | wc -l)
    COLUMN_COUNT=$(head -1 "$OUTPUT_FILE" | tr ',' '\n' | wc -l)
    log_success "✅ NPFC comprehensive cleaning complete:"
    log_success "   - $RECORD_COUNT vessel records processed"
    log_success "   - $COLUMN_COUNT output columns (includes all enum fields)"
    log_success "   - All 33 NPFC input columns processed and mapped to comprehensive vessel schema"
    log_success "   - CORRECTED: Unit enums fixed (METER not METERS, CUBIC_METER not CUBIC_METERS)"
    log_success "   - CORRECTED: Vessel type translations using EXACT user-provided mapping"
    log_success "   - CORRECTED: Fishing method translations using EXACT user-provided mapping"
    log_success "   - Comprehensive freezer type enum standardization (19 types supported)"
    log_success "   - CORRECTED: Freezer units mapped to standard database enums (METRIC_TONS / DAY, TONS / DAY)"
    log_success "   - Country codes standardized to alpha3"
    log_success "   - Measurement types differentiated (LOA/RGL/LBP, Draft/Moulded, etc.)"
    log_success "   - Units standardized to database enum values (CORRECTED)"
    log_success "   - Previous vessel history preserved"
    log_success "   - Equipment data structured with comprehensive enum support"
    log_success "   - ALL ORIGINAL COMPREHENSIVE FUNCTIONALITY MAINTAINED + EXACT USER TRANSLATIONS"
    
    rm -f /tmp/clean_npfc_comprehensive_corrected.py
else
    log_error "NPFC comprehensive cleaning failed"
    exit 1
fi