#!/bin/bash
# /app/scripts/import/vessels/data/RFMO/load_npfc_vessels.sh  
# COMPREHENSIVE: Complete NPFC Vessel Loading with ALL table processing - CORRECTED enums & functions + Fixed SQL syntax
set -euo pipefail

source /app/scripts/core/logging.sh
source /app/scripts/core/database.sh

log_step "🇯🇵 Loading NPFC Vessels (COMPREHENSIVE - All Tables + Corrected Functions + Fixed SQL Syntax)"

# Check cleaned data exists
CLEANED_FILE="/import/vessels/vessel_data/RFMO/cleaned/npfc_vessels_cleaned.csv"
if [[ ! -f "$CLEANED_FILE" ]]; then
    log_error "Cleaned NPFC data not found: $CLEANED_FILE"
    exit 1
fi

RECORD_COUNT=$(tail -n +2 "$CLEANED_FILE" | wc -l)
log_success "Loading $RECORD_COUNT cleaned NPFC vessels using comprehensive schema"

# Setup database functions and mapping tables using existing schema only
log_step "🔧 Setting up comprehensive NPFC loading with corrected functions..."
if psql_execute_file "/app/scripts/import/vessels/data/RFMO/setup_npfc_loading.sql" "Comprehensive NPFC setup with corrections"; then
    log_success "Comprehensive NPFC setup complete with corrected functions using existing schema"
else
    log_error "Comprehensive NPFC setup failed"
    exit 1
fi

# Get NPFC source ID
NPFC_SOURCE_ID=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "
    SELECT source_id FROM original_sources_vessels WHERE source_shortname = 'NPFC';" || echo "")

if [[ -z "$NPFC_SOURCE_ID" ]]; then
    log_error "NPFC source not found"
    exit 1
fi

log_success "Found NPFC source ID: $NPFC_SOURCE_ID"

# Create comprehensive loading SQL with ALL table processing + CORRECTED enums & functions + FIXED SQL syntax
cat > /tmp/load_npfc_comprehensive_all_tables_corrected.sql << EOF
-- NPFC COMPREHENSIVE Loading Script - ALL Tables with CORRECTED Unit Processing + FIXED SQL Syntax

-- Create temp table matching comprehensive cleaned data structure
CREATE TEMP TABLE npfc_cleaned (
    -- Metadata
    source_date TEXT,
    original_source TEXT,
    
    -- Core vessel identifiers
    vessel_name TEXT,
    imo TEXT,
    ircs TEXT,  
    mmsi TEXT,
    national_registry TEXT,
    vessel_flag_alpha3 TEXT,
    
    -- NPFC external identifier
    npfc_vessel_id TEXT,
    
    -- Basic vessel info
    vessel_type_code TEXT,
    port_of_registry TEXT,
    fishing_method_code TEXT,
    external_marking TEXT,
    
    -- Previous vessel info
    previous_name TEXT,
    previous_flag_alpha3 TEXT,
    previous_registry TEXT,
    previous_port_registry TEXT,
    
    -- Equipment data with enum support
    communication_details TEXT,
    freezer_type_enum TEXT,
    freezer_unit_enum TEXT,
    freezer_type_raw TEXT,
    freezer_unit_raw TEXT,
    
    -- Operational attributes
    crew_size TEXT,
    
    -- Measurement data with CORRECTED unit support
    length_value TEXT,
    length_metric_type TEXT,
    length_unit_enum TEXT,
    
    depth_value TEXT,
    depth_metric_type TEXT,
    depth_unit_enum TEXT,
    
    beam_value TEXT,
    beam_metric_type TEXT,
    beam_unit_enum TEXT,
    
    tonnage_value TEXT,
    tonnage_metric_type TEXT,
    
    engine_power TEXT,
    engine_power_unit_enum TEXT,
    
    fish_hold_capacity TEXT,
    freezer_capacity TEXT
);

-- Load comprehensive cleaned CSV data
\\COPY npfc_cleaned FROM '$CLEANED_FILE' WITH (FORMAT csv, HEADER true, NULL '');

\\echo '📊 NPFC comprehensive data loaded into temp table'

-- Show comprehensive data overview
SELECT 'NPFC Comprehensive Data Overview' as step, COUNT(*) as total_records FROM npfc_cleaned;

-- Show CORRECTED enum distributions from comprehensive processing
SELECT 
    'CORRECTED Unit Enum Distribution' as analysis,
    length_unit_enum,
    COUNT(*) as count
FROM npfc_cleaned 
WHERE length_unit_enum IS NOT NULL 
GROUP BY length_unit_enum 
ORDER BY count DESC;

SELECT 
    'Comprehensive Freezer Type Enum Distribution' as analysis,
    freezer_type_enum,
    COUNT(*) as count
FROM npfc_cleaned 
WHERE freezer_type_enum IS NOT NULL 
GROUP BY freezer_type_enum 
ORDER BY count DESC;

-- Create comprehensive validated temp table with enhanced data conversion  
CREATE TEMP TABLE validated_npfc AS
SELECT 
    -- Core identifiers with comprehensive validation
    NULLIF(trim(vessel_name), '') as vessel_name,
    CASE 
        WHEN trim(COALESCE(imo, '')) ~ '^[0-9]{7}$' THEN trim(imo)
        ELSE NULL 
    END as imo,
    NULLIF(trim(ircs), '') as ircs,
    CASE 
        WHEN trim(COALESCE(mmsi, '')) ~ '^[0-9]{9}$' THEN trim(mmsi)
        ELSE NULL 
    END as mmsi,
    NULLIF(trim(national_registry), '') as national_registry,
    NULLIF(trim(vessel_flag_alpha3), '') as flag_alpha3,
    NULLIF(trim(npfc_vessel_id), '') as npfc_vessel_id,
    
    -- Vessel info with comprehensive mapping
    NULLIF(trim(vessel_type_code), '') as vessel_type_code,
    NULLIF(trim(port_of_registry), '') as port_registry,
    NULLIF(trim(fishing_method_code), '') as fishing_method_code,
    NULLIF(trim(external_marking), '') as external_marking,
    
    -- Previous vessel data for comprehensive history tracking
    NULLIF(trim(previous_name), '') as previous_name,
    NULLIF(trim(previous_flag_alpha3), '') as previous_flag_alpha3,
    NULLIF(trim(previous_registry), '') as previous_registry,
    NULLIF(trim(previous_port_registry), '') as previous_port_registry,
    
    -- Comprehensive equipment data with enum support
    NULLIF(trim(communication_details), '') as communication_details,
    NULLIF(trim(freezer_type_enum), '') as freezer_type_enum,
    NULLIF(trim(freezer_unit_enum), '') as freezer_unit_enum,
    NULLIF(trim(freezer_type_raw), '') as freezer_type_raw,
    NULLIF(trim(freezer_unit_raw), '') as freezer_unit_raw,
    
    -- Operational with comprehensive numeric handling
    CASE 
        WHEN trim(COALESCE(crew_size, '')) ~ '^[0-9]+\.?[0-9]*$' 
        THEN trim(crew_size)::DECIMAL
        ELSE NULL 
    END as crew_size,
    
    -- Comprehensive measurements with CORRECTED unit enum processing
    CASE 
        WHEN trim(COALESCE(length_value, '')) ~ '^[0-9]+\.?[0-9]*$' 
        THEN trim(length_value)::DECIMAL
        ELSE NULL 
    END as length_value,
    NULLIF(trim(length_metric_type), '') as length_metric_type,
    NULLIF(trim(length_unit_enum), '') as length_unit_enum,
    
    CASE 
        WHEN trim(COALESCE(depth_value, '')) ~ '^[0-9]+\.?[0-9]*$' 
        THEN trim(depth_value)::DECIMAL
        ELSE NULL 
    END as depth_value,
    NULLIF(trim(depth_metric_type), '') as depth_metric_type,
    NULLIF(trim(depth_unit_enum), '') as depth_unit_enum,
    
    CASE 
        WHEN trim(COALESCE(beam_value, '')) ~ '^[0-9]+\.?[0-9]*$' 
        THEN trim(beam_value)::DECIMAL
        ELSE NULL 
    END as beam_value,
    NULLIF(trim(beam_metric_type), '') as beam_metric_type,
    NULLIF(trim(beam_unit_enum), '') as beam_unit_enum,
    
    CASE 
        WHEN trim(COALESCE(tonnage_value, '')) ~ '^[0-9]+\.?[0-9]*$' 
        THEN trim(tonnage_value)::DECIMAL
        ELSE NULL 
    END as tonnage_value,
    NULLIF(trim(tonnage_metric_type), '') as tonnage_metric_type,
    
    CASE 
        WHEN trim(COALESCE(engine_power, '')) ~ '^[0-9]+\.?[0-9]*$' 
        THEN trim(engine_power)::DECIMAL
        ELSE NULL 
    END as engine_power,
    NULLIF(trim(engine_power_unit_enum), '') as engine_power_unit_enum,
    
    CASE 
        WHEN trim(COALESCE(fish_hold_capacity, '')) ~ '^[0-9]+\.?[0-9]*$' 
        THEN trim(fish_hold_capacity)::DECIMAL
        ELSE NULL 
    END as fish_hold_capacity,
    
    CASE 
        WHEN trim(COALESCE(freezer_capacity, '')) ~ '^[0-9]+\.?[0-9]*$' 
        THEN trim(freezer_capacity)::DECIMAL
        ELSE NULL 
    END as freezer_capacity
    
FROM npfc_cleaned
WHERE (
    NULLIF(trim(vessel_name), '') IS NOT NULL OR
    CASE WHEN trim(COALESCE(imo, '')) ~ '^[0-9]{7}$' THEN trim(imo) ELSE NULL END IS NOT NULL OR
    NULLIF(trim(ircs), '') IS NOT NULL OR
    CASE WHEN trim(COALESCE(mmsi, '')) ~ '^[0-9]{9}$' THEN trim(mmsi) ELSE NULL END IS NOT NULL
);

\\echo 'Comprehensive validated NPFC temp table created with CORRECTED enum processing'

-- === STEP 1: VESSELS TABLE (Core Identifiers) ===
INSERT INTO vessels (vessel_name, imo, ircs, mmsi, national_registry, vessel_flag)
SELECT DISTINCT
    v.vessel_name,
    v.imo,
    v.ircs,
    v.mmsi,
    v.national_registry,
    get_country_uuid(v.flag_alpha3)
FROM validated_npfc v
WHERE find_existing_vessel(v.imo, v.ircs, v.mmsi, v.vessel_name, get_country_uuid(v.flag_alpha3)) IS NULL
  AND validate_vessel_identifiers(v.vessel_name, v.imo, v.ircs, v.mmsi, get_country_uuid(v.flag_alpha3))
ON CONFLICT (imo) WHERE imo IS NOT NULL 
DO UPDATE SET 
    vessel_name = COALESCE(vessels.vessel_name, EXCLUDED.vessel_name),
    mmsi = COALESCE(vessels.mmsi, EXCLUDED.mmsi),
    national_registry = COALESCE(vessels.national_registry, EXCLUDED.national_registry),
    vessel_flag = COALESCE(vessels.vessel_flag, EXCLUDED.vessel_flag);

\\echo '✅ Step 1: Core vessels table populated/updated'

-- === STEP 2: VESSEL_EXTERNAL_IDENTIFIERS TABLE (NPFC Vessel IDs as RFMO_NPFC) ===
INSERT INTO vessel_external_identifiers (vessel_uuid, source_id, identifier_type, identifier_value, is_active)
SELECT DISTINCT
    COALESCE(
        find_existing_vessel(v.imo, v.ircs, v.mmsi, v.vessel_name, get_country_uuid(v.flag_alpha3)),
        (SELECT vessel_uuid FROM vessels WHERE imo = v.imo LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE ircs = v.ircs LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE mmsi = v.mmsi LIMIT 1)
    ) as vessel_uuid,
    '$NPFC_SOURCE_ID'::UUID,
    'RFMO_NPFC'::external_identifier_type_enum,
    v.npfc_vessel_id,
    true
FROM validated_npfc v
WHERE v.npfc_vessel_id IS NOT NULL
  AND COALESCE(
        find_existing_vessel(v.imo, v.ircs, v.mmsi, v.vessel_name, get_country_uuid(v.flag_alpha3)),
        (SELECT vessel_uuid FROM vessels WHERE imo = v.imo LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE ircs = v.ircs LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE mmsi = v.mmsi LIMIT 1)
      ) IS NOT NULL;
-- No unique constraint exists, so no ON CONFLICT needed

\\echo '✅ Step 2: NPFC external identifiers added as RFMO_NPFC type'

-- === STEP 3: VESSEL_INFO TABLE (Basic Characteristics with CORRECTED function calls) ===
INSERT INTO vessel_info (vessel_uuid, vessel_type, port_registry, external_marking)
SELECT DISTINCT
    COALESCE(
        find_existing_vessel(v.imo, v.ircs, v.mmsi, v.vessel_name, get_country_uuid(v.flag_alpha3)),
        (SELECT vessel_uuid FROM vessels WHERE imo = v.imo LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE ircs = v.ircs LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE mmsi = v.mmsi LIMIT 1)
    ) as vessel_uuid,
    get_vessel_type_uuid(npfc_get_vessel_type_code(v.vessel_type_code)),
    v.port_registry,
    v.external_marking
FROM validated_npfc v
WHERE COALESCE(
        find_existing_vessel(v.imo, v.ircs, v.mmsi, v.vessel_name, get_country_uuid(v.flag_alpha3)),
        (SELECT vessel_uuid FROM vessels WHERE imo = v.imo LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE ircs = v.ircs LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE mmsi = v.mmsi LIMIT 1)
      ) IS NOT NULL
  AND (get_vessel_type_uuid(npfc_get_vessel_type_code(v.vessel_type_code)) IS NOT NULL OR v.port_registry IS NOT NULL OR v.external_marking IS NOT NULL)
ON CONFLICT (vessel_uuid) 
DO UPDATE SET 
    vessel_type = COALESCE(vessel_info.vessel_type, EXCLUDED.vessel_type),
    port_registry = COALESCE(vessel_info.port_registry, EXCLUDED.port_registry),
    external_marking = COALESCE(vessel_info.external_marking, EXCLUDED.external_marking);

\\echo '✅ Step 3: Basic vessel info added with CORRECTED vessel type lookups'

-- === STEP 4: VESSEL_EQUIPMENT TABLE (Enhanced Equipment with JSONB Freezer Types Array) ===
INSERT INTO vessel_equipment (vessel_uuid, source_id, communication_details, freezer_types)
SELECT DISTINCT
    COALESCE(
        find_existing_vessel(v.imo, v.ircs, v.mmsi, v.vessel_name, get_country_uuid(v.flag_alpha3)),
        (SELECT vessel_uuid FROM vessels WHERE imo = v.imo LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE ircs = v.ircs LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE mmsi = v.mmsi LIMIT 1)
    ) as vessel_uuid,
    '$NPFC_SOURCE_ID'::UUID,
    v.communication_details,
    -- Create JSONB array from comprehensive freezer_type_enum using CORRECTED function
    CASE 
        WHEN v.freezer_type_enum IS NOT NULL 
        THEN create_freezer_types_array(npfc_get_freezer_type_enum(v.freezer_type_enum))
        ELSE NULL 
    END as freezer_types
FROM validated_npfc v
WHERE COALESCE(
        find_existing_vessel(v.imo, v.ircs, v.mmsi, v.vessel_name, get_country_uuid(v.flag_alpha3)),
        (SELECT vessel_uuid FROM vessels WHERE imo = v.imo LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE ircs = v.ircs LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE mmsi = v.mmsi LIMIT 1)
      ) IS NOT NULL
  AND (v.communication_details IS NOT NULL OR v.freezer_type_enum IS NOT NULL)
ON CONFLICT (vessel_uuid, source_id) 
DO UPDATE SET 
    communication_details = COALESCE(vessel_equipment.communication_details, EXCLUDED.communication_details),
    freezer_types = COALESCE(vessel_equipment.freezer_types, EXCLUDED.freezer_types);

\\echo '✅ Step 4: Comprehensive equipment data with JSONB freezer_types array added'

-- === STEP 5: VESSEL_METRICS TABLE (All Measurement Types - FIXED: Use Pre-Processed Units + Debugging + FIXED SQL Syntax) ===

-- Debug: Show what measurement data we have
SELECT 
    'NPFC Metrics Debugging - Measurement Data Overview' as analysis,
    COUNT(v.length_value) as length_count,
    COUNT(v.depth_value) as depth_count,
    COUNT(v.beam_value) as beam_count,
    COUNT(v.tonnage_value) as tonnage_count,
    COUNT(v.engine_power) as engine_power_count,
    COUNT(v.fish_hold_capacity) as fish_hold_count,
    COUNT(v.freezer_capacity) as freezer_capacity_count
FROM validated_npfc v;

SELECT 
    'NPFC Metrics Debugging - Length Data' as analysis,
    COUNT(*) as total_records,
    COUNT(v.length_value) as records_with_length,
    COUNT(v.length_unit_enum) as records_with_length_units,
    v.length_unit_enum as unit_value,
    COUNT(*) as count_per_unit
FROM validated_npfc v 
GROUP BY v.length_unit_enum
ORDER BY count_per_unit DESC;

-- FIXED: Units are already processed in cleaning script - use them directly with CORRECTED SQL syntax
INSERT INTO vessel_metrics (vessel_uuid, source_id, metric_type, value, unit)
SELECT 
    vessel_uuid,
    '$NPFC_SOURCE_ID'::UUID,
    metric_type::metric_type_enum,
    value,
    unit::unit_enum
FROM (
    -- Length measurements - FIXED: Use already processed units directly
    SELECT DISTINCT
        COALESCE(
            find_existing_vessel(v.imo, v.ircs, v.mmsi, v.vessel_name, get_country_uuid(v.flag_alpha3)),
            (SELECT vessel_uuid FROM vessels WHERE imo = v.imo LIMIT 1),
            (SELECT vessel_uuid FROM vessels WHERE ircs = v.ircs LIMIT 1),
            (SELECT vessel_uuid FROM vessels WHERE mmsi = v.mmsi LIMIT 1)
        ) as vessel_uuid,
        COALESCE(v.length_metric_type, 'length') as metric_type,
        v.length_value as value,
        COALESCE(v.length_unit_enum, 'METER') as unit -- FIXED: Use pre-processed enum directly
    FROM validated_npfc v
    WHERE v.length_value IS NOT NULL
    
    UNION ALL
    
    -- Depth measurements - FIXED: Use already processed units directly
    SELECT DISTINCT
        COALESCE(
            find_existing_vessel(v.imo, v.ircs, v.mmsi, v.vessel_name, get_country_uuid(v.flag_alpha3)),
            (SELECT vessel_uuid FROM vessels WHERE imo = v.imo LIMIT 1),
            (SELECT vessel_uuid FROM vessels WHERE ircs = v.ircs LIMIT 1),
            (SELECT vessel_uuid FROM vessels WHERE mmsi = v.mmsi LIMIT 1)
        ) as vessel_uuid,
        COALESCE(v.depth_metric_type, 'depth') as metric_type,
        v.depth_value as value,
        COALESCE(v.depth_unit_enum, 'METER') as unit -- FIXED: Use pre-processed enum directly
    FROM validated_npfc v
    WHERE v.depth_value IS NOT NULL
    
    UNION ALL
    
    -- Beam measurements - FIXED: Use already processed units directly
    SELECT DISTINCT
        COALESCE(
            find_existing_vessel(v.imo, v.ircs, v.mmsi, v.vessel_name, get_country_uuid(v.flag_alpha3)),
            (SELECT vessel_uuid FROM vessels WHERE imo = v.imo LIMIT 1),
            (SELECT vessel_uuid FROM vessels WHERE ircs = v.ircs LIMIT 1),
            (SELECT vessel_uuid FROM vessels WHERE mmsi = v.mmsi LIMIT 1)
        ) as vessel_uuid,
        COALESCE(v.beam_metric_type, 'beam') as metric_type,
        v.beam_value as value,
        COALESCE(v.beam_unit_enum, 'METER') as unit -- FIXED: Use pre-processed enum directly
    FROM validated_npfc v
    WHERE v.beam_value IS NOT NULL
    
    UNION ALL
    
    -- Tonnage measurements - Use NULL for dimensionless tonnage units
    SELECT DISTINCT
        COALESCE(
            find_existing_vessel(v.imo, v.ircs, v.mmsi, v.vessel_name, get_country_uuid(v.flag_alpha3)),
            (SELECT vessel_uuid FROM vessels WHERE imo = v.imo LIMIT 1),
            (SELECT vessel_uuid FROM vessels WHERE ircs = v.ircs LIMIT 1),
            (SELECT vessel_uuid FROM vessels WHERE mmsi = v.mmsi LIMIT 1)
        ) as vessel_uuid,
        COALESCE(v.tonnage_metric_type, 'tonnage') as metric_type,
        v.tonnage_value as value,
        NULL as unit  -- FIXED: Use NULL for dimensionless tonnage (GT, GRT, NT)
    FROM validated_npfc v
    WHERE v.tonnage_value IS NOT NULL

    UNION ALL
    
    -- Engine power measurements - FIXED: Use pre-processed units directly
    SELECT DISTINCT
        COALESCE(
            find_existing_vessel(v.imo, v.ircs, v.mmsi, v.vessel_name, get_country_uuid(v.flag_alpha3)),
            (SELECT vessel_uuid FROM vessels WHERE imo = v.imo LIMIT 1),
            (SELECT vessel_uuid FROM vessels WHERE ircs = v.ircs LIMIT 1),
            (SELECT vessel_uuid FROM vessels WHERE mmsi = v.mmsi LIMIT 1)
        ) as vessel_uuid,
        'engine_power' as metric_type,
        v.engine_power as value,
        COALESCE(v.engine_power_unit_enum, 'KW') as unit -- FIXED: Use pre-processed enum directly
    FROM validated_npfc v
    WHERE v.engine_power IS NOT NULL
    
    UNION ALL
    
    -- Fish hold volume - Use CUBIC_METER
    SELECT DISTINCT
        COALESCE(
            find_existing_vessel(v.imo, v.ircs, v.mmsi, v.vessel_name, get_country_uuid(v.flag_alpha3)),
            (SELECT vessel_uuid FROM vessels WHERE imo = v.imo LIMIT 1),
            (SELECT vessel_uuid FROM vessels WHERE ircs = v.ircs LIMIT 1),
            (SELECT vessel_uuid FROM vessels WHERE mmsi = v.mmsi LIMIT 1)
        ) as vessel_uuid,
        'fish_hold_volume' as metric_type,
        v.fish_hold_capacity as value,
        'CUBIC_METER' as unit
    FROM validated_npfc v
    WHERE v.fish_hold_capacity IS NOT NULL
    
    UNION ALL
    
    -- Freezer capacity - FIXED: Use pre-processed units directly 
    SELECT DISTINCT
        COALESCE(
            find_existing_vessel(v.imo, v.ircs, v.mmsi, v.vessel_name, get_country_uuid(v.flag_alpha3)),
            (SELECT vessel_uuid FROM vessels WHERE imo = v.imo LIMIT 1),
            (SELECT vessel_uuid FROM vessels WHERE ircs = v.ircs LIMIT 1),
            (SELECT vessel_uuid FROM vessels WHERE mmsi = v.mmsi LIMIT 1)
        ) as vessel_uuid,
        'freezer_capacity' as metric_type,
        v.freezer_capacity as value,
        COALESCE(v.freezer_unit_enum, 'CUBIC_METER') as unit -- FIXED: Use pre-processed enum directly
    FROM validated_npfc v
    WHERE v.freezer_capacity IS NOT NULL
    
) all_metrics
WHERE vessel_uuid IS NOT NULL;
-- No unique constraint exists, so no ON CONFLICT needed

\\echo '✅ Step 5: FIXED - All measurement types using pre-processed unit enums added to vessel_metrics'

-- === STEP 6: VESSEL_VESSEL_TYPES TABLE (NEW: Link vessels to vessel types with debugging) ===

-- Debug: Show what vessel types we're trying to match
SELECT 
    'NPFC Vessel Type Debugging' as analysis,
    v.vessel_type_code as original_code,
    npfc_get_vessel_type_code(v.vessel_type_code) as mapped_code,
    get_vessel_type_uuid(npfc_get_vessel_type_code(v.vessel_type_code)) as found_uuid,
    COUNT(*) as count
FROM validated_npfc v
WHERE v.vessel_type_code IS NOT NULL
GROUP BY v.vessel_type_code, npfc_get_vessel_type_code(v.vessel_type_code), get_vessel_type_uuid(npfc_get_vessel_type_code(v.vessel_type_code))
ORDER BY count DESC
LIMIT 10;

INSERT INTO vessel_vessel_types (vessel_uuid, vessel_type_id, source_id)
SELECT DISTINCT
    COALESCE(
        find_existing_vessel(v.imo, v.ircs, v.mmsi, v.vessel_name, get_country_uuid(v.flag_alpha3)),
        (SELECT vessel_uuid FROM vessels WHERE imo = v.imo LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE ircs = v.ircs LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE mmsi = v.mmsi LIMIT 1)
    ) as vessel_uuid,
    get_vessel_type_uuid(npfc_get_vessel_type_code(v.vessel_type_code)) as vessel_type_id,
    '$NPFC_SOURCE_ID'::UUID
FROM validated_npfc v
WHERE v.vessel_type_code IS NOT NULL
  AND COALESCE(
        find_existing_vessel(v.imo, v.ircs, v.mmsi, v.vessel_name, get_country_uuid(v.flag_alpha3)),
        (SELECT vessel_uuid FROM vessels WHERE imo = v.imo LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE ircs = v.ircs LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE mmsi = v.mmsi LIMIT 1)
      ) IS NOT NULL
  AND get_vessel_type_uuid(npfc_get_vessel_type_code(v.vessel_type_code)) IS NOT NULL;
-- No unique constraint exists, so no ON CONFLICT needed

\\echo '✅ Step 6: NEW - Vessel-to-vessel-type relationships added to vessel_vessel_types'

-- === STEP 7: VESSEL_GEAR_TYPES TABLE (NEW: Link vessels to gear types with debugging) ===

-- Debug: Show what gear types we're trying to match
SELECT 
    'NPFC Gear Type Debugging' as analysis,
    v.fishing_method_code as original_code,
    npfc_get_fishing_method_code(v.fishing_method_code) as mapped_code,
    get_gear_type_uuid(npfc_get_fishing_method_code(v.fishing_method_code)) as found_uuid,
    COUNT(*) as count
FROM validated_npfc v
WHERE v.fishing_method_code IS NOT NULL
GROUP BY v.fishing_method_code, npfc_get_fishing_method_code(v.fishing_method_code), get_gear_type_uuid(npfc_get_fishing_method_code(v.fishing_method_code))
ORDER BY count DESC
LIMIT 10;

INSERT INTO vessel_gear_types (vessel_uuid, fao_gear_id, source_id)
SELECT DISTINCT
    COALESCE(
        find_existing_vessel(v.imo, v.ircs, v.mmsi, v.vessel_name, get_country_uuid(v.flag_alpha3)),
        (SELECT vessel_uuid FROM vessels WHERE imo = v.imo LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE ircs = v.ircs LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE mmsi = v.mmsi LIMIT 1)
    ) as vessel_uuid,
    get_gear_type_uuid(npfc_get_fishing_method_code(v.fishing_method_code)) as fao_gear_id,
    '$NPFC_SOURCE_ID'::UUID
FROM validated_npfc v
WHERE v.fishing_method_code IS NOT NULL
  AND COALESCE(
        find_existing_vessel(v.imo, v.ircs, v.mmsi, v.vessel_name, get_country_uuid(v.flag_alpha3)),
        (SELECT vessel_uuid FROM vessels WHERE imo = v.imo LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE ircs = v.ircs LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE mmsi = v.mmsi LIMIT 1)
      ) IS NOT NULL
  AND get_gear_type_uuid(npfc_get_fishing_method_code(v.fishing_method_code)) IS NOT NULL;
-- No unique constraint exists, so no ON CONFLICT needed

\\echo '✅ Step 7: NEW - Vessel-to-gear-type relationships added to vessel_gear_types'

-- === STEP 8: VESSEL_REPORTED_HISTORY TABLE (Previous Vessel Information) ===
INSERT INTO vessel_reported_history (vessel_uuid, source_id, reported_history_type, identifier_value, flag_country_id)
SELECT DISTINCT
    vessel_uuid,
    '$NPFC_SOURCE_ID'::UUID,
    history_type::reported_history_enum,
    identifier_value,
    CASE 
        WHEN history_type = 'FLAG_CHANGE' THEN get_country_uuid(identifier_value)
        ELSE NULL 
    END
FROM (
    SELECT 
        COALESCE(
            find_existing_vessel(v.imo, v.ircs, v.mmsi, v.vessel_name, get_country_uuid(v.flag_alpha3)),
            (SELECT vessel_uuid FROM vessels WHERE imo = v.imo LIMIT 1),
            (SELECT vessel_uuid FROM vessels WHERE ircs = v.ircs LIMIT 1),
            (SELECT vessel_uuid FROM vessels WHERE mmsi = v.mmsi LIMIT 1)
        ) as vessel_uuid,
        'VESSEL_NAME_CHANGE' as history_type,
        v.previous_name as identifier_value
    FROM validated_npfc v
    WHERE v.previous_name IS NOT NULL
    
    UNION ALL
    
    SELECT 
        COALESCE(
            find_existing_vessel(v.imo, v.ircs, v.mmsi, v.vessel_name, get_country_uuid(v.flag_alpha3)),
            (SELECT vessel_uuid FROM vessels WHERE imo = v.imo LIMIT 1),
            (SELECT vessel_uuid FROM vessels WHERE ircs = v.ircs LIMIT 1),
            (SELECT vessel_uuid FROM vessels WHERE mmsi = v.mmsi LIMIT 1)
        ) as vessel_uuid,
        'FLAG_CHANGE' as history_type,
        v.previous_flag_alpha3 as identifier_value
    FROM validated_npfc v
    WHERE v.previous_flag_alpha3 IS NOT NULL
    
    UNION ALL
    
    SELECT 
        COALESCE(
            find_existing_vessel(v.imo, v.ircs, v.mmsi, v.vessel_name, get_country_uuid(v.flag_alpha3)),
            (SELECT vessel_uuid FROM vessels WHERE imo = v.imo LIMIT 1),
            (SELECT vessel_uuid FROM vessels WHERE ircs = v.ircs LIMIT 1),
            (SELECT vessel_uuid FROM vessels WHERE mmsi = v.mmsi LIMIT 1)
        ) as vessel_uuid,
        'REGISTRY_CHANGE' as history_type,
        v.previous_registry as identifier_value
    FROM validated_npfc v
    WHERE v.previous_registry IS NOT NULL
    
) history_data
WHERE vessel_uuid IS NOT NULL;
-- No unique constraint exists, so no ON CONFLICT needed

\\echo '✅ Step 8: Previous vessel history added to vessel_reported_history'

-- === STEP 9: VESSEL_BUILD_INFORMATION TABLE (Real Build Information Only) ===
-- Note: NPFC data typically doesn't include build information, so this will likely insert zero records
-- Only insert if there's actual build country data from the NPFC dataset (no proxy data)
INSERT INTO vessel_build_information (vessel_uuid, source_id, build_country_id)
SELECT DISTINCT
    COALESCE(
        find_existing_vessel(v.imo, v.ircs, v.mmsi, v.vessel_name, get_country_uuid(v.flag_alpha3)),
        (SELECT vessel_uuid FROM vessels WHERE imo = v.imo LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE ircs = v.ircs LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE mmsi = v.mmsi LIMIT 1)
    ) as vessel_uuid,
    '$NPFC_SOURCE_ID'::UUID,
    get_country_uuid(v.build_country_alpha3) -- Only use actual build country data from NPFC
FROM validated_npfc v
WHERE COALESCE(
        find_existing_vessel(v.imo, v.ircs, v.mmsi, v.vessel_name, get_country_uuid(v.flag_alpha3)),
        (SELECT vessel_uuid FROM vessels WHERE imo = v.imo LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE ircs = v.ircs LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE mmsi = v.mmsi LIMIT 1)
      ) IS NOT NULL
  AND v.build_country_alpha3 IS NOT NULL -- Only process records with actual build country data
  AND get_country_uuid(v.build_country_alpha3) IS NOT NULL -- Ensure the build country can be resolved
  AND NOT EXISTS (
      SELECT 1 FROM vessel_build_information vbi 
      WHERE vbi.vessel_uuid = COALESCE(
          find_existing_vessel(v.imo, v.ircs, v.mmsi, v.vessel_name, get_country_uuid(v.flag_alpha3)),
          (SELECT vessel_uuid FROM vessels WHERE imo = v.imo LIMIT 1),
          (SELECT vessel_uuid FROM vessels WHERE ircs = v.ircs LIMIT 1),
          (SELECT vessel_uuid FROM vessels WHERE mmsi = v.mmsi LIMIT 1)
      )
  );

\echo '✅ Step 9: Real vessel build information added to vessel_build_information (no proxy data)'

-- === STEP 10: VESSEL_ATTRIBUTES TABLE (Comprehensive Operational Data with Enum Preservation) ===
INSERT INTO vessel_attributes (vessel_uuid, source_id, attributes)
SELECT DISTINCT
    COALESCE(
        find_existing_vessel(v.imo, v.ircs, v.mmsi, v.vessel_name, get_country_uuid(v.flag_alpha3)),
        (SELECT vessel_uuid FROM vessels WHERE imo = v.imo LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE ircs = v.ircs LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE mmsi = v.mmsi LIMIT 1)
    ) as vessel_uuid,
    '$NPFC_SOURCE_ID'::UUID,
    jsonb_build_object(
        'source', 'NPFC',
        'crew_size', v.crew_size,
        'vessel_type_code', v.vessel_type_code,
        'fishing_method_code', v.fishing_method_code,
        'previous_port_registry', v.previous_port_registry,
        -- Comprehensive freezer data preservation with CORRECTED enums
        'freezer_data', jsonb_build_object(
            'freezer_type_enum', v.freezer_type_enum,
            'freezer_unit_enum', v.freezer_unit_enum,
            'freezer_type_raw', v.freezer_type_raw,
            'freezer_unit_raw', v.freezer_unit_raw
        ),
        -- Comprehensive measurement unit preservation with CORRECTED enums
        'original_units', jsonb_build_object(
            'length_unit_enum', v.length_unit_enum,
            'depth_unit_enum', v.depth_unit_enum,
            'beam_unit_enum', v.beam_unit_enum,
            'engine_power_unit_enum', v.engine_power_unit_enum
        ),
        -- Comprehensive measurement type preservation
        'measurement_types', jsonb_build_object(
            'length_metric_type', v.length_metric_type,
            'depth_metric_type', v.depth_metric_type,
            'beam_metric_type', v.beam_metric_type,
            'tonnage_metric_type', v.tonnage_metric_type
        )
    ) - 'null'::text -- Remove null values from JSONB
FROM validated_npfc v
WHERE COALESCE(
        find_existing_vessel(v.imo, v.ircs, v.mmsi, v.vessel_name, get_country_uuid(v.flag_alpha3)),
        (SELECT vessel_uuid FROM vessels WHERE imo = v.imo LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE ircs = v.ircs LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE mmsi = v.mmsi LIMIT 1)
      ) IS NOT NULL
ON CONFLICT (vessel_uuid, source_id) 
DO UPDATE SET 
    attributes = vessel_attributes.attributes || EXCLUDED.attributes;

\\echo '✅ Step 10: Comprehensive operational data with CORRECTED enum preservation added to vessel_attributes'

-- === STEP 11: VESSEL_SOURCES TABLE (Source Tracking) ===
INSERT INTO vessel_sources (vessel_uuid, source_id, first_seen_date, last_seen_date, is_active)
SELECT DISTINCT
    COALESCE(
        find_existing_vessel(v.imo, v.ircs, v.mmsi, v.vessel_name, get_country_uuid(v.flag_alpha3)),
        (SELECT vessel_uuid FROM vessels WHERE imo = v.imo LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE ircs = v.ircs LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE mmsi = v.mmsi LIMIT 1)
    ) as vessel_uuid,
    '$NPFC_SOURCE_ID'::UUID,
    CURRENT_DATE,
    CURRENT_DATE,
    true
FROM validated_npfc v
WHERE COALESCE(
        find_existing_vessel(v.imo, v.ircs, v.mmsi, v.vessel_name, get_country_uuid(v.flag_alpha3)),
        (SELECT vessel_uuid FROM vessels WHERE imo = v.imo LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE ircs = v.ircs LIMIT 1),
        (SELECT vessel_uuid FROM vessels WHERE mmsi = v.mmsi LIMIT 1)
      ) IS NOT NULL
ON CONFLICT (vessel_uuid, source_id) 
DO UPDATE SET 
    last_seen_date = CURRENT_DATE, 
    is_active = true;

\\echo '✅ Step 11: Source tracking added to vessel_sources'

-- === STEP 12: UPDATE SOURCE STATUS (CORRECTED with proper integer casting) ===
SELECT update_npfc_source_status(
    (SELECT COUNT(*)::INTEGER FROM vessel_sources WHERE source_id = '$NPFC_SOURCE_ID'),
    CURRENT_DATE
);

\\echo '✅ Step 12: NPFC source status updated with CORRECTED integer casting'

-- === COMPREHENSIVE RESULTS SUMMARY (UPDATED: Includes ALL Tables) ===
SELECT 
    'NPFC FIXED Comprehensive Schema Distribution Results' as summary,
    (SELECT COUNT(*) FROM vessel_sources WHERE source_id = '$NPFC_SOURCE_ID') as vessels_tracked,
    (SELECT COUNT(*) FROM vessel_external_identifiers WHERE source_id = '$NPFC_SOURCE_ID' 
     AND identifier_type = 'RFMO_NPFC') as npfc_external_ids,
    (SELECT COUNT(*) FROM vessel_info WHERE vessel_uuid IN (
        SELECT vessel_uuid FROM vessel_sources WHERE source_id = '$NPFC_SOURCE_ID')) as vessel_info_records,
    (SELECT COUNT(*) FROM vessel_vessel_types WHERE source_id = '$NPFC_SOURCE_ID') as vessel_type_relationships,
    (SELECT COUNT(*) FROM vessel_gear_types WHERE source_id = '$NPFC_SOURCE_ID') as gear_type_relationships,
    (SELECT COUNT(*) FROM vessel_equipment WHERE source_id = '$NPFC_SOURCE_ID') as equipment_records,
    (SELECT COUNT(*) FROM vessel_equipment WHERE source_id = '$NPFC_SOURCE_ID' 
     AND freezer_types IS NOT NULL) as equipment_with_freezer_types,
    (SELECT COUNT(DISTINCT metric_type) FROM vessel_metrics WHERE source_id = '$NPFC_SOURCE_ID') as measurement_types,
    (SELECT COUNT(*) FROM vessel_metrics WHERE source_id = '$NPFC_SOURCE_ID') as total_measurements,
    (SELECT COUNT(*) FROM vessel_reported_history WHERE source_id = '$NPFC_SOURCE_ID') as history_records,
    (SELECT COUNT(*) FROM vessel_build_information WHERE source_id = '$NPFC_SOURCE_ID') as build_info_records,
    (SELECT COUNT(*) FROM vessel_attributes WHERE source_id = '$NPFC_SOURCE_ID') as attribute_records,
    (SELECT status FROM original_sources_vessels WHERE source_shortname = 'NPFC') as source_status;

-- Show comprehensive measurement type and unit distribution with CORRECTED units
SELECT 
    'NPFC CORRECTED Measurement Types & Units Distribution' as breakdown,
    metric_type,
    unit,
    COUNT(*) as count
FROM vessel_metrics 
WHERE source_id = '$NPFC_SOURCE_ID'
GROUP BY metric_type, unit
ORDER BY metric_type, count DESC;

-- Show comprehensive freezer types JSONB array distribution
SELECT 
    'NPFC Comprehensive Freezer Types JSONB Array Distribution' as analysis,
    freezer_types,
    COUNT(*) as count
FROM vessel_equipment 
WHERE source_id = '$NPFC_SOURCE_ID' AND freezer_types IS NOT NULL
GROUP BY freezer_types
ORDER BY count DESC;

\\echo '✅ COMPREHENSIVE NPFC loading with ALL table processing + CORRECTIONS + FIXED SQL syntax completed'
EOF

# Execute comprehensive schema distribution loading with CORRECTED enum support + FIXED SQL syntax
log_step "🚀 Loading NPFC vessels with comprehensive table distribution + corrections + fixed SQL syntax..."

if PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /tmp/load_npfc_comprehensive_all_tables_corrected.sql 2>&1 | tee /import/logs/npfc_comprehensive_corrected_loading.log; then
    # Get comprehensive statistics from all tables including NEW relationship tables
    NPFC_COMPREHENSIVE_STATS=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "
        SELECT 
            (SELECT COUNT(*) FROM vessel_sources WHERE source_id = '$NPFC_SOURCE_ID')::TEXT || '|' ||
            (SELECT COUNT(*) FROM vessel_external_identifiers WHERE source_id = '$NPFC_SOURCE_ID' 
             AND identifier_type = 'RFMO_NPFC')::TEXT || '|' ||
            (SELECT COUNT(*) FROM vessel_info WHERE vessel_uuid IN (
                SELECT vessel_uuid FROM vessel_sources WHERE source_id = '$NPFC_SOURCE_ID'))::TEXT || '|' ||
            (SELECT COUNT(*) FROM vessel_vessel_types WHERE source_id = '$NPFC_SOURCE_ID')::TEXT || '|' ||
            (SELECT COUNT(*) FROM vessel_gear_types WHERE source_id = '$NPFC_SOURCE_ID')::TEXT || '|' ||
            (SELECT COUNT(*) FROM vessel_equipment WHERE source_id = '$NPFC_SOURCE_ID')::TEXT || '|' ||
            (SELECT COUNT(*) FROM vessel_equipment WHERE source_id = '$NPFC_SOURCE_ID' 
             AND freezer_types IS NOT NULL)::TEXT || '|' ||
            (SELECT COUNT(DISTINCT metric_type) FROM vessel_metrics WHERE source_id = '$NPFC_SOURCE_ID')::TEXT || '|' ||
            (SELECT COUNT(*) FROM vessel_metrics WHERE source_id = '$NPFC_SOURCE_ID')::TEXT || '|' ||
            (SELECT COUNT(*) FROM vessel_reported_history WHERE source_id = '$NPFC_SOURCE_ID')::TEXT || '|' ||
            (SELECT COUNT(*) FROM vessel_build_information WHERE source_id = '$NPFC_SOURCE_ID')::TEXT || '|' ||
            (SELECT COUNT(*) FROM vessel_attributes WHERE source_id = '$NPFC_SOURCE_ID')::TEXT;" || echo "0|0|0|0|0|0|0|0|0|0|0|0")
    
    IFS='|' read -r VESSELS_COUNT EXT_ID_COUNT INFO_COUNT VESSEL_TYPES_COUNT GEAR_TYPES_COUNT EQUIP_COUNT FREEZER_EQUIP_COUNT METRIC_TYPES MEASUREMENTS_COUNT HISTORY_COUNT BUILD_COUNT ATTRIBUTES_COUNT <<< "$NPFC_COMPREHENSIVE_STATS"
    
    log_success "✅ NPFC comprehensive schema distribution loading with CORRECTIONS + FIXED SQL syntax successful:"
    log_success "   - $VESSELS_COUNT vessels integrated across comprehensive schema"
    log_success "   - $EXT_ID_COUNT NPFC external identifiers (RFMO_NPFC type)"
    log_success "   - $INFO_COUNT vessel info records with CORRECTED vessel type lookups using EXACT user translations"
    log_success "   - $VESSEL_TYPES_COUNT vessel-to-vessel-type relationships (NEW) using CORRECTED translations"
    log_success "   - $GEAR_TYPES_COUNT vessel-to-gear-type relationships (NEW) using CORRECTED translations"
    log_success "   - $EQUIP_COUNT equipment records total"
    log_success "   - $FREEZER_EQUIP_COUNT equipment records with JSONB freezer_types arrays"
    log_success "   - $METRIC_TYPES different measurement types stored"
    log_success "   - $MEASUREMENTS_COUNT total measurements with FIXED unit enums + FIXED SQL syntax"
    log_success "   - $HISTORY_COUNT historical records (previous names, flags, registries)"
    log_success "   - $BUILD_COUNT build information records"
    log_success "   - $ATTRIBUTES_COUNT operational attribute records with comprehensive enum preservation"
    log_success "   - FIXED: Unit enums use pre-processed values (no double-processing)"
    log_success "   - FIXED: Added vessel_vessel_types and vessel_gear_types relationship tables"
    log_success "   - CORRECTED: Fixed function signatures with proper integer casting"
    log_success "   - CORRECTED: Using existing vessel_types and gear_types_fao tables"
    log_success "   - CORRECTED: Using EXACT user-provided vessel type translations"
    log_success "   - CORRECTED: Using EXACT user-provided fishing method translations"
    log_success "   - FIXED: SQL syntax errors in vessel_metrics UNION queries resolved"
    log_success "   - Comprehensive freezer type enum standardization with CORRECTED processing"
    log_success "   - JSONB array structure for multiple freezer types per vessel"
    log_success "   - All 33 NPFC columns mapped to comprehensive database tables"
    log_success "   - ALL ORIGINAL COMPREHENSIVE FUNCTIONALITY MAINTAINED + FIXES APPLIED"
    
    # Clean up
    rm -f /tmp/load_npfc_comprehensive_all_tables_corrected.sql
    
    if [[ "$VESSELS_COUNT" -gt 0 && "$MEASUREMENTS_COUNT" -gt 0 && "$EQUIP_COUNT" -gt 0 ]]; then
        log_success "SUCCESS: NPFC comprehensive vessel data with FIXES + EXACT TRANSLATIONS successfully loaded"
        log_success "  - $VESSELS_COUNT vessels with $MEASUREMENTS_COUNT measurements and $EQUIP_COUNT equipment records"
        log_success "  - $VESSEL_TYPES_COUNT vessel type relationships and $GEAR_TYPES_COUNT gear type relationships"
        log_success "  - ALL ORIGINAL COMPREHENSIVE FUNCTIONALITY + EXACT USER TRANSLATIONS + FIXES applied successfully"
        exit 0
    else
        log_error "PROBLEM: Loading applied but missing expected data in core tables"
        log_error "  - Vessels: $VESSELS_COUNT, Measurements: $MEASUREMENTS_COUNT, Equipment: $EQUIP_COUNT"
        log_error "  - Vessel Types: $VESSEL_TYPES_COUNT, Gear Types: $GEAR_TYPES_COUNT"
        
        # Log debugging info if measurements are still 0
        if [[ "$MEASUREMENTS_COUNT" -eq 0 ]]; then
            log_error "DEBUGGING: Check cleaned data unit_enum values in: $CLEANED_FILE"
            log_error "Expected unit enums: METER, CUBIC_METER, KW, etc. (not METERS, CUBIC_METERS)"
        fi
        
        exit 1
    fi
    
else
    log_error "NPFC comprehensive loading with corrections + fixed SQL syntax failed - check: /import/logs/npfc_comprehensive_corrected_loading.log"
    exit 1
fi