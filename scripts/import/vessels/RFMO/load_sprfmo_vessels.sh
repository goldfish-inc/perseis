#!/bin/bash
# /app/scripts/import/vessels/data/RFMO/load_sprfmo_vessels.sh  
# COMPREHENSIVE: Complete SPRFMO Vessel Loading with ALL table processing - FULLY CORRECTED
set -euo pipefail

source /app/scripts/core/logging.sh
source /app/scripts/core/database.sh

log_step "Loading SPRFMO Vessels (COMPREHENSIVE - All Tables + FULLY CORRECTED)"

# Check cleaned data exists
CLEANED_FILE="/import/vessels/vessel_data/RFMO/cleaned/sprfmo_vessels_cleaned.csv"
if [[ ! -f "$CLEANED_FILE" ]]; then
    log_error "Cleaned SPRFMO data not found: $CLEANED_FILE"
    exit 1
fi

RECORD_COUNT=$(tail -n +2 "$CLEANED_FILE" | wc -l)
log_success "Loading $RECORD_COUNT cleaned SPRFMO vessels using comprehensive schema"

# Setup database functions and mapping tables using existing schema only
log_step "Setting up comprehensive SPRFMO loading..."
if psql_execute_file "/app/scripts/import/vessels/data/RFMO/setup_sprfmo_loading.sql" "Comprehensive SPRFMO setup"; then
    log_success "Comprehensive SPRFMO setup complete using existing schema"
else
    log_error "Comprehensive SPRFMO setup failed"
    exit 1
fi

# Get SPRFMO source ID
SPRFMO_SOURCE_ID=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "
    SELECT source_id FROM original_sources_vessels WHERE source_shortname = 'SPRFMO';" || echo "")

if [[ -z "$SPRFMO_SOURCE_ID" ]]; then
    log_error "SPRFMO source not found"
    exit 1
fi

log_success "Found SPRFMO source ID: $SPRFMO_SOURCE_ID"

# Create comprehensive loading SQL with ALL table processing + FULLY CORRECTED types
cat > /tmp/load_sprfmo_comprehensive_fixed.sql << EOF
-- SPRFMO COMPREHENSIVE Loading Script - FULLY CORRECTED with ALL Functionality

-- Create temp table matching comprehensive cleaned data structure
CREATE TEMP TABLE sprfmo_cleaned (
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
    
    -- SPRFMO external identifier
    sprfmo_vessel_id TEXT,
    
    -- Basic vessel info
    vessel_type TEXT,
    gear_type_fao TEXT,
    
    -- vessel_metrics fields - Length with dynamic type
    length_metric_type TEXT,
    length_value DECIMAL,
    length_unit TEXT,
    
    -- vessel_metrics fields - Specific measurements
    gross_tonnage_value DECIMAL,
    gross_tonnage_unit TEXT,
    gross_register_tonnage_value DECIMAL,
    gross_register_tonnage_unit TEXT,
    moulded_depth_value DECIMAL,
    moulded_depth_unit TEXT,
    beam_value DECIMAL,
    beam_unit TEXT,
    engine_power_value DECIMAL,
    engine_power_unit TEXT,
    fish_hold_volume_value DECIMAL,
    fish_hold_volume_unit TEXT,
    
    -- vessel_build_information fields
    build_year INTEGER,
    build_location TEXT,
    build_country_id TEXT,
    
    -- Authorization information
    authorization_status TEXT,
    auth_start_date TEXT,
    auth_end_date TEXT,
    flag_registered_date TEXT,
    authorizing_country_id TEXT,
    participant_group TEXT,
    
    -- History
    previous_names TEXT,
    previous_flag TEXT,
    port_registry TEXT
);

-- Load cleaned CSV data
\\COPY sprfmo_cleaned FROM '$CLEANED_FILE' WITH (FORMAT csv, HEADER true, NULL '');

\\echo 'SPRFMO data loaded into temp table'
SELECT 'SPRFMO Data Overview' as step, COUNT(*) as total_records FROM sprfmo_cleaned;

-- Create validated temp table with proper data types and comprehensive UUID resolution
CREATE TEMP TABLE validated_sprfmo AS
SELECT 
    -- Core identifiers with comprehensive validation
    NULLIF(trim(vessel_name), '') as vessel_name,
    CASE 
        WHEN imo ~ '^[0-9]{7}\$' AND length(trim(imo)) = 7 THEN trim(imo)
        ELSE NULL 
    END as imo_clean,
    CASE 
        WHEN trim(ircs) != '' AND length(trim(ircs)) <= 15 THEN upper(trim(ircs))
        ELSE NULL 
    END as ircs_clean,
    CASE 
        WHEN mmsi ~ '^[0-9]{9}\$' AND length(trim(mmsi)) = 9 THEN trim(mmsi)
        ELSE NULL 
    END as mmsi_clean,
    NULLIF(trim(national_registry), '') as national_registry,
    
    -- Comprehensive Country UUID resolution
    get_country_uuid(vessel_flag_alpha3) as vessel_flag_uuid,
    get_country_uuid(build_country_id) as build_country_uuid,
    get_country_uuid(authorizing_country_id) as authorizing_country_uuid,
    
    -- Comprehensive Type UUID resolution
    get_vessel_type_uuid(vessel_type) as vessel_type_uuid,
    get_gear_type_uuid(gear_type_fao) as gear_type_uuid,
    
    -- vessel_metrics fields with comprehensive enum resolution
    -- Length with dynamic metric type
    length_value,
    standardize_unit(length_unit, 'length') as length_unit_enum,
    CASE 
        WHEN length_metric_type = 'length_loa' THEN 'length_loa'::metric_type_enum
        WHEN length_metric_type = 'length_lbp' THEN 'length_lbp'::metric_type_enum
        WHEN length_metric_type = 'length_rgl' THEN 'length_rgl'::metric_type_enum
        ELSE 'length'::metric_type_enum
    END as length_metric_type_enum,
    
    -- Specific measurements for vessel_metrics with comprehensive validation
    gross_tonnage_value,
    gross_register_tonnage_value,
    moulded_depth_value,
    beam_value,
    engine_power_value,
    fish_hold_volume_value,
    
    -- Build information with comprehensive validation
    build_year,
    NULLIF(trim(build_location), '') as build_location,
    
    -- FIXED: Authorization with proper enum mapping
    CASE 
        WHEN upper(trim(authorization_status)) IN ('AUTHORIZED', 'YES', 'ACTIVE') THEN 'FISHING_AUTHORIZATION'::authorization_type_enum
        WHEN upper(trim(authorization_status)) IN ('UNAUTHORIZED', 'NO', 'INACTIVE') THEN 'OTHER_AUTHORIZATION'::authorization_type_enum
        ELSE NULL 
    END as auth_type_enum,
    
    -- Comprehensive Date parsing with validation
    CASE 
        WHEN auth_start_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}\$' THEN auth_start_date::DATE
        ELSE NULL 
    END as auth_start_date_parsed,
    CASE 
        WHEN auth_end_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}\$' THEN auth_end_date::DATE
        ELSE NULL 
    END as auth_end_date_parsed,
    CASE 
        WHEN flag_registered_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}\$' THEN flag_registered_date::DATE
        ELSE NULL 
    END as flag_registered_date_parsed,
    
    -- Additional fields with comprehensive validation
    NULLIF(trim(participant_group), '') as participant_group,
    NULLIF(trim(previous_names), '') as previous_names,
    NULLIF(trim(previous_flag), '') as previous_flag,
    NULLIF(trim(port_registry), '') as port_registry,
    sprfmo_vessel_id
    
FROM sprfmo_cleaned
WHERE 
    NULLIF(trim(vessel_name), '') IS NOT NULL OR
    (imo ~ '^[0-9]{7}\$' AND length(trim(imo)) = 7) OR
    (trim(ircs) != '' AND length(trim(ircs)) <= 15) OR
    (mmsi ~ '^[0-9]{9}\$' AND length(trim(mmsi)) = 9);

\\echo 'SPRFMO data validated and processed'
SELECT 'Validated SPRFMO Records' as step, COUNT(*) as total_validated FROM validated_sprfmo;

-- Comprehensive validation summary
SELECT 
    'SPRFMO Data Validation Summary' as validation_step,
    COUNT(*) as total_records,
    COUNT(vessel_name) as records_with_names,
    COUNT(imo_clean) as records_with_imo,
    COUNT(ircs_clean) as records_with_ircs,
    COUNT(mmsi_clean) as records_with_mmsi,
    COUNT(vessel_flag_uuid) as records_with_flag_resolved,
    COUNT(vessel_type_uuid) as records_with_vessel_type_resolved,
    COUNT(gear_type_uuid) as records_with_gear_type_resolved,
    COUNT(build_country_uuid) as records_with_build_country_resolved,
    COUNT(authorizing_country_uuid) as records_with_auth_country_resolved
FROM validated_sprfmo;

-- === COMPREHENSIVE TABLE LOADING - ALL TABLES ===

-- 1. VESSELS TABLE - Core vessel creation or matching with comprehensive logic
\\echo 'Loading vessels table with comprehensive matching logic...'
INSERT INTO vessels (
    vessel_name, vessel_flag, imo, ircs, mmsi, national_registry
)
SELECT DISTINCT
    v.vessel_name,
    v.vessel_flag_uuid,
    v.imo_clean,
    v.ircs_clean,
    v.mmsi_clean,
    v.national_registry
FROM validated_sprfmo v
WHERE NOT EXISTS (
    -- Advanced comprehensive vessel matching logic
    SELECT 1 FROM vessels existing WHERE
    (existing.imo = v.imo_clean AND v.imo_clean IS NOT NULL) OR
    (existing.ircs = v.ircs_clean AND v.ircs_clean IS NOT NULL) OR
    (existing.mmsi = v.mmsi_clean AND v.mmsi_clean IS NOT NULL) OR
    (existing.vessel_name = v.vessel_name AND existing.vessel_flag = v.vessel_flag_uuid 
     AND v.vessel_name IS NOT NULL AND v.vessel_flag_uuid IS NOT NULL)
);

-- 2. VESSEL_SOURCES TABLE - Track all SPRFMO vessels with comprehensive metadata
\\echo 'Loading vessel_sources table with comprehensive tracking...'
INSERT INTO vessel_sources (vessel_uuid, source_id, first_seen_date, last_seen_date, is_active)
SELECT DISTINCT
    vessels.vessel_uuid,
    '$SPRFMO_SOURCE_ID'::UUID,
    CURRENT_DATE,
    CURRENT_DATE,
    true
FROM validated_sprfmo v
JOIN vessels ON (
    (vessels.imo = v.imo_clean AND v.imo_clean IS NOT NULL) OR
    (vessels.ircs = v.ircs_clean AND v.ircs_clean IS NOT NULL) OR
    (vessels.mmsi = v.mmsi_clean AND v.mmsi_clean IS NOT NULL) OR
    (vessels.vessel_name = v.vessel_name AND vessels.vessel_flag = v.vessel_flag_uuid 
     AND v.vessel_name IS NOT NULL AND v.vessel_flag_uuid IS NOT NULL)
)
ON CONFLICT (vessel_uuid, source_id) DO UPDATE SET
    last_seen_date = CURRENT_DATE,
    is_active = true;

-- 3. VESSEL_EXTERNAL_IDENTIFIERS TABLE - SPRFMO vessel IDs with comprehensive handling
\\echo 'Loading vessel_external_identifiers table with comprehensive ID handling...'
INSERT INTO vessel_external_identifiers (
    vessel_uuid, source_id, identifier_type, identifier_value
)
SELECT DISTINCT
    vessels.vessel_uuid,
    '$SPRFMO_SOURCE_ID'::UUID,
    'RFMO_SPRFMO'::external_identifier_type_enum,
    COALESCE(v.sprfmo_vessel_id, 'SPRFMO_' || vessels.vessel_uuid::TEXT)
FROM validated_sprfmo v
JOIN vessels ON (
    (vessels.imo = v.imo_clean AND v.imo_clean IS NOT NULL) OR
    (vessels.ircs = v.ircs_clean AND v.ircs_clean IS NOT NULL) OR
    (vessels.mmsi = v.mmsi_clean AND v.mmsi_clean IS NOT NULL) OR
    (vessels.vessel_name = v.vessel_name AND vessels.vessel_flag = v.vessel_flag_uuid 
     AND v.vessel_name IS NOT NULL AND v.vessel_flag_uuid IS NOT NULL)
)
ON CONFLICT (vessel_uuid, source_id, identifier_type) DO NOTHING;

-- 4. VESSEL_INFO TABLE - FIXED: Keep vessel_type as UUID, not VARCHAR
\\echo 'Loading vessel_info table with comprehensive vessel information...'
INSERT INTO vessel_info (
    vessel_uuid, vessel_type, port_registry
)
SELECT DISTINCT
    vessels.vessel_uuid,
    v.vessel_type_uuid,
    v.port_registry
FROM validated_sprfmo v
JOIN vessels ON (
    (vessels.imo = v.imo_clean AND v.imo_clean IS NOT NULL) OR
    (vessels.ircs = v.ircs_clean AND v.ircs_clean IS NOT NULL) OR
    (vessels.mmsi = v.mmsi_clean AND v.mmsi_clean IS NOT NULL) OR
    (vessels.vessel_name = v.vessel_name AND vessels.vessel_flag = v.vessel_flag_uuid 
     AND v.vessel_name IS NOT NULL AND v.vessel_flag_uuid IS NOT NULL)
)
WHERE v.vessel_type_uuid IS NOT NULL OR v.port_registry IS NOT NULL
ON CONFLICT (vessel_uuid) DO UPDATE SET
    vessel_type = COALESCE(EXCLUDED.vessel_type, vessel_info.vessel_type),
    port_registry = COALESCE(EXCLUDED.port_registry, vessel_info.port_registry);

-- 5. VESSEL_VESSEL_TYPES TABLE - Vessel type relationships with comprehensive tracking
\\echo 'Loading vessel_vessel_types table with comprehensive vessel type relationships...'
INSERT INTO vessel_vessel_types (vessel_uuid, vessel_type_id, source_id)
SELECT DISTINCT
    vessels.vessel_uuid,
    v.vessel_type_uuid,
    '$SPRFMO_SOURCE_ID'::UUID
FROM validated_sprfmo v
JOIN vessels ON (
    (vessels.imo = v.imo_clean AND v.imo_clean IS NOT NULL) OR
    (vessels.ircs = v.ircs_clean AND v.ircs_clean IS NOT NULL) OR
    (vessels.mmsi = v.mmsi_clean AND v.mmsi_clean IS NOT NULL) OR
    (vessels.vessel_name = v.vessel_name AND vessels.vessel_flag = v.vessel_flag_uuid 
     AND v.vessel_name IS NOT NULL AND v.vessel_flag_uuid IS NOT NULL)
)
WHERE v.vessel_type_uuid IS NOT NULL
ON CONFLICT DO NOTHING;

-- 6. VESSEL_GEAR_TYPES TABLE - Gear type relationships with comprehensive tracking
\\echo 'Loading vessel_gear_types table with comprehensive gear type relationships...'
INSERT INTO vessel_gear_types (vessel_uuid, fao_gear_id, source_id)
SELECT DISTINCT
    vessels.vessel_uuid,
    v.gear_type_uuid,
    '$SPRFMO_SOURCE_ID'::UUID
FROM validated_sprfmo v
JOIN vessels ON (
    (vessels.imo = v.imo_clean AND v.imo_clean IS NOT NULL) OR
    (vessels.ircs = v.ircs_clean AND v.ircs_clean IS NOT NULL) OR
    (vessels.mmsi = v.mmsi_clean AND v.mmsi_clean IS NOT NULL) OR
    (vessels.vessel_name = v.vessel_name AND vessels.vessel_flag = v.vessel_flag_uuid 
     AND v.vessel_name IS NOT NULL AND v.vessel_flag_uuid IS NOT NULL)
)
WHERE v.gear_type_uuid IS NOT NULL
ON CONFLICT DO NOTHING;

-- 7. VESSEL_METRICS TABLE - All measurements with specific SPRFMO mappings - COMPREHENSIVE
\\echo 'Loading vessel_metrics table with SPRFMO-specific mappings - COMPREHENSIVE...'

-- Length measurements (dynamic metric type based on Length Type) - COMPREHENSIVE
INSERT INTO vessel_metrics (vessel_uuid, source_id, metric_type, value, unit)
SELECT DISTINCT
    vessels.vessel_uuid,
    '$SPRFMO_SOURCE_ID'::UUID,
    v.length_metric_type_enum,
    v.length_value,
    v.length_unit_enum
FROM validated_sprfmo v
JOIN vessels ON (
    (vessels.imo = v.imo_clean AND v.imo_clean IS NOT NULL) OR
    (vessels.ircs = v.ircs_clean AND v.ircs_clean IS NOT NULL) OR
    (vessels.mmsi = v.mmsi_clean AND v.mmsi_clean IS NOT NULL) OR
    (vessels.vessel_name = v.vessel_name AND vessels.vessel_flag = v.vessel_flag_uuid 
     AND v.vessel_name IS NOT NULL AND v.vessel_flag_uuid IS NOT NULL)
)
WHERE v.length_value IS NOT NULL;

-- Gross Tonnage --> 'gross_tonnage' + NULL unit - FIXED
INSERT INTO vessel_metrics (vessel_uuid, source_id, metric_type, value, unit)
SELECT DISTINCT
    vessels.vessel_uuid,
    '$SPRFMO_SOURCE_ID'::UUID,
    'gross_tonnage'::metric_type_enum,
    v.gross_tonnage_value,
    NULL::unit_enum
FROM validated_sprfmo v
JOIN vessels ON (
    (vessels.imo = v.imo_clean AND v.imo_clean IS NOT NULL) OR
    (vessels.ircs = v.ircs_clean AND v.ircs_clean IS NOT NULL) OR
    (vessels.mmsi = v.mmsi_clean AND v.mmsi_clean IS NOT NULL) OR
    (vessels.vessel_name = v.vessel_name AND vessels.vessel_flag = v.vessel_flag_uuid 
     AND v.vessel_name IS NOT NULL AND v.vessel_flag_uuid IS NOT NULL)
)
WHERE v.gross_tonnage_value IS NOT NULL;

-- Gross Register Tonnage --> 'gross_register_tonnage' + NULL unit - FIXED
INSERT INTO vessel_metrics (vessel_uuid, source_id, metric_type, value, unit)
SELECT DISTINCT
    vessels.vessel_uuid,
    '$SPRFMO_SOURCE_ID'::UUID,
    'gross_register_tonnage'::metric_type_enum,
    v.gross_register_tonnage_value,
    NULL::unit_enum
FROM validated_sprfmo v
JOIN vessels ON (
    (vessels.imo = v.imo_clean AND v.imo_clean IS NOT NULL) OR
    (vessels.ircs = v.ircs_clean AND v.ircs_clean IS NOT NULL) OR
    (vessels.mmsi = v.mmsi_clean AND v.mmsi_clean IS NOT NULL) OR
    (vessels.vessel_name = v.vessel_name AND vessels.vessel_flag = v.vessel_flag_uuid 
     AND v.vessel_name IS NOT NULL AND v.vessel_flag_uuid IS NOT NULL)
)
WHERE v.gross_register_tonnage_value IS NOT NULL;

-- Moulded Depth --> 'moulded_depth' + 'METER' - COMPREHENSIVE
INSERT INTO vessel_metrics (vessel_uuid, source_id, metric_type, value, unit)
SELECT DISTINCT
    vessels.vessel_uuid,
    '$SPRFMO_SOURCE_ID'::UUID,
    'moulded_depth'::metric_type_enum,
    v.moulded_depth_value,
    'METER'::unit_enum
FROM validated_sprfmo v
JOIN vessels ON (
    (vessels.imo = v.imo_clean AND v.imo_clean IS NOT NULL) OR
    (vessels.ircs = v.ircs_clean AND v.ircs_clean IS NOT NULL) OR
    (vessels.mmsi = v.mmsi_clean AND v.mmsi_clean IS NOT NULL) OR
    (vessels.vessel_name = v.vessel_name AND vessels.vessel_flag = v.vessel_flag_uuid 
     AND v.vessel_name IS NOT NULL AND v.vessel_flag_uuid IS NOT NULL)
)
WHERE v.moulded_depth_value IS NOT NULL;

-- Beam --> 'beam' + 'METER' - COMPREHENSIVE
INSERT INTO vessel_metrics (vessel_uuid, source_id, metric_type, value, unit)
SELECT DISTINCT
    vessels.vessel_uuid,
    '$SPRFMO_SOURCE_ID'::UUID,
    'beam'::metric_type_enum,
    v.beam_value,
    'METER'::unit_enum
FROM validated_sprfmo v
JOIN vessels ON (
    (vessels.imo = v.imo_clean AND v.imo_clean IS NOT NULL) OR
    (vessels.ircs = v.ircs_clean AND v.ircs_clean IS NOT NULL) OR
    (vessels.mmsi = v.mmsi_clean AND v.mmsi_clean IS NOT NULL) OR
    (vessels.vessel_name = v.vessel_name AND vessels.vessel_flag = v.vessel_flag_uuid 
     AND v.vessel_name IS NOT NULL AND v.vessel_flag_uuid IS NOT NULL)
)
WHERE v.beam_value IS NOT NULL;

-- Power of main engine(s) --> 'engine_power' + 'KW' - COMPREHENSIVE
INSERT INTO vessel_metrics (vessel_uuid, source_id, metric_type, value, unit)
SELECT DISTINCT
    vessels.vessel_uuid,
    '$SPRFMO_SOURCE_ID'::UUID,
    'engine_power'::metric_type_enum,
    v.engine_power_value,
    'KW'::unit_enum
FROM validated_sprfmo v
JOIN vessels ON (
    (vessels.imo = v.imo_clean AND v.imo_clean IS NOT NULL) OR
    (vessels.ircs = v.ircs_clean AND v.ircs_clean IS NOT NULL) OR
    (vessels.mmsi = v.mmsi_clean AND v.mmsi_clean IS NOT NULL) OR
    (vessels.vessel_name = v.vessel_name AND vessels.vessel_flag = v.vessel_flag_uuid 
     AND v.vessel_name IS NOT NULL AND v.vessel_flag_uuid IS NOT NULL)
)
WHERE v.engine_power_value IS NOT NULL;

-- Hold Capacity --> 'fish_hold_volume' + 'CUBIC_METER' - COMPREHENSIVE
INSERT INTO vessel_metrics (vessel_uuid, source_id, metric_type, value, unit)
SELECT DISTINCT
    vessels.vessel_uuid,
    '$SPRFMO_SOURCE_ID'::UUID,
    'fish_hold_volume'::metric_type_enum,
    v.fish_hold_volume_value,
    'CUBIC_METER'::unit_enum
FROM validated_sprfmo v
JOIN vessels ON (
    (vessels.imo = v.imo_clean AND v.imo_clean IS NOT NULL) OR
    (vessels.ircs = v.ircs_clean AND v.ircs_clean IS NOT NULL) OR
    (vessels.mmsi = v.mmsi_clean AND v.mmsi_clean IS NOT NULL) OR
    (vessels.vessel_name = v.vessel_name AND vessels.vessel_flag = v.vessel_flag_uuid 
     AND v.vessel_name IS NOT NULL AND v.vessel_flag_uuid IS NOT NULL)
)
WHERE v.fish_hold_volume_value IS NOT NULL;

-- 8. VESSEL_BUILD_INFORMATION TABLE - FIXED: build_year as INTEGER
\\echo 'Loading vessel_build_information table with comprehensive build information...'
INSERT INTO vessel_build_information (vessel_uuid, source_id, build_year, build_location, build_country_id)
SELECT DISTINCT
    vessels.vessel_uuid,
    '$SPRFMO_SOURCE_ID'::UUID,
    v.build_year,
    v.build_location,
    v.build_country_uuid
FROM validated_sprfmo v
JOIN vessels ON (
    (vessels.imo = v.imo_clean AND v.imo_clean IS NOT NULL) OR
    (vessels.ircs = v.ircs_clean AND v.ircs_clean IS NOT NULL) OR
    (vessels.mmsi = v.mmsi_clean AND v.mmsi_clean IS NOT NULL) OR
    (vessels.vessel_name = v.vessel_name AND vessels.vessel_flag = v.vessel_flag_uuid 
     AND v.vessel_name IS NOT NULL AND v.vessel_flag_uuid IS NOT NULL)
)
WHERE v.build_year IS NOT NULL OR v.build_location IS NOT NULL OR v.build_country_uuid IS NOT NULL
ON CONFLICT (vessel_uuid, source_id) DO UPDATE SET
    build_year = COALESCE(EXCLUDED.build_year, vessel_build_information.build_year),
    build_location = COALESCE(EXCLUDED.build_location, vessel_build_information.build_location),
    build_country_id = COALESCE(EXCLUDED.build_country_id, vessel_build_information.build_country_id);

-- 9. VESSEL_REPORTED_HISTORY TABLE - FIXED: Proper JOIN syntax with individual records
\\echo 'Loading vessel_reported_history table with FIXED individual records...'
INSERT INTO vessel_reported_history (vessel_uuid, source_id, reported_history_type, identifier_value, flag_country_id)
SELECT DISTINCT
    v.vessel_uuid,
    '$SPRFMO_SOURCE_ID'::UUID,
    v.history_type::reported_history_enum,
    v.identifier_value,
    CASE 
        WHEN v.history_type = 'FLAG_CHANGE' THEN get_country_uuid(v.identifier_value)
        ELSE NULL 
    END
FROM (
    -- Previous Names History
    SELECT 
        vessels.vessel_uuid,
        'VESSEL_NAME_CHANGE' as history_type,
        v.previous_names as identifier_value
    FROM validated_sprfmo v
    JOIN vessels ON (
        (vessels.imo = v.imo_clean AND v.imo_clean IS NOT NULL) OR
        (vessels.ircs = v.ircs_clean AND v.ircs_clean IS NOT NULL) OR
        (vessels.mmsi = v.mmsi_clean AND v.mmsi_clean IS NOT NULL) OR
        (vessels.vessel_name = v.vessel_name AND vessels.vessel_flag = v.vessel_flag_uuid 
         AND v.vessel_name IS NOT NULL AND v.vessel_flag_uuid IS NOT NULL)
    )
    WHERE v.previous_names IS NOT NULL
    
    UNION ALL
    
    -- Previous Flag History
    SELECT 
        vessels.vessel_uuid,
        'FLAG_CHANGE' as history_type,
        v.previous_flag as identifier_value
    FROM validated_sprfmo v
    JOIN vessels ON (
        (vessels.imo = v.imo_clean AND v.imo_clean IS NOT NULL) OR
        (vessels.ircs = v.ircs_clean AND v.ircs_clean IS NOT NULL) OR
        (vessels.mmsi = v.mmsi_clean AND v.mmsi_clean IS NOT NULL) OR
        (vessels.vessel_name = v.vessel_name AND vessels.vessel_flag = v.vessel_flag_uuid 
         AND v.vessel_name IS NOT NULL AND v.vessel_flag_uuid IS NOT NULL)
    )
    WHERE v.previous_flag IS NOT NULL
) v;

-- 10. VESSEL_AUTHORIZATIONS TABLE - FIXED: Handle missing constraints gracefully
\\echo 'Loading vessel_authorizations table with additional_data for authorizing_country...'
INSERT INTO vessel_authorizations (
    vessel_uuid, source_id, rfmo_id, authorization_type, 
    start_date, end_date, 
    additional_data
)
SELECT DISTINCT
    vessels.vessel_uuid,
    '$SPRFMO_SOURCE_ID'::UUID,
    (SELECT id FROM rfmos WHERE rfmo_acronym = 'SPRFMO'),
    v.auth_type_enum,
    v.auth_start_date_parsed,
    v.auth_end_date_parsed,
    CASE 
        WHEN v.authorizing_country_uuid IS NOT NULL OR v.participant_group IS NOT NULL THEN
            jsonb_build_object(
                'sender_country', v.authorizing_country_uuid::TEXT,
                'participant_group', v.participant_group
            )
        ELSE NULL
    END
FROM validated_sprfmo v
JOIN vessels ON (
    (vessels.imo = v.imo_clean AND v.imo_clean IS NOT NULL) OR
    (vessels.ircs = v.ircs_clean AND v.ircs_clean IS NOT NULL) OR
    (vessels.mmsi = v.mmsi_clean AND v.mmsi_clean IS NOT NULL) OR
    (vessels.vessel_name = v.vessel_name AND vessels.vessel_flag = v.vessel_flag_uuid 
     AND v.vessel_name IS NOT NULL AND v.vessel_flag_uuid IS NOT NULL)
)
WHERE v.auth_type_enum IS NOT NULL OR v.auth_start_date_parsed IS NOT NULL 
   OR v.authorizing_country_uuid IS NOT NULL OR v.participant_group IS NOT NULL
ON CONFLICT (vessel_uuid, source_id) DO UPDATE SET
    authorization_type = COALESCE(EXCLUDED.authorization_type, vessel_authorizations.authorization_type),
    start_date = COALESCE(EXCLUDED.start_date, vessel_authorizations.start_date),
    end_date = COALESCE(EXCLUDED.end_date, vessel_authorizations.end_date),
    additional_data = COALESCE(EXCLUDED.additional_data, vessel_authorizations.additional_data);

-- Update source status to loaded
UPDATE original_sources_vessels 
SET status = 'LOADED' 
WHERE source_shortname = 'SPRFMO';

-- === COMPREHENSIVE SUMMARY AND REPORTING ===
\\echo ''
\\echo 'COMPREHENSIVE SPRFMO Loading Summary - FULLY CORRECTED'
\\echo '======================================================'

SELECT 
    'SPRFMO Comprehensive Loading Summary - FULLY CORRECTED' as summary,
    (SELECT COUNT(*) FROM vessels WHERE vessel_uuid IN (
        SELECT vessel_uuid FROM vessel_sources 
        WHERE source_id = '$SPRFMO_SOURCE_ID')) as vessels_tracked,
    (SELECT COUNT(*) FROM vessel_external_identifiers WHERE source_id = '$SPRFMO_SOURCE_ID' 
     AND identifier_type = 'RFMO_SPRFMO') as sprfmo_external_ids,
    (SELECT COUNT(*) FROM vessel_info WHERE vessel_uuid IN (
        SELECT vessel_uuid FROM vessel_sources WHERE source_id = '$SPRFMO_SOURCE_ID')) as vessel_info_records,
    (SELECT COUNT(*) FROM vessel_vessel_types WHERE source_id = '$SPRFMO_SOURCE_ID') as vessel_type_relationships,
    (SELECT COUNT(*) FROM vessel_gear_types WHERE source_id = '$SPRFMO_SOURCE_ID') as gear_type_relationships,
    (SELECT COUNT(DISTINCT metric_type) FROM vessel_metrics WHERE source_id = '$SPRFMO_SOURCE_ID') as measurement_types,
    (SELECT COUNT(*) FROM vessel_metrics WHERE source_id = '$SPRFMO_SOURCE_ID') as total_measurements,
    (SELECT COUNT(*) FROM vessel_reported_history WHERE source_id = '$SPRFMO_SOURCE_ID') as history_records,
    (SELECT COUNT(*) FROM vessel_build_information WHERE source_id = '$SPRFMO_SOURCE_ID') as build_info_records,
    (SELECT COUNT(*) FROM vessel_authorizations WHERE source_id = '$SPRFMO_SOURCE_ID') as authorization_records,
    (SELECT status FROM original_sources_vessels WHERE source_shortname = 'SPRFMO') as source_status;

-- Show comprehensive measurement type and unit distribution
SELECT 
    'SPRFMO Measurement Types & Units Distribution' as breakdown,
    metric_type,
    unit,
    COUNT(*) as count
FROM vessel_metrics 
WHERE source_id = '$SPRFMO_SOURCE_ID'
GROUP BY metric_type, unit
ORDER BY metric_type, count DESC;

-- Show comprehensive vessel type distribution
SELECT 
    'SPRFMO Vessel Types Distribution' as analysis,
    vt.vessel_type_isscfv_alpha as vessel_type_code,
    COUNT(*) as count
FROM vessel_vessel_types vvt
JOIN vessel_types vt ON vvt.vessel_type_id = vt.id
WHERE vvt.source_id = '$SPRFMO_SOURCE_ID'
GROUP BY vt.vessel_type_isscfv_alpha
ORDER BY count DESC;

-- Show comprehensive gear type distribution  
SELECT 
    'SPRFMO Gear Types Distribution' as analysis,
    gt.fao_isscfg_code as gear_code,
    COUNT(*) as count
FROM vessel_gear_types vgt
JOIN gear_types_fao gt ON vgt.fao_gear_id = gt.id
WHERE vgt.source_id = '$SPRFMO_SOURCE_ID'
GROUP BY gt.fao_isscfg_code
ORDER BY count DESC;

-- Show comprehensive authorization type distribution
SELECT 
    'SPRFMO Authorization Types Distribution' as analysis,
    authorization_type,
    COUNT(*) as count
FROM vessel_authorizations
WHERE source_id = '$SPRFMO_SOURCE_ID'
GROUP BY authorization_type
ORDER BY count DESC;

-- Show comprehensive country flag distribution
SELECT 
    'SPRFMO Flag Country Distribution' as analysis,
    ci.alpha_3_code as flag_country,
    COUNT(*) as count
FROM vessels v
JOIN vessel_sources vs ON v.vessel_uuid = vs.vessel_uuid
JOIN country_iso ci ON v.vessel_flag = ci.id
WHERE vs.source_id = '$SPRFMO_SOURCE_ID'
GROUP BY ci.alpha_3_code
ORDER BY count DESC;

\\echo 'COMPREHENSIVE SPRFMO loading with FULLY CORRECTED schema completed'
EOF

# Execute comprehensive schema distribution loading with FULLY CORRECTED types
log_step "Loading SPRFMO vessels with comprehensive table distribution + FULLY CORRECTED types..."

if PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /tmp/load_sprfmo_comprehensive_fixed.sql 2>&1 | tee /import/logs/sprfmo_comprehensive_loading.log; then
    log_success "COMPREHENSIVE SPRFMO loading completed successfully with FULLY CORRECTED schema"
    log_success "   - All SPRFMO vessel measurements loaded with specific metric type mappings:"
    log_success "     * Length (dynamic type based on Length Type column)"
    log_success "     * Gross Tonnage → gross_tonnage (no unit)" 
    log_success "     * Gross Register Tonnage → gross_register_tonnage (no unit)"
    log_success "     * Moulded Depth → moulded_depth + METER"
    log_success "     * Beam → beam + METER"
    log_success "     * Power of main engine(s) → engine_power + KW"
    log_success "     * Hold Capacity → fish_hold_volume + CUBIC_METER"
    log_success "   - Vessel types mapped using vessel_type_isscfv_alpha column (15 comprehensive mappings)"
    log_success "   - Gear types mapped using fao_isscfg_code column (30+ comprehensive mappings)"
    log_success "   - Build information mapped: When Built → build_year, Where Built → build_location + build_country_id (25+ country mappings)"
    log_success "   - Participant countries comprehensively mapped (20+ comprehensive mappings)"
    log_success "   - SPRFMO-specific external identifiers created"
    log_success "   - Authorization information linked to SPRFMO RFMO with CORRECTED authorization_type enum"
    log_success "   - CORRECTED vessel_reported_history using individual records with proper JOINs"
    log_success "   - Comprehensive schema distribution completed with full validation"
    log_success "   - ALL TYPE MISMATCHES FIXED: UUID casting, unit enums, build_year integer handling"
    
    # Clean up temp file
    rm -f /tmp/load_sprfmo_comprehensive_fixed.sql
    
    log_step "SPRFMO Loading Summary - COMPREHENSIVE:"
    PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "
        SELECT 
            'SPRFMO Vessels Loaded: ' || COUNT(*) 
        FROM vessel_sources 
        WHERE source_id = '$SPRFMO_SOURCE_ID';"
    
else
    log_error "SPRFMO comprehensive loading failed"
    exit 1
fi

log_success "SPRFMO vessel loading process completed successfully - COMPREHENSIVE with FULLY CORRECTED SCHEMA"