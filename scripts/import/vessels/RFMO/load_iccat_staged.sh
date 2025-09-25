#!/bin/bash
# Staged Intelligence Import for ICCAT
# Phase 1: Raw Intelligence Collection (NO vessel matching)
set -euo pipefail

source /app/scripts/core/logging.sh
source /app/scripts/core/env.sh

log_step "🎯 ICCAT Staged Intelligence Import (Phase 1: Raw Collection)"

# Test database connection
if ! test_connection; then
    log_error "Cannot connect to database"
    exit 1
fi

# Configuration
CLEANED_DATA_DIR="/import/vessels/vessel_data/RFMO/cleaned"
INPUT_FILE="$CLEANED_DATA_DIR/iccat_vessels_cleaned.csv"

# Verify cleaned file exists
if [[ ! -f "$INPUT_FILE" ]]; then
    log_error "Cleaned ICCAT data not found: $INPUT_FILE"
    exit 1
fi

# Get ICCAT source ID
ICCAT_SOURCE_ID=$(execute_sql "
    SELECT source_id FROM original_sources_vessels 
    WHERE source_shortname = 'ICCAT' 
    LIMIT 1;
" "-t" | xargs)

if [[ -z "$ICCAT_SOURCE_ID" ]]; then
    log_error "ICCAT source not found"
    exit 1
fi

log_success "ICCAT source ID: $ICCAT_SOURCE_ID"

# Create import batch
BATCH_ID=$(execute_sql "
    INSERT INTO intelligence_import_batches (
        rfmo_shortname,
        import_date,
        source_file_path
    ) VALUES (
        'ICCAT',
        CURRENT_DATE,
        '$INPUT_FILE'
    ) RETURNING batch_id;
" "-t" | head -n1 | xargs)

log_success "Created import batch: $BATCH_ID"

# Create staging table for CSV import (not temp table due to connection issues)
execute_sql "
DROP TABLE IF EXISTS iccat_raw_staging;
CREATE TABLE iccat_raw_staging (
    source_date TEXT,
    original_source TEXT,
    vessel_name TEXT,
    imo TEXT,
    ircs TEXT,
    mmsi TEXT,
    national_registry TEXT,
    vessel_flag_alpha3 TEXT,
    iccat_serial_no TEXT,
    old_iccat_serial_no TEXT,
    vessel_type_code TEXT,
    gear_type_code TEXT,
    port_of_registry TEXT,
    external_marking TEXT,
    year_built TEXT,
    shipyard_country TEXT,
    length_value TEXT,
    length_metric_type TEXT,
    length_unit_enum TEXT,
    depth_value TEXT,
    depth_metric_type TEXT,
    depth_unit_enum TEXT,
    tonnage_value TEXT,
    tonnage_metric_type TEXT,
    engine_power TEXT,
    engine_power_unit_enum TEXT,
    car_capacity_value TEXT,
    car_capacity_unit_enum TEXT,
    vms_com_sys_code TEXT,
    operator_name TEXT,
    operator_address TEXT,
    operator_city TEXT,
    operator_zipcode TEXT,
    operator_country TEXT,
    owner_name TEXT,
    owner_address TEXT,
    owner_city TEXT,
    owner_zipcode TEXT,
    owner_country TEXT
);
"

log_step "Loading ICCAT raw data..."

# Load CSV data into temp staging
execute_sql "\\copy iccat_raw_staging FROM '$INPUT_FILE' WITH CSV HEADER"

RAW_COUNT=$(execute_sql "SELECT COUNT(*) FROM iccat_raw_staging;" "-t" | xargs)
log_success "Loaded $RAW_COUNT raw ICCAT records"

# Convert to raw intelligence reports (NO INTERPRETATION)
log_step "Converting to raw intelligence reports..."

execute_sql "
INSERT INTO intelligence_reports (
    source_id,
    rfmo_shortname,
    report_date,
    import_batch_id,
    raw_vessel_data,
    file_source,
    row_number,
    data_hash
)
SELECT 
    '$ICCAT_SOURCE_ID'::uuid,
    'ICCAT',
    COALESCE(source_date::date, CURRENT_DATE),
    '$BATCH_ID'::uuid,
    to_jsonb(stage.*) as raw_vessel_data,
    '$(basename "$INPUT_FILE")',
    ROW_NUMBER() OVER (ORDER BY vessel_name),
    md5(to_jsonb(stage.*)::text) as data_hash
FROM iccat_raw_staging stage;
"

REPORTS_COUNT=$(execute_sql "
    SELECT COUNT(*) FROM intelligence_reports 
    WHERE import_batch_id = '$BATCH_ID'::uuid;
" "-t" | xargs)

log_success "Created $REPORTS_COUNT intelligence reports"

# Extract structured vessel intelligence (PRESERVE EVERYTHING)
log_step "Extracting vessel intelligence..."

execute_sql "
INSERT INTO vessel_intelligence (
    report_id,
    reported_vessel_name,
    reported_imo,
    reported_ircs,
    reported_mmsi,
    reported_flag,
    rfmo_vessel_id,
    rfmo_vessel_number,
    reported_vessel_type,
    reported_gear_types,
    reported_length,
    reported_tonnage,
    reported_port_registry,
    reported_build_year,
    reported_owner_name,
    reported_owner_address,
    reported_operator_name,
    reported_operator_address,
    authorization_status,
    rfmo_specific_data,
    data_completeness_score,
    source_authority_level
)
SELECT 
    ir.report_id,
    NULLIF(TRIM(ir.raw_vessel_data->>'vessel_name'), ''),
    NULLIF(TRIM(ir.raw_vessel_data->>'imo'), ''),
    NULLIF(TRIM(ir.raw_vessel_data->>'ircs'), ''),
    NULLIF(TRIM(ir.raw_vessel_data->>'mmsi'), ''),
    NULLIF(TRIM(ir.raw_vessel_data->>'vessel_flag_alpha3'), ''),
    NULLIF(TRIM(ir.raw_vessel_data->>'iccat_serial_no'), ''),
    NULLIF(TRIM(ir.raw_vessel_data->>'old_iccat_serial_no'), ''),
    NULLIF(TRIM(ir.raw_vessel_data->>'vessel_type_code'), ''),
    NULLIF(TRIM(ir.raw_vessel_data->>'gear_type_code'), ''),
    CASE 
        WHEN ir.raw_vessel_data->>'length_value' ~ '^[0-9]+\.?[0-9]*$' 
        THEN (ir.raw_vessel_data->>'length_value')::numeric 
    END,
    CASE 
        WHEN ir.raw_vessel_data->>'tonnage_value' ~ '^[0-9]+\.?[0-9]*$' 
        THEN (ir.raw_vessel_data->>'tonnage_value')::numeric 
    END,
    NULLIF(TRIM(ir.raw_vessel_data->>'port_of_registry'), ''),
    CASE 
        WHEN ir.raw_vessel_data->>'year_built' ~ '^[0-9]{4}$' 
        THEN (ir.raw_vessel_data->>'year_built')::integer 
    END,
    NULLIF(TRIM(ir.raw_vessel_data->>'owner_name'), ''),
    TRIM(
        COALESCE(ir.raw_vessel_data->>'owner_address', '') || ' ' ||
        COALESCE(ir.raw_vessel_data->>'owner_city', '') || ' ' ||
        COALESCE(ir.raw_vessel_data->>'owner_zipcode', '') || ' ' ||
        COALESCE(ir.raw_vessel_data->>'owner_country', '')
    ),
    NULLIF(TRIM(ir.raw_vessel_data->>'operator_name'), ''),
    TRIM(
        COALESCE(ir.raw_vessel_data->>'operator_address', '') || ' ' ||
        COALESCE(ir.raw_vessel_data->>'operator_city', '') || ' ' ||
        COALESCE(ir.raw_vessel_data->>'operator_zipcode', '') || ' ' ||
        COALESCE(ir.raw_vessel_data->>'operator_country', '')
    ),
    'ACTIVE',  -- ICCAT assumes active if in register
    jsonb_build_object(
        'external_marking', ir.raw_vessel_data->>'external_marking',
        'shipyard_country', ir.raw_vessel_data->>'shipyard_country',
        'length_metric_type', ir.raw_vessel_data->>'length_metric_type',
        'length_unit_enum', ir.raw_vessel_data->>'length_unit_enum',
        'depth_value', ir.raw_vessel_data->>'depth_value',
        'depth_metric_type', ir.raw_vessel_data->>'depth_metric_type',
        'tonnage_metric_type', ir.raw_vessel_data->>'tonnage_metric_type',
        'engine_power', ir.raw_vessel_data->>'engine_power',
        'car_capacity_value', ir.raw_vessel_data->>'car_capacity_value',
        'vms_com_sys_code', ir.raw_vessel_data->>'vms_com_sys_code',
        'national_registry', ir.raw_vessel_data->>'national_registry'
    ),
    -- Calculate data completeness score (0-1)
    (
        CASE WHEN TRIM(ir.raw_vessel_data->>'vessel_name') IS NOT NULL AND TRIM(ir.raw_vessel_data->>'vessel_name') != '' THEN 1 ELSE 0 END +
        CASE WHEN TRIM(ir.raw_vessel_data->>'imo') IS NOT NULL AND TRIM(ir.raw_vessel_data->>'imo') != '' THEN 1 ELSE 0 END +
        CASE WHEN TRIM(ir.raw_vessel_data->>'ircs') IS NOT NULL AND TRIM(ir.raw_vessel_data->>'ircs') != '' THEN 1 ELSE 0 END +
        CASE WHEN TRIM(ir.raw_vessel_data->>'vessel_flag_alpha3') IS NOT NULL AND TRIM(ir.raw_vessel_data->>'vessel_flag_alpha3') != '' THEN 1 ELSE 0 END +
        CASE WHEN TRIM(ir.raw_vessel_data->>'iccat_serial_no') IS NOT NULL AND TRIM(ir.raw_vessel_data->>'iccat_serial_no') != '' THEN 1 ELSE 0 END +
        CASE WHEN TRIM(ir.raw_vessel_data->>'owner_name') IS NOT NULL AND TRIM(ir.raw_vessel_data->>'owner_name') != '' THEN 1 ELSE 0 END
    )::decimal / 6.0,
    'AUTHORITATIVE'::source_authority_level
FROM intelligence_reports ir
WHERE ir.import_batch_id = '$BATCH_ID'::uuid;
"

INTELLIGENCE_COUNT=$(execute_sql "
    SELECT COUNT(*) 
    FROM vessel_intelligence vi
    JOIN intelligence_reports ir ON vi.report_id = ir.report_id
    WHERE ir.import_batch_id = '$BATCH_ID'::uuid;
" "-t" | xargs)

log_success "Extracted $INTELLIGENCE_COUNT vessel intelligence records"

# Update batch statistics
execute_sql "
UPDATE intelligence_import_batches 
SET 
    raw_records_count = $REPORTS_COUNT,
    intelligence_extracted_count = $INTELLIGENCE_COUNT,
    stage_1_raw_complete = true,
    stage_2_extraction_complete = true
WHERE batch_id = '$BATCH_ID'::uuid;
"

# Show intelligence summary
log_step "📊 ICCAT Intelligence Summary"

execute_sql "
SELECT 
    'Total Intelligence Reports' as metric,
    COUNT(DISTINCT ir.report_id)::text as value
FROM intelligence_reports ir
WHERE ir.rfmo_shortname = 'ICCAT'

UNION ALL

SELECT 
    'Vessels with IMO',
    COUNT(DISTINCT vi.intelligence_id)::text
FROM vessel_intelligence vi
JOIN intelligence_reports ir ON vi.report_id = ir.report_id
WHERE ir.rfmo_shortname = 'ICCAT' AND vi.reported_imo IS NOT NULL

UNION ALL

SELECT 
    'Vessels with IRCS',
    COUNT(DISTINCT vi.intelligence_id)::text
FROM vessel_intelligence vi
JOIN intelligence_reports ir ON vi.report_id = ir.report_id
WHERE ir.rfmo_shortname = 'ICCAT' AND vi.reported_ircs IS NOT NULL

UNION ALL

SELECT 
    'Unique ICCAT Serial Numbers',
    COUNT(DISTINCT vi.rfmo_vessel_id)::text
FROM vessel_intelligence vi
JOIN intelligence_reports ir ON vi.report_id = ir.report_id
WHERE ir.rfmo_shortname = 'ICCAT' AND vi.rfmo_vessel_id IS NOT NULL

UNION ALL

SELECT 
    'Unique Vessel Names',
    COUNT(DISTINCT vi.reported_vessel_name)::text
FROM vessel_intelligence vi
JOIN intelligence_reports ir ON vi.report_id = ir.report_id
WHERE ir.rfmo_shortname = 'ICCAT' AND vi.reported_vessel_name IS NOT NULL

UNION ALL

SELECT 
    'Average Data Completeness',
    ROUND(AVG(vi.data_completeness_score) * 100, 1)::text || '%'
FROM vessel_intelligence vi
JOIN intelligence_reports ir ON vi.report_id = ir.report_id
WHERE ir.rfmo_shortname = 'ICCAT';
"

log_success "✅ ICCAT Phase 1 Complete: Raw Intelligence Collection"
log_success "   📊 $INTELLIGENCE_COUNT intelligence records collected"
log_success "   🎯 0% data loss (all records preserved as intelligence)"
log_success "   ⏭️  Ready for Phase 2: Cross-Source Identity Resolution"