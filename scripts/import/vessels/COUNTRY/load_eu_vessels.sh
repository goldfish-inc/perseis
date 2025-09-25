#!/bin/bash
# Import script for EU Fleet Register vessels
# Handles any EU country code passed as parameter
set -euo pipefail

# Source the environment and logging functions
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source /app/scripts/core/logging.sh
source /app/scripts/core/env.sh

# =====================================================
# CONFIGURATION
# =====================================================
# Get country code from parameter or environment
EU_COUNTRY="${1:-${EU_COUNTRY:-}}"
if [[ -z "$EU_COUNTRY" ]]; then
    log_error "EU country code required. Usage: $0 DEU"
    log_error "Valid codes: BEL BGR CYP DEU DNK ESP EST FIN FRA GRC HRV IRL ITA LTU LVA MLT NLD POL PRT ROU SVN SWE"
    exit 1
fi

SOURCE_NAME="EU_${EU_COUNTRY}"
# Default to the most recent file in the raw directory
RAW_DIR="/import/vessels/vessel_data/COUNTRY/EU_${EU_COUNTRY}/raw"
if [[ -z "${INPUT_FILE:-}" ]]; then
    INPUT_FILE=$(ls -t "$RAW_DIR"/*.csv 2>/dev/null | head -n1)
    if [[ -z "$INPUT_FILE" ]]; then
        log_error "No CSV files found in $RAW_DIR"
        exit 1
    fi
fi

log_step "🇪🇺 Starting EU Fleet Register Import for $EU_COUNTRY"

# =====================================================
# PRE-FLIGHT CHECKS
# =====================================================

# Test database connection
if ! test_connection; then
    log_error "Cannot connect to database"
    exit 1
fi

# Verify input file exists
if [[ ! -f "$INPUT_FILE" ]]; then
    log_error "Input file not found: $INPUT_FILE"
    exit 1
fi

# =====================================================
# FILE VALIDATION
# =====================================================

# Record file metadata
FILE_HASH=$(sha256sum "$INPUT_FILE" | cut -d' ' -f1)
FILE_SIZE=$(stat -f%z "$INPUT_FILE" 2>/dev/null || stat -c%s "$INPUT_FILE")
FILE_MODIFIED=$(stat -f%m "$INPUT_FILE" 2>/dev/null || stat -c%Y "$INPUT_FILE")

log_success "File validation:"
log_success "  File: $(basename "$INPUT_FILE")"
log_success "  Size: $(numfmt --to=iec-i --suffix=B $FILE_SIZE 2>/dev/null || echo "$FILE_SIZE bytes")"
log_success "  Hash: ${FILE_HASH:0:16}..."

# Check if this exact file was already imported
EXISTING_IMPORT=$(execute_sql "
    SELECT COUNT(*) 
    FROM data_lineage 
    WHERE source_file_hash = '$FILE_HASH'
        AND source_id = (
            SELECT source_id FROM original_sources_vessels 
            WHERE source_shortname = '$SOURCE_NAME' LIMIT 1
        );
" "-t" | xargs)

if [[ "$EXISTING_IMPORT" -gt 0 ]]; then
    log_warning "⚠️  This exact file has already been imported"
    log_warning "   Skipping to prevent duplicate import"
    exit 0
fi

# =====================================================
# SOURCE VALIDATION
# =====================================================

# Get source ID
SOURCE_ID=$(execute_sql "
    SELECT source_id FROM original_sources_vessels 
    WHERE source_shortname = '$SOURCE_NAME' 
    LIMIT 1;
" "-t" | xargs)

if [[ -z "$SOURCE_ID" ]]; then
    log_error "$SOURCE_NAME not found in original_sources_vessels"
    log_error "Source should exist from initial setup"
    exit 1
fi

log_success "Source ID: $SOURCE_ID"

# =====================================================
# DATA ANALYSIS
# =====================================================

# Count total lines in file
TOTAL_LINES=$(wc -l < "$INPUT_FILE" | xargs)
# Data records = total - header
RECORD_COUNT=$((TOTAL_LINES - 1))
log_success "Records to import: $RECORD_COUNT"

# =====================================================
# CREATE STAGING TABLE
# =====================================================

log_step "Creating staging table..."

# Create staging table for EU Fleet Register structure
execute_sql "
DROP TABLE IF EXISTS eu_vessels_staging;
CREATE TABLE eu_vessels_staging (
    country_of_registration TEXT,
    cfr TEXT,
    uvi TEXT,
    event TEXT,
    event_start_date TEXT,
    event_end_date TEXT,
    registration_number TEXT,
    external_marking TEXT,
    name_of_vessel TEXT,
    place_of_registration TEXT,
    place_of_registration_name TEXT,
    ircs TEXT,
    ircs_indicator TEXT,
    licence_indicator TEXT,
    vms_indicator TEXT,
    ers_indicator TEXT,
    ers_exempt_indicator TEXT,
    ais_indicator TEXT,
    mmsi TEXT,
    vessel_type TEXT,
    main_fishing_gear TEXT,
    subsidiary_fishing_gear_1 TEXT,
    subsidiary_fishing_gear_2 TEXT,
    subsidiary_fishing_gear_3 TEXT,
    subsidiary_fishing_gear_4 TEXT,
    subsidiary_fishing_gear_5 TEXT,
    loa TEXT,
    lbp TEXT,
    tonnage_gt TEXT,
    other_tonnage TEXT,
    gts TEXT,
    power_of_main_engine TEXT,
    power_of_auxiliary_engine TEXT,
    hull_material TEXT,
    date_of_entry_into_service TEXT,
    segment TEXT,
    country_of_importation_exportation TEXT,
    type_of_export TEXT,
    public_aid TEXT,
    year_of_construction TEXT
);
"

# =====================================================
# LOAD DATA TO STAGING
# =====================================================

log_step "Loading data to staging..."

# Check if cleaned file exists
CLEANED_FILE="${RAW_DIR}/../cleaned/${EU_COUNTRY}_vessels_cleaned.csv"
if [[ -f "$CLEANED_FILE" ]]; then
    log_success "Using cleaned data file"
    LOAD_FILE="$CLEANED_FILE"
else
    log_warning "No cleaned file found, using raw data"
    LOAD_FILE="$INPUT_FILE"
fi

# Use COPY with CSV header and semicolon delimiter
execute_sql "\\copy eu_vessels_staging FROM '$LOAD_FILE' WITH CSV HEADER DELIMITER ';'"

# Verify load
STAGED_COUNT=$(execute_sql "SELECT COUNT(*) FROM eu_vessels_staging WHERE name_of_vessel IS NOT NULL;" "-t" | xargs)

log_success "Staged $STAGED_COUNT records"

# =====================================================
# CREATE IMPORT BATCH
# =====================================================

BATCH_ID=$(execute_sql "
    INSERT INTO intelligence_import_batches (
        rfmo_shortname,
        import_date,
        source_file_path,
        source_file_hash,
        source_file_size,
        raw_records_count,
        source_version,
        is_incremental,
        is_current
    ) VALUES (
        '$SOURCE_NAME',
        CURRENT_DATE,
        '$INPUT_FILE',
        '$FILE_HASH',
        $FILE_SIZE,
        $STAGED_COUNT,
        '2025-09',
        FALSE,
        TRUE
    ) RETURNING batch_id;
" "-t" | head -n1 | xargs)

log_success "Created import batch: $BATCH_ID"

# =====================================================
# CREATE DATA LINEAGE
# =====================================================

LINEAGE_ID=$(execute_sql "
    INSERT INTO data_lineage (
        source_id,
        source_file_path,
        source_file_hash,
        source_file_size,
        source_file_modified,
        import_batch_id,
        records_in_file,
        processing_status
    ) VALUES (
        '$SOURCE_ID'::uuid,
        '$INPUT_FILE',
        '$FILE_HASH',
        $FILE_SIZE,
        to_timestamp($FILE_MODIFIED),
        '$BATCH_ID'::uuid,
        $STAGED_COUNT,
        'PROCESSING'
    ) RETURNING lineage_id;
" "-t" | head -n1 | xargs)

log_success "Created lineage record: $LINEAGE_ID"

# =====================================================
# CONVERT TO INTELLIGENCE REPORTS
# =====================================================

log_step "Creating intelligence reports..."

execute_sql "
INSERT INTO intelligence_reports (
    source_id,
    rfmo_shortname,
    report_date,
    import_batch_id,
    raw_vessel_data,
    file_source,
    row_number,
    data_hash,
    valid_from,
    is_current
)
SELECT 
    '$SOURCE_ID'::uuid,
    '$SOURCE_NAME',
    '2025-09-08'::date,
    '$BATCH_ID'::uuid,
    jsonb_strip_nulls(jsonb_build_object(
        'vessel_name', NULLIF(name_of_vessel, '---'),
        'cfr', NULLIF(cfr, ''),
        'imo', NULLIF(uvi, ''),
        'ircs', NULLIF(ircs, ''),
        'mmsi', NULLIF(mmsi, ''),
        'external_marking', NULLIF(external_marking, ''),
        'registration_number', NULLIF(registration_number, ''),
        'vessel_flag_alpha3', country_of_registration,
        'vessel_type', NULLIF(vessel_type, ''),
        'main_gear', NULLIF(main_fishing_gear, ''),
        'subsidiary_gears', ARRAY_REMOVE(ARRAY[
            NULLIF(subsidiary_fishing_gear_1, ''),
            NULLIF(subsidiary_fishing_gear_2, ''),
            NULLIF(subsidiary_fishing_gear_3, ''),
            NULLIF(subsidiary_fishing_gear_4, ''),
            NULLIF(subsidiary_fishing_gear_5, '')
        ], NULL),
        'loa', NULLIF(loa, ''),
        'lbp', NULLIF(lbp, ''),
        'tonnage_gt', NULLIF(tonnage_gt, ''),
        'engine_power_kw', NULLIF(power_of_main_engine, ''),
        'auxiliary_power_kw', NULLIF(power_of_auxiliary_engine, ''),
        'hull_material', NULLIF(hull_material, ''),
        'year_of_construction', NULLIF(year_of_construction, ''),
        'place_of_registration', NULLIF(place_of_registration_name, ''),
        'event', NULLIF(event, ''),
        'event_start_date', NULLIF(event_start_date, ''),
        'event_end_date', NULLIF(event_end_date, ''),
        'indicators', jsonb_build_object(
            'ircs', NULLIF(ircs_indicator, ''),
            'licence', NULLIF(licence_indicator, ''),
            'vms', NULLIF(vms_indicator, ''),
            'ers', NULLIF(ers_indicator, ''),
            'ers_exempt', NULLIF(ers_exempt_indicator, ''),
            'ais', NULLIF(ais_indicator, '')
        ),
        'segment', NULLIF(segment, ''),
        'original_source', '$SOURCE_NAME'
    )),
    '$(basename "$INPUT_FILE")',
    ROW_NUMBER() OVER (ORDER BY cfr, name_of_vessel),
    md5(cfr || COALESCE(name_of_vessel, '') || COALESCE(ircs, '')),
    CURRENT_DATE,
    TRUE
FROM eu_vessels_staging;
"

REPORTS_COUNT=$(execute_sql "
    SELECT COUNT(*) FROM intelligence_reports 
    WHERE import_batch_id = '$BATCH_ID'::uuid;
" "-t" | xargs)

log_success "Created $REPORTS_COUNT intelligence reports"

# =====================================================
# EXTRACT VESSEL INTELLIGENCE
# =====================================================

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
    reported_vessel_type,
    reported_gear_types,
    reported_length,
    reported_tonnage,
    reported_build_year,
    reported_port_registry,
    authorization_status,
    rfmo_specific_data,
    data_completeness_score,
    source_authority_level,
    valid_from,
    is_current
)
SELECT 
    ir.report_id,
    NULLIF(TRIM(ir.raw_vessel_data->>'vessel_name'), ''),
    NULLIF(TRIM(ir.raw_vessel_data->>'imo'), ''),
    NULLIF(TRIM(ir.raw_vessel_data->>'ircs'), ''),
    NULLIF(TRIM(ir.raw_vessel_data->>'mmsi'), ''),
    NULLIF(TRIM(ir.raw_vessel_data->>'vessel_flag_alpha3'), ''),
    NULLIF(TRIM(ir.raw_vessel_data->>'cfr'), ''), -- CFR as vessel ID
    NULLIF(TRIM(ir.raw_vessel_data->>'vessel_type'), ''),
    NULLIF(TRIM(ir.raw_vessel_data->>'main_gear'), ''),
    CASE 
        WHEN ir.raw_vessel_data->>'loa' ~ '^[0-9]+\.?[0-9]*$' 
        THEN (ir.raw_vessel_data->>'loa')::numeric 
    END,
    CASE 
        WHEN ir.raw_vessel_data->>'tonnage_gt' ~ '^[0-9]+\.?[0-9]*$' 
        THEN (ir.raw_vessel_data->>'tonnage_gt')::numeric 
    END,
    CASE 
        WHEN ir.raw_vessel_data->>'year_of_construction' ~ '^[0-9]{4}$' 
        THEN (ir.raw_vessel_data->>'year_of_construction')::integer 
    END,
    NULLIF(TRIM(ir.raw_vessel_data->>'place_of_registration'), ''),
    CASE 
        WHEN ir.raw_vessel_data->>'event' = 'LIC' THEN 'ACTIVE'
        WHEN ir.raw_vessel_data->>'event' = 'MOD' THEN 'ACTIVE'
        WHEN ir.raw_vessel_data->>'event' = 'DES' THEN 'INACTIVE'
        ELSE 'ACTIVE'
    END,
    jsonb_build_object(
        'cfr', ir.raw_vessel_data->>'cfr',
        'external_marking', ir.raw_vessel_data->>'external_marking',
        'registration_number', ir.raw_vessel_data->>'registration_number',
        'engine_power_kw', ir.raw_vessel_data->>'engine_power_kw',
        'auxiliary_power_kw', ir.raw_vessel_data->>'auxiliary_power_kw',
        'hull_material', ir.raw_vessel_data->>'hull_material',
        'subsidiary_gears', ir.raw_vessel_data->'subsidiary_gears',
        'indicators', ir.raw_vessel_data->'indicators',
        'event', ir.raw_vessel_data->>'event',
        'event_dates', jsonb_build_object(
            'start', ir.raw_vessel_data->>'event_start_date',
            'end', ir.raw_vessel_data->>'event_end_date'
        )
    ),
    -- Calculate data completeness
    (
        CASE WHEN ir.raw_vessel_data->>'vessel_name' IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN ir.raw_vessel_data->>'cfr' IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN ir.raw_vessel_data->>'ircs' IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN ir.raw_vessel_data->>'vessel_type' IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN ir.raw_vessel_data->>'tonnage_gt' IS NOT NULL THEN 1 ELSE 0 END
    )::decimal / 5.0,
    'AUTHORITATIVE'::source_authority_level, -- Government registry
    CURRENT_DATE,
    TRUE
FROM intelligence_reports ir
WHERE ir.import_batch_id = '$BATCH_ID'::uuid
  AND ir.raw_vessel_data->>'vessel_name' IS NOT NULL;
"

INTELLIGENCE_COUNT=$(execute_sql "
    SELECT COUNT(*) 
    FROM vessel_intelligence vi
    JOIN intelligence_reports ir ON vi.report_id = ir.report_id
    WHERE ir.import_batch_id = '$BATCH_ID'::uuid;
" "-t" | xargs)

log_success "Extracted $INTELLIGENCE_COUNT vessel intelligence records"

# =====================================================
# UPDATE CONFIRMATIONS
# =====================================================

log_step "Updating cross-source confirmations..."

# Update vessel confirmations
execute_sql "SELECT * FROM update_vessel_confirmations('$BATCH_ID'::uuid);"

# Rebuild confirmation tracking
execute_sql "SELECT * FROM rebuild_vessel_confirmations();"

# =====================================================
# FINALIZE IMPORT
# =====================================================

# Update batch completion
execute_sql "
UPDATE intelligence_import_batches 
SET 
    raw_records_count = $REPORTS_COUNT,
    intelligence_extracted_count = $INTELLIGENCE_COUNT,
    stage_1_raw_complete = true,
    stage_2_extraction_complete = true,
    processing_completed_at = NOW()
WHERE batch_id = '$BATCH_ID'::uuid;
"

# Update lineage
execute_sql "
UPDATE data_lineage 
SET 
    records_imported = $REPORTS_COUNT,
    processing_status = 'COMPLETED',
    processing_completed_at = NOW()
WHERE lineage_id = '$LINEAGE_ID'::uuid;
"

# =====================================================
# CLEANUP
# =====================================================

execute_sql "DROP TABLE IF EXISTS eu_vessels_staging;"

# =====================================================
# SUMMARY
# =====================================================

log_step "📊 Import Summary"
log_success "✅ EU Fleet Register Import Complete for $EU_COUNTRY"
log_success "   Source: EU Fleet Register - $EU_COUNTRY"
log_success "   Records: $INTELLIGENCE_COUNT vessels"
log_success "   Quality: Authoritative government source"
log_success "   Coverage: European Union registered vessels"

# Show some sample data
log_step "Sample imported vessels:"
execute_sql "
SELECT 
    vi.reported_vessel_name as vessel,
    vi.rfmo_vessel_id as cfr,
    vi.reported_ircs as ircs,
    vi.reported_tonnage as gt,
    vi.authorization_status as status
FROM vessel_intelligence vi
JOIN intelligence_reports ir ON vi.report_id = ir.report_id
WHERE ir.import_batch_id = '$BATCH_ID'::uuid
LIMIT 5;
"

exit 0