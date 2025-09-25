#!/bin/bash
# Import script for USA Alaska vessel registry
# Single dataset import - runs independently
set -euo pipefail

# Source the environment and logging functions
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source /app/scripts/core/logging.sh
source /app/scripts/core/env.sh

# =====================================================
# CONFIGURATION
# =====================================================
SOURCE_NAME="USA_AK"
# Default to the most recent file in the raw directory
RAW_DIR="/import/vessels/vessel_data/COUNTRY/USA_AK/raw"
if [[ -z "${INPUT_FILE:-}" ]]; then
    INPUT_FILE=$(ls -t "$RAW_DIR"/*.csv 2>/dev/null | head -n1)
    if [[ -z "$INPUT_FILE" ]]; then
        log_error "No CSV files found in $RAW_DIR"
        exit 1
    fi
fi

log_step "🇺🇸 Starting USA Alaska Vessel Registry Import"

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

# Count total lines in file (skip header)
TOTAL_LINES=$(wc -l < "$INPUT_FILE" | xargs)
RECORD_COUNT=$((TOTAL_LINES - 1))
log_success "Records to import: $RECORD_COUNT"

# =====================================================
# CREATE STAGING TABLE
# =====================================================

log_step "Creating staging table..."

# Create staging table - first read header to understand structure
HEADER=$(head -n1 "$INPUT_FILE")
log_success "CSV structure detected"

# Create comprehensive staging table for Alaska data
execute_sql "
DROP TABLE IF EXISTS usa_ak_staging;
CREATE TABLE usa_ak_staging (
    year TEXT,
    adfg_number TEXT,
    year_built TEXT,
    length TEXT,
    gross_tons TEXT,
    net_tons TEXT,
    horse_power TEXT,
    hold_tank_capacity TEXT,
    live_tank_capacity TEXT,
    fuel_capacity TEXT,
    home_port_city TEXT,
    home_port_state TEXT,
    coast_guard_number TEXT,
    vessel_name TEXT,
    owner_name TEXT,
    name_type TEXT,
    file_number TEXT,
    street TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    country TEXT,
    effective_date TEXT,
    engine TEXT,
    hull_type TEXT,
    refrigeration TEXT,
    salmon_registration_area TEXT,
    freezer_canner TEXT,
    tender_packer TEXT,
    charter TEXT,
    fishing TEXT,
    purse_seine TEXT,
    beach_seine TEXT,
    drift_gill_net TEXT,
    set_gill_net TEXT,
    hand_troll TEXT,
    long_line TEXT,
    otter_trawl TEXT,
    fish_wheel TEXT,
    pots TEXT,
    power_troll TEXT,
    beam_trawl TEXT,
    scallop_dredge TEXT,
    mechanical_jig TEXT,
    double_otter_trawl TEXT,
    herring_gill_net TEXT,
    pair_trawl TEXT,
    diving_hand_picking TEXT,
    active_date TEXT,
    active_end TEXT,
    hull_id TEXT,
    last_name TEXT,
    first_name TEXT,
    middle TEXT
);
"

# =====================================================
# LOAD DATA TO STAGING
# =====================================================

log_step "Loading data to staging..."

# Use COPY with CSV header
execute_sql "\\copy usa_ak_staging FROM '$INPUT_FILE' WITH CSV HEADER"

# Verify load
STAGED_COUNT=$(execute_sql "SELECT COUNT(*) FROM usa_ak_staging WHERE vessel_name IS NOT NULL AND vessel_name != '';" "-t" | xargs)

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
        '2025-01',
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
    '2025-01-01'::date, -- From filename
    '$BATCH_ID'::uuid,
    jsonb_strip_nulls(jsonb_build_object(
        'vessel_name', NULLIF(vessel_name, ''),
        'adfg_number', NULLIF(adfg_number, ''),
        'coast_guard_number', NULLIF(coast_guard_number, ''),
        'year_built', NULLIF(year_built, ''),
        'length', NULLIF(length, ''),
        'gross_tons', NULLIF(gross_tons, ''),
        'net_tons', NULLIF(net_tons, ''),
        'horse_power', NULLIF(horse_power, ''),
        'home_port', COALESCE(
            NULLIF(home_port_city || ', ' || home_port_state, ', '),
            home_port_city
        ),
        'owner_name', NULLIF(owner_name, ''),
        'owner_details', jsonb_build_object(
            'last_name', NULLIF(last_name, ''),
            'first_name', NULLIF(first_name, ''),
            'middle', NULLIF(middle, ''),
            'address', jsonb_build_object(
                'street', NULLIF(street, ''),
                'city', NULLIF(city, ''),
                'state', NULLIF(state, ''),
                'zip', NULLIF(zip_code, ''),
                'country', NULLIF(country, '')
            )
        ),
        'vessel_characteristics', jsonb_build_object(
            'engine', NULLIF(engine, ''),
            'hull_type', NULLIF(hull_type, ''),
            'refrigeration', NULLIF(refrigeration, ''),
            'hull_id', NULLIF(hull_id, ''),
            'hold_capacity', NULLIF(hold_tank_capacity, ''),
            'fuel_capacity', NULLIF(fuel_capacity, '')
        ),
        'gear_types', jsonb_build_object(
            'purse_seine', NULLIF(purse_seine, ''),
            'drift_gill_net', NULLIF(drift_gill_net, ''),
            'set_gill_net', NULLIF(set_gill_net, ''),
            'long_line', NULLIF(long_line, ''),
            'otter_trawl', NULLIF(otter_trawl, ''),
            'pots', NULLIF(pots, ''),
            'power_troll', NULLIF(power_troll, ''),
            'hand_troll', NULLIF(hand_troll, ''),
            'other_gears', jsonb_build_object(
                'beach_seine', NULLIF(beach_seine, ''),
                'fish_wheel', NULLIF(fish_wheel, ''),
                'beam_trawl', NULLIF(beam_trawl, ''),
                'scallop_dredge', NULLIF(scallop_dredge, ''),
                'mechanical_jig', NULLIF(mechanical_jig, ''),
                'double_otter_trawl', NULLIF(double_otter_trawl, ''),
                'herring_gill_net', NULLIF(herring_gill_net, ''),
                'pair_trawl', NULLIF(pair_trawl, ''),
                'diving_hand_picking', NULLIF(diving_hand_picking, '')
            )
        ),
        'registration_year', NULLIF(year, ''),
        'vessel_flag_alpha3', 'USA',
        'original_source', '$SOURCE_NAME'
    )),
    '$(basename "$INPUT_FILE")',
    ROW_NUMBER() OVER (ORDER BY adfg_number, vessel_name),
    md5(adfg_number || COALESCE(vessel_name, '') || COALESCE(coast_guard_number, '')),
    CURRENT_DATE,
    TRUE
FROM usa_ak_staging
WHERE vessel_name IS NOT NULL AND vessel_name != '';
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
    reported_flag,
    rfmo_vessel_id,
    reported_length,
    reported_tonnage,
    reported_build_year,
    reported_port_registry,
    reported_owner_name,
    rfmo_specific_data,
    data_completeness_score,
    source_authority_level,
    valid_from,
    is_current
)
SELECT 
    ir.report_id,
    NULLIF(TRIM(ir.raw_vessel_data->>'vessel_name'), ''),
    'USA',
    COALESCE(
        NULLIF(TRIM(ir.raw_vessel_data->>'adfg_number'), ''),
        NULLIF(TRIM(ir.raw_vessel_data->>'coast_guard_number'), '')
    ),
    CASE 
        WHEN ir.raw_vessel_data->>'length' ~ '^[0-9]+\.?[0-9]*$' 
        THEN (ir.raw_vessel_data->>'length')::numeric 
    END,
    CASE 
        WHEN ir.raw_vessel_data->>'gross_tons' ~ '^[0-9]+\.?[0-9]*$' 
        THEN (ir.raw_vessel_data->>'gross_tons')::numeric 
    END,
    CASE 
        WHEN ir.raw_vessel_data->>'year_built' ~ '^[0-9]{4}$' 
        THEN (ir.raw_vessel_data->>'year_built')::integer 
    END,
    NULLIF(TRIM(ir.raw_vessel_data->>'home_port'), ''),
    NULLIF(TRIM(ir.raw_vessel_data->>'owner_name'), ''),
    jsonb_build_object(
        'adfg_number', ir.raw_vessel_data->>'adfg_number',
        'coast_guard_number', ir.raw_vessel_data->>'coast_guard_number',
        'net_tons', ir.raw_vessel_data->>'net_tons',
        'horse_power', ir.raw_vessel_data->>'horse_power',
        'owner_details', ir.raw_vessel_data->'owner_details',
        'vessel_characteristics', ir.raw_vessel_data->'vessel_characteristics',
        'gear_types', ir.raw_vessel_data->'gear_types'
    ),
    -- Calculate data completeness
    (
        CASE WHEN ir.raw_vessel_data->>'vessel_name' IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN ir.raw_vessel_data->>'adfg_number' IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN ir.raw_vessel_data->>'length' IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN ir.raw_vessel_data->>'home_port' IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN ir.raw_vessel_data->>'owner_name' IS NOT NULL THEN 1 ELSE 0 END
    )::decimal / 5.0,
    'AUTHORITATIVE'::source_authority_level, -- State government registry
    CURRENT_DATE,
    TRUE
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

execute_sql "DROP TABLE IF EXISTS usa_ak_staging;"

# =====================================================
# SUMMARY
# =====================================================

log_step "📊 Import Summary"
log_success "✅ USA Alaska Vessel Registry Import Complete"
log_success "   Source: State of Alaska Vessel Registry"
log_success "   Records: $INTELLIGENCE_COUNT vessels"
log_success "   Quality: Authoritative government source"
log_success "   Coverage: Alaska registered fishing vessels"

# Show gear type distribution
log_step "Gear Type Distribution:"
execute_sql "
WITH gear_counts AS (
    SELECT 
        COUNT(*) FILTER (WHERE ir.raw_vessel_data->'gear_types'->>'purse_seine' = 'Yes') as purse_seine,
        COUNT(*) FILTER (WHERE ir.raw_vessel_data->'gear_types'->>'drift_gill_net' = 'Yes') as drift_gillnet,
        COUNT(*) FILTER (WHERE ir.raw_vessel_data->'gear_types'->>'long_line' = 'Yes') as longline,
        COUNT(*) FILTER (WHERE ir.raw_vessel_data->'gear_types'->>'otter_trawl' = 'Yes') as trawl,
        COUNT(*) FILTER (WHERE ir.raw_vessel_data->'gear_types'->>'pots' = 'Yes') as pots
    FROM intelligence_reports ir
    WHERE ir.import_batch_id = '$BATCH_ID'::uuid
)
SELECT * FROM gear_counts;
"

exit 0