#!/bin/bash
# Enhanced Staged Intelligence Import Template
# Uses Phase 1 improvements: temporal tracking, change detection, confirmations, lineage
set -euo pipefail

source /app/scripts/core/logging.sh
source /app/scripts/core/env.sh

# =====================================================
# CONFIGURATION
# =====================================================
RFMO="${RFMO:-NAFO}"  # North Atlantic Fisheries Organization
CLEANED_DATA_DIR="/import/vessels/vessel_data/RFMO/cleaned"
INPUT_FILE="${INPUT_FILE:-$CLEANED_DATA_DIR/${RFMO}_vessels_cleaned.csv}"

log_step "🎯 $RFMO Enhanced Staged Intelligence Import (Phase 1)"

# =====================================================
# PRE-FLIGHT CHECKS
# =====================================================

# Test database connection
if ! test_connection; then
    log_error "Cannot connect to database"
    exit 1
fi

# Verify cleaned file exists
if [[ ! -f "$INPUT_FILE" ]]; then
    log_error "Cleaned data not found: $INPUT_FILE"
    exit 1
fi

# =====================================================
# FILE METADATA & LINEAGE TRACKING
# =====================================================

# Record file metadata for lineage
FILE_HASH=$(sha256sum "$INPUT_FILE" | cut -d' ' -f1)
FILE_SIZE=$(stat -f%z "$INPUT_FILE" 2>/dev/null || stat -c%s "$INPUT_FILE")
FILE_MODIFIED=$(stat -f%m "$INPUT_FILE" 2>/dev/null || stat -c%Y "$INPUT_FILE")

log_success "File metadata captured:"
log_success "  Hash: ${FILE_HASH:0:16}..."
log_success "  Size: $(numfmt --to=iec-i --suffix=B $FILE_SIZE)"

# Check if this exact file was already imported
EXISTING_LINEAGE=$(execute_sql "
    SELECT 
        dl.lineage_id,
        dl.import_batch_id,
        ib.import_date
    FROM data_lineage dl
    JOIN intelligence_import_batches ib ON dl.import_batch_id = ib.batch_id
    WHERE dl.source_file_hash = '$FILE_HASH'
        AND dl.source_id = (
            SELECT source_id FROM original_sources_vessels 
            WHERE source_shortname = '$RFMO' LIMIT 1
        )
    LIMIT 1;
" "-t" | head -n1)

if [[ -n "$EXISTING_LINEAGE" ]]; then
    log_warning "⚠️  This exact file was already imported"
    log_warning "   Lineage ID: $(echo $EXISTING_LINEAGE | cut -d'|' -f1)"
    log_warning "   Batch ID: $(echo $EXISTING_LINEAGE | cut -d'|' -f2)"
    log_warning "   Import Date: $(echo $EXISTING_LINEAGE | cut -d'|' -f3)"
    log_warning "   Proceeding will create a new version..."
    
    # Optional: exit if you don't want to reimport
    # exit 0
fi

# =====================================================
# CHANGE DETECTION SETUP
# =====================================================

# Get previous batch for change detection
PREVIOUS_BATCH=$(execute_sql "
    SELECT batch_id 
    FROM intelligence_import_batches 
    WHERE rfmo_shortname = '$RFMO' 
        AND stage_1_raw_complete = TRUE
        AND is_current = TRUE
    ORDER BY import_date DESC 
    LIMIT 1;
" "-t" | xargs)

if [[ -n "$PREVIOUS_BATCH" ]]; then
    log_success "Previous import found: $PREVIOUS_BATCH"
    log_success "Will perform incremental import with change detection"
    IS_INCREMENTAL="TRUE"
    
    # Mark previous as no longer current
    execute_sql "
        UPDATE intelligence_import_batches 
        SET is_current = FALSE 
        WHERE batch_id = '$PREVIOUS_BATCH'::uuid;"
else
    log_warning "No previous import found - this will be a full import"
    IS_INCREMENTAL="FALSE"
fi

# =====================================================
# SOURCE VALIDATION
# =====================================================

# Get source ID
SOURCE_ID=$(execute_sql "
    SELECT source_id FROM original_sources_vessels 
    WHERE source_shortname = '$RFMO' 
    LIMIT 1;
" "-t" | head -n1 | xargs)

if [[ -z "$SOURCE_ID" ]]; then
    log_error "$RFMO source not found in original_sources_vessels"
    exit 1
fi

log_success "$RFMO source ID: $SOURCE_ID"

# Extract source date from filename or use current date
SOURCE_DATE=$(echo "$(basename "$INPUT_FILE")" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' || date '+%Y-%m-%d')
SOURCE_VERSION=$(date -d "$SOURCE_DATE" '+%Y-%m' 2>/dev/null || date '+%Y-%m')

# =====================================================
# DATA VALIDATION
# =====================================================

# Count input records (handles CSV with embedded newlines)
INPUT_COUNT=$(python3 -c "
import csv
with open('$INPUT_FILE', 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader)  # Skip header
    print(sum(1 for row in reader))
")

# Sample data quality check
COLUMN_COUNT=$(head -1 "$INPUT_FILE" | tr ',' '\n' | wc -l)
EMPTY_VESSEL_NAMES=$(python3 -c "
import csv
with open('$INPUT_FILE', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    empty = sum(1 for row in reader if not row.get('vessel_name', '').strip())
    print(empty)
")

DATA_QUALITY_SCORE=$(python3 -c "
score = 100
score -= min(30, $EMPTY_VESSEL_NAMES * 100 / $INPUT_COUNT) if $INPUT_COUNT > 0 else 30
print(round(score, 1))
")

log_success "Data validation:"
log_success "  Records: $INPUT_COUNT"
log_success "  Columns: $COLUMN_COUNT"
log_success "  Quality Score: $DATA_QUALITY_SCORE%"

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
        previous_batch_id,
        source_version,
        is_incremental,
        is_current
    ) VALUES (
        '$RFMO',
        CURRENT_DATE,
        '$INPUT_FILE',
        '$FILE_HASH',
        $FILE_SIZE,
        $INPUT_COUNT,
        $([ -z "$PREVIOUS_BATCH" ] && echo "NULL" || echo "'$PREVIOUS_BATCH'::uuid"),
        '$SOURCE_VERSION',
        $IS_INCREMENTAL,
        TRUE
    ) RETURNING batch_id;
" "-t" | head -n1 | xargs)

log_success "Created import batch: $BATCH_ID"

# =====================================================
# RECORD DATA LINEAGE
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
        data_quality_score,
        processing_status
    ) VALUES (
        '$SOURCE_ID'::uuid,
        '$INPUT_FILE',
        '$FILE_HASH',
        $FILE_SIZE,
        to_timestamp($FILE_MODIFIED),
        '$BATCH_ID'::uuid,
        $INPUT_COUNT,
        $DATA_QUALITY_SCORE,
        'PROCESSING'
    ) RETURNING lineage_id;
" "-t" | head -n1 | xargs)

log_success "Lineage tracking ID: $LINEAGE_ID"

# =====================================================
# CREATE STAGING TABLE
# =====================================================

# NAFO specific staging table
execute_sql "
DROP TABLE IF EXISTS ${RFMO}_raw_staging;
CREATE TABLE ${RFMO}_raw_staging (
    source_date TEXT,
    original_source TEXT,
    vessel_name TEXT,
    imo TEXT,
    ircs TEXT,
    mmsi TEXT,
    national_registry TEXT,
    vessel_flag_alpha3 TEXT,
    vessel_type_code TEXT,
    vessel_type_fao_isscfv_code TEXT,
    gear_type TEXT,
    gear_type_fao_isscfg_code TEXT,
    port_of_registry TEXT,
    year_built TEXT,
    length_value TEXT,
    length_metric_type TEXT,
    length_unit_enum TEXT,
    gross_tonnage TEXT,
    tonnage_metric_type TEXT,
    engine_power TEXT,
    engine_power_unit TEXT,
    owner_name TEXT,
    operator_name TEXT,
    nafo_division TEXT,
    species_quota TEXT,
    notification_date TEXT
);
"

# =====================================================
# LOAD RAW DATA
# =====================================================

log_step "Loading raw data into staging..."

execute_sql "\\copy ${RFMO}_raw_staging FROM '$INPUT_FILE' WITH CSV HEADER"

RAW_COUNT=$(execute_sql "SELECT COUNT(*) FROM ${RFMO}_raw_staging;" "-t" | xargs)
log_success "Loaded $RAW_COUNT raw records"

# Validate no data loss at load stage
if [[ $RAW_COUNT -ne $INPUT_COUNT ]]; then
    log_error "Data loss detected! Input: $INPUT_COUNT, Loaded: $RAW_COUNT"
    
    # Update lineage with failure
    execute_sql "
        UPDATE data_lineage 
        SET processing_status = 'FAILED',
            validation_errors = jsonb_build_array(
                jsonb_build_object(
                    'error', 'Data loss at load stage',
                    'expected', $INPUT_COUNT,
                    'actual', $RAW_COUNT
                )
            )
        WHERE lineage_id = '$LINEAGE_ID'::uuid;"
    
    exit 1
fi

# =====================================================
# CONVERT TO INTELLIGENCE REPORTS
# =====================================================

log_step "Converting to raw intelligence reports..."

# Mark previous reports as not current if incremental
if [[ "$IS_INCREMENTAL" == "TRUE" ]]; then
    execute_sql "
        UPDATE intelligence_reports
        SET is_current = FALSE,
            valid_to = CURRENT_DATE,
            superseded_by = NULL
        WHERE source_id = '$SOURCE_ID'::uuid
            AND is_current = TRUE;"
fi

# Insert new intelligence reports
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
    '$RFMO',
    COALESCE(source_date::date, '$SOURCE_DATE'::date),
    '$BATCH_ID'::uuid,
    to_jsonb(stage.*) as raw_vessel_data,
    '$(basename "$INPUT_FILE")',
    ROW_NUMBER() OVER (ORDER BY vessel_name),
    md5(to_jsonb(stage.*)::text) as data_hash,
    CURRENT_DATE,
    TRUE
FROM ${RFMO}_raw_staging stage;
"

REPORTS_COUNT=$(execute_sql "
    SELECT COUNT(*) FROM intelligence_reports 
    WHERE import_batch_id = '$BATCH_ID'::uuid;
" "-t" | xargs)

log_success "Created $REPORTS_COUNT intelligence reports"

# =====================================================
# EXTRACT VESSEL INTELLIGENCE
# =====================================================

log_step "Extracting structured vessel intelligence..."

# Mark previous intelligence as not current if incremental
if [[ "$IS_INCREMENTAL" == "TRUE" ]]; then
    execute_sql "
        UPDATE vessel_intelligence vi
        SET is_current = FALSE,
            valid_to = CURRENT_DATE
        FROM intelligence_reports ir
        WHERE vi.report_id = ir.report_id
            AND ir.source_id = '$SOURCE_ID'::uuid
            AND vi.is_current = TRUE;"
fi

# Extract vessel intelligence (customize based on your data)
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
    reported_port_registry,
    reported_build_year,
    reported_owner_name,
    reported_operator_name,
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
    NULLIF(TRIM(ir.raw_vessel_data->>'rfmo_vessel_id'), ''),
    NULLIF(TRIM(ir.raw_vessel_data->>'vessel_type'), ''),
    NULLIF(TRIM(ir.raw_vessel_data->>'gear_type'), ''),
    CASE 
        WHEN ir.raw_vessel_data->>'length_value' ~ '^[0-9]+\.?[0-9]*$' 
        THEN (ir.raw_vessel_data->>'length_value')::numeric 
    END,
    CASE 
        WHEN ir.raw_vessel_data->>'gross_tonnage' ~ '^[0-9]+\.?[0-9]*$' 
        THEN (ir.raw_vessel_data->>'gross_tonnage')::numeric 
    END,
    NULLIF(TRIM(ir.raw_vessel_data->>'port_of_registry'), ''),
    CASE 
        WHEN ir.raw_vessel_data->>'build_year' ~ '^[0-9]{4}$' 
        THEN (ir.raw_vessel_data->>'build_year')::integer 
    END,
    NULLIF(TRIM(ir.raw_vessel_data->>'owner_name'), ''),
    NULLIF(TRIM(ir.raw_vessel_data->>'operator_name'), ''),
    COALESCE(
        NULLIF(TRIM(ir.raw_vessel_data->>'authorization_status'), ''),
        'ACTIVE'  -- Default if not specified
    ),
    -- Store any RFMO-specific fields
    ir.raw_vessel_data - ARRAY[
        'vessel_name', 'imo', 'ircs', 'mmsi', 'vessel_flag_alpha3',
        'vessel_type', 'gear_type', 'owner_name', 'operator_name'
    ],
    -- Calculate data completeness
    (
        CASE WHEN TRIM(ir.raw_vessel_data->>'vessel_name') IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN TRIM(ir.raw_vessel_data->>'imo') IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN TRIM(ir.raw_vessel_data->>'ircs') IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN TRIM(ir.raw_vessel_data->>'vessel_flag_alpha3') IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN TRIM(ir.raw_vessel_data->>'owner_name') IS NOT NULL THEN 1 ELSE 0 END
    )::decimal / 5.0,
    'AUTHORITATIVE'::source_authority_level,
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
# CHANGE DETECTION (IF INCREMENTAL)
# =====================================================

if [[ "$IS_INCREMENTAL" == "TRUE" ]]; then
    log_step "Detecting changes from previous import..."
    
    # Run change detection
    execute_sql "
    WITH changes AS (
        SELECT * FROM detect_vessel_changes('$BATCH_ID'::uuid, '$PREVIOUS_BATCH'::uuid)
    )
    INSERT INTO intelligence_change_log (
        source_id,
        current_batch_id,
        previous_batch_id,
        vessel_identifier,
        change_type,
        changed_fields,
        risk_indicators
    )
    SELECT 
        '$SOURCE_ID'::uuid,
        '$BATCH_ID'::uuid,
        '$PREVIOUS_BATCH'::uuid,
        vessel_identifier,
        change_type,
        changed_fields,
        jsonb_build_object('risk_score', risk_score)
    FROM changes;
    "
    
    # Update batch with change summary
    execute_sql "
    UPDATE intelligence_import_batches
    SET changes_detected = (
        SELECT jsonb_build_object(
            'new_vessels', COUNT(*) FILTER (WHERE change_type = 'NEW'),
            'updated_vessels', COUNT(*) FILTER (WHERE change_type = 'UPDATED'),
            'removed_vessels', COUNT(*) FILTER (WHERE change_type = 'REMOVED'),
            'high_risk_changes', COUNT(*) FILTER (WHERE risk_score >= 0.5)
        )
        FROM intelligence_change_log
        WHERE current_batch_id = '$BATCH_ID'::uuid
    )
    WHERE batch_id = '$BATCH_ID'::uuid;
    "
    
    # Display change summary
    execute_sql "
    SELECT 
        change_type,
        COUNT(*) as count,
        AVG(risk_score) as avg_risk
    FROM intelligence_change_log
    WHERE current_batch_id = '$BATCH_ID'::uuid
    GROUP BY change_type
    ORDER BY change_type;
    "
fi

# =====================================================
# UPDATE CONFIRMATIONS
# =====================================================

log_step "Updating vessel confirmations across sources..."

CONFIRMATION_RESULT=$(execute_sql "
    SELECT * FROM update_vessel_confirmations('$BATCH_ID'::uuid);
" "-t" | xargs)

log_success "Confirmation tracking updated:"
log_success "  New confirmations: $(echo $CONFIRMATION_RESULT | cut -d'|' -f1)"
log_success "  Updated confirmations: $(echo $CONFIRMATION_RESULT | cut -d'|' -f2)"

# Rebuild vessel identity confirmations
execute_sql "SELECT * FROM rebuild_vessel_confirmations();"

# =====================================================
# FINAL VALIDATION & STATISTICS
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

# Update lineage completion
execute_sql "
UPDATE data_lineage 
SET 
    records_imported = $REPORTS_COUNT,
    processing_status = 'COMPLETED',
    processing_completed_at = NOW(),
    completeness_percentage = ROUND(100.0 * $INTELLIGENCE_COUNT / NULLIF($INPUT_COUNT, 0), 1)
WHERE lineage_id = '$LINEAGE_ID'::uuid;
"

# =====================================================
# DISPLAY SUMMARY
# =====================================================

log_step "📊 $RFMO Intelligence Import Summary"

execute_sql "
WITH import_stats AS (
    SELECT 
        '$INPUT_COUNT'::integer as input_records,
        '$RAW_COUNT'::integer as staged_records,
        '$REPORTS_COUNT'::integer as report_records,
        '$INTELLIGENCE_COUNT'::integer as intelligence_records,
        '$DATA_QUALITY_SCORE'::numeric as quality_score
),
confirmation_stats AS (
    SELECT 
        COUNT(*) FILTER (WHERE '$SOURCE_ID'::uuid = ANY(confirming_sources)) as vessels_in_source,
        COUNT(*) FILTER (
            WHERE '$SOURCE_ID'::uuid = ANY(confirming_sources) 
            AND confirmation_count > 1
        ) as confirmed_elsewhere
    FROM vessel_identity_confirmations
),
risk_stats AS (
    SELECT 
        COUNT(*) FILTER (WHERE vi.authorization_status IN ('SANCTIONED', 'RESTRICTED')) as sanctioned,
        COUNT(*) FILTER (WHERE vi.data_completeness_score < 0.5) as low_quality
    FROM vessel_intelligence vi
    JOIN intelligence_reports ir ON vi.report_id = ir.report_id
    WHERE ir.import_batch_id = '$BATCH_ID'::uuid
)
SELECT 
    'Input File Records' as metric,
    input_records::text as value,
    '📁' as icon
FROM import_stats
UNION ALL
SELECT 
    'Intelligence Extracted',
    intelligence_records::text,
    CASE 
        WHEN intelligence_records = input_records THEN '✅'
        WHEN intelligence_records >= input_records * 0.95 THEN '⚠️'
        ELSE '❌'
    END
FROM import_stats
UNION ALL
SELECT 
    'Data Quality Score',
    quality_score::text || '%',
    CASE 
        WHEN quality_score >= 90 THEN '⭐'
        WHEN quality_score >= 70 THEN '👍'
        ELSE '⚠️'
    END
FROM import_stats
UNION ALL
SELECT 
    'Vessels Confirmed Elsewhere',
    confirmed_elsewhere || ' of ' || vessels_in_source || 
    ' (' || ROUND(100.0 * confirmed_elsewhere / NULLIF(vessels_in_source, 0), 1) || '%)',
    '🔗'
FROM confirmation_stats
UNION ALL
SELECT 
    'High Risk Vessels',
    sanctioned::text,
    CASE WHEN sanctioned > 0 THEN '⚠️' ELSE '✅' END
FROM risk_stats
UNION ALL
SELECT 
    'Low Data Quality',
    low_quality::text,
    CASE WHEN low_quality > 10 THEN '⚠️' ELSE '📊' END
FROM risk_stats;
"

# =====================================================
# CLEANUP
# =====================================================

execute_sql "DROP TABLE IF EXISTS ${RFMO}_raw_staging;"

# =====================================================
# FINAL SUCCESS MESSAGE
# =====================================================

if [[ $INTELLIGENCE_COUNT -eq $INPUT_COUNT ]]; then
    log_success "✅ $RFMO Enhanced Import Complete"
    log_success "   📊 $INTELLIGENCE_COUNT vessels imported with 0% data loss"
    log_success "   🔗 Cross-source confirmations updated"
    log_success "   📈 Change detection active for next import"
    log_success "   🏷️  Full data lineage tracked"
else
    LOSS_PCT=$(python3 -c "print(round((1 - $INTELLIGENCE_COUNT / $INPUT_COUNT) * 100, 2))")
    log_warning "⚠️  $RFMO import completed with data loss"
    log_warning "   Expected: $INPUT_COUNT records"
    log_warning "   Actual: $INTELLIGENCE_COUNT records"
    log_warning "   Loss: $LOSS_PCT%"
fi

exit 0