#!/bin/bash
# Staged Intelligence Import for FFA
# Phase 1: Raw Intelligence Collection (NO vessel matching)
set -euo pipefail

source /app/scripts/core/logging.sh
source /app/scripts/core/env.sh

log_step "🎯 FFA Staged Intelligence Import (Phase 1: Raw Collection)"

# Test database connection
if ! test_connection; then
    log_error "Cannot connect to database"
    exit 1
fi

# Configuration
CLEANED_DATA_DIR="/import/vessels/vessel_data/RFMO/cleaned"
INPUT_FILE="$CLEANED_DATA_DIR/ffa_vessels_cleaned.csv"

# Verify cleaned file exists
if [[ ! -f "$INPUT_FILE" ]]; then
    log_error "Cleaned FFA data not found: $INPUT_FILE"
    exit 1
fi

# Count input records for validation (use Python to handle CSV with embedded newlines)
INPUT_COUNT=$(python3 -c "
import csv
with open('$INPUT_FILE', 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader)  # Skip header
    print(sum(1 for row in reader))
")
log_success "Input file has $INPUT_COUNT records"

# Get FFA source ID
FFA_SOURCE_ID=$(execute_sql "
    SELECT source_id FROM original_sources_vessels 
    WHERE source_shortname = 'FFA' 
    LIMIT 1;
" "-t" | head -n1 | xargs)

if [[ -z "$FFA_SOURCE_ID" ]]; then
    log_error "FFA source not found"
    exit 1
fi

log_success "FFA source ID: $FFA_SOURCE_ID"

# Create import batch
BATCH_ID=$(execute_sql "
    INSERT INTO intelligence_import_batches (
        rfmo_shortname,
        import_date,
        source_file_path,
        raw_records_count
    ) VALUES (
        'FFA',
        CURRENT_DATE,
        '$INPUT_FILE',
        $INPUT_COUNT
    ) RETURNING batch_id;
" "-t" | head -n1 | xargs)

log_success "Created import batch: $BATCH_ID"

# Create staging table for CSV import
execute_sql "
DROP TABLE IF EXISTS ffa_raw_staging;
CREATE TABLE ffa_raw_staging (
    source_date TEXT,
    original_source TEXT,
    vessel_name TEXT,
    imo TEXT,
    ircs TEXT,
    mmsi TEXT,
    national_registry TEXT,
    vessel_flag_alpha3 TEXT,
    wcpfc_id TEXT,
    ffa_id TEXT,
    vessel_type TEXT,
    port_of_registry TEXT,
    hull_material TEXT,
    build_location TEXT,
    build_year TEXT,
    length_value TEXT,
    length_metric_type TEXT,
    length_unit TEXT,
    gross_tonnage TEXT,
    tonnage_metric_type TEXT,
    rated_speed TEXT,
    engine_power TEXT,
    engine_power_unit TEXT,
    fish_hold_capacity TEXT,
    fish_hold_capacity_unit TEXT,
    fuel_capacity TEXT,
    owner_name TEXT,
    owner_address TEXT,
    operator_name TEXT,
    operator_address TEXT,
    registrant TEXT,
    registrant_role TEXT,
    registration_start TEXT,
    registration_period TEXT,
    current_status TEXT,
    application_type TEXT,
    vessel_master_name TEXT,
    vessel_master_nationality TEXT,
    fishing_master_name TEXT,
    fishing_master_nationality TEXT,
    crew_complement TEXT,
    previous_names TEXT,
    previous_flag TEXT,
    previous_ircs TEXT,
    previous_registry TEXT,
    side_photo_url TEXT,
    plan_photo_url TEXT
);
"

log_step "Loading FFA raw data..."

# Load CSV data
execute_sql "\\copy ffa_raw_staging FROM '$INPUT_FILE' WITH CSV HEADER"

RAW_COUNT=$(execute_sql "SELECT COUNT(*) FROM ffa_raw_staging;" "-t" | xargs)
log_success "Loaded $RAW_COUNT raw FFA records"

# Validate no data loss at load stage
if [[ $RAW_COUNT -ne $INPUT_COUNT ]]; then
    log_error "Data loss detected! Input: $INPUT_COUNT, Loaded: $RAW_COUNT"
    exit 1
fi

# Convert to raw intelligence reports (PRESERVE EVERYTHING)
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
    '$FFA_SOURCE_ID'::uuid,
    'FFA',
    COALESCE(source_date::date, CURRENT_DATE),
    '$BATCH_ID'::uuid,
    to_jsonb(stage.*) as raw_vessel_data,
    '$(basename "$INPUT_FILE")',
    ROW_NUMBER() OVER (ORDER BY vessel_name),
    md5(to_jsonb(stage.*)::text) as data_hash
FROM ffa_raw_staging stage;
"

REPORTS_COUNT=$(execute_sql "
    SELECT COUNT(*) FROM intelligence_reports 
    WHERE import_batch_id = '$BATCH_ID'::uuid;
" "-t" | xargs)

log_success "Created $REPORTS_COUNT intelligence reports"

# Validate no data loss at report stage
if [[ $REPORTS_COUNT -ne $RAW_COUNT ]]; then
    log_error "Data loss detected! Raw: $RAW_COUNT, Reports: $REPORTS_COUNT"
    exit 1
fi

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
    reported_length,
    reported_tonnage,
    reported_port_registry,
    reported_build_year,
    reported_owner_name,
    reported_owner_address,
    reported_operator_name,
    reported_operator_address,
    authorization_status,
    authorization_from,
    authorization_to,
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
    NULLIF(TRIM(ir.raw_vessel_data->>'ffa_id'), ''),
    NULLIF(TRIM(ir.raw_vessel_data->>'wcpfc_id'), ''),
    NULLIF(TRIM(ir.raw_vessel_data->>'vessel_type'), ''),
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
    NULLIF(TRIM(ir.raw_vessel_data->>'owner_address'), ''),
    NULLIF(TRIM(ir.raw_vessel_data->>'operator_name'), ''),
    NULLIF(TRIM(ir.raw_vessel_data->>'operator_address'), ''),
    UPPER(NULLIF(TRIM(ir.raw_vessel_data->>'current_status'), '')),
    CASE 
        WHEN ir.raw_vessel_data->>'registration_start' ~ '^\d{4}-\d{2}-\d{2}$'
        THEN (ir.raw_vessel_data->>'registration_start')::date
    END,
    CASE 
        WHEN ir.raw_vessel_data->>'registration_period' ~ '^\d{4}-\d{2}-\d{2}$'
        THEN (ir.raw_vessel_data->>'registration_period')::date
    END,
    jsonb_build_object(
        'national_registry', ir.raw_vessel_data->>'national_registry',
        'hull_material', ir.raw_vessel_data->>'hull_material',
        'build_location', ir.raw_vessel_data->>'build_location',
        'length_metric_type', ir.raw_vessel_data->>'length_metric_type',
        'length_unit', ir.raw_vessel_data->>'length_unit',
        'tonnage_metric_type', ir.raw_vessel_data->>'tonnage_metric_type',
        'rated_speed', ir.raw_vessel_data->>'rated_speed',
        'engine_power', ir.raw_vessel_data->>'engine_power',
        'engine_power_unit', ir.raw_vessel_data->>'engine_power_unit',
        'fish_hold_capacity', ir.raw_vessel_data->>'fish_hold_capacity',
        'fish_hold_capacity_unit', ir.raw_vessel_data->>'fish_hold_capacity_unit',
        'fuel_capacity', ir.raw_vessel_data->>'fuel_capacity',
        'registrant', ir.raw_vessel_data->>'registrant',
        'registrant_role', ir.raw_vessel_data->>'registrant_role',
        'application_type', ir.raw_vessel_data->>'application_type',
        'vessel_master_name', ir.raw_vessel_data->>'vessel_master_name',
        'vessel_master_nationality', ir.raw_vessel_data->>'vessel_master_nationality',
        'fishing_master_name', ir.raw_vessel_data->>'fishing_master_name',
        'fishing_master_nationality', ir.raw_vessel_data->>'fishing_master_nationality',
        'crew_complement', ir.raw_vessel_data->>'crew_complement',
        'previous_names', ir.raw_vessel_data->>'previous_names',
        'previous_flag', ir.raw_vessel_data->>'previous_flag',
        'previous_ircs', ir.raw_vessel_data->>'previous_ircs',
        'previous_registry', ir.raw_vessel_data->>'previous_registry',
        'side_photo_url', ir.raw_vessel_data->>'side_photo_url',
        'plan_photo_url', ir.raw_vessel_data->>'plan_photo_url'
    ),
    -- Calculate data completeness score
    (
        CASE WHEN TRIM(ir.raw_vessel_data->>'vessel_name') IS NOT NULL AND TRIM(ir.raw_vessel_data->>'vessel_name') != '' THEN 1 ELSE 0 END +
        CASE WHEN TRIM(ir.raw_vessel_data->>'imo') IS NOT NULL AND TRIM(ir.raw_vessel_data->>'imo') != '' THEN 1 ELSE 0 END +
        CASE WHEN TRIM(ir.raw_vessel_data->>'ircs') IS NOT NULL AND TRIM(ir.raw_vessel_data->>'ircs') != '' THEN 1 ELSE 0 END +
        CASE WHEN TRIM(ir.raw_vessel_data->>'vessel_flag_alpha3') IS NOT NULL AND TRIM(ir.raw_vessel_data->>'vessel_flag_alpha3') != '' THEN 1 ELSE 0 END +
        CASE WHEN TRIM(ir.raw_vessel_data->>'ffa_id') IS NOT NULL AND TRIM(ir.raw_vessel_data->>'ffa_id') != '' THEN 1 ELSE 0 END +
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

# Final validation - 100% data preservation
if [[ $INTELLIGENCE_COUNT -ne $INPUT_COUNT ]]; then
    log_error "Data loss detected! Input: $INPUT_COUNT, Intelligence: $INTELLIGENCE_COUNT"
    exit 1
fi

# Update batch statistics
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

# Show intelligence summary and validation
log_step "📊 FFA Intelligence Summary & Validation"

execute_sql "
SELECT 
    'Input File Records' as metric,
    '$INPUT_COUNT'::text as value,
    '✅' as status
    
UNION ALL

SELECT 
    'Raw Records Loaded',
    '$RAW_COUNT'::text,
    CASE WHEN $RAW_COUNT = $INPUT_COUNT THEN '✅' ELSE '❌' END
    
UNION ALL

SELECT 
    'Intelligence Reports Created',
    '$REPORTS_COUNT'::text,
    CASE WHEN $REPORTS_COUNT = $INPUT_COUNT THEN '✅' ELSE '❌' END
    
UNION ALL

SELECT 
    'Vessel Intelligence Extracted',
    '$INTELLIGENCE_COUNT'::text,
    CASE WHEN $INTELLIGENCE_COUNT = $INPUT_COUNT THEN '✅' ELSE '❌' END
    
UNION ALL

SELECT 
    'Data Loss Percentage',
    ROUND((($INPUT_COUNT - $INTELLIGENCE_COUNT)::numeric / $INPUT_COUNT) * 100, 2)::text || '%',
    CASE WHEN $INTELLIGENCE_COUNT = $INPUT_COUNT THEN '✅' ELSE '❌' END

UNION ALL

SELECT 
    'Vessels with IMO',
    COUNT(DISTINCT vi.intelligence_id)::text,
    '📊'
FROM vessel_intelligence vi
JOIN intelligence_reports ir ON vi.report_id = ir.report_id
WHERE ir.import_batch_id = '$BATCH_ID'::uuid AND vi.reported_imo IS NOT NULL

UNION ALL

SELECT 
    'Vessels with IRCS',
    COUNT(DISTINCT vi.intelligence_id)::text,
    '📊'
FROM vessel_intelligence vi
JOIN intelligence_reports ir ON vi.report_id = ir.report_id
WHERE ir.import_batch_id = '$BATCH_ID'::uuid AND vi.reported_ircs IS NOT NULL

UNION ALL

SELECT 
    'Vessels with FFA ID',
    COUNT(DISTINCT vi.intelligence_id)::text,
    '📊'
FROM vessel_intelligence vi
JOIN intelligence_reports ir ON vi.report_id = ir.report_id
WHERE ir.import_batch_id = '$BATCH_ID'::uuid AND vi.rfmo_vessel_id IS NOT NULL

UNION ALL

SELECT 
    'Vessels with WCPFC ID',
    COUNT(DISTINCT vi.intelligence_id)::text,
    '📊'
FROM vessel_intelligence vi
JOIN intelligence_reports ir ON vi.report_id = ir.report_id
WHERE ir.import_batch_id = '$BATCH_ID'::uuid AND vi.rfmo_vessel_number IS NOT NULL

UNION ALL

SELECT 
    'Average Data Completeness',
    ROUND(AVG(vi.data_completeness_score) * 100, 1)::text || '%',
    '📊'
FROM vessel_intelligence vi
JOIN intelligence_reports ir ON vi.report_id = ir.report_id
WHERE ir.import_batch_id = '$BATCH_ID'::uuid;
"

# Final success message
if [[ $INTELLIGENCE_COUNT -eq $INPUT_COUNT ]]; then
    log_success "✅ FFA Phase 1 Complete: Raw Intelligence Collection"
    log_success "   📊 $INTELLIGENCE_COUNT intelligence records collected"
    log_success "   🎯 0% data loss - PERFECT IMPORT!"
    log_success "   ⏭️  Ready for Phase 2: Cross-Source Identity Resolution"
    log_success "   📝 Note: FFA data overlaps with WCPFC - this provides valuable confirmation"
else
    log_error "❌ FFA import validation FAILED"
    log_error "   Expected: $INPUT_COUNT records"
    log_error "   Actual: $INTELLIGENCE_COUNT records"
    exit 1
fi