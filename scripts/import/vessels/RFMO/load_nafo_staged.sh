#!/bin/bash
# Staged Intelligence Import for NAFO
# Phase 1: Raw Intelligence Collection (NO vessel matching)
set -euo pipefail

source /app/scripts/core/logging.sh
source /app/scripts/core/env.sh

log_step "🎯 NAFO Staged Intelligence Import (Phase 1: Raw Collection)"

# Test database connection
if ! test_connection; then
    log_error "Cannot connect to database"
    exit 1
fi

# Configuration
RAW_DATA_DIR="/import/vessels/vessel_data/RFMO/raw"
INPUT_FILE=$(find "$RAW_DATA_DIR" -name "*NAFO*vessel*.csv" -o -name "*nafo*vessel*.csv" | head -1)

if [[ -z "$INPUT_FILE" ]]; then
    log_error "No NAFO vessel file found in $RAW_DATA_DIR"
    exit 1
fi

log_success "Found NAFO file: $(basename "$INPUT_FILE")"

# Count input records for validation (use Python to handle CSV with embedded newlines)
INPUT_COUNT=$(python3 -c "
import csv
with open('$INPUT_FILE', 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader)  # Skip header
    print(sum(1 for row in reader))
")
log_success "Input file has $INPUT_COUNT records"

# Get NAFO source ID
NAFO_SOURCE_ID=$(execute_sql "
    SELECT source_id FROM original_sources_vessels 
    WHERE source_shortname = 'NAFO' 
    LIMIT 1;
" "-t" | head -n1 | xargs)

if [[ -z "$NAFO_SOURCE_ID" ]]; then
    log_error "NAFO source not found"
    exit 1
fi

log_success "NAFO source ID: $NAFO_SOURCE_ID"

# Create import batch
BATCH_ID=$(execute_sql "
    INSERT INTO intelligence_import_batches (
        rfmo_shortname,
        import_date,
        source_file_path,
        raw_records_count
    ) VALUES (
        'NAFO',
        CURRENT_DATE,
        '$INPUT_FILE',
        $INPUT_COUNT
    ) RETURNING batch_id;
" "-t" | head -n1 | xargs)

log_success "Created import batch: $BATCH_ID"

# Create staging table for CSV import with actual NAFO columns
execute_sql "
DROP TABLE IF EXISTS nafo_raw_staging;
CREATE TABLE nafo_raw_staging (
    countryId TEXT,
    name TEXT,
    IRCS TEXT,
    IMO TEXT,
    originalSource TEXT,
    originalSourceType TEXT,
    sourceRefreshDate TEXT,
    sourceGoldfishUpdate TEXT
);
"

log_step "Loading NAFO raw data..."

# Load CSV data (handles UTF-8 BOM)
execute_sql "\\copy nafo_raw_staging FROM '$INPUT_FILE' WITH CSV HEADER ENCODING 'UTF8'"

RAW_COUNT=$(execute_sql "SELECT COUNT(*) FROM nafo_raw_staging;" "-t" | xargs)
log_success "Loaded $RAW_COUNT raw NAFO records"

# Validate no data loss at load stage
if [[ $RAW_COUNT -ne $INPUT_COUNT ]]; then
    log_error "Data loss detected! Input: $INPUT_COUNT, Loaded: $RAW_COUNT"
    exit 1
fi

# Extract source date from filename or use refresh date
SOURCE_DATE=$(echo "$(basename "$INPUT_FILE")" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' || date '+%Y-%m-%d')

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
    '$NAFO_SOURCE_ID'::uuid,
    'NAFO',
    COALESCE(
        CASE 
            WHEN sourceRefreshDate ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2}$' 
            THEN to_date(sourceRefreshDate, 'MM/DD/YY')
            ELSE '$SOURCE_DATE'::date
        END,
        CURRENT_DATE
    ),
    '$BATCH_ID'::uuid,
    to_jsonb(stage.*) as raw_vessel_data,
    '$(basename "$INPUT_FILE")',
    ROW_NUMBER() OVER (ORDER BY name),
    md5(to_jsonb(stage.*)::text) as data_hash
FROM nafo_raw_staging stage;
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
    reported_flag,
    authorization_status,
    rfmo_specific_data,
    data_completeness_score,
    source_authority_level
)
SELECT 
    ir.report_id,
    NULLIF(TRIM(ir.raw_vessel_data->>'name'), ''),
    CASE 
        WHEN ir.raw_vessel_data->>'IMO' ~ '^[0-9]{7}$' 
            AND ir.raw_vessel_data->>'IMO' NOT IN ('0000000', '1111111', '9999999')
        THEN ir.raw_vessel_data->>'IMO'
        ELSE NULL
    END,
    NULLIF(TRIM(ir.raw_vessel_data->>'IRCS'), ''),
    UPPER(NULLIF(TRIM(ir.raw_vessel_data->>'countryId'), '')),
    'ACTIVE',  -- NAFO vessels assumed active if in register
    jsonb_build_object(
        'countryId', ir.raw_vessel_data->>'countryId',
        'originalSource', ir.raw_vessel_data->>'originalSource',
        'originalSourceType', ir.raw_vessel_data->>'originalSourceType',
        'sourceRefreshDate', ir.raw_vessel_data->>'sourceRefreshDate',
        'sourceGoldfishUpdate', ir.raw_vessel_data->>'sourceGoldfishUpdate'
    ),
    -- Calculate data completeness score (limited fields available)
    (
        CASE WHEN TRIM(ir.raw_vessel_data->>'name') IS NOT NULL AND TRIM(ir.raw_vessel_data->>'name') != '' THEN 1 ELSE 0 END +
        CASE WHEN TRIM(ir.raw_vessel_data->>'IMO') IS NOT NULL AND TRIM(ir.raw_vessel_data->>'IMO') != '' THEN 1 ELSE 0 END +
        CASE WHEN TRIM(ir.raw_vessel_data->>'IRCS') IS NOT NULL AND TRIM(ir.raw_vessel_data->>'IRCS') != '' THEN 1 ELSE 0 END +
        CASE WHEN TRIM(ir.raw_vessel_data->>'countryId') IS NOT NULL AND TRIM(ir.raw_vessel_data->>'countryId') != '' THEN 1 ELSE 0 END
    )::decimal / 4.0,
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
log_step "📊 NAFO Intelligence Summary & Validation"

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
    'Average Data Completeness',
    ROUND(AVG(vi.data_completeness_score) * 100, 1)::text || '%',
    '📊'
FROM vessel_intelligence vi
JOIN intelligence_reports ir ON vi.report_id = ir.report_id
WHERE ir.import_batch_id = '$BATCH_ID'::uuid

UNION ALL

SELECT 
    'Unique Countries',
    COUNT(DISTINCT vi.reported_flag)::text,
    '📊'
FROM vessel_intelligence vi
JOIN intelligence_reports ir ON vi.report_id = ir.report_id
WHERE ir.import_batch_id = '$BATCH_ID'::uuid AND vi.reported_flag IS NOT NULL;
"

# Final success message
if [[ $INTELLIGENCE_COUNT -eq $INPUT_COUNT ]]; then
    log_success "✅ NAFO Phase 1 Complete: Raw Intelligence Collection"
    log_success "   📊 $INTELLIGENCE_COUNT intelligence records collected"
    log_success "   🎯 0% data loss - PERFECT IMPORT!"
    log_success "   ⏭️  Ready for Phase 2: Cross-Source Identity Resolution"
    log_success "   📝 Note: NAFO data has limited fields (name, IMO, IRCS, flag only)"
else
    log_error "❌ NAFO import validation FAILED"
    log_error "   Expected: $INPUT_COUNT records"
    log_error "   Actual: $INTELLIGENCE_COUNT records"
    exit 1
fi