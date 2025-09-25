#!/bin/bash
# Import script for Chile regional vessel registries
# Handles any Chilean region code passed as parameter
set -euo pipefail

# Source the environment and logging functions
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source /app/scripts/core/logging.sh
source /app/scripts/core/env.sh
source "$SCRIPT_DIR/chile_common.sh"

# =====================================================
# CONFIGURATION
# =====================================================
# Get region code from parameter or environment
REGION_CODE="${1:-${CHILE_REGION:-}}"
if [[ -z "$REGION_CODE" ]]; then
    log_error "Chile region code required. Usage: $0 X"
    log_error "Valid codes: I II III IV V VI VII VIII IX X XI XII XIV XV XVI RM"
    exit 1
fi

# Validate region code
if [[ -z "${CHILE_REGIONS[$REGION_CODE]:-}" ]]; then
    log_error "Invalid region code: $REGION_CODE"
    log_error "Valid codes: ${!CHILE_REGIONS[*]}"
    exit 1
fi

REGION_NAME="${CHILE_REGIONS[$REGION_CODE]}"
SOURCE_NAME="CHILE_${REGION_CODE}"

# Default to the most recent file in the raw directory
RAW_DIR="/import/vessels/vessel_data/COUNTRY/CHILE_${REGION_CODE}/raw"
if [[ -z "${INPUT_FILE:-}" ]]; then
    INPUT_FILE=$(ls -t "$RAW_DIR"/*.csv 2>/dev/null | head -n1)
    if [[ -z "$INPUT_FILE" ]]; then
        log_error "No CSV files found in $RAW_DIR"
        exit 1
    fi
fi

log_step "🇨🇱 Starting Chile $REGION_NAME Vessel Registry Import"

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
create_chile_staging_table

# =====================================================
# LOAD DATA TO STAGING
# =====================================================

log_step "Loading data to staging..."

# Use COPY with CSV header
execute_sql "\\copy chile_vessels_staging FROM '$INPUT_FILE' WITH CSV HEADER"

# Verify load
STAGED_COUNT=$(execute_sql "SELECT COUNT(*) FROM chile_vessels_staging WHERE nombre IS NOT NULL AND nombre != '';" "-t" | xargs)

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
create_chile_intelligence_reports "$SOURCE_ID" "$SOURCE_NAME" "$BATCH_ID" "$INPUT_FILE" "$REGION_CODE" "$REGION_NAME"

REPORTS_COUNT=$(execute_sql "
    SELECT COUNT(*) FROM intelligence_reports 
    WHERE import_batch_id = '$BATCH_ID'::uuid;
" "-t" | xargs)

log_success "Created $REPORTS_COUNT intelligence reports"

# =====================================================
# EXTRACT VESSEL INTELLIGENCE
# =====================================================

log_step "Extracting vessel intelligence..."
extract_chile_vessel_intelligence "$BATCH_ID"

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

execute_sql "DROP TABLE IF EXISTS chile_vessels_staging;"

# =====================================================
# SUMMARY
# =====================================================

show_chile_import_summary "$BATCH_ID" "$INTELLIGENCE_COUNT" "$REGION_NAME" "$SOURCE_NAME"

exit 0