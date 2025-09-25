#!/bin/bash
# Import script for Chile LTP-PEP (Tradeable Fishing Licenses and Extraordinary Permits)
# This is a special registry type, not a regional registry
set -euo pipefail

# Source the environment and logging functions
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source /app/scripts/core/logging.sh
source /app/scripts/core/env.sh

# =====================================================
# CONFIGURATION
# =====================================================
SOURCE_NAME="CHL_LTP_PEP"
# Default to the most recent file in the raw directory
RAW_DIR="/import/vessels/vessel_data/COUNTRY/CHILE_LTP-PEP/raw"
if [[ -z "${INPUT_FILE:-}" ]]; then
    INPUT_FILE=$(ls -t "$RAW_DIR"/*.csv 2>/dev/null | head -n1)
    if [[ -z "$INPUT_FILE" ]]; then
        log_error "No CSV files found in $RAW_DIR"
        exit 1
    fi
fi

log_step "🇨🇱 Starting Chile LTP-PEP Vessel Registry Import"
log_success "This registry contains tradeable fishing licenses and extraordinary permits"

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

# Count total lines in file (skip first two rows)
TOTAL_LINES=$(wc -l < "$INPUT_FILE" | xargs)
RECORD_COUNT=$((TOTAL_LINES - 2))
log_success "Records to import: $RECORD_COUNT"

# =====================================================
# CREATE STAGING TABLE
# =====================================================

log_step "Creating staging table..."

# Skip first row (generic headers) and use second row as headers
# The CSV has Spanish headers with some containing newlines
# Create a simple staging table to capture all columns
execute_sql "
DROP TABLE IF EXISTS chl_ltp_pep_staging;
CREATE TABLE chl_ltp_pep_staging (
    id TEXT,                              -- ID
    principal_or_associated TEXT,         -- PRINCIPAL (P) O ASOCIADO (A)
    fishery_name TEXT,                    -- Nombre PESQUERIA
    license_type TEXT,                    -- TIPO (PEP/ LTP A/ LTP B/ ORP)
    capture_holder TEXT,                  -- TITULAR CAPTURA (quien inscribe la nave)
    registration_person_type TEXT,        -- Inscribe Nave Persona NAT O JUR
    vessel_name TEXT,                     -- NOMBRE NAVE
    holder_type TEXT,                     -- TIPO TITULAR (Industrial, Artesanal u Otro)
    vessel_registration_status TEXT,      -- TIPO NAVE (Inscrita en RPI, RPA o no inscrita)
    registration_number TEXT,             -- NÚMERO DE INSCRIPCIÓN
    authorized_fishery TEXT,              -- PESQUERIA AUTORIZADA A LA NAVE
    registration_start_date TEXT,         -- INICIO INSCRIPCIÓN
    registration_end_date TEXT,           -- TERMINO INSCRIPCIÓN
    cancellation_type TEXT,               -- TIPO CANCELACIÓN
    original_holder TEXT,                 -- TITULAR ORIGEN (Propietario del LTP o PEP)
    administrative_act_type TEXT,         -- Tipo de Acto administrativo
    administrative_act_number TEXT,       -- N° Acto administrativo
    administrative_act_date TEXT,         -- FECHA promulgación acto administrativo
    validity_start TEXT,                  -- DESDE
    validity_end TEXT,                    -- HASTA
    contract_start TEXT,                  -- INICIO CONTRATO
    contract_end TEXT,                    -- TERMINO VIGENCIA CONTRATO
    t_55_indicator TEXT,                  -- 55T (señalar T)
    observations TEXT,                    -- OBSERVACIONES
    fishing_gear_type TEXT                -- ARTE O APAREJO DE PESCA
);
"

# =====================================================
# LOAD DATA TO STAGING
# =====================================================

log_step "Loading data to staging..."

# Skip first two rows (generic header + actual headers) and load data
# First create a temporary file without the first two rows
TEMP_FILE="/tmp/chl_ltp_pep_clean.csv"
tail -n +3 "$INPUT_FILE" > "$TEMP_FILE"

# Use COPY without header since we already skipped it
execute_sql "\\copy chl_ltp_pep_staging FROM '$TEMP_FILE' WITH CSV"

# Clean up temp file
rm -f "$TEMP_FILE"

# Verify load
STAGED_COUNT=$(execute_sql "SELECT COUNT(*) FROM chl_ltp_pep_staging WHERE vessel_name IS NOT NULL AND vessel_name != '';" "-t" | xargs)

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
    '2025-09-08'::date, -- From filename
    '$BATCH_ID'::uuid,
    jsonb_strip_nulls(jsonb_build_object(
        'vessel_name', NULLIF(vessel_name, ''),
        'registration_number', NULLIF(registration_number, ''),
        'license_type', NULLIF(license_type, ''),
        'fishery_name', NULLIF(fishery_name, ''),
        'capture_holder', NULLIF(capture_holder, ''),
        'registration_person_type', NULLIF(registration_person_type, ''),
        'holder_type', NULLIF(holder_type, ''),
        'vessel_registration_status', NULLIF(vessel_registration_status, ''),
        'authorized_fishery', NULLIF(authorized_fishery, ''),
        'fishing_gear_type', NULLIF(fishing_gear_type, ''),
        'original_holder', NULLIF(original_holder, ''),
        'administrative_details', jsonb_build_object(
            'act_type', NULLIF(administrative_act_type, ''),
            'act_number', NULLIF(administrative_act_number, ''),
            'act_date', NULLIF(administrative_act_date, ''),
            'validity_start', NULLIF(validity_start, ''),
            'validity_end', NULLIF(validity_end, '')
        ),
        'registration_details', jsonb_build_object(
            'start_date', NULLIF(registration_start_date, ''),
            'end_date', NULLIF(registration_end_date, ''),
            'cancellation_type', NULLIF(cancellation_type, '')
        ),
        'contract_details', jsonb_build_object(
            'start', NULLIF(contract_start, ''),
            'end', NULLIF(contract_end, '')
        ),
        'observations', NULLIF(observations, ''),
        'vessel_flag_alpha3', 'CHL',
        'original_source', '$SOURCE_NAME'
    )),
    '$(basename "$INPUT_FILE")',
    ROW_NUMBER() OVER (ORDER BY vessel_name, registration_number),
    md5(COALESCE(vessel_name, '') || COALESCE(registration_number, '') || COALESCE(capture_holder, '')),
    CURRENT_DATE,
    TRUE
FROM chl_ltp_pep_staging
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
    'CHL',
    NULLIF(TRIM(ir.raw_vessel_data->>'registration_number'), ''),
    NULL, -- No length data in this dataset
    NULL, -- No tonnage data in this dataset
    NULL, -- No build year in this dataset
    NULL, -- No port registry in this dataset
    NULLIF(TRIM(ir.raw_vessel_data->>'capture_holder'), ''),
    jsonb_build_object(
        'license_type', ir.raw_vessel_data->>'license_type',
        'fishery_name', ir.raw_vessel_data->>'fishery_name',
        'holder_type', ir.raw_vessel_data->>'holder_type',
        'vessel_registration_status', ir.raw_vessel_data->>'vessel_registration_status',
        'authorized_fishery', ir.raw_vessel_data->>'authorized_fishery',
        'fishing_gear_type', ir.raw_vessel_data->>'fishing_gear_type',
        'original_holder', ir.raw_vessel_data->>'original_holder',
        'administrative_details', ir.raw_vessel_data->'administrative_details',
        'registration_details', ir.raw_vessel_data->'registration_details',
        'contract_details', ir.raw_vessel_data->'contract_details',
        'observations', ir.raw_vessel_data->>'observations'
    ),
    -- Calculate data completeness based on available fields
    (
        CASE WHEN ir.raw_vessel_data->>'vessel_name' IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN ir.raw_vessel_data->>'registration_number' IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN ir.raw_vessel_data->>'license_type' IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN ir.raw_vessel_data->>'capture_holder' IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN ir.raw_vessel_data->>'fishery_name' IS NOT NULL THEN 1 ELSE 0 END
    )::decimal / 5.0,
    'AUTHORITATIVE'::source_authority_level, -- Government fishing license registry
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

execute_sql "DROP TABLE IF EXISTS chl_ltp_pep_staging;"

# =====================================================
# SUMMARY
# =====================================================

log_step "📊 Import Summary"
log_success "✅ Chile LTP-PEP Vessel Registry Import Complete"
log_success "   Source: Chilean Tradeable Fishing Licenses & Extraordinary Permits"
log_success "   Records: $INTELLIGENCE_COUNT vessels"
log_success "   Quality: Authoritative government source"
log_success "   Coverage: Licensed fishing vessels with special permits"

# Show license type distribution
log_step "License Type Distribution:"
execute_sql "
WITH license_counts AS (
    SELECT 
        ir.raw_vessel_data->>'license_type' as license_type,
        COUNT(*) as vessel_count
    FROM intelligence_reports ir
    WHERE ir.import_batch_id = '$BATCH_ID'::uuid
    GROUP BY ir.raw_vessel_data->>'license_type'
)
SELECT * FROM license_counts ORDER BY vessel_count DESC;
"

exit 0