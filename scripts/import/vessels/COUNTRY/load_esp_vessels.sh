#!/usr/bin/env bash
set -euo pipefail

# EU Spain (ESP) vessels import script
# Loads official Spain vessel registry data

# Configuration
SOURCE_NAME="EU_ESP"
SOURCE_TYPE="COUNTRY"
SOURCE_COUNTRY="ES"  # Spain
DATA_FILE="/import/vessels/vessel_data/COUNTRY/EU_ESP/cleaned/ESP_vessels_cleaned.csv"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║              EU Spain (ESP) Vessel Import Script             ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════════╝${NC}"
echo

# Database connection using environment variables - all required
: "${POSTGRES_HOST:?Error: POSTGRES_HOST not set}"
: "${POSTGRES_PORT:?Error: POSTGRES_PORT not set}"
: "${POSTGRES_DB:?Error: POSTGRES_DB not set}"
: "${POSTGRES_USER:?Error: POSTGRES_USER not set}"
: "${POSTGRES_PASSWORD:?Error: POSTGRES_PASSWORD not set}"

export PGPASSWORD=$POSTGRES_PASSWORD

# Function to execute SQL
execute_sql() {
    local sql=$1
    local options=${2:-}
    psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB $options -c "$sql"
}

# Function to get file hash
get_file_hash() {
    local file=$1
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$file" | cut -d' ' -f1
    else
        shasum -a 256 "$file" | cut -d' ' -f1
    fi
}

# Check if file exists
if [ ! -f "$DATA_FILE" ]; then
    echo -e "${RED}✗ Error: Data file not found: $DATA_FILE${NC}"
    exit 1
fi

# Get file hash for deduplication
FILE_HASH=$(get_file_hash "$DATA_FILE")
echo -e "${BLUE}📄 File hash: ${FILE_HASH:0:16}...${NC}"

# Check if this file has already been imported
echo -e "${YELLOW}🔍 Checking if file has already been imported...${NC}"

EXISTING_IMPORT=$(execute_sql "
    SELECT COUNT(*) 
    FROM data_lineage 
    WHERE source_file_hash = '$FILE_HASH'
        AND source_id = (
            SELECT source_id FROM original_sources_vessels 
            WHERE source_shortname = '$SOURCE_NAME' LIMIT 1
        );
" "-t" | xargs)

if [ "$EXISTING_IMPORT" -gt 0 ]; then
    echo -e "${YELLOW}⚠️  This file has already been imported. Skipping to avoid duplicates.${NC}"
    
    # Show import stats
    echo -e "${BLUE}📊 Previous import statistics:${NC}"
    execute_sql "
        SELECT 
            COUNT(DISTINCT ir.vessel_key_hash) as vessels_imported,
            MIN(dl.import_timestamp) as first_imported,
            MAX(dl.import_timestamp) as last_imported
        FROM data_lineage dl
        JOIN intelligence_reports ir ON dl.report_id = ir.report_id
        WHERE dl.source_file_hash = '$FILE_HASH'
            AND dl.source_id = (
                SELECT source_id FROM original_sources_vessels 
                WHERE source_shortname = '$SOURCE_NAME' LIMIT 1
            );
    "
    exit 0
fi

# Get or create source record
echo -e "${YELLOW}📋 Setting up source record...${NC}"

SOURCE_ID=$(execute_sql "
    WITH src AS (
        INSERT INTO original_sources_vessels (
            source_shortname, source_fullname, source_types,
            source_urls, update_frequency, authority_level, 
            data_quality_score, last_updated
        ) VALUES (
            '$SOURCE_NAME',
            'European Union Fleet Register - Spain',
            ARRAY['$SOURCE_TYPE'],
            jsonb_build_object(
                'main_url', 'https://webgate.ec.europa.eu/fleet-europa/index_en',
                'download_date', CURRENT_DATE
            ),
            'MONTHLY',
            'AUTHORITATIVE',
            0.85,
            CURRENT_DATE
        )
        ON CONFLICT (source_shortname) 
        DO UPDATE SET 
            last_updated = CURRENT_DATE,
            source_urls = EXCLUDED.source_urls
        RETURNING source_id
    )
    SELECT source_id FROM src;
" "-t" | xargs)

echo -e "${GREEN}✓ Source configured: $SOURCE_ID${NC}"

# Create import batch
echo -e "${YELLOW}📦 Creating import batch...${NC}"

BATCH_ID=$(execute_sql "SELECT gen_random_uuid()::TEXT;" "-t" | xargs)
IMPORT_TIMESTAMP=$(date -u +"%Y-%m-%d %H:%M:%S")

# Count records
TOTAL_RECORDS=$(wc -l < "$DATA_FILE")
TOTAL_RECORDS=$((TOTAL_RECORDS - 1))  # Subtract header

echo -e "${BLUE}📊 Processing $TOTAL_RECORDS vessels from Spain registry...${NC}"

# Import vessels
echo -e "${YELLOW}🚢 Importing vessels...${NC}"

# Create temporary staging table (not TEMP, use regular table)
execute_sql "
DROP TABLE IF EXISTS staging_esp_vessels;

CREATE TABLE staging_esp_vessels (
    country_code TEXT,
    national_registry_id TEXT,
    imo TEXT,
    event_type TEXT,
    event_start_date DATE,
    event_end_date DATE,
    registration_number TEXT,
    external_marking TEXT,
    vessel_name TEXT,
    port_code TEXT,
    port_name TEXT,
    ircs TEXT,
    vms_indicator TEXT,
    ers_indicator TEXT,
    ais_indicator TEXT,
    fishing_license TEXT,
    license_in_rfmo TEXT,
    third_country_license TEXT,
    mmsi TEXT,
    main_fishing_gear TEXT,
    subsidiary_gear_1 TEXT,
    subsidiary_gear_2 TEXT,
    subsidiary_gear_3 TEXT,
    subsidiary_gear_4 TEXT,
    subsidiary_gear_5 TEXT,
    loa NUMERIC,
    lbp NUMERIC,
    tonnage_gt NUMERIC,
    tonnage_grt NUMERIC,
    tonnage_nt NUMERIC,
    tonnage_other NUMERIC,
    power_main NUMERIC,
    power_auxiliary NUMERIC,
    hull_material TEXT,
    date_entry_service DATE,
    segment TEXT,
    country_import_export TEXT,
    export_type TEXT,
    public_aid TEXT,
    year_construction DATE
);"

# Copy data
echo -e "${BLUE}📥 Loading data into staging table...${NC}"
# Use COPY with stdin to avoid permission issues
cat "$DATA_FILE" | psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB -c "COPY staging_esp_vessels FROM STDIN WITH (FORMAT CSV, HEADER true, DELIMITER ';', QUOTE '\"', ENCODING 'UTF8')"

# Process staging data into intelligence reports
echo -e "${YELLOW}🔄 Processing intelligence reports...${NC}"

execute_sql "
-- Insert intelligence reports with Phase 1 improvements
WITH vessel_intel AS (
    INSERT INTO intelligence_reports (
        source_id,
        rfmo_shortname,
        report_date,
        raw_data,
        raw_vessel_data,
        processed_data,
        validation_status,
        import_batch_id,
        import_timestamp,
        data_hash,
        trust_score,
        valid_from,
        valid_to
    )
    SELECT 
        '$SOURCE_ID'::UUID,
        '$SOURCE_NAME',
        CURRENT_DATE,
        jsonb_build_object(
            'country_code', country_code,
            'national_registry_id', national_registry_id,
            'imo', NULLIF(imo, ''),
            'event_type', event_type,
            'event_start_date', event_start_date,
            'event_end_date', event_end_date,
            'registration_number', registration_number,
            'external_marking', external_marking,
            'vessel_name', vessel_name,
            'port_code', port_code,
            'port_name', port_name,
            'ircs', NULLIF(ircs, ''),
            'vms_indicator', vms_indicator,
            'ers_indicator', ers_indicator,
            'ais_indicator', ais_indicator,
            'fishing_license', fishing_license,
            'license_in_rfmo', license_in_rfmo,
            'third_country_license', third_country_license,
            'mmsi', NULLIF(mmsi, ''),
            'main_fishing_gear', main_fishing_gear,
            'subsidiary_gears', ARRAY_REMOVE(ARRAY[
                subsidiary_gear_1, subsidiary_gear_2, subsidiary_gear_3,
                subsidiary_gear_4, subsidiary_gear_5
            ], NULL),
            'loa', loa,
            'lbp', lbp,
            'tonnage_gt', tonnage_gt,
            'tonnage_grt', tonnage_grt,
            'tonnage_nt', tonnage_nt,
            'tonnage_other', tonnage_other,
            'power_main', power_main,
            'power_auxiliary', power_auxiliary,
            'hull_material', hull_material,
            'year_construction', EXTRACT(YEAR FROM year_construction)::INTEGER,
            'date_entry_service', date_entry_service,
            'segment', segment,
            'country_import_export', country_import_export,
            'export_type', export_type,
            'public_aid', public_aid
        ),
        jsonb_build_object(
            'imo', NULLIF(imo, ''),
            'vessel_name', vessel_name,
            'vessel_flag_alpha3', '$SOURCE_COUNTRY',
            'ircs', NULLIF(ircs, ''),
            'mmsi', NULLIF(mmsi, ''),
            'external_marking', external_marking,
            'national_registry_id', national_registry_id,
            'loa', loa,
            'tonnage_gt', tonnage_gt,
            'power_main', power_main,
            'year_built', EXTRACT(YEAR FROM year_construction)::INTEGER
        ),
        jsonb_build_object(
            'processing_timestamp', CURRENT_TIMESTAMP,
            'processor_version', '2.0',
            'validation_rules_applied', ARRAY['required_fields', 'data_types', 'esp_specific']
        ),
        'VALIDATED',
        '$BATCH_ID'::UUID,
        '$IMPORT_TIMESTAMP'::TIMESTAMP,
        -- data_hash
        encode(
            digest(
                COALESCE(imo, '') || '|' ||
                COALESCE(vessel_name, '') || '|' ||
                COALESCE(ircs, '') || '|' ||
                COALESCE(mmsi, '') || '|' ||
                COALESCE(loa::TEXT, '') || '|' ||
                COALESCE(tonnage_gt::TEXT, '') || '|' ||
                COALESCE(power_main::TEXT, ''),
                'sha256'
            ),
            'hex'
        ),
        0.85,  -- Trust score for official registry
        COALESCE(event_start_date, CURRENT_DATE),
        COALESCE(event_end_date, '2100-12-31'::DATE)
    FROM staging_esp_vessels
    WHERE vessel_name IS NOT NULL
    RETURNING report_id, vessel_key_hash
)
-- Track data lineage separately
SELECT 1;"

# Insert data lineage record
execute_sql "
INSERT INTO data_lineage (
    source_id,
    source_file_hash,
    source_file_path,
    import_batch_id,
    import_timestamp,
    processor_version,
    processing_notes,
    records_in_file,
    records_imported
)
VALUES (
    '$SOURCE_ID'::UUID,
    '$FILE_HASH',
    '$DATA_FILE',
    '$BATCH_ID'::UUID,
    '$IMPORT_TIMESTAMP'::TIMESTAMP,
    '2.0',
    jsonb_build_object(
        'source_type', '$SOURCE_TYPE',
        'source_country', '$SOURCE_COUNTRY',
        'import_batch_id', '$BATCH_ID'
    ),
    $TOTAL_RECORDS,
    $TOTAL_RECORDS
)
ON CONFLICT (source_file_hash, source_id) DO NOTHING;"

# Update vessel_intelligence
echo -e "${YELLOW}🎯 Updating vessel intelligence...${NC}"

execute_sql "
-- Update vessel_intelligence with latest data
WITH vessel_updates AS (
    SELECT DISTINCT ON (ir.vessel_key_hash)
        ir.vessel_key_hash,
        ir.raw_vessel_data,
        ir.trust_score,
        ir.import_timestamp
    FROM intelligence_reports ir
    WHERE ir.source_id = '$SOURCE_ID'::UUID
        AND ir.import_batch_id = '$BATCH_ID'::UUID
    ORDER BY ir.vessel_key_hash, ir.import_timestamp DESC
)
INSERT INTO vessel_intelligence (
    vessel_key_hash,
    vessel_name,
    flag_state,
    imo_number,
    ircs,
    mmsi,
    external_marking,
    national_registry_id,
    reported_length,
    reported_tonnage,
    reported_engine_power,
    year_built,
    last_updated,
    trust_score,
    risk_score,
    data_completeness_score
)
SELECT 
    vu.vessel_key_hash,
    vu.raw_vessel_data->>'vessel_name',
    vu.raw_vessel_data->>'vessel_flag_alpha3',
    vu.raw_vessel_data->>'imo',
    vu.raw_vessel_data->>'ircs',
    vu.raw_vessel_data->>'mmsi',
    vu.raw_vessel_data->>'external_marking',
    vu.raw_vessel_data->>'national_registry_id',
    (vu.raw_vessel_data->>'loa')::NUMERIC,
    (vu.raw_vessel_data->>'tonnage_gt')::NUMERIC,
    (vu.raw_vessel_data->>'power_main')::NUMERIC,
    (vu.raw_vessel_data->>'year_built')::INTEGER,
    CURRENT_TIMESTAMP,
    vu.trust_score,
    0.0,  -- Risk score to be calculated separately
    -- Calculate data completeness
    (
        CASE WHEN vu.raw_vessel_data->>'imo' IS NOT NULL THEN 0.2 ELSE 0 END +
        CASE WHEN vu.raw_vessel_data->>'ircs' IS NOT NULL THEN 0.2 ELSE 0 END +
        CASE WHEN vu.raw_vessel_data->>'mmsi' IS NOT NULL THEN 0.2 ELSE 0 END +
        CASE WHEN vu.raw_vessel_data->>'loa' IS NOT NULL THEN 0.2 ELSE 0 END +
        CASE WHEN vu.raw_vessel_data->>'vessel_name' IS NOT NULL THEN 0.2 ELSE 0 END
    )::NUMERIC(3,2)
FROM vessel_updates vu
ON CONFLICT (vessel_key_hash) DO UPDATE SET
    vessel_name = EXCLUDED.vessel_name,
    flag_state = EXCLUDED.flag_state,
    imo_number = EXCLUDED.imo_number,
    ircs = EXCLUDED.ircs,
    mmsi = EXCLUDED.mmsi,
    external_marking = EXCLUDED.external_marking,
    national_registry_id = EXCLUDED.national_registry_id,
    reported_length = EXCLUDED.reported_length,
    reported_tonnage = EXCLUDED.reported_tonnage,
    reported_engine_power = EXCLUDED.reported_engine_power,
    year_built = EXCLUDED.year_built,
    last_updated = EXCLUDED.last_updated,
    trust_score = GREATEST(vessel_intelligence.trust_score, EXCLUDED.trust_score),
    data_completeness_score = GREATEST(vessel_intelligence.data_completeness_score, EXCLUDED.data_completeness_score);"

# Update cross-source confirmations
echo -e "${YELLOW}🔗 Updating cross-source confirmations...${NC}"

execute_sql "
-- Update vessel_data_confirmations for vessels seen in multiple sources
INSERT INTO vessel_data_confirmations (
    vessel_key_hash,
    field_name,
    field_value,
    source_ids,
    confirmation_count,
    first_confirmed,
    last_confirmed,
    confidence_score
)
SELECT 
    ir.vessel_key_hash,
    'vessel_identity',
    jsonb_build_object(
        'name', ir.raw_vessel_data->>'vessel_name',
        'flag', ir.raw_vessel_data->>'vessel_flag_alpha3',
        'imo', ir.raw_vessel_data->>'imo'
    )::TEXT,
    array_agg(DISTINCT ir.source_id),
    COUNT(DISTINCT ir.source_id),
    MIN(ir.import_timestamp),
    MAX(ir.import_timestamp),
    -- Higher confidence with more sources
    LEAST(1.0, COUNT(DISTINCT ir.source_id) * 0.2)::NUMERIC(3,2)
FROM intelligence_reports ir
WHERE ir.vessel_key_hash IN (
    SELECT vessel_key_hash 
    FROM intelligence_reports 
    WHERE import_batch_id = '$BATCH_ID'::UUID
)
GROUP BY ir.vessel_key_hash, ir.raw_vessel_data->>'vessel_name', 
         ir.raw_vessel_data->>'vessel_flag_alpha3', ir.raw_vessel_data->>'imo'
ON CONFLICT (vessel_key_hash, field_name, field_value) DO UPDATE SET
    source_ids = ARRAY(SELECT DISTINCT unnest(vessel_data_confirmations.source_ids || EXCLUDED.source_ids)),
    confirmation_count = array_length(ARRAY(SELECT DISTINCT unnest(vessel_data_confirmations.source_ids || EXCLUDED.source_ids)), 1),
    last_confirmed = GREATEST(vessel_data_confirmations.last_confirmed, EXCLUDED.last_confirmed),
    confidence_score = LEAST(1.0, array_length(ARRAY(SELECT DISTINCT unnest(vessel_data_confirmations.source_ids || EXCLUDED.source_ids)), 1) * 0.2)::NUMERIC(3,2);"

# Get import statistics
echo -e "${BLUE}📊 Import statistics:${NC}"

execute_sql "
WITH import_stats AS (
    SELECT 
        COUNT(DISTINCT ir.vessel_key_hash) as total_vessels,
        COUNT(*) as total_reports,
        COUNT(DISTINCT CASE WHEN ir.raw_vessel_data->>'imo' IS NOT NULL THEN ir.vessel_key_hash END) as vessels_with_imo,
        COUNT(DISTINCT CASE WHEN ir.raw_vessel_data->>'mmsi' IS NOT NULL THEN ir.vessel_key_hash END) as vessels_with_mmsi,
        COUNT(DISTINCT CASE WHEN ir.raw_vessel_data->>'ircs' IS NOT NULL THEN ir.vessel_key_hash END) as vessels_with_ircs,
        AVG(ir.trust_score)::NUMERIC(3,2) as avg_trust_score
    FROM intelligence_reports ir
    WHERE ir.import_batch_id = '$BATCH_ID'::UUID
),
confirmation_stats AS (
    SELECT 
        COUNT(DISTINCT vessel_key_hash) as confirmed_vessels,
        MAX(confirmation_count) as max_confirmations,
        AVG(confidence_score)::NUMERIC(3,2) as avg_confidence
    FROM vessel_data_confirmations
    WHERE vessel_key_hash IN (
        SELECT vessel_key_hash 
        FROM intelligence_reports 
        WHERE import_batch_id = '$BATCH_ID'::UUID
    )
)
SELECT 
    i.*,
    c.confirmed_vessels,
    c.max_confirmations,
    c.avg_confidence
FROM import_stats i
CROSS JOIN confirmation_stats c;"

echo -e "${GREEN}✅ Spain vessel import completed successfully!${NC}"

# Clean up
execute_sql "DROP TABLE IF EXISTS staging_esp_vessels;"

echo -e "${BLUE}🎉 Import process finished${NC}"