#!/bin/bash
# /app/scripts/import/vessels/data/RFMO/load_iotc_vessels.sh
# IOTC Vessel Data Loading Script - Environment-aware version with trust scoring
set -euo pipefail

source /app/scripts/core/logging.sh
source /app/scripts/core/env.sh

log_step "🐟 IOTC Vessel Data Loading"

# Test database connection
if ! test_connection; then
    log_error "Cannot connect to database"
    exit 1
fi

# Configuration
CLEANED_DATA_DIR="/import/vessels/vessel_data/RFMO/cleaned"
INPUT_FILE="$CLEANED_DATA_DIR/iotc_vessels_cleaned.csv"

# Verify cleaned file exists
if [[ ! -f "$INPUT_FILE" ]]; then
    log_error "Cleaned IOTC data not found: $INPUT_FILE"
    log_error "Please run clean_iotc_vessels.sh first"
    exit 1
fi

# Get IOTC source ID
IOTC_SOURCE_ID=$(execute_sql "
    SELECT source_id FROM original_sources_vessels 
    WHERE source_shortname = 'IOTC' 
    LIMIT 1;
" "-t -A" | xargs)

if [[ -z "$IOTC_SOURCE_ID" ]]; then
    log_error "IOTC source not found in original_sources_vessels table"
    exit 1
fi

log_success "Found IOTC source ID: $IOTC_SOURCE_ID"

# Create import run
IMPORT_RUN_ID=$(execute_sql "
    INSERT INTO import_runs (
        run_id,
        run_type,
        start_time,
        status,
        configuration
    ) VALUES (
        gen_random_uuid(),
        'VESSEL_IMPORT',
        NOW(),
        'RUNNING',
        jsonb_build_object(
            'source', 'IOTC',
            'source_id', '$IOTC_SOURCE_ID'::uuid,
            'file', '$(basename "$INPUT_FILE")'
        )
    ) RETURNING run_id;
" "-t" | head -n1 | xargs)

log_success "Created import run: $IMPORT_RUN_ID"

# Load IOTC setup functions
log_step "Setting up IOTC-specific functions"
execute_sql_file "/app/scripts/import/vessels/data/RFMO/setup_iotc_loading.sql" > /dev/null 2>&1 || true

# Load vessel matching and trust functions
log_step "Loading vessel matching infrastructure"
execute_sql_file "/app/scripts/import/vessels/vessel_matching_helpers.sql" > /dev/null 2>&1 || true
execute_sql_file "/app/scripts/import/vessels/vessel_trust_functions.sql" > /dev/null 2>&1 || true

# Create staging table
cat > /tmp/iotc_staging.sql << 'EOF'
-- Drop if exists
DROP TABLE IF EXISTS iotc_vessels_staging;

-- Create staging table matching cleaned CSV structure
CREATE TABLE iotc_vessels_staging (
    row_id SERIAL PRIMARY KEY,
    
    -- Source metadata
    source_date TEXT,
    original_source TEXT,
    
    -- Core vessel identifiers
    vessel_name TEXT,
    imo TEXT,
    ircs TEXT,
    mmsi TEXT,
    national_registry TEXT,
    vessel_flag_alpha3 TEXT,
    
    -- IOTC external identifiers
    iotc_number TEXT,
    
    -- Vessel characteristics
    vessel_type_code TEXT,
    gear_type_code TEXT,
    vessel_kind TEXT,
    range_code TEXT,
    port_code TEXT,
    port_name TEXT,
    
    -- Measurements
    length_value NUMERIC,
    length_metric_type TEXT,
    length_unit_enum TEXT,
    
    gt_value NUMERIC,
    grt_value NUMERIC,
    
    volume_value NUMERIC,
    volume_unit_enum TEXT,
    
    cc_value NUMERIC,
    cc_unit_enum TEXT,
    
    -- Ownership structure
    owner_name TEXT,
    owner_address TEXT,
    
    operator_name TEXT,
    operator_address TEXT,
    
    operating_company TEXT,
    operating_company_address TEXT,
    operating_company_reg_num TEXT,
    
    beneficial_owner TEXT,
    beneficial_owner_address TEXT,
    
    -- Authorization
    auth_from_date DATE,
    auth_to_date DATE,
    
    -- Metadata
    last_updated DATE,
    starboard_photo TEXT,
    portside_photo TEXT,
    bow_photo TEXT,
    
    -- Processing fields
    vessel_uuid UUID,
    is_new_vessel BOOLEAN DEFAULT FALSE,
    match_confidence vessel_match_confidence,
    confidence_score DECIMAL(3,2),
    trust_score DECIMAL(3,2),
    processing_status TEXT DEFAULT 'PENDING',
    processing_notes TEXT
);

-- Create indexes for performance
CREATE INDEX idx_iotc_staging_vessel_name ON iotc_vessels_staging(vessel_name);
CREATE INDEX idx_iotc_staging_imo ON iotc_vessels_staging(imo);
CREATE INDEX idx_iotc_staging_ircs ON iotc_vessels_staging(ircs);
CREATE INDEX idx_iotc_staging_iotc_number ON iotc_vessels_staging(iotc_number);
EOF

log_step "Creating IOTC staging table"
execute_sql_file "/tmp/iotc_staging.sql"
rm -f /tmp/iotc_staging.sql

# Load data into staging
log_step "Loading IOTC data into staging table"

# Use appropriate COPY method based on environment
if [[ "$ENVIRONMENT" == "production" ]]; then
    cat "$INPUT_FILE" | psql -h "$POSTGRES_HOST" \
         -p "$POSTGRES_PORT" \
         -U "$POSTGRES_USER" \
         -d "$POSTGRES_DB" \
         -c "COPY iotc_vessels_staging (
                source_date, original_source, vessel_name, imo, ircs, mmsi,
                national_registry, vessel_flag_alpha3, iotc_number,
                vessel_type_code, gear_type_code, vessel_kind, range_code,
                port_code, port_name, length_value, length_metric_type, length_unit_enum,
                gt_value, grt_value, volume_value, volume_unit_enum,
                cc_value, cc_unit_enum, owner_name, owner_address,
                operator_name, operator_address, operating_company, operating_company_address,
                operating_company_reg_num, beneficial_owner, beneficial_owner_address,
                auth_from_date, auth_to_date, last_updated,
                starboard_photo, portside_photo, bow_photo
            ) FROM STDIN WITH (FORMAT CSV, HEADER true);"
else
    # Local/Docker with single-line \copy
    COPY_CMD="\\copy iotc_vessels_staging (source_date, original_source, vessel_name, imo, ircs, mmsi, national_registry, vessel_flag_alpha3, iotc_number, vessel_type_code, gear_type_code, vessel_kind, range_code, port_code, port_name, length_value, length_metric_type, length_unit_enum, gt_value, grt_value, volume_value, volume_unit_enum, cc_value, cc_unit_enum, owner_name, owner_address, operator_name, operator_address, operating_company, operating_company_address, operating_company_reg_num, beneficial_owner, beneficial_owner_address, auth_from_date, auth_to_date, last_updated, starboard_photo, portside_photo, bow_photo) FROM '$INPUT_FILE' WITH CSV HEADER"
    
    psql -h "$POSTGRES_HOST" \
         -p "$POSTGRES_PORT" \
         -U "$POSTGRES_USER" \
         -d "$POSTGRES_DB" \
         -c "$COPY_CMD"
fi

STAGING_COUNT=$(execute_sql "SELECT COUNT(*) FROM iotc_vessels_staging;" "-t")
log_success "Loaded $STAGING_COUNT records into IOTC staging table"

# Preprocess IRCS field to handle special cases
log_step "Processing IRCS special cases"
execute_sql "
-- Add processing_notes for IRCS status tracking
UPDATE iotc_vessels_staging
SET processing_notes = jsonb_build_object(
    'ircs_status', 'APPLIED_NOT_YET_RECEIVED',
    'original_ircs', ircs
)::text,
ircs = NULL
WHERE LENGTH(ircs) > 15 
   OR UPPER(ircs) LIKE '%APPLIED%'
   OR UPPER(ircs) LIKE '%NOT%RECEIVED%';"

# Match vessels with trust scoring
log_step "Matching IOTC vessels against existing database with trust scoring"

BATCH_SIZE=1000
TOTAL_ROWS=$(execute_sql "SELECT COUNT(*) FROM iotc_vessels_staging;" "-t")
PROCESSED=0

while [ $PROCESSED -lt $TOTAL_ROWS ]; do
    BATCH_END=$((PROCESSED + BATCH_SIZE))
    
    execute_sql "
    -- Process batch of vessels with trust scoring
    WITH batch AS (
        SELECT * FROM iotc_vessels_staging 
        WHERE row_id > $PROCESSED AND row_id <= $BATCH_END
    ),
    matched AS (
        SELECT 
            b.row_id,
            m.*,
            analyze_vessel_fields_from_staging('iotc_vessels_staging', b.row_id) as fields_present
        FROM batch b
        CROSS JOIN LATERAL find_or_create_vessel_with_trust(
            b.vessel_name,
            b.imo,
            b.ircs,
            b.mmsi,
            b.vessel_flag_alpha3,
            '$IOTC_SOURCE_ID'::uuid,
            '$IMPORT_RUN_ID'::uuid,
            b.source_date::date,
            analyze_vessel_fields_from_staging('iotc_vessels_staging', b.row_id)
        ) m
    )
    UPDATE iotc_vessels_staging s
    SET vessel_uuid = m.vessel_uuid,
        is_new_vessel = m.is_new,
        match_confidence = m.match_confidence,
        confidence_score = m.confidence_score,
        trust_score = m.trust_score
    FROM matched m
    WHERE s.row_id = m.row_id;
    "
    
    PROCESSED=$BATCH_END
    echo "Processed $PROCESSED of $TOTAL_ROWS vessels..."
done

log_success "Vessel matching completed"

# Show matching statistics
execute_sql "
WITH match_stats AS (
    SELECT 
        match_confidence,
        COUNT(*) as count
    FROM iotc_vessels_staging
    GROUP BY match_confidence
)
SELECT 
    'IOTC vessel matching:' as info,
    match_confidence,
    count
FROM match_stats
ORDER BY match_confidence;
"

# Process all vessel data inserts
log_step "Loading IOTC vessel data"

# Create comprehensive insert SQL
cat > /tmp/iotc_inserts.sql << 'EOSQL'
-- Vessel sources and external identifiers are now handled by find_or_create_vessel_with_trust
-- which calls record_vessel_source_presence that properly:
-- 1. Inserts into vessel_sources with correct column names
-- 2. Stores vessel_id_in_source (IOTC number) in vessel_external_identifiers
-- No manual INSERT needed here

-- Insert vessel info
INSERT INTO vessel_info (
    vessel_uuid,
    port_registry,
    home_port,
    created_at,
    updated_at
)
SELECT DISTINCT
    s.vessel_uuid,
    s.port_code,
    s.port_name,
    NOW(),
    NOW()
FROM iotc_vessels_staging s
WHERE s.vessel_uuid IS NOT NULL
  AND (s.port_code IS NOT NULL OR s.port_name IS NOT NULL)
ON CONFLICT (vessel_uuid) DO UPDATE SET
    port_registry = COALESCE(EXCLUDED.port_registry, vessel_info.port_registry),
    home_port = COALESCE(EXCLUDED.home_port, vessel_info.home_port),
    updated_at = NOW();

-- Store IRCS status and other attributes
INSERT INTO vessel_attributes (
    vessel_uuid,
    source_id,
    attributes,
    last_updated
)
SELECT DISTINCT
    s.vessel_uuid,
    :source_id::uuid,
    CASE 
        WHEN s.processing_notes IS NOT NULL THEN
            jsonb_build_object(
                'vessel_kind', s.vessel_kind,
                'range_code', s.range_code,
                'last_updated_in_source', s.last_updated
            ) || s.processing_notes::jsonb
        ELSE
            jsonb_build_object(
                'vessel_kind', s.vessel_kind,
                'range_code', s.range_code,
                'last_updated_in_source', s.last_updated
            )
    END,
    NOW()
FROM iotc_vessels_staging s
WHERE s.vessel_uuid IS NOT NULL
  AND (s.processing_notes IS NOT NULL 
       OR s.vessel_kind IS NOT NULL 
       OR s.range_code IS NOT NULL)
ON CONFLICT (vessel_uuid, source_id) DO UPDATE SET
    attributes = vessel_attributes.attributes || EXCLUDED.attributes,
    last_updated = NOW();

-- Process vessel types
INSERT INTO vessel_vessel_types (vessel_uuid, vessel_type_id)
SELECT DISTINCT
    s.vessel_uuid,
    iotc_get_vessel_type_id(s.vessel_type_code)
FROM iotc_vessels_staging s
WHERE s.vessel_uuid IS NOT NULL
  AND s.vessel_type_code IS NOT NULL
  AND iotc_get_vessel_type_id(s.vessel_type_code) IS NOT NULL
ON CONFLICT (vessel_uuid, vessel_type_id) DO NOTHING;

-- Process gear types (multiple gears possible)
WITH gear_mappings AS (
    SELECT 
        s.vessel_uuid,
        unnest(iotc_get_gear_type_id(s.gear_type_code)) as gear_type_id
    FROM iotc_vessels_staging s
    WHERE s.vessel_uuid IS NOT NULL
      AND s.gear_type_code IS NOT NULL
)
INSERT INTO vessel_gear_types (vessel_uuid, gear_type_id)
SELECT DISTINCT vessel_uuid, gear_type_id
FROM gear_mappings
WHERE gear_type_id IS NOT NULL
ON CONFLICT (vessel_uuid, gear_type_id) DO NOTHING;

-- Insert length metrics
INSERT INTO vessel_metrics (
    vessel_uuid,
    source_id,
    metric_type,
    value,
    unit_enum,
    created_at
)
SELECT DISTINCT
    s.vessel_uuid,
    :source_id::uuid,
    s.length_metric_type,
    s.length_value,
    s.length_unit_enum,
    NOW()
FROM iotc_vessels_staging s
WHERE s.vessel_uuid IS NOT NULL
  AND s.length_value IS NOT NULL
  AND s.length_value > 0
ON CONFLICT (vessel_uuid, source_id, metric_type) 
DO UPDATE SET
    value = EXCLUDED.value,
    unit_enum = EXCLUDED.unit_enum,
    updated_at = NOW();

-- Process tonnage and capacity metrics
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN 
        SELECT vessel_uuid, gt_value, grt_value, volume_value, cc_value
        FROM iotc_vessels_staging
        WHERE vessel_uuid IS NOT NULL
    LOOP
        PERFORM iotc_process_vessel_metrics(
            r.vessel_uuid,
            :source_id::uuid,
            r.gt_value,
            r.grt_value,
            r.volume_value,
            r.cc_value
        );
    END LOOP;
END $$;

-- Process ownership structure
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN 
        SELECT DISTINCT
            vessel_uuid, owner_name, owner_address, operator_name, operator_address,
            operating_company, operating_company_address, operating_company_reg_num,
            beneficial_owner, beneficial_owner_address
        FROM iotc_vessels_staging
        WHERE vessel_uuid IS NOT NULL
    LOOP
        PERFORM iotc_process_vessel_ownership(
            r.vessel_uuid,
            :source_id::uuid,
            r.owner_name,
            r.owner_address,
            r.operator_name,
            r.operator_address,
            r.operating_company,
            r.operating_company_address,
            r.operating_company_reg_num,
            r.beneficial_owner,
            r.beneficial_owner_address
        );
    END LOOP;
END $$;

-- Process authorizations
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN 
        SELECT vessel_uuid, auth_from_date, auth_to_date, range_code
        FROM iotc_vessels_staging
        WHERE vessel_uuid IS NOT NULL
          AND (auth_from_date IS NOT NULL OR auth_to_date IS NOT NULL)
    LOOP
        PERFORM iotc_process_vessel_authorization(
            r.vessel_uuid,
            :source_id::uuid,
            r.auth_from_date,
            r.auth_to_date,
            r.range_code
        );
    END LOOP;
END $$;

-- Store external identifier
INSERT INTO vessel_external_ids (
    vessel_uuid,
    id_type,
    id_value,
    source_id,
    created_at
)
SELECT DISTINCT
    s.vessel_uuid,
    'IOTC_NUMBER',
    s.iotc_number,
    :source_id::uuid,
    NOW()
FROM iotc_vessels_staging s
WHERE s.vessel_uuid IS NOT NULL
  AND s.iotc_number IS NOT NULL
ON CONFLICT (vessel_uuid, id_type, id_value) 
DO UPDATE SET
    source_id = EXCLUDED.source_id,
    updated_at = NOW();

-- Store vessel photos for future use
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN 
        SELECT vessel_uuid, starboard_photo, portside_photo, bow_photo
        FROM iotc_vessels_staging
        WHERE vessel_uuid IS NOT NULL
          AND (starboard_photo IS NOT NULL OR portside_photo IS NOT NULL OR bow_photo IS NOT NULL)
    LOOP
        PERFORM iotc_store_vessel_photos(
            r.vessel_uuid,
            :source_id::uuid,
            r.starboard_photo,
            r.portside_photo,
            r.bow_photo
        );
    END LOOP;
END $$;

-- Capture historical snapshots for all vessels
INSERT INTO import_run_stats (
    import_run_id,
    stat_name,
    stat_value,
    recorded_at
)
SELECT 
    :import_run_id::uuid,
    'historical_snapshots_captured',
    COUNT(*),
    NOW()
FROM (
    SELECT DISTINCT vessel_uuid, source_date
    FROM iotc_vessels_staging
    WHERE vessel_uuid IS NOT NULL
) snapshots
WHERE EXISTS (
    SELECT 1 FROM capture_vessel_snapshot(
        snapshots.vessel_uuid,
        :source_id::uuid,
        snapshots.source_date::date,
        :import_run_id::uuid
    )
);

-- Calculate trust scores for imported vessels
SELECT calculate_all_vessel_trust_scores();
EOSQL

# Execute with parameter substitution
psql -h "$POSTGRES_HOST" \
     -p "$POSTGRES_PORT" \
     -U "$POSTGRES_USER" \
     -d "$POSTGRES_DB" \
     -v source_id="$IOTC_SOURCE_ID" \
     -v import_run_id="$IMPORT_RUN_ID" \
     -f /tmp/iotc_inserts.sql

# Update import run status
execute_sql "
WITH stats AS (
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT vessel_uuid) as unique_vessels,
        SUM(CASE WHEN is_new_vessel THEN 1 ELSE 0 END) as new_vessels,
        SUM(CASE WHEN NOT is_new_vessel THEN 1 ELSE 0 END) as existing_vessels,
        AVG(trust_score) as avg_trust_score
    FROM iotc_vessels_staging
)
UPDATE import_runs
SET 
    end_time = NOW(),
    status = 'COMPLETED',
    completed_phases = 1,
    total_phases = 1,
    configuration = configuration || 
        jsonb_build_object(
            'records_processed', (SELECT total_records FROM stats),
            'unique_vessels', (SELECT unique_vessels FROM stats),
            'new_vessels', (SELECT new_vessels FROM stats),
            'existing_vessels', (SELECT existing_vessels FROM stats),
            'avg_trust_score', (SELECT ROUND(avg_trust_score::numeric, 2) FROM stats)
        )
WHERE run_id = '$IMPORT_RUN_ID'::uuid;
"

# Display summary
log_step "IOTC Import Summary"
execute_sql "
SELECT 
    'Total vessels processed:' as metric,
    COUNT(*) as value
FROM iotc_vessels_staging
UNION ALL
SELECT 
    'New vessels created:',
    COUNT(*) 
FROM iotc_vessels_staging 
WHERE is_new_vessel = true
UNION ALL
SELECT 
    'Existing vessels matched:',
    COUNT(*) 
FROM iotc_vessels_staging 
WHERE is_new_vessel = false
UNION ALL
SELECT 
    'Average trust score:',
    ROUND(AVG(trust_score), 2)::TEXT
FROM iotc_vessels_staging
WHERE trust_score IS NOT NULL
UNION ALL
SELECT 
    'Active authorizations:',
    COUNT(DISTINCT vessel_uuid)::TEXT
FROM iotc_vessels_staging
WHERE auth_to_date >= CURRENT_DATE
UNION ALL
SELECT 
    'Vessels with beneficial owners:',
    COUNT(*)
FROM iotc_vessels_staging
WHERE beneficial_owner IS NOT NULL;
"

# Cleanup
log_step "Cleaning up"
execute_sql "DROP TABLE IF EXISTS iotc_vessels_staging;"
rm -f /tmp/iotc_inserts.sql

log_success "✅ IOTC vessel import completed successfully!"
log_success "Import run ID: $IMPORT_RUN_ID"

# Show trust score distribution
log_step "Trust Score Distribution for IOTC vessels"
execute_sql "
SELECT 
    CASE 
        WHEN trust_score >= 0.9 THEN '0.9-1.0 (Excellent)'
        WHEN trust_score >= 0.8 THEN '0.8-0.9 (Very Good)'
        WHEN trust_score >= 0.7 THEN '0.7-0.8 (Good)'
        WHEN trust_score >= 0.6 THEN '0.6-0.7 (Fair)'
        WHEN trust_score >= 0.5 THEN '0.5-0.6 (Average)'
        ELSE '< 0.5 (Poor)'
    END as trust_range,
    COUNT(*) as vessel_count
FROM vessel_trust_scores vts
WHERE EXISTS (
    SELECT 1 FROM vessel_sources vs 
    WHERE vs.vessel_uuid = vts.vessel_uuid 
    AND vs.source_id = '$IOTC_SOURCE_ID'::uuid
)
GROUP BY trust_range
ORDER BY trust_range;
"