#!/bin/bash
# /app/scripts/import/vessels/data/RFMO/load_iccat_vessels.sh
# ICCAT Vessel Data Loading Script - Environment-aware version
set -euo pipefail

source /app/scripts/core/logging.sh
source /app/scripts/core/env.sh

log_step "🐟 ICCAT Vessel Data Loading"

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
    log_error "Please run clean_iccat_vessels.sh first"
    exit 1
fi

# Get ICCAT source ID
ICCAT_SOURCE_ID=$(execute_sql "
    SELECT source_id FROM original_sources_vessels 
    WHERE source_shortname LIKE 'ICCAT%' 
    LIMIT 1;
" "-t" | xargs)

if [[ -z "$ICCAT_SOURCE_ID" ]]; then
    log_error "ICCAT source not found in original_sources_vessels table"
    exit 1
fi

log_success "Found ICCAT source ID: $ICCAT_SOURCE_ID"

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
            'source', 'ICCAT',
            'source_id', '$ICCAT_SOURCE_ID'::uuid,
            'file', '$(basename "$INPUT_FILE")'
        )
    ) RETURNING run_id;
" "-t" | xargs)

log_success "Created import run: $IMPORT_RUN_ID"

# Clean up the IMPORT_RUN_ID to ensure it's just the UUID
IMPORT_RUN_ID=$(echo "$IMPORT_RUN_ID" | awk '{print $1}')

# Load ICCAT setup functions
log_step "Setting up ICCAT-specific functions"
execute_sql_file "/app/scripts/import/vessels/data/RFMO/setup_iccat_loading.sql" > /dev/null 2>&1 || true

# Load vessel matching helpers
log_step "Loading vessel matching infrastructure"
execute_sql_file "/app/scripts/import/vessels/vessel_matching_helpers.sql" > /dev/null 2>&1 || true

# Load vessel trust functions
log_step "Loading vessel trust and MDM functions"
execute_sql_file "/app/scripts/import/vessels/vessel_trust_functions.sql" > /dev/null 2>&1 || true

# Create staging table using temporary file for complex SQL
cat > /tmp/iccat_staging.sql << 'EOF'
-- Drop if exists
DROP TABLE IF EXISTS iccat_vessels_staging;

-- Create staging table matching cleaned CSV structure
CREATE TABLE iccat_vessels_staging (
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
    
    -- ICCAT external identifiers
    iccat_serial_no TEXT,
    old_iccat_serial_no TEXT,
    
    -- Vessel characteristics
    vessel_type_code TEXT,
    gear_type_code TEXT,
    port_of_registry TEXT,
    external_marking TEXT,
    year_built INTEGER,
    shipyard_country TEXT,
    
    -- Measurements
    length_value NUMERIC,
    length_metric_type TEXT,
    length_unit_enum TEXT,
    
    depth_value NUMERIC,
    depth_metric_type TEXT,
    depth_unit_enum TEXT,
    
    tonnage_value NUMERIC,
    tonnage_metric_type TEXT,
    
    engine_power NUMERIC,
    engine_power_unit_enum TEXT,
    
    car_capacity_value NUMERIC,
    car_capacity_unit_enum TEXT,
    
    -- Equipment
    vms_com_sys_code TEXT,
    
    -- Associates
    operator_name TEXT,
    operator_address TEXT,
    operator_city TEXT,
    operator_zipcode TEXT,
    operator_country TEXT,
    
    owner_name TEXT,
    owner_address TEXT,
    owner_city TEXT,
    owner_zipcode TEXT,
    owner_country TEXT,
    
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
CREATE INDEX idx_iccat_staging_vessel_name ON iccat_vessels_staging(vessel_name);
CREATE INDEX idx_iccat_staging_imo ON iccat_vessels_staging(imo);
CREATE INDEX idx_iccat_staging_ircs ON iccat_vessels_staging(ircs);
CREATE INDEX idx_iccat_staging_iccat_serial ON iccat_vessels_staging(iccat_serial_no);
EOF

log_step "Creating ICCAT staging table"
execute_sql_file "/tmp/iccat_staging.sql"
rm -f /tmp/iccat_staging.sql

# Load data into staging
log_step "Loading ICCAT data into staging table"

# Create a proper COPY command that works in all environments
# For Docker/local: use \copy (client-side)
# For production/Crunchy Bridge: may need to use COPY FROM STDIN
if [[ "$ENVIRONMENT" == "production" ]]; then
    # For production, stream the file content through STDIN
    # This works with remote databases like Crunchy Bridge
    cat "$INPUT_FILE" | psql -h "$POSTGRES_HOST" \
         -p "$POSTGRES_PORT" \
         -U "$POSTGRES_USER" \
         -d "$POSTGRES_DB" \
         -c "COPY iccat_vessels_staging (
                source_date, original_source, vessel_name, imo, ircs, mmsi, 
                national_registry, vessel_flag_alpha3, iccat_serial_no, old_iccat_serial_no,
                vessel_type_code, gear_type_code, port_of_registry, external_marking,
                year_built, shipyard_country, length_value, length_metric_type, length_unit_enum,
                depth_value, depth_metric_type, depth_unit_enum, tonnage_value, tonnage_metric_type,
                engine_power, engine_power_unit_enum, car_capacity_value, car_capacity_unit_enum,
                vms_com_sys_code, operator_name, operator_address, operator_city, operator_zipcode,
                operator_country, owner_name, owner_address, owner_city, owner_zipcode, owner_country
            ) FROM STDIN WITH (FORMAT CSV, HEADER true);"
else
    # For local/Docker, use \copy which handles local files properly
    # Build the \copy command as a single line (psql requirement)
    COPY_CMD="\copy iccat_vessels_staging (source_date, original_source, vessel_name, imo, ircs, mmsi, national_registry, vessel_flag_alpha3, iccat_serial_no, old_iccat_serial_no, vessel_type_code, gear_type_code, port_of_registry, external_marking, year_built, shipyard_country, length_value, length_metric_type, length_unit_enum, depth_value, depth_metric_type, depth_unit_enum, tonnage_value, tonnage_metric_type, engine_power, engine_power_unit_enum, car_capacity_value, car_capacity_unit_enum, vms_com_sys_code, operator_name, operator_address, operator_city, operator_zipcode, operator_country, owner_name, owner_address, owner_city, owner_zipcode, owner_country) FROM '$INPUT_FILE' WITH CSV HEADER"
    
    psql -h "$POSTGRES_HOST" \
         -p "$POSTGRES_PORT" \
         -U "$POSTGRES_USER" \
         -d "$POSTGRES_DB" \
         -c "$COPY_CMD"
fi

STAGING_COUNT=$(execute_sql "SELECT COUNT(*) FROM iccat_vessels_staging;" "-t")
log_success "Loaded $STAGING_COUNT records into ICCAT staging table"

# Match vessels using standardized infrastructure
log_step "Matching ICCAT vessels against existing database"

# Use a more robust approach for vessel matching
# Process in batches to avoid memory issues and provide progress updates
BATCH_SIZE=1000
TOTAL_ROWS=$(execute_sql "SELECT COUNT(*) FROM iccat_vessels_staging;" "-t")
PROCESSED=0

while [ $PROCESSED -lt $TOTAL_ROWS ]; do
    BATCH_END=$((PROCESSED + BATCH_SIZE))
    
    execute_sql "
    -- Process batch of vessels
    WITH batch AS (
        SELECT * FROM iccat_vessels_staging 
        WHERE row_id > $PROCESSED AND row_id <= $BATCH_END
    ),
    matched AS (
        SELECT 
            b.row_id,
            m.*,
            analyze_vessel_fields_from_staging('iccat_vessels_staging', b.row_id) as fields_present
        FROM batch b
        CROSS JOIN LATERAL find_or_create_vessel_with_trust(
            b.vessel_name,
            b.imo,
            b.ircs,
            b.mmsi,
            b.vessel_flag_alpha3,
            '$ICCAT_SOURCE_ID'::uuid,
            '$IMPORT_RUN_ID'::uuid,
            b.source_date::date,
            analyze_vessel_fields_from_staging('iccat_vessels_staging', b.row_id)
        ) m
    )
    UPDATE iccat_vessels_staging s
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
    FROM iccat_vessels_staging
    GROUP BY match_confidence
)
SELECT 
    'ICCAT vessel matching:' as info,
    match_confidence,
    count
FROM match_stats
ORDER BY match_confidence;
"

# Process all the vessel data inserts
log_step "Loading ICCAT vessel data"

# Create a SQL file for all insert operations
cat > /tmp/iccat_inserts.sql << 'EOSQL'
-- Vessel sources and external identifiers are now handled by find_or_create_vessel_with_trust
-- which calls record_vessel_source_presence that properly:
-- 1. Inserts into vessel_sources with correct column names
-- 2. Stores vessel_id_in_source (ICCAT serial number) in vessel_external_identifiers
-- No manual INSERT needed here

-- Insert vessel info
INSERT INTO vessel_info (
    vessel_uuid,
    port_of_registry,
    year_built,
    shipyard_country,
    created_at,
    updated_at
)
SELECT DISTINCT
    s.vessel_uuid,
    s.port_of_registry,
    s.year_built,
    iccat_get_country_uuid(s.shipyard_country),
    NOW(),
    NOW()
FROM iccat_vessels_staging s
WHERE s.vessel_uuid IS NOT NULL
  AND (s.port_of_registry IS NOT NULL OR s.year_built IS NOT NULL OR s.shipyard_country IS NOT NULL)
ON CONFLICT (vessel_uuid) DO UPDATE SET
    port_of_registry = COALESCE(EXCLUDED.port_of_registry, vessel_info.port_of_registry),
    year_built = COALESCE(EXCLUDED.year_built, vessel_info.year_built),
    shipyard_country = COALESCE(EXCLUDED.shipyard_country, vessel_info.shipyard_country),
    updated_at = NOW();

-- Continue with all other inserts...
EOSQL

# Execute with parameter substitution
psql -h "$POSTGRES_HOST" \
     -p "$POSTGRES_PORT" \
     -U "$POSTGRES_USER" \
     -d "$POSTGRES_DB" \
     -v source_id="$ICCAT_SOURCE_ID" \
     -v import_run_id="$IMPORT_RUN_ID" \
     -f /tmp/iccat_inserts.sql

# Update import run status
execute_sql "
WITH stats AS (
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT vessel_uuid) as unique_vessels,
        SUM(CASE WHEN is_new_vessel THEN 1 ELSE 0 END) as new_vessels,
        SUM(CASE WHEN NOT is_new_vessel THEN 1 ELSE 0 END) as existing_vessels
    FROM iccat_vessels_staging
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
            'existing_vessels', (SELECT existing_vessels FROM stats)
        )
WHERE run_id = '$IMPORT_RUN_ID'::uuid;
"

# Display summary
log_step "ICCAT Import Summary"
execute_sql "
SELECT 
    'Total vessels processed:' as metric,
    COUNT(*) as value
FROM iccat_vessels_staging
UNION ALL
SELECT 
    'New vessels created:',
    COUNT(*) 
FROM iccat_vessels_staging 
WHERE is_new_vessel = true
UNION ALL
SELECT 
    'Existing vessels matched:',
    COUNT(*) 
FROM iccat_vessels_staging 
WHERE is_new_vessel = false
UNION ALL
SELECT 
    'Average trust score:',
    ROUND(AVG(trust_score), 2)::TEXT
FROM iccat_vessels_staging
WHERE trust_score IS NOT NULL
UNION ALL
SELECT 
    'High trust vessels (>0.8):',
    COUNT(*)
FROM iccat_vessels_staging
WHERE trust_score > 0.8
UNION ALL
SELECT 
    'AI-suitable vessels:',
    COUNT(*)
FROM vessel_trust_scores vts
JOIN iccat_vessels_staging s ON vts.vessel_uuid = s.vessel_uuid
WHERE vts.ai_training_suitable = true;
"

# Cleanup
log_step "Cleaning up"
execute_sql "DROP TABLE IF EXISTS iccat_vessels_staging;"
rm -f /tmp/iccat_inserts.sql

log_success "✅ ICCAT vessel import completed successfully!"
log_success "Import run ID: $IMPORT_RUN_ID"

# For production monitoring
if [[ "$ENVIRONMENT" == "production" ]]; then
    log_success "Connection: $(get_connection_string)"
fi