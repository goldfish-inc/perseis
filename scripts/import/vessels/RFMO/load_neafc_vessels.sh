#!/bin/bash
# /app/scripts/import/vessels/data/RFMO/load_neafc_vessels.sh  
# NEAFC Vessel Loading - Complete & Thorough (FIXED UUID casting)
set -euo pipefail

source /app/scripts/core/logging.sh
source /app/scripts/core/database.sh

log_step "🌊 Loading NEAFC Vessels - Complete Processing (FIXED)"

# Validate cleaned file exists
CLEANED_FILE="/import/vessels/vessel_data/RFMO/cleaned/neafc_vessels_cleaned.csv"
if [[ ! -f "$CLEANED_FILE" ]]; then
    log_error "Cleaned NEAFC data not found: $CLEANED_FILE"
    exit 1
fi

RECORD_COUNT=$(tail -n +2 "$CLEANED_FILE" | wc -l)
log_success "Found $RECORD_COUNT NEAFC records to process"

# Get NEAFC source ID
log_step "Getting NEAFC source ID..."
NEAFC_SOURCE_ID=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT source_id FROM original_sources_vessels WHERE source_shortname = 'NEAFC';" 2>/dev/null || echo "")

if [[ -z "$NEAFC_SOURCE_ID" ]]; then
    log_error "NEAFC source not found in database"
    log_error "Available sources:"
    PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT source_shortname, status FROM original_sources_vessels;" 2>/dev/null || true
    exit 1
fi

log_success "NEAFC source ID: $NEAFC_SOURCE_ID"

# Ensure NEAFC RFMO exists
log_step "Ensuring NEAFC RFMO exists..."
PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
INSERT INTO rfmos (rfmo_acronym, rfmo_name, rfmo_full_name, established_year)
VALUES ('NEAFC', 'NEAFC', 'North East Atlantic Fisheries Commission', 1963)
ON CONFLICT (rfmo_acronym) DO NOTHING;" 2>/dev/null || log_warning "Could not ensure NEAFC RFMO"

# Create comprehensive SQL file for loading (FIXED UUID syntax)
log_step "Creating NEAFC loading SQL with FIXED UUID casting..."
cat > /tmp/neafc_complete_load_fixed.sql << SQLEOF
-- NEAFC Complete Loading Script (FIXED UUID casting)
\echo 'Starting NEAFC complete loading process...'

-- Load CSV into temporary table
CREATE TEMP TABLE neafc_raw (
    source_date TEXT,
    original_source TEXT,
    vessel_name TEXT,
    imo TEXT,
    ircs TEXT,
    vessel_flag_alpha3 TEXT,
    vessel_type_code TEXT,
    external_marking TEXT,
    gross_tonnage TEXT,
    length_type TEXT,
    length_value TEXT,
    length_unit TEXT,
    engine_power TEXT,
    engine_power_unit TEXT,
    authorization_status TEXT,
    auth_start_date TEXT,
    auth_end_date TEXT,
    authorizing_country_alpha3 TEXT,
    authorizing_country_group TEXT,
    species_description TEXT,
    scientific_name TEXT,
    sender TEXT
);

\echo 'Loading CSV data...'
\COPY neafc_raw FROM '/import/vessels/vessel_data/RFMO/cleaned/neafc_vessels_cleaned.csv' WITH (FORMAT csv, HEADER true, NULL '');

SELECT 'CSV loaded: ' || COUNT(*) || ' records' FROM neafc_raw;

-- Process and clean data
\echo 'Processing and validating data...'
CREATE TEMP TABLE neafc_clean AS
SELECT 
    -- Core vessel identifiers
    NULLIF(trim(vessel_name), '') as vessel_name,
    CASE 
        WHEN trim(imo) ~ '^[0-9]{7}$' THEN trim(imo) 
        ELSE NULL 
    END as imo,
    CASE 
        WHEN trim(ircs) != '' AND trim(ircs) NOT IN ('N/A', 'UNKNOWN', '') 
        THEN upper(trim(ircs)) 
        ELSE NULL 
    END as ircs,
    
    -- Country flag lookup
    (SELECT id FROM country_iso 
     WHERE upper(alpha_3_code) = upper(trim(vessel_flag_alpha3)) 
        OR upper(alpha_2_code) = upper(trim(vessel_flag_alpha3))
     LIMIT 1) as vessel_flag,
    upper(trim(vessel_flag_alpha3)) as flag_code,
    
    -- Vessel type lookup
    (SELECT id FROM vessel_types 
     WHERE upper(vessel_type_isscfv_code) = upper(trim(vessel_type_code))
        OR upper(vessel_type_isscfv_alpha) = upper(trim(vessel_type_code))
     LIMIT 1) as vessel_type,
    
    -- External marking
    NULLIF(trim(external_marking), '') as external_marking,
    
    -- Metrics with validation
    CASE 
        WHEN trim(gross_tonnage) ~ '^[0-9]+\.?[0-9]*$' 
             AND trim(gross_tonnage)::DECIMAL > 0 
             AND trim(gross_tonnage)::DECIMAL < 1000000
        THEN trim(gross_tonnage)::DECIMAL 
        ELSE NULL
    END as gross_tonnage,
    CASE 
        WHEN trim(length_value) ~ '^[0-9]+\.?[0-9]*$' 
             AND trim(length_value)::DECIMAL > 0 
             AND trim(length_value)::DECIMAL < 500
        THEN trim(length_value)::DECIMAL 
        ELSE NULL
    END as length_value,
    CASE 
        WHEN trim(engine_power) ~ '^[0-9]+\.?[0-9]*$' 
             AND trim(engine_power)::DECIMAL > 0 
             AND trim(engine_power)::DECIMAL < 100000
        THEN trim(engine_power)::DECIMAL 
        ELSE NULL
    END as engine_power,
    
    -- Authorization dates
    CASE 
        WHEN trim(auth_start_date) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
        THEN trim(auth_start_date)::DATE 
        ELSE NULL
    END as start_date,
    CASE 
        WHEN trim(auth_end_date) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
        THEN trim(auth_end_date)::DATE 
        ELSE NULL
    END as end_date,
    
    -- Authorization status
    CASE 
        WHEN upper(trim(authorization_status)) IN ('ACTIVE', 'YES', 'Y', '1', 'TRUE')
        THEN 'ACTIVE'
        ELSE 'INACTIVE'
    END as auth_status,
    
    -- Species information
    NULLIF(trim(species_description), '') as species_desc,
    NULLIF(trim(scientific_name), '') as species_name,
    
    -- Sender information
    NULLIF(upper(trim(sender)), '') as sender,
    
    -- Row tracking for debugging
    row_number() OVER (ORDER BY trim(vessel_name), trim(ircs)) as row_id
    
FROM neafc_raw
WHERE NULLIF(trim(vessel_name), '') IS NOT NULL 
   OR NULLIF(trim(imo), '') IS NOT NULL 
   OR NULLIF(trim(ircs), '') IS NOT NULL;

SELECT 'Data processed: ' || COUNT(*) || ' valid records' FROM neafc_clean;

-- Add vessel matching columns
ALTER TABLE neafc_clean ADD COLUMN vessel_uuid UUID;
ALTER TABLE neafc_clean ADD COLUMN match_type TEXT;

\echo 'Step 1: Vessel matching using priority hierarchy...'

-- Priority 1: IMO match (highest priority)
UPDATE neafc_clean 
SET vessel_uuid = v.vessel_uuid, match_type = 'IMO_MATCH'
FROM vessels v
WHERE neafc_clean.imo IS NOT NULL 
  AND v.imo = neafc_clean.imo;

-- Priority 2: IRCS + Name + Flag match (only if no IMO match)
UPDATE neafc_clean 
SET vessel_uuid = v.vessel_uuid, match_type = 'IRCS_NAME_FLAG'
FROM vessels v
WHERE neafc_clean.vessel_uuid IS NULL
  AND neafc_clean.ircs IS NOT NULL 
  AND neafc_clean.vessel_name IS NOT NULL 
  AND neafc_clean.vessel_flag IS NOT NULL
  AND v.ircs = neafc_clean.ircs 
  AND v.vessel_name = neafc_clean.vessel_name 
  AND v.vessel_flag = neafc_clean.vessel_flag;

-- Priority 3: IRCS + Flag match (only if no previous matches)
UPDATE neafc_clean 
SET vessel_uuid = v.vessel_uuid, match_type = 'IRCS_FLAG'
FROM vessels v
WHERE neafc_clean.vessel_uuid IS NULL
  AND neafc_clean.ircs IS NOT NULL 
  AND neafc_clean.vessel_flag IS NOT NULL
  AND v.ircs = neafc_clean.ircs 
  AND v.vessel_flag = neafc_clean.vessel_flag;

-- Show matching results
SELECT 
    'Vessel Matching Results:' as status,
    COALESCE(match_type, 'NO_MATCH') as match_type,
    COUNT(*) as vessel_count
FROM neafc_clean 
GROUP BY COALESCE(match_type, 'NO_MATCH')
ORDER BY COUNT(*) DESC;

\echo 'Step 2: Creating new vessels for unmatched records...'

-- Insert new vessels
INSERT INTO vessels (vessel_name, imo, ircs, vessel_flag, created_at, updated_at)
SELECT DISTINCT 
    vessel_name, 
    imo, 
    ircs, 
    vessel_flag, 
    CURRENT_TIMESTAMP, 
    CURRENT_TIMESTAMP
FROM neafc_clean 
WHERE vessel_uuid IS NULL 
  AND vessel_flag IS NOT NULL
  AND (vessel_name IS NOT NULL OR imo IS NOT NULL OR ircs IS NOT NULL)
ON CONFLICT (imo) WHERE imo IS NOT NULL DO NOTHING;

-- Link newly created vessels back to neafc_clean
UPDATE neafc_clean 
SET vessel_uuid = v.vessel_uuid, match_type = 'CREATED_NEW'
FROM vessels v
WHERE neafc_clean.vessel_uuid IS NULL
  AND (
    (neafc_clean.imo IS NOT NULL AND v.imo = neafc_clean.imo) OR
    (neafc_clean.imo IS NULL AND 
     neafc_clean.ircs IS NOT NULL AND neafc_clean.vessel_flag IS NOT NULL 
     AND v.ircs = neafc_clean.ircs AND v.vessel_flag = neafc_clean.vessel_flag) OR
    (neafc_clean.imo IS NULL AND neafc_clean.ircs IS NULL AND
     neafc_clean.vessel_name IS NOT NULL AND neafc_clean.vessel_flag IS NOT NULL
     AND v.vessel_name = neafc_clean.vessel_name AND v.vessel_flag = neafc_clean.vessel_flag)
  );

SELECT 'Vessels with UUIDs: ' || COUNT(*) FROM neafc_clean WHERE vessel_uuid IS NOT NULL;

\echo 'Step 3: Adding vessel information...'

-- Insert vessel info
INSERT INTO vessel_info (vessel_uuid, vessel_type, external_marking)
SELECT DISTINCT 
    vessel_uuid, 
    vessel_type, 
    external_marking
FROM neafc_clean
WHERE vessel_uuid IS NOT NULL
  AND (vessel_type IS NOT NULL OR external_marking IS NOT NULL)
ON CONFLICT (vessel_uuid) DO UPDATE SET
    vessel_type = COALESCE(vessel_info.vessel_type, EXCLUDED.vessel_type),
    external_marking = COALESCE(vessel_info.external_marking, EXCLUDED.external_marking);

SELECT 'Vessel info records added: ' || COUNT(*) 
FROM vessel_info 
WHERE vessel_uuid IN (SELECT vessel_uuid FROM neafc_clean WHERE vessel_uuid IS NOT NULL);

\echo 'Step 4: Adding vessel metrics...'

-- FIXED: Insert vessel metrics with correct enum values
INSERT INTO vessel_metrics (vessel_uuid, source_id, metric_type, value, unit)
SELECT vessel_uuid, source_id, metric_type::metric_type_enum, value, unit::unit_enum
FROM (
    -- Gross tonnage (dimensionless - NULL unit)
    SELECT DISTINCT 
        vessel_uuid, 
        '$NEAFC_SOURCE_ID'::UUID as source_id,
        'gross_tonnage' as metric_type, 
        gross_tonnage as value, 
        NULL as unit
    FROM neafc_clean 
    WHERE vessel_uuid IS NOT NULL AND gross_tonnage IS NOT NULL
    
    UNION ALL
    
    -- Length (use proper enum values)
    SELECT DISTINCT 
        vessel_uuid, 
        '$NEAFC_SOURCE_ID'::UUID,
        'length_loa' as metric_type, 
        length_value as value, 
        'METER' as unit
    FROM neafc_clean 
    WHERE vessel_uuid IS NOT NULL AND length_value IS NOT NULL
    
    UNION ALL
    
    -- Engine power (use KW not KILOWATT)
    SELECT DISTINCT 
        vessel_uuid, 
        '$NEAFC_SOURCE_ID'::UUID,
        'engine_power' as metric_type, 
        engine_power as value, 
        'KW' as unit
    FROM neafc_clean 
    WHERE vessel_uuid IS NOT NULL AND engine_power IS NOT NULL
) metrics_data
ON CONFLICT DO NOTHING;

SELECT 'Vessel metrics added: ' || COUNT(*) 
FROM vessel_metrics 
WHERE source_id = '$NEAFC_SOURCE_ID'::UUID;

\echo 'Step 5: Adding source tracking...'

-- FIXED: Insert vessel sources tracking with proper UUID casting
INSERT INTO vessel_sources (vessel_uuid, source_id, first_seen_date, last_seen_date, is_active)
SELECT DISTINCT 
    vessel_uuid, 
    '$NEAFC_SOURCE_ID'::UUID, 
    CURRENT_DATE, 
    CURRENT_DATE, 
    true
FROM neafc_clean
WHERE vessel_uuid IS NOT NULL
ON CONFLICT (vessel_uuid, source_id) DO UPDATE SET 
    last_seen_date = CURRENT_DATE, 
    is_active = true;

SELECT 'Source tracking records: ' || COUNT(*) 
FROM vessel_sources 
WHERE source_id = '$NEAFC_SOURCE_ID'::UUID;

\echo 'Step 6: Adding authorization records with proper species mapping...'

-- Add species lookup helper for NEAFC
CREATE TEMP TABLE neafc_species_lookup AS
SELECT DISTINCT
    vessel_uuid,
    species_desc,
    species_name,
    start_date,
    end_date,
    auth_status,
    sender,
    row_id,
    match_type,
    flag_code,
    CASE 
        -- Special case: XDS = All deep-sea species (no harmonized_id)
        WHEN species_desc = 'All deep-sea species' THEN 'ALL DEEP-SEA SPECIES'
        ELSE NULL
    END as special_species_description,
    CASE 
        -- Only look up harmonized_id if NOT the special XDS case
        WHEN species_desc != 'All deep-sea species' AND species_name IS NOT NULL THEN (
            SELECT harmonized_id 
            FROM harmonized_species 
            WHERE canonical_scientific_name ILIKE species_name
               OR alternative_names::text ILIKE '%' || species_name || '%'
            LIMIT 1
        )
        ELSE NULL
    END as harmonized_species_id
FROM neafc_clean
WHERE vessel_uuid IS NOT NULL;

-- FIXED: Insert vessel authorizations with proper species handling
INSERT INTO vessel_authorizations (
    vessel_uuid, 
    source_id, 
    authorization_type, 
    rfmo_id, 
    start_date, 
    end_date, 
    status, 
    species_description,
    species_ids,
    additional_data
)
SELECT DISTINCT
    vessel_uuid,
    '$NEAFC_SOURCE_ID'::UUID,
    'FISHING_AUTHORIZATION'::authorization_type_enum,
    (SELECT id FROM rfmos WHERE rfmo_acronym = 'NEAFC' LIMIT 1),
    start_date,
    end_date,
    auth_status,
    special_species_description, -- Only populated for XDS case
    CASE 
        WHEN harmonized_species_id IS NOT NULL 
        THEN jsonb_build_array(harmonized_species_id)
        ELSE NULL
    END,
    jsonb_build_object(
        'sender_country', sender
    )
FROM neafc_species_lookup;

SELECT 'Authorization records added: ' || COUNT(*) 
FROM vessel_authorizations 
WHERE source_id = '$NEAFC_SOURCE_ID'::UUID;

\echo 'Step 7: Updating source status...'

-- Update source status
UPDATE original_sources_vessels 
SET status = 'LOADED',
    size_approx = (SELECT COUNT(DISTINCT vessel_uuid) FROM neafc_clean WHERE vessel_uuid IS NOT NULL),
    refresh_date = CURRENT_DATE,
    last_updated = CURRENT_TIMESTAMP
WHERE source_shortname = 'NEAFC';

\echo 'NEAFC loading completed successfully!'

-- FIXED: Final comprehensive results with proper UUID casting
SELECT 
    'NEAFC Loading Summary' as report_section,
    (SELECT COUNT(DISTINCT vessel_uuid) FROM vessel_sources WHERE source_id = '$NEAFC_SOURCE_ID'::UUID) as total_vessels,
    (SELECT COUNT(*) FROM vessel_authorizations WHERE source_id = '$NEAFC_SOURCE_ID'::UUID) as total_authorizations,
    (SELECT COUNT(*) FROM vessel_metrics WHERE source_id = '$NEAFC_SOURCE_ID'::UUID) as total_metrics,
    (SELECT COUNT(*) FROM vessel_info WHERE vessel_uuid IN 
        (SELECT vessel_uuid FROM vessel_sources WHERE source_id = '$NEAFC_SOURCE_ID'::UUID)) as vessel_info_records;

SQLEOF

# FIXED: Execute the complete loading script with proper variable substitution
log_step "Executing NEAFC complete loading with FIXED UUID casting..."
if PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
   -f /tmp/neafc_complete_load_fixed.sql 2>&1 | tee /import/logs/neafc_complete_fixed.log; then
    log_success "NEAFC SQL execution completed"
else
    log_error "NEAFC SQL execution failed"
    log_error "Check detailed log: /import/logs/neafc_complete_fixed.log"
    exit 1
fi

# Get comprehensive final results
log_step "Gathering final results..."
FINAL_RESULTS=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "
    SELECT 
        (SELECT COUNT(DISTINCT vessel_uuid) FROM vessel_sources WHERE source_id = '$NEAFC_SOURCE_ID')::TEXT || '|' ||
        (SELECT COUNT(*) FROM vessel_authorizations WHERE source_id = '$NEAFC_SOURCE_ID')::TEXT || '|' ||
        (SELECT COUNT(*) FROM vessel_metrics WHERE source_id = '$NEAFC_SOURCE_ID')::TEXT || '|' ||
        (SELECT COUNT(*) FROM vessel_info WHERE vessel_uuid IN 
            (SELECT vessel_uuid FROM vessel_sources WHERE source_id = '$NEAFC_SOURCE_ID'))::TEXT;" 2>/dev/null || echo "0|0|0|0")

IFS='|' read -r VESSELS AUTHS METRICS INFO <<< "$FINAL_RESULTS"

# Display comprehensive results
log_success "NEAFC Loading Complete:"
log_success "  Vessels: $VESSELS"
log_success "  Authorizations: $AUTHS"
log_success "  Metrics: $METRICS"
log_success "  Vessel Info: $INFO"

# Show breakdown by match type if possible
log_step "Breakdown by vessel matching:"
PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
    SELECT 
        additional_data->>'match_type' as match_type,
        COUNT(*) as count
    FROM vessel_authorizations 
    WHERE source_id = '$NEAFC_SOURCE_ID'
    GROUP BY additional_data->>'match_type'
    ORDER BY count DESC;" 2>/dev/null || log_warning "Could not show match type breakdown"

# Cleanup temporary files
rm -f /tmp/neafc_complete_load_fixed.sql

# Final validation and exit
if [[ "$VESSELS" -gt 0 && "$AUTHS" -gt 0 ]]; then
    log_success "SUCCESS: NEAFC data loaded successfully"
    log_success "Processed $RECORD_COUNT input records into $VESSELS vessels with $AUTHS authorizations"
    exit 0
else
    log_error "FAILED: Insufficient data loaded"
    log_error "Expected: >0 vessels and >0 authorizations"
    log_error "Got: $VESSELS vessels, $AUTHS authorizations"
    exit 1
fi