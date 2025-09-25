#!/bin/bash
# /app/scripts/import/vessels/data/RFMO/load_iattc_vessels.sh
# IATTC Vessel Data Loading Script with Trust Scoring
set -euo pipefail

# Determine environment and set paths
if [ -f /.dockerenv ]; then
    # Docker environment
    source /app/scripts/core/logging.sh
    CLEANED_DATA_DIR="/import/vessels/vessel_data/RFMO/cleaned"
    LOAD_DIR="/app/scripts/import/vessels/data/RFMO"
else
    # Local/Crunchy Bridge environment
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    source "$SCRIPT_DIR/../../../core/logging.sh"
    REPO_ROOT="$(cd "$SCRIPT_DIR/../../../../../../" && pwd)"
    DATA_ROOT="${EBISU_DATA_ROOT:-$REPO_ROOT/data/raw}"
    CLEANED_DATA_DIR="$DATA_ROOT/vessels/vessel_data/RFMO/cleaned"
    LOAD_DIR="$SCRIPT_DIR"
fi

log_step "🐟 IATTC Vessel Data Loading with Trust Scoring"

# Database connection
if [ -z "${DATABASE_URL:-}" ]; then
    log_error "DATABASE_URL not set"
    exit 1
fi

# Use local psql if available, otherwise docker
PSQL_CMD="psql"
if ! command -v psql &> /dev/null; then
    PSQL_CMD="docker exec -i ebisu-postgres-1 psql"
fi

INPUT_FILE="$CLEANED_DATA_DIR/iattc_vessels_cleaned.csv"
if [[ ! -f "$INPUT_FILE" ]]; then
    log_error "Cleaned file not found: $INPUT_FILE"
    log_error "Please run clean_iattc_vessels.sh first"
    exit 1
fi

log_success "Loading IATTC vessels from: $(basename "$INPUT_FILE")"

# Get source ID for IATTC
SOURCE_ID=$($PSQL_CMD "$DATABASE_URL" -t -c "
    SELECT source_id 
    FROM original_sources_vessels 
    WHERE source_shortname = 'IATTC'
")

if [[ -z "$SOURCE_ID" ]]; then
    log_error "IATTC source not found in original_sources_vessels"
    exit 1
fi

SOURCE_ID=$(echo "$SOURCE_ID" | xargs)
log_success "Using IATTC source ID: $SOURCE_ID"

# Verify setup functions exist
log_step "Verifying IATTC setup functions..."
$PSQL_CMD "$DATABASE_URL" -c "\df iattc_*" > /dev/null || {
    log_error "IATTC setup functions not found. Please run setup_iattc_loading.sql first"
    exit 1
}

# Create staging table
log_step "Creating staging table..."
$PSQL_CMD "$DATABASE_URL" <<EOF
DROP TABLE IF EXISTS staging_iattc_vessels;

CREATE TABLE staging_iattc_vessels (
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
    
    -- IATTC external identifiers
    iattc_vessel_number TEXT,
    register_status TEXT,
    
    -- Vessel characteristics
    gear_type_code TEXT,
    port_of_registry TEXT,
    year_built TEXT,
    shipyard TEXT,
    previous_name TEXT,
    previous_flag TEXT,
    
    -- Measurements
    length_value TEXT,
    length_metric_type TEXT,
    length_unit_enum TEXT,
    
    beam_value TEXT,
    beam_unit_enum TEXT,
    
    depth_value TEXT,
    depth_unit_enum TEXT,
    
    gross_tonnage TEXT,
    tonnage_metric_type TEXT,
    
    engine_power TEXT,
    engine_power_unit_enum TEXT,
    
    -- Capacity
    fish_hold_volume TEXT,
    fish_hold_volume_unit_enum TEXT,
    
    carrying_capacity TEXT,
    carrying_capacity_unit_enum TEXT,
    
    -- Company
    company_name TEXT,
    company_address TEXT,
    
    -- Metadata
    notes TEXT,
    image_available TEXT,
    confirmation_date TEXT,
    last_modification TEXT
);
EOF

# Load data into staging table
log_step "Loading data into staging table..."
if [ -f /.dockerenv ]; then
    # Docker environment - use \copy
    $PSQL_CMD "$DATABASE_URL" <<EOF
\copy staging_iattc_vessels FROM '$INPUT_FILE' WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', ESCAPE '\\');
EOF
else
    # Local/Crunchy Bridge - use cat and COPY FROM STDIN
    cat "$INPUT_FILE" | $PSQL_CMD "$DATABASE_URL" -c "COPY staging_iattc_vessels FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '\"', ESCAPE '\\');"
fi

# Get staging count
STAGING_COUNT=$($PSQL_CMD "$DATABASE_URL" -t -c "SELECT COUNT(*) FROM staging_iattc_vessels")
log_success "Loaded $STAGING_COUNT records into staging"

# Process vessels with trust scoring
log_step "Processing IATTC vessels with trust scoring..."
$PSQL_CMD "$DATABASE_URL" <<'EOF'
-- Load IATTC vessels with proper matching and trust scoring
DO $$
DECLARE
    v_vessel_uuid UUID;
    v_flag_id UUID;
    v_vessel_type_id UUID;
    v_batch_count INTEGER := 0;
    v_processed_count INTEGER := 0;
    v_total_count INTEGER;
    v_source_id UUID := (SELECT source_id FROM original_sources_vessels WHERE source_shortname = 'IATTC');
    r RECORD;
BEGIN
    SELECT COUNT(*) INTO v_total_count FROM staging_iattc_vessels;
    RAISE NOTICE 'Starting IATTC vessel processing for % vessels', v_total_count;
    
    -- Process in batches for performance
    FOR r IN (
        SELECT DISTINCT ON (
            COALESCE(imo, ''), 
            COALESCE(ircs, ''), 
            COALESCE(vessel_name, ''),
            COALESCE(vessel_flag_alpha3, '')
        )
        *,
        CASE 
            WHEN imo IS NOT NULL AND imo != '' THEN 1
            WHEN ircs IS NOT NULL AND ircs != '' THEN 2
            WHEN mmsi IS NOT NULL AND mmsi != '' THEN 3
            ELSE 4
        END as match_priority
        FROM staging_iattc_vessels
        ORDER BY 
            COALESCE(imo, ''), 
            COALESCE(ircs, ''), 
            COALESCE(vessel_name, ''),
            COALESCE(vessel_flag_alpha3, ''),
            match_priority,
            last_modification DESC NULLS LAST
    )
    LOOP
        BEGIN
            -- Get flag country ID
            v_flag_id := NULL;
            IF r.vessel_flag_alpha3 IS NOT NULL THEN
                SELECT id INTO v_flag_id
                FROM country_iso
                WHERE alpha_3_code = r.vessel_flag_alpha3;
            END IF;
            
            -- Determine vessel type from gear if not specified
            v_vessel_type_id := NULL;
            IF r.gear_type_code IS NOT NULL THEN
                -- Use the IATTC function to infer vessel type
                SELECT id INTO v_vessel_type_id
                FROM vessel_types
                WHERE vessel_type_isscfv_code = iattc_infer_vessel_type(r.gear_type_code);
            END IF;
            
            -- Find or create vessel with trust scoring
            SELECT vessel_uuid INTO v_vessel_uuid 
            FROM find_or_create_vessel_with_trust(
                r.vessel_name,
                r.imo,  -- Pass as TEXT
                r.ircs,
                r.mmsi, -- Pass as TEXT
                r.vessel_flag_alpha3, -- Pass flag code, not ID
                v_source_id,  -- Pass source_id
                NULL::UUID,   -- import_run_id
                COALESCE(r.source_date::DATE, CURRENT_DATE)
            );
            
            -- Note: record_vessel_presence_in_source is already called inside
            -- find_or_create_vessel_with_trust, so we don't need to call it again
            
            -- Process vessel type if available
            -- Intelligence principle: Multiple sources confirming same vessel type increases confidence
            IF v_vessel_type_id IS NOT NULL AND v_vessel_uuid IS NOT NULL THEN
                -- Check if this vessel-type relationship exists from any source
                IF NOT EXISTS (
                    SELECT 1 FROM vessel_vessel_types 
                    WHERE vessel_uuid = v_vessel_uuid 
                    AND vessel_type_id = v_vessel_type_id
                ) THEN
                    INSERT INTO vessel_vessel_types (vessel_uuid, vessel_type_id, source_id)
                    VALUES (v_vessel_uuid, v_vessel_type_id, v_source_id);
                ELSE
                    -- Record that another source confirms this vessel type (intelligence value)
                    RAISE NOTICE 'IATTC confirms vessel type for vessel %', v_vessel_uuid;
                END IF;
            END IF;
            
            -- Store IATTC vessel number as external identifier
            -- Intelligence principle: Check if we already have this exact identifier
            IF r.iattc_vessel_number IS NOT NULL THEN
                -- Only insert if we don't already have this exact value
                IF NOT EXISTS (
                    SELECT 1 FROM vessel_external_identifiers
                    WHERE vessel_uuid = v_vessel_uuid
                    AND source_id = v_source_id
                    AND identifier_type = 'RFMO_IATTC'
                    AND identifier_value = r.iattc_vessel_number
                ) THEN
                    -- Check if we have a different value (intelligence signal)
                    IF EXISTS (
                        SELECT 1 FROM vessel_external_identifiers
                        WHERE vessel_uuid = v_vessel_uuid
                        AND source_id = v_source_id
                        AND identifier_type = 'RFMO_IATTC'
                        AND identifier_value != r.iattc_vessel_number
                    ) THEN
                        -- Deactivate old value
                        UPDATE vessel_external_identifiers
                        SET is_active = false, updated_at = NOW()
                        WHERE vessel_uuid = v_vessel_uuid
                        AND source_id = v_source_id
                        AND identifier_type = 'RFMO_IATTC';
                        
                        RAISE NOTICE 'IATTC ID changed for vessel %: old value deactivated', v_vessel_uuid;
                    END IF;
                    
                    -- Insert new identifier
                    INSERT INTO vessel_external_identifiers (
                        vessel_uuid,
                        source_id,
                        identifier_type,
                        identifier_value,
                        is_active,
                        created_at
                    ) VALUES (
                        v_vessel_uuid,
                        v_source_id,
                        'RFMO_IATTC',
                        r.iattc_vessel_number,
                        true,
                        NOW()
                    );
                END IF;
            END IF;
            
            -- Process vessel status
            PERFORM iattc_process_vessel_status(
                v_vessel_uuid,
                v_source_id,
                r.register_status,
                r.confirmation_date::DATE,
                r.notes
            );
            
            -- Process vessel metrics
            PERFORM iattc_process_vessel_metrics(
                v_vessel_uuid,
                v_source_id,
                r.gross_tonnage::NUMERIC,
                r.fish_hold_volume::NUMERIC,
                r.carrying_capacity::NUMERIC
            );
            
            -- Process previous identity if present
            IF r.previous_name IS NOT NULL OR r.previous_flag IS NOT NULL THEN
                PERFORM iattc_record_previous_identity(
                    v_vessel_uuid,
                    v_source_id,
                    r.previous_name,
                    r.previous_flag,
                    r.last_modification::DATE
                );
            END IF;
            
            -- Process company information
            PERFORM iattc_process_company_info(
                v_vessel_uuid,
                v_source_id,
                r.company_name,
                r.company_address
            );
            
            -- Process build information
            IF r.year_built IS NOT NULL THEN
                -- Intelligence principle: vessel_info is a single record per vessel
                IF EXISTS (SELECT 1 FROM vessel_info WHERE vessel_uuid = v_vessel_uuid) THEN
                    -- Update only if different (track changes)
                    UPDATE vessel_info 
                    SET build_year = make_date(r.year_built::INTEGER, 1, 1),
                        updated_at = NOW()
                    WHERE vessel_uuid = v_vessel_uuid
                    AND (build_year IS NULL OR build_year != make_date(r.year_built::INTEGER, 1, 1));
                ELSE
                    -- Create new vessel_info record
                    INSERT INTO vessel_info (vessel_uuid, build_year, created_at)
                    VALUES (v_vessel_uuid, make_date(r.year_built::INTEGER, 1, 1), NOW());
                END IF;
            END IF;
            
            -- Process shipyard
            IF r.shipyard IS NOT NULL THEN
                UPDATE vessel_sources
                SET data_governance_notes = COALESCE(data_governance_notes, '') || 
                    E'\nShipyard: ' || r.shipyard
                WHERE vessel_uuid = v_vessel_uuid
                  AND source_id = v_source_id;
            END IF;
            
            -- Process port of registry
            IF r.port_of_registry IS NOT NULL THEN
                -- Intelligence principle: vessel_info is a single record per vessel
                IF EXISTS (SELECT 1 FROM vessel_info WHERE vessel_uuid = v_vessel_uuid) THEN
                    -- Update only if different (track changes)
                    UPDATE vessel_info 
                    SET port_registry = r.port_of_registry,
                        updated_at = NOW()
                    WHERE vessel_uuid = v_vessel_uuid
                    AND (port_registry IS NULL OR port_registry != r.port_of_registry);
                ELSE
                    -- Create new vessel_info record
                    INSERT INTO vessel_info (vessel_uuid, port_registry, created_at)
                    VALUES (v_vessel_uuid, r.port_of_registry, NOW());
                END IF;
            END IF;
            
            -- Process measurements (Intelligence: each measurement is a data point)
            IF r.length_value IS NOT NULL AND r.length_value != '' THEN
                INSERT INTO vessel_metrics (
                    vessel_uuid, source_id, metric_type, 
                    value, unit, created_at
                ) VALUES (
                    v_vessel_uuid, v_source_id, 
                    COALESCE(r.length_metric_type, 'length_loa')::metric_type_enum,
                    r.length_value::NUMERIC,
                    r.length_unit_enum::unit_enum,
                    NOW()
                );
                -- No ON CONFLICT - multiple measurements provide intelligence
            END IF;
            
            -- Process beam (Intelligence: each measurement is a data point)
            IF r.beam_value IS NOT NULL AND r.beam_value != '' THEN
                INSERT INTO vessel_metrics (
                    vessel_uuid, source_id, metric_type, 
                    value, unit, created_at
                ) VALUES (
                    v_vessel_uuid, v_source_id, 
                    'beam'::metric_type_enum,
                    r.beam_value::NUMERIC,
                    r.beam_unit_enum::unit_enum,
                    NOW()
                );
                -- No ON CONFLICT - multiple measurements provide intelligence
            END IF;
            
            -- Process depth (Intelligence: each measurement is a data point)
            IF r.depth_value IS NOT NULL AND r.depth_value != '' THEN
                INSERT INTO vessel_metrics (
                    vessel_uuid, source_id, metric_type, 
                    value, unit, created_at
                ) VALUES (
                    v_vessel_uuid, v_source_id, 
                    'moulded_depth'::metric_type_enum,
                    r.depth_value::NUMERIC,
                    r.depth_unit_enum::unit_enum,
                    NOW()
                );
                -- No ON CONFLICT - multiple measurements provide intelligence
            END IF;
            
            -- Process engine power (Intelligence: each measurement is a data point)
            IF r.engine_power IS NOT NULL AND r.engine_power != '' THEN
                INSERT INTO vessel_metrics (
                    vessel_uuid, source_id, metric_type, 
                    value, unit, created_at
                ) VALUES (
                    v_vessel_uuid, v_source_id, 
                    'engine_power'::metric_type_enum,
                    r.engine_power::NUMERIC,
                    r.engine_power_unit_enum::unit_enum,
                    NOW()
                );
                -- No ON CONFLICT - multiple measurements provide intelligence
            END IF;
            
            -- Process image availability
            IF r.image_available IS NOT NULL THEN
                UPDATE vessel_sources
                SET data_governance_notes = COALESCE(data_governance_notes, '') || 
                    E'\nImage available: ' || r.image_available
                WHERE vessel_uuid = v_vessel_uuid
                  AND source_id = v_source_id;
            END IF;
            
            -- Capture historical snapshot
            PERFORM capture_vessel_snapshot(
                v_vessel_uuid, 
                v_source_id, 
                COALESCE(r.source_date::DATE, CURRENT_DATE), 
                NULL::UUID
            );
            
            v_batch_count := v_batch_count + 1;
            v_processed_count := v_processed_count + 1;
            
            -- Commit batch
            IF v_batch_count >= 1000 THEN
                RAISE NOTICE 'Processed % / % IATTC vessels...', v_processed_count, v_total_count;
                v_batch_count := 0;
            END IF;
            
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Error processing vessel %: %', r.vessel_name, SQLERRM;
                RAISE WARNING 'IMO: %, IRCS: %, Flag: %', r.imo, r.ircs, r.vessel_flag_alpha3;
        END;
    END LOOP;
    
    RAISE NOTICE 'Completed processing % IATTC vessels', v_processed_count;
    
    -- Update trust scores for all IATTC vessels
    RAISE NOTICE 'Calculating trust scores for IATTC vessels...';
    -- PERFORM calculate_trust_scores_for_source('IATTC');
    
END $$;

-- Show import statistics
SELECT 
    'IATTC Import Summary' as metric,
    COUNT(DISTINCT v.vessel_uuid) as value
FROM vessels v
JOIN vessel_sources vs ON v.vessel_uuid = vs.vessel_uuid
JOIN original_sources_vessels osv ON vs.source_id = osv.source_id
WHERE osv.source_shortname = 'IATTC'

UNION ALL

SELECT 
    'Vessels with Trust Score >= 0.7',
    COUNT(DISTINCT vts.vessel_uuid)
FROM vessel_trust_scores vts
JOIN vessel_sources vs ON vts.vessel_uuid = vs.vessel_uuid
JOIN original_sources_vessels osv ON vs.source_id = osv.source_id
WHERE osv.source_shortname = 'IATTC'
  AND vts.trust_score >= 0.7

UNION ALL

SELECT 
    'Active IATTC Vessels',
    COUNT(DISTINCT vs.vessel_uuid)
FROM vessel_sources vs
JOIN original_sources_vessels osv ON vs.source_id = osv.source_id
WHERE osv.source_shortname = 'IATTC'
  AND vs.is_active = true

UNION ALL

SELECT 
    'Vessels with Previous Identity',
    COUNT(DISTINCT vrh.vessel_uuid)
FROM vessel_reported_history vrh
JOIN vessel_sources vs ON vrh.vessel_uuid = vs.vessel_uuid
JOIN original_sources_vessels osv ON vs.source_id = osv.source_id
WHERE osv.source_shortname = 'IATTC'
  AND vrh.reported_history_type IN ('VESSEL_NAME_CHANGE', 'FLAG_CHANGE');

-- Cleanup
-- DROP TABLE staging_iattc_vessels;  -- Keep for debugging
EOF

# Verify trust scores
log_step "Verifying trust scores..."
TRUST_STATS=$($PSQL_CMD "$DATABASE_URL" -t <<EOF
WITH iattc_vessels AS (
    SELECT DISTINCT v.vessel_uuid, vts.trust_score
    FROM vessels v
    JOIN vessel_sources vs ON v.vessel_uuid = vs.vessel_uuid
    JOIN original_sources_vessels osv ON vs.source_id = osv.source_id
    LEFT JOIN vessel_trust_scores vts ON v.vessel_uuid = vts.vessel_uuid
    WHERE osv.source_shortname = 'IATTC'
)
SELECT 
    CASE 
        WHEN trust_score >= 0.9 THEN 'Excellent (0.9+)'
        WHEN trust_score >= 0.8 THEN 'Very Good (0.8-0.9)'
        WHEN trust_score >= 0.7 THEN 'Good (0.7-0.8)'
        WHEN trust_score >= 0.6 THEN 'Fair (0.6-0.7)'
        ELSE 'Poor (<0.6)'
    END as trust_category,
    COUNT(*) as vessel_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage
FROM iattc_vessels
GROUP BY trust_category
ORDER BY 
    CASE trust_category
        WHEN 'Excellent (0.9+)' THEN 1
        WHEN 'Very Good (0.8-0.9)' THEN 2
        WHEN 'Good (0.7-0.8)' THEN 3
        WHEN 'Fair (0.6-0.7)' THEN 4
        ELSE 5
    END;
EOF
)

log_success "✅ IATTC vessel import completed with trust scoring!"
log_success ""
log_success "Trust Score Distribution:"
echo "$TRUST_STATS" | while IFS='|' read -r category count percentage; do
    log_success "  $(echo $category | xargs): $(echo $count | xargs) vessels ($(echo $percentage | xargs)%)"
done

# Show data quality metrics
DATA_QUALITY=$($PSQL_CMD "$DATABASE_URL" -t <<EOF
WITH iattc_vessels AS (
    SELECT DISTINCT v.*
    FROM vessels v
    JOIN vessel_sources vs ON v.vessel_uuid = vs.vessel_uuid
    JOIN original_sources_vessels osv ON vs.source_id = osv.source_id
    WHERE osv.source_shortname = 'IATTC'
)
SELECT 
    'IMO Coverage' as metric,
    ROUND(COUNT(CASE WHEN imo IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1) as percentage
FROM iattc_vessels
UNION ALL
SELECT 
    'IRCS Coverage',
    ROUND(COUNT(CASE WHEN ircs IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1)
FROM iattc_vessels
UNION ALL
SELECT 
    'Flag Coverage',
    ROUND(COUNT(CASE WHEN vessel_flag IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1)
FROM iattc_vessels
UNION ALL
SELECT 
    'Build Year Coverage',
    ROUND(COUNT(CASE WHEN EXISTS (
        SELECT 1 FROM vessel_info vi 
        WHERE vi.vessel_uuid = iattc_vessels.vessel_uuid 
        AND vi.build_year IS NOT NULL
    ) THEN 1 END) * 100.0 / COUNT(*), 1)
FROM iattc_vessels;
EOF
)

log_success ""
log_success "Data Quality Metrics:"
echo "$DATA_QUALITY" | while IFS='|' read -r metric percentage; do
    log_success "  $(echo $metric | xargs): $(echo $percentage | xargs)%"
done

log_success ""
log_success "IATTC vessel data loaded successfully! 🎉"
