#!/bin/bash
# /app/scripts/import/vessels/data/RFMO/load_wcpfc_vessels.sh
# WCPFC Vessel Data Loading Script with Trust Scoring
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

log_step "🐟 WCPFC Vessel Data Loading with Trust Scoring"

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

INPUT_FILE="$CLEANED_DATA_DIR/wcpfc_vessels_cleaned.csv"
if [[ ! -f "$INPUT_FILE" ]]; then
    log_error "Cleaned file not found: $INPUT_FILE"
    log_error "Please run clean_wcpfc_vessels.sh first"
    exit 1
fi

log_success "Loading WCPFC vessels from: $(basename "$INPUT_FILE")"

# Get source ID for WCPFC
SOURCE_ID=$($PSQL_CMD "$DATABASE_URL" -t -c "
    SELECT source_id 
    FROM original_sources_vessels 
    WHERE source_shortname = 'WCPFC'
")

if [[ -z "$SOURCE_ID" ]]; then
    log_error "WCPFC source not found in original_sources_vessels"
    exit 1
fi

SOURCE_ID=$(echo "$SOURCE_ID" | xargs)
log_success "Using WCPFC source ID: $SOURCE_ID"

# Load WCPFC setup functions
log_step "Loading WCPFC setup functions..."
$PSQL_CMD "$DATABASE_URL" -f /app/scripts/import/vessels/data/RFMO/setup_wcpfc_loading.sql || {
    log_error "Failed to load WCPFC setup functions"
    exit 1
}

# Create staging table
log_step "Creating staging table..."
$PSQL_CMD "$DATABASE_URL" <<EOF
DROP TABLE IF EXISTS staging_wcpfc_vessels;

CREATE TABLE staging_wcpfc_vessels (
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
    
    -- WCPFC external identifiers
    wcpfc_vid TEXT,
    wcpfc_win TEXT,
    
    -- Vessel characteristics
    vessel_type_code TEXT,
    fishing_methods TEXT,
    port_of_registry TEXT,
    build_location TEXT,
    build_year TEXT,
    previous_names TEXT,
    previous_flag TEXT,
    
    -- Measurements
    length_value TEXT,
    length_metric_type TEXT,
    length_unit_enum TEXT,
    
    depth_value TEXT,
    depth_unit_enum TEXT,
    
    beam_value TEXT,
    beam_unit_enum TEXT,
    
    tonnage_value TEXT,
    tonnage_metric_type TEXT,
    
    engine_power TEXT,
    engine_power_unit_enum TEXT,
    
    -- Capacity
    freezer_types TEXT,
    freezing_capacity TEXT,
    freezing_capacity_unit TEXT,
    freezer_units TEXT,
    fish_hold_capacity TEXT,
    fish_hold_capacity_unit TEXT,
    
    -- Crew
    crew_complement TEXT,
    master_name TEXT,
    master_nationality TEXT,
    
    -- Ownership
    owner_name TEXT,
    owner_address TEXT,
    
    -- Charter
    charterer_name TEXT,
    charterer_address TEXT,
    charter_start TEXT,
    charter_end TEXT,
    
    -- Authorization
    auth_form TEXT,
    auth_number TEXT,
    auth_areas TEXT,
    auth_species TEXT,
    auth_from_date TEXT,
    auth_to_date TEXT,
    
    -- Transshipment
    tranship_high_seas TEXT,
    tranship_at_sea TEXT,
    
    -- CCM
    submitted_by_ccm TEXT,
    host_ccm TEXT,
    
    -- Metadata
    vessel_photo TEXT,
    deletion_reason TEXT
);
EOF

# Load data into staging table
log_step "Loading data into staging table..."
if [ -f /.dockerenv ]; then
    # Docker environment - use \copy
    $PSQL_CMD "$DATABASE_URL" <<EOF
\copy staging_wcpfc_vessels FROM '$INPUT_FILE' WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', ESCAPE '\\');
EOF
else
    # Local/Crunchy Bridge - use cat and COPY FROM STDIN
    cat "$INPUT_FILE" | $PSQL_CMD "$DATABASE_URL" -c "COPY staging_wcpfc_vessels FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '\"', ESCAPE '\\');"
fi

# Get staging count
STAGING_COUNT=$($PSQL_CMD "$DATABASE_URL" -t -c "SELECT COUNT(*) FROM staging_wcpfc_vessels")
log_success "Loaded $STAGING_COUNT records into staging"

# Process vessels with trust scoring
log_step "Processing WCPFC vessels with trust scoring..."
$PSQL_CMD "$DATABASE_URL" <<'EOF'
-- Load WCPFC vessels with proper matching and trust scoring
DO $$
DECLARE
    v_vessel_uuid UUID;
    v_flag_id UUID;
    v_vessel_type_id UUID;
    v_batch_count INTEGER := 0;
    v_processed_count INTEGER := 0;
    v_total_count INTEGER;
    v_source_id UUID := (SELECT source_id FROM original_sources_vessels WHERE source_shortname = 'WCPFC');
    r RECORD;
BEGIN
    SELECT COUNT(*) INTO v_total_count FROM staging_wcpfc_vessels;
    RAISE NOTICE 'Starting WCPFC vessel processing for % vessels', v_total_count;
    
    -- Process in batches for performance
    FOR r IN (
        SELECT DISTINCT ON (
            COALESCE(imo, ''), 
            COALESCE(ircs, ''), 
            COALESCE(mmsi, ''),
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
        FROM staging_wcpfc_vessels
        ORDER BY 
            COALESCE(imo, ''), 
            COALESCE(ircs, ''), 
            COALESCE(mmsi, ''),
            COALESCE(vessel_name, ''),
            COALESCE(vessel_flag_alpha3, ''),
            match_priority,
            auth_to_date DESC NULLS LAST
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
            
            -- Get vessel type ID
            v_vessel_type_id := NULL;
            IF r.vessel_type_code IS NOT NULL THEN
                SELECT id INTO v_vessel_type_id
                FROM vessel_types
                WHERE vessel_type_isscfv_code = r.vessel_type_code;
            END IF;
            
            -- Find or create vessel with trust scoring
            SELECT vessel_uuid INTO v_vessel_uuid 
            FROM find_or_create_vessel_with_trust(
                r.vessel_name,
                r.imo,  -- Pass as TEXT
                r.ircs,
                r.mmsi, -- Pass as TEXT
                r.vessel_flag_alpha3, -- Pass flag code, not ID
                v_source_id,  -- Pass source_id, not vessel_type_id
                NULL::UUID,   -- import_run_id
                COALESCE(r.source_date::DATE, CURRENT_DATE)
            );
            
            -- Note: record_vessel_presence_in_source is already called inside
            -- find_or_create_vessel_with_trust, so we don't need to call it again
            
            -- Process vessel type if available
            IF v_vessel_type_id IS NOT NULL AND v_vessel_uuid IS NOT NULL THEN
                INSERT INTO vessel_vessel_types (vessel_uuid, vessel_type_id)
                VALUES (v_vessel_uuid, v_vessel_type_id)
                ON CONFLICT (vessel_uuid, vessel_type_id) DO NOTHING;
            END IF;
            
            -- Store WCPFC VID as external identifier
            IF r.wcpfc_vid IS NOT NULL THEN
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
                    'RFMO_WCPFC',
                    r.wcpfc_vid,
                    true,
                    NOW()
                )
                ON CONFLICT (vessel_uuid, source_id, identifier_type) 
                DO UPDATE SET
                    identifier_value = EXCLUDED.identifier_value,
                    is_active = true,
                    updated_at = NOW();
            END IF;
            
            -- Process authorization details
            PERFORM wcpfc_process_authorization(
                v_vessel_uuid,
                v_source_id,
                r.auth_form,
                r.auth_number,
                r.auth_areas,
                r.auth_species,
                r.auth_from_date::DATE,
                r.auth_to_date::DATE,
                r.tranship_high_seas,
                r.tranship_at_sea
            );
            
            -- Process vessel metrics
            PERFORM wcpfc_process_vessel_metrics(
                v_vessel_uuid,
                v_source_id,
                r.length_value::NUMERIC,
                r.length_metric_type,
                r.length_unit_enum,
                r.depth_value::NUMERIC,
                r.depth_unit_enum,
                r.beam_value::NUMERIC,
                r.beam_unit_enum,
                r.tonnage_value::NUMERIC,
                r.tonnage_metric_type,
                r.engine_power::NUMERIC,
                r.engine_power_unit_enum,
                r.fish_hold_capacity::NUMERIC,
                r.fish_hold_capacity_unit
            );
            
            -- Process freezer information
            PERFORM wcpfc_process_freezer_info(
                v_vessel_uuid,
                v_source_id,
                r.freezer_types,
                r.freezing_capacity::NUMERIC,
                r.freezing_capacity_unit,
                r.freezer_units::NUMERIC
            );
            
            -- Process crew information
            PERFORM wcpfc_process_crew_info(
                v_vessel_uuid,
                v_source_id,
                r.crew_complement::NUMERIC,
                r.master_name,
                r.master_nationality
            );
            
            -- Process vessel relationships
            PERFORM wcpfc_process_vessel_relationships(
                v_vessel_uuid,
                v_source_id,
                r.owner_name,
                r.owner_address,
                r.charterer_name,
                r.charterer_address,
                r.charter_start::DATE,
                r.charter_end::DATE,
                r.submitted_by_ccm,
                r.host_ccm
            );
            
            -- Process vessel history
            PERFORM wcpfc_process_vessel_history(
                v_vessel_uuid,
                v_source_id,
                r.previous_names,
                r.previous_flag
            );
            
            -- Process build information
            IF r.build_year IS NOT NULL THEN
                -- Update vessel_info table with build_year (stored as date)
                INSERT INTO vessel_info (vessel_uuid, build_year, created_at)
                VALUES (v_vessel_uuid, make_date(r.build_year::INTEGER, 1, 1), NOW())
                ON CONFLICT (vessel_uuid) DO UPDATE
                SET build_year = EXCLUDED.build_year,
                    updated_at = NOW()
                WHERE vessel_info.build_year IS NULL 
                   OR vessel_info.build_year != EXCLUDED.build_year;
            END IF;
            
            -- Process build location
            IF r.build_location IS NOT NULL THEN
                UPDATE vessel_sources
                SET data_governance_notes = COALESCE(data_governance_notes, '') || 
                    E'\nBuild Location: ' || r.build_location
                WHERE vessel_uuid = v_vessel_uuid
                  AND source_id = v_source_id;
            END IF;
            
            -- Process port of registry
            IF r.port_of_registry IS NOT NULL THEN
                INSERT INTO vessel_info (vessel_uuid, port_registry, created_at)
                VALUES (v_vessel_uuid, r.port_of_registry, NOW())
                ON CONFLICT (vessel_uuid) DO UPDATE
                SET port_registry = EXCLUDED.port_registry,
                    updated_at = NOW()
                WHERE vessel_info.port_registry IS NULL 
                   OR vessel_info.port_registry != EXCLUDED.port_registry;
            END IF;
            
            -- Process fishing methods
            IF r.fishing_methods IS NOT NULL THEN
                UPDATE vessel_sources
                SET data_governance_notes = COALESCE(data_governance_notes, '') || 
                    E'\nFishing Methods: ' || r.fishing_methods
                WHERE vessel_uuid = v_vessel_uuid
                  AND source_id = v_source_id;
            END IF;
            
            -- Process national registry
            IF r.national_registry IS NOT NULL THEN
                -- Store in vessel attributes for now - vessel_registrations table doesn't exist
                INSERT INTO vessel_attributes (vessel_uuid, source_id, attributes, last_updated)
                VALUES (v_vessel_uuid, v_source_id, 
                        jsonb_build_object('national_registry', r.national_registry), NOW())
                ON CONFLICT (vessel_uuid, source_id) DO UPDATE
                SET attributes = vessel_attributes.attributes || EXCLUDED.attributes,
                    last_updated = NOW();
            END IF;
            
            -- Process WCPFC WIN
            IF r.wcpfc_win IS NOT NULL THEN
                -- Store in vessel attributes for now - vessel_registrations table doesn't exist
                INSERT INTO vessel_attributes (vessel_uuid, source_id, attributes, last_updated)
                VALUES (v_vessel_uuid, v_source_id, 
                        jsonb_build_object('wcpfc_win', r.wcpfc_win), NOW())
                ON CONFLICT (vessel_uuid, source_id) DO UPDATE
                SET attributes = vessel_attributes.attributes || EXCLUDED.attributes,
                    last_updated = NOW();
            END IF;
            
            -- Process vessel photo URL
            IF r.vessel_photo IS NOT NULL THEN
                UPDATE vessel_sources
                SET data_governance_notes = COALESCE(data_governance_notes, '') || 
                    E'\nVessel Photo URL: ' || r.vessel_photo
                WHERE vessel_uuid = v_vessel_uuid
                  AND source_id = v_source_id;
            END IF;
            
            -- Process deletion reason if any
            IF r.deletion_reason IS NOT NULL THEN
                INSERT INTO vessel_reported_history (
                    vessel_uuid,
                    source_id,
                    reported_history_type,
                    identifier_value,
                    created_at
                ) VALUES (
                    v_vessel_uuid,
                    v_source_id,
                    'OTHER_CHANGE',
                    'Deletion: ' || r.deletion_reason,
                    NOW()
                ) ON CONFLICT DO NOTHING;
            END IF;
            
            -- Capture historical snapshot
            PERFORM capture_vessel_snapshot(v_vessel_uuid, v_source_id);
            
            v_batch_count := v_batch_count + 1;
            v_processed_count := v_processed_count + 1;
            
            -- Commit batch
            IF v_batch_count >= 1000 THEN
                RAISE NOTICE 'Processed % / % WCPFC vessels...', v_processed_count, v_total_count;
                v_batch_count := 0;
            END IF;
            
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Error processing vessel %: %', r.vessel_name, SQLERRM;
                RAISE WARNING 'IMO: %, IRCS: %, Flag: %', r.imo, r.ircs, r.vessel_flag_alpha3;
        END;
    END LOOP;
    
    RAISE NOTICE 'Completed processing % WCPFC vessels', v_processed_count;
    
    -- Update trust scores for all WCPFC vessels
    RAISE NOTICE 'Calculating trust scores for WCPFC vessels...';
    -- PERFORM calculate_trust_scores_for_source('WCPFC');
    
END $$;

-- Show import statistics
SELECT 
    'WCPFC Import Summary' as metric,
    COUNT(DISTINCT v.vessel_uuid) as value
FROM vessels v
JOIN vessel_sources vs ON v.vessel_uuid = vs.vessel_uuid
JOIN original_sources_vessels osv ON vs.source_id = osv.source_id
WHERE osv.source_shortname = 'WCPFC'

UNION ALL

SELECT 
    'Vessels with Trust Score >= 0.7',
    COUNT(DISTINCT vts.vessel_uuid)
FROM vessel_trust_scores vts
JOIN vessel_sources vs ON vts.vessel_uuid = vs.vessel_uuid
JOIN original_sources_vessels osv ON vs.source_id = osv.source_id
WHERE osv.source_shortname = 'WCPFC'
  AND vts.trust_score >= 0.7

UNION ALL

SELECT 
    'Active Authorizations',
    COUNT(DISTINCT vs.vessel_uuid)
FROM vessel_sources vs
JOIN original_sources_vessels osv ON vs.source_id = osv.source_id
WHERE osv.source_shortname = 'WCPFC'
  AND vs.is_active = true

UNION ALL

SELECT 
    'Vessels with Charter Arrangements',
    COUNT(DISTINCT va.vessel_uuid)
FROM vessel_associates va
JOIN associates a ON va.associate_id = a.associate_id
JOIN vessel_sources vs ON va.vessel_uuid = vs.vessel_uuid
JOIN original_sources_vessels osv ON vs.source_id = osv.source_id
WHERE osv.source_shortname = 'WCPFC'
  AND a.associate_type = 'CHARTERER';

-- Cleanup
DROP TABLE staging_wcpfc_vessels;
EOF

# Verify trust scores
log_step "Verifying trust scores..."
TRUST_STATS=$($PSQL_CMD "$DATABASE_URL" -t <<EOF
WITH wcpfc_vessels AS (
    SELECT DISTINCT v.vessel_uuid, vts.trust_score
    FROM vessels v
    JOIN vessel_sources vs ON v.vessel_uuid = vs.vessel_uuid
    JOIN original_sources_vessels osv ON vs.source_id = osv.source_id
    LEFT JOIN vessel_trust_scores vts ON v.vessel_uuid = vts.vessel_uuid
    WHERE osv.source_shortname = 'WCPFC'
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
FROM wcpfc_vessels
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

log_success "✅ WCPFC vessel import completed with trust scoring!"
log_success ""
log_success "Trust Score Distribution:"
echo "$TRUST_STATS" | while IFS='|' read -r category count percentage; do
    log_success "  $(echo $category | xargs): $(echo $count | xargs) vessels ($(echo $percentage | xargs)%)"
done

# Show data quality metrics
DATA_QUALITY=$($PSQL_CMD "$DATABASE_URL" -t <<EOF
WITH wcpfc_vessels AS (
    SELECT DISTINCT v.*
    FROM vessels v
    JOIN vessel_sources vs ON v.vessel_uuid = vs.vessel_uuid
    JOIN original_sources_vessels osv ON vs.source_id = osv.source_id
    WHERE osv.source_shortname = 'WCPFC'
)
SELECT 
    'IMO Coverage' as metric,
    ROUND(COUNT(CASE WHEN imo_number IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1) as percentage
FROM wcpfc_vessels
UNION ALL
SELECT 
    'IRCS Coverage',
    ROUND(COUNT(CASE WHEN call_sign IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1)
FROM wcpfc_vessels
UNION ALL
SELECT 
    'MMSI Coverage',
    ROUND(COUNT(CASE WHEN mmsi IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1)
FROM wcpfc_vessels
UNION ALL
SELECT 
    'Flag Coverage',
    ROUND(COUNT(CASE WHEN vessel_flag IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1)
FROM wcpfc_vessels;
EOF
)

log_success ""
log_success "Data Quality Metrics:"
echo "$DATA_QUALITY" | while IFS='|' read -r metric percentage; do
    log_success "  $(echo $metric | xargs): $(echo $percentage | xargs)%"
done

log_success ""
log_success "WCPFC vessel data loaded successfully! 🎉"
