#!/bin/bash
# /app/scripts/import/vessels/data/RFMO/load_nafo_vessels.sh
# NAFO Vessel Data Loading Script with Trust Scoring
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

log_step "🐟 NAFO Vessel Data Loading with Trust Scoring"

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

INPUT_FILE="$CLEANED_DATA_DIR/nafo_vessels_cleaned.csv"
if [[ ! -f "$INPUT_FILE" ]]; then
    log_error "Cleaned file not found: $INPUT_FILE"
    log_error "Please run clean_nafo_vessels.sh first"
    exit 1
fi

log_success "Loading NAFO vessels from: $(basename "$INPUT_FILE")"

# Get source ID for NAFO
SOURCE_ID=$($PSQL_CMD "$DATABASE_URL" -t -c "
    SELECT source_id 
    FROM original_sources_vessels 
    WHERE source_shortname = 'NAFO'
")

if [[ -z "$SOURCE_ID" ]]; then
    log_error "NAFO source not found in original_sources_vessels"
    exit 1
fi

SOURCE_ID=$(echo "$SOURCE_ID" | xargs)
log_success "Using NAFO source ID: $SOURCE_ID"

# Verify setup functions exist
log_step "Verifying NAFO setup functions..."
$PSQL_CMD "$DATABASE_URL" -c "\df nafo_*" > /dev/null || {
    log_error "NAFO setup functions not found. Please run setup_nafo_loading.sql first"
    exit 1
}

# Create staging table
log_step "Creating staging table..."
$PSQL_CMD "$DATABASE_URL" <<EOF
DROP TABLE IF EXISTS staging_nafo_vessels;

CREATE TABLE staging_nafo_vessels (
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
    
    -- NAFO external identifiers
    nafo_id TEXT,
    authorization_status TEXT,
    
    -- Vessel characteristics
    vessel_type_code TEXT,
    gear_type_code TEXT,
    port_of_registry TEXT,
    year_built TEXT,
    
    -- Measurements
    length_value TEXT,
    length_metric_type TEXT,
    length_unit_enum TEXT,
    
    gross_tonnage TEXT,
    tonnage_metric_type TEXT,
    
    engine_power TEXT,
    engine_power_unit_enum TEXT,
    
    -- Ownership
    owner_name TEXT,
    manager_name TEXT,
    
    -- NAFO specific
    nafo_divisions TEXT,
    species_quotas TEXT,
    notification_date TEXT
);
EOF

# Load data into staging table
log_step "Loading data into staging table..."
if [ -f /.dockerenv ]; then
    # Docker environment - use \copy
    $PSQL_CMD "$DATABASE_URL" <<EOF
\copy staging_nafo_vessels FROM '$INPUT_FILE' WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', ESCAPE '\\');
EOF
else
    # Local/Crunchy Bridge - use cat and COPY FROM STDIN
    cat "$INPUT_FILE" | $PSQL_CMD "$DATABASE_URL" -c "COPY staging_nafo_vessels FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '\"', ESCAPE '\\');"
fi

# Get staging count
STAGING_COUNT=$($PSQL_CMD "$DATABASE_URL" -t -c "SELECT COUNT(*) FROM staging_nafo_vessels")
log_success "Loaded $STAGING_COUNT records into staging"

# Process vessels with trust scoring
log_step "Processing NAFO vessels with trust scoring..."
$PSQL_CMD "$DATABASE_URL" <<'EOF'
-- Load NAFO vessels with proper matching and trust scoring
DO $$
DECLARE
    v_vessel_uuid UUID;
    v_flag_id UUID;
    v_vessel_type_id UUID;
    v_batch_count INTEGER := 0;
    v_processed_count INTEGER := 0;
    v_total_count INTEGER;
    v_source_id UUID := (SELECT source_id FROM original_sources_vessels WHERE source_shortname = 'NAFO');
    r RECORD;
BEGIN
    SELECT COUNT(*) INTO v_total_count FROM staging_nafo_vessels;
    RAISE NOTICE 'Starting NAFO vessel processing for % vessels', v_total_count;
    
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
        FROM staging_nafo_vessels
        ORDER BY 
            COALESCE(imo, ''), 
            COALESCE(ircs, ''), 
            COALESCE(mmsi, ''),
            COALESCE(vessel_name, ''),
            COALESCE(vessel_flag_alpha3, ''),
            match_priority,
            notification_date DESC NULLS LAST
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
            
            -- Determine vessel type
            v_vessel_type_id := NULL;
            SELECT vessel_type_id INTO v_vessel_type_id
            FROM vessel_types
            WHERE code = nafo_determine_vessel_type(r.vessel_type_code, r.gear_type_code);
            
            -- Find or create vessel with trust scoring
            v_vessel_uuid := find_or_create_vessel_with_trust(
                r.vessel_name,
                r.imo::INTEGER,
                r.ircs,
                r.mmsi::BIGINT,
                v_flag_id,
                v_vessel_type_id
            );
            
            -- Always record presence in NAFO source
            PERFORM record_vessel_presence_in_source(
                v_vessel_uuid,
                v_source_id,
                COALESCE(r.source_date::DATE, CURRENT_DATE),
                r.nafo_id
            );
            
            -- Process authorization and divisions
            PERFORM nafo_process_authorization(
                v_vessel_uuid,
                v_source_id,
                r.authorization_status,
                r.nafo_divisions,
                r.notification_date::DATE
            );
            
            -- Process species quotas
            PERFORM nafo_process_species_quotas(
                v_vessel_uuid,
                v_source_id,
                r.species_quotas
            );
            
            -- Process vessel metrics
            PERFORM nafo_process_vessel_metrics(
                v_vessel_uuid,
                v_source_id,
                r.gross_tonnage::NUMERIC,
                r.length_value::NUMERIC,
                r.engine_power::NUMERIC,
                r.engine_power_unit_enum
            );
            
            -- Process operators
            PERFORM nafo_process_vessel_operators(
                v_vessel_uuid,
                v_source_id,
                r.owner_name,
                r.manager_name
            );
            
            -- Process build information
            IF r.year_built IS NOT NULL THEN
                UPDATE vessels
                SET year_built = r.year_built::INTEGER
                WHERE vessel_uuid = v_vessel_uuid
                  AND (year_built IS NULL OR year_built != r.year_built::INTEGER);
            END IF;
            
            -- Process port of registry
            IF r.port_of_registry IS NOT NULL THEN
                UPDATE vessels
                SET port_of_registry = r.port_of_registry
                WHERE vessel_uuid = v_vessel_uuid
                  AND (port_of_registry IS NULL OR port_of_registry != r.port_of_registry);
            END IF;
            
            -- Process gear type
            IF r.gear_type_code IS NOT NULL THEN
                INSERT INTO vessel_gear_types (
                    vessel_uuid,
                    source_id,
                    gear_type_code,
                    is_primary,
                    created_at
                ) VALUES (
                    v_vessel_uuid,
                    v_source_id,
                    r.gear_type_code,
                    true,
                    NOW()
                ) ON CONFLICT (vessel_uuid, source_id, gear_type_code) DO NOTHING;
            END IF;
            
            -- Process national registry
            IF r.national_registry IS NOT NULL THEN
                INSERT INTO vessel_registrations (
                    vessel_uuid,
                    source_id,
                    registration_number,
                    registration_type,
                    created_at
                ) VALUES (
                    v_vessel_uuid,
                    v_source_id,
                    r.national_registry,
                    'NATIONAL',
                    NOW()
                ) ON CONFLICT (vessel_uuid, source_id, registration_number) DO NOTHING;
            END IF;
            
            -- Process MMSI
            IF r.mmsi IS NOT NULL AND r.mmsi != '' THEN
                UPDATE vessels
                SET mmsi = r.mmsi::BIGINT
                WHERE vessel_uuid = v_vessel_uuid
                  AND (mmsi IS NULL OR mmsi != r.mmsi::BIGINT);
            END IF;
            
            -- Capture historical snapshot
            PERFORM capture_vessel_snapshot(v_vessel_uuid, v_source_id);
            
            v_batch_count := v_batch_count + 1;
            v_processed_count := v_processed_count + 1;
            
            -- Commit batch
            IF v_batch_count >= 1000 THEN
                RAISE NOTICE 'Processed % / % NAFO vessels...', v_processed_count, v_total_count;
                v_batch_count := 0;
            END IF;
            
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Error processing vessel %: %', r.vessel_name, SQLERRM;
                RAISE WARNING 'IMO: %, IRCS: %, Flag: %', r.imo, r.ircs, r.vessel_flag_alpha3;
        END;
    END LOOP;
    
    RAISE NOTICE 'Completed processing % NAFO vessels', v_processed_count;
    
    -- Update trust scores for all NAFO vessels
    RAISE NOTICE 'Calculating trust scores for NAFO vessels...';
    PERFORM calculate_trust_scores_for_source('NAFO');
    
END $$;

-- Show import statistics
SELECT 
    'NAFO Import Summary' as metric,
    COUNT(DISTINCT v.vessel_uuid) as value
FROM vessels v
JOIN vessel_sources vs ON v.vessel_uuid = vs.vessel_uuid
JOIN original_sources_vessels osv ON vs.source_id = osv.source_id
WHERE osv.source_shortname = 'NAFO'

UNION ALL

SELECT 
    'Vessels with Trust Score >= 0.7',
    COUNT(DISTINCT vts.vessel_uuid)
FROM vessel_trust_scores vts
JOIN vessel_sources vs ON vts.vessel_uuid = vs.vessel_uuid
JOIN original_sources_vessels osv ON vs.source_id = osv.source_id
WHERE osv.source_shortname = 'NAFO'
  AND vts.trust_score >= 0.7

UNION ALL

SELECT 
    'Active/Notified Vessels',
    COUNT(DISTINCT vs.vessel_uuid)
FROM vessel_sources vs
JOIN original_sources_vessels osv ON vs.source_id = osv.source_id
WHERE osv.source_shortname = 'NAFO'
  AND vs.is_active = true

UNION ALL

SELECT 
    'Vessels with NAFO Divisions',
    COUNT(DISTINCT vs.vessel_uuid)
FROM vessel_sources vs
JOIN original_sources_vessels osv ON vs.source_id = osv.source_id
WHERE osv.source_shortname = 'NAFO'
  AND vs.data_governance_notes ILIKE '%NAFO Divisions:%';

-- Cleanup
DROP TABLE staging_nafo_vessels;
EOF

# Verify trust scores
log_step "Verifying trust scores..."
TRUST_STATS=$($PSQL_CMD "$DATABASE_URL" -t <<EOF
WITH nafo_vessels AS (
    SELECT DISTINCT v.vessel_uuid, vts.trust_score
    FROM vessels v
    JOIN vessel_sources vs ON v.vessel_uuid = vs.vessel_uuid
    JOIN original_sources_vessels osv ON vs.source_id = osv.source_id
    LEFT JOIN vessel_trust_scores vts ON v.vessel_uuid = vts.vessel_uuid
    WHERE osv.source_shortname = 'NAFO'
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
FROM nafo_vessels
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

log_success "✅ NAFO vessel import completed with trust scoring!"
log_success ""
log_success "Trust Score Distribution:"
echo "$TRUST_STATS" | while IFS='|' read -r category count percentage; do
    log_success "  $(echo $category | xargs): $(echo $count | xargs) vessels ($(echo $percentage | xargs)%)"
done

# Show data quality metrics
DATA_QUALITY=$($PSQL_CMD "$DATABASE_URL" -t <<EOF
WITH nafo_vessels AS (
    SELECT DISTINCT v.*
    FROM vessels v
    JOIN vessel_sources vs ON v.vessel_uuid = vs.vessel_uuid
    JOIN original_sources_vessels osv ON vs.source_id = osv.source_id
    WHERE osv.source_shortname = 'NAFO'
)
SELECT 
    'IMO Coverage' as metric,
    ROUND(COUNT(CASE WHEN imo_number IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1) as percentage
FROM nafo_vessels
UNION ALL
SELECT 
    'IRCS Coverage',
    ROUND(COUNT(CASE WHEN call_sign IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1)
FROM nafo_vessels
UNION ALL
SELECT 
    'MMSI Coverage',
    ROUND(COUNT(CASE WHEN mmsi IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1)
FROM nafo_vessels
UNION ALL
SELECT 
    'Flag Coverage',
    ROUND(COUNT(CASE WHEN vessel_flag IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1)
FROM nafo_vessels;
EOF
)

log_success ""
log_success "Data Quality Metrics:"
echo "$DATA_QUALITY" | while IFS='|' read -r metric percentage; do
    log_success "  $(echo $metric | xargs): $(echo $percentage | xargs)%"
done

log_success ""
log_success "NAFO vessel data loaded successfully! 🎉"
