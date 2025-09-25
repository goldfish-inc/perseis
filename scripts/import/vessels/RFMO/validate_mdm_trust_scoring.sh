#!/bin/bash
# /app/scripts/import/vessels/data/RFMO/validate_mdm_trust_scoring.sh
# Validate trust scoring accuracy in the vessel MDM system
set -euo pipefail

# Determine environment and set paths
if [ -f /.dockerenv ]; then
    source /app/scripts/core/logging.sh
else
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    source "$SCRIPT_DIR/../../../core/logging.sh"
fi

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

log_success "🔍 Validating MDM Trust Scoring System"

# 1. Validate trust score calculation logic
log_step "1. Validating trust score calculation logic..."
$PSQL_CMD "$DATABASE_URL" <<'EOF'
-- Test trust score components
WITH trust_components AS (
    SELECT 
        v.vessel_uuid,
        v.vessel_name,
        v.imo_number,
        vts.trust_score,
        vts.identifier_score,
        vts.source_score,
        vts.data_score,
        vts.consistency_score,
        vts.reputation_score,
        vts.blacklist_penalty,
        vts.data_completeness,
        -- Recalculate to verify
        (vts.identifier_score * 0.30 + 
         vts.source_score * 0.25 + 
         vts.data_score * 0.20 + 
         vts.consistency_score * 0.15 + 
         vts.reputation_score * 0.10 - 
         vts.blacklist_penalty) as calculated_score
    FROM vessels v
    JOIN vessel_trust_scores vts ON v.vessel_uuid = vts.vessel_uuid
    LIMIT 100
)
SELECT 
    COUNT(*) as total_checked,
    COUNT(CASE WHEN ABS(trust_score - calculated_score) < 0.001 THEN 1 END) as correct_calculations,
    COUNT(CASE WHEN ABS(trust_score - calculated_score) >= 0.001 THEN 1 END) as incorrect_calculations,
    MAX(ABS(trust_score - calculated_score)) as max_deviation
FROM trust_components;
EOF

# 2. Validate identifier scoring
log_step "2. Validating identifier scoring..."
$PSQL_CMD "$DATABASE_URL" <<'EOF'
-- Check identifier score distribution
WITH identifier_analysis AS (
    SELECT 
        v.vessel_uuid,
        v.imo_number IS NOT NULL as has_imo,
        v.call_sign IS NOT NULL as has_ircs,
        v.mmsi IS NOT NULL as has_mmsi,
        vts.identifier_score,
        -- Expected scores based on identifiers
        CASE 
            WHEN v.imo_number IS NOT NULL THEN 1.0
            WHEN v.call_sign IS NOT NULL THEN 0.7
            WHEN v.mmsi IS NOT NULL THEN 0.5
            ELSE 0.3
        END as expected_score
    FROM vessels v
    JOIN vessel_trust_scores vts ON v.vessel_uuid = vts.vessel_uuid
)
SELECT 
    has_imo,
    has_ircs,
    has_mmsi,
    COUNT(*) as vessel_count,
    ROUND(AVG(identifier_score)::numeric, 3) as avg_identifier_score,
    ROUND(AVG(expected_score)::numeric, 3) as expected_avg,
    ROUND(AVG(ABS(identifier_score - expected_score))::numeric, 3) as avg_deviation
FROM identifier_analysis
GROUP BY has_imo, has_ircs, has_mmsi
ORDER BY has_imo DESC, has_ircs DESC, has_mmsi DESC;
EOF

# 3. Validate source authority scoring
log_step "3. Validating source authority scoring..."
$PSQL_CMD "$DATABASE_URL" <<'EOF'
-- Analyze source scores by RFMO
SELECT 
    osv.source_shortname as source,
    osv.source_authority,
    COUNT(DISTINCT vs.vessel_uuid) as vessels,
    ROUND(AVG(vts.source_score)::numeric, 3) as avg_source_score,
    ROUND(MIN(vts.source_score)::numeric, 3) as min_source_score,
    ROUND(MAX(vts.source_score)::numeric, 3) as max_source_score
FROM vessel_sources vs
JOIN original_sources_vessels osv ON vs.source_id = osv.source_id
JOIN vessel_trust_scores vts ON vs.vessel_uuid = vts.vessel_uuid
WHERE osv.source_type = 'RFMO'
GROUP BY osv.source_shortname, osv.source_authority
ORDER BY osv.source_authority DESC, vessels DESC;
EOF

# 4. Validate blacklist penalty application
log_step "4. Validating blacklist penalty application..."
$PSQL_CMD "$DATABASE_URL" <<'EOF'
-- Check blacklist penalties
WITH blacklist_analysis AS (
    SELECT 
        v.vessel_uuid,
        v.vessel_name,
        vts.blacklist_penalty,
        vts.reputation_score,
        vts.trust_score,
        COUNT(DISTINCT vre.event_date) as blacklist_events,
        MAX(vre.event_date) as latest_blacklist,
        CURRENT_DATE - MAX(vre.event_date) as days_since_blacklist
    FROM vessels v
    JOIN vessel_trust_scores vts ON v.vessel_uuid = vts.vessel_uuid
    LEFT JOIN vessel_reputation_events vre ON v.vessel_uuid = vre.vessel_uuid 
        AND vre.event_type = 'BLACKLISTED'
    WHERE vts.blacklist_penalty > 0 OR vre.event_type IS NOT NULL
    GROUP BY v.vessel_uuid, v.vessel_name, vts.blacklist_penalty, vts.reputation_score, vts.trust_score
)
SELECT 
    CASE 
        WHEN days_since_blacklist <= 365 THEN 'Recent (< 1 year)'
        WHEN days_since_blacklist <= 1095 THEN 'Medium (1-3 years)'
        WHEN days_since_blacklist > 1095 THEN 'Old (> 3 years)'
        ELSE 'Never blacklisted'
    END as blacklist_age,
    COUNT(*) as vessel_count,
    ROUND(AVG(blacklist_penalty)::numeric, 3) as avg_penalty,
    ROUND(AVG(trust_score)::numeric, 3) as avg_trust_score,
    ROUND(MIN(trust_score)::numeric, 3) as min_trust_score,
    ROUND(MAX(trust_score)::numeric, 3) as max_trust_score
FROM blacklist_analysis
GROUP BY blacklist_age
ORDER BY 
    CASE blacklist_age
        WHEN 'Recent (< 1 year)' THEN 1
        WHEN 'Medium (1-3 years)' THEN 2
        WHEN 'Old (> 3 years)' THEN 3
        ELSE 4
    END;
EOF

# 5. Validate data completeness calculation
log_step "5. Validating data completeness calculation..."
$PSQL_CMD "$DATABASE_URL" <<'EOF'
-- Check data completeness accuracy
WITH completeness_check AS (
    SELECT 
        v.vessel_uuid,
        vts.data_completeness,
        -- Count filled fields
        (CASE WHEN v.vessel_name IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN v.imo_number IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN v.call_sign IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN v.mmsi IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN v.vessel_flag IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN v.vessel_type IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN v.year_built IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN v.port_of_registry IS NOT NULL THEN 1 ELSE 0 END)::numeric / 8.0 as calculated_completeness
    FROM vessels v
    JOIN vessel_trust_scores vts ON v.vessel_uuid = vts.vessel_uuid
)
SELECT 
    CASE 
        WHEN data_completeness >= 0.8 THEN 'High (80%+)'
        WHEN data_completeness >= 0.6 THEN 'Medium (60-80%)'
        WHEN data_completeness >= 0.4 THEN 'Low (40-60%)'
        ELSE 'Very Low (<40%)'
    END as completeness_category,
    COUNT(*) as vessel_count,
    ROUND(AVG(data_completeness)::numeric, 3) as avg_completeness,
    ROUND(AVG(calculated_completeness)::numeric, 3) as avg_calculated
FROM completeness_check
GROUP BY completeness_category
ORDER BY avg_completeness DESC;
EOF

# 6. Validate historical tracking impact
log_step "6. Validating historical tracking impact on trust scores..."
$PSQL_CMD "$DATABASE_URL" <<'EOF'
-- Analyze vessels with history changes
WITH history_impact AS (
    SELECT 
        v.vessel_uuid,
        v.vessel_name,
        vts.trust_score,
        vts.consistency_score,
        COUNT(DISTINCT vrh.change_type) as change_types,
        COUNT(vrh.history_id) as total_changes,
        STRING_AGG(DISTINCT vrh.change_type, ', ') as changes
    FROM vessels v
    JOIN vessel_trust_scores vts ON v.vessel_uuid = vts.vessel_uuid
    LEFT JOIN vessel_reported_history vrh ON v.vessel_uuid = vrh.vessel_uuid
    GROUP BY v.vessel_uuid, v.vessel_name, vts.trust_score, vts.consistency_score
)
SELECT 
    CASE 
        WHEN total_changes = 0 THEN 'No changes'
        WHEN total_changes <= 2 THEN 'Few changes (1-2)'
        WHEN total_changes <= 5 THEN 'Some changes (3-5)'
        ELSE 'Many changes (>5)'
    END as change_category,
    COUNT(*) as vessel_count,
    ROUND(AVG(trust_score)::numeric, 3) as avg_trust_score,
    ROUND(AVG(consistency_score)::numeric, 3) as avg_consistency_score,
    ROUND(MIN(consistency_score)::numeric, 3) as min_consistency,
    ROUND(MAX(consistency_score)::numeric, 3) as max_consistency
FROM history_impact
GROUP BY change_category
ORDER BY avg_trust_score DESC;
EOF

# 7. Validate vessel matching accuracy
log_step "7. Validating vessel matching accuracy..."
$PSQL_CMD "$DATABASE_URL" <<'EOF'
-- Check for potential duplicates with high trust scores
WITH vessel_pairs AS (
    SELECT 
        v1.vessel_uuid as vessel1_uuid,
        v1.vessel_name as vessel1_name,
        v1.imo_number as vessel1_imo,
        v1.call_sign as vessel1_ircs,
        v2.vessel_uuid as vessel2_uuid,
        v2.vessel_name as vessel2_name,
        v2.imo_number as vessel2_imo,
        v2.call_sign as vessel2_ircs,
        vts1.trust_score as vessel1_trust,
        vts2.trust_score as vessel2_trust,
        -- Similarity checks
        CASE 
            WHEN v1.imo_number IS NOT NULL AND v1.imo_number = v2.imo_number THEN 'IMO Match'
            WHEN v1.call_sign IS NOT NULL AND v1.call_sign = v2.call_sign THEN 'IRCS Match'
            WHEN v1.vessel_name IS NOT NULL AND v1.vessel_name = v2.vessel_name 
                 AND v1.vessel_flag = v2.vessel_flag THEN 'Name+Flag Match'
            ELSE 'No Clear Match'
        END as match_type
    FROM vessels v1
    JOIN vessels v2 ON v1.vessel_uuid < v2.vessel_uuid
    JOIN vessel_trust_scores vts1 ON v1.vessel_uuid = vts1.vessel_uuid
    JOIN vessel_trust_scores vts2 ON v2.vessel_uuid = vts2.vessel_uuid
    WHERE (
        -- Potential duplicates
        (v1.imo_number IS NOT NULL AND v1.imo_number = v2.imo_number) OR
        (v1.call_sign IS NOT NULL AND v1.call_sign = v2.call_sign) OR
        (v1.vessel_name IS NOT NULL AND v1.vessel_name = v2.vessel_name AND v1.vessel_flag = v2.vessel_flag)
    )
    AND vts1.trust_score >= 0.7 AND vts2.trust_score >= 0.7
)
SELECT 
    match_type,
    COUNT(*) as potential_duplicate_pairs,
    ROUND(AVG(LEAST(vessel1_trust, vessel2_trust))::numeric, 3) as avg_min_trust,
    ROUND(AVG(GREATEST(vessel1_trust, vessel2_trust))::numeric, 3) as avg_max_trust
FROM vessel_pairs
GROUP BY match_type
ORDER BY potential_duplicate_pairs DESC;
EOF

# 8. AI readiness validation
log_step "8. Validating AI training readiness criteria..."
$PSQL_CMD "$DATABASE_URL" <<'EOF'
-- Comprehensive AI readiness check
WITH ai_readiness AS (
    SELECT 
        v.vessel_uuid,
        v.imo_number,
        v.vessel_name,
        vts.trust_score,
        vts.data_completeness,
        vts.source_score,
        vts.identifier_score,
        vts.blacklist_penalty,
        -- AI criteria
        vts.trust_score >= 0.7 as meets_trust_threshold,
        vts.data_completeness >= 0.6 as meets_completeness_threshold,
        vts.blacklist_penalty = 0 as no_blacklist,
        vts.source_score >= 0.7 as reliable_source,
        -- Overall AI readiness
        (vts.trust_score >= 0.7 AND vts.data_completeness >= 0.6) as is_ai_ready
    FROM vessels v
    JOIN vessel_trust_scores vts ON v.vessel_uuid = vts.vessel_uuid
)
SELECT 
    'Total Vessels' as metric,
    COUNT(*) as count,
    '100.0' as percentage
FROM ai_readiness
UNION ALL
SELECT 
    'Meets Trust Threshold (≥0.7)',
    COUNT(CASE WHEN meets_trust_threshold THEN 1 END),
    ROUND(COUNT(CASE WHEN meets_trust_threshold THEN 1 END) * 100.0 / COUNT(*), 1)::text
FROM ai_readiness
UNION ALL
SELECT 
    'Meets Completeness Threshold (≥0.6)',
    COUNT(CASE WHEN meets_completeness_threshold THEN 1 END),
    ROUND(COUNT(CASE WHEN meets_completeness_threshold THEN 1 END) * 100.0 / COUNT(*), 1)::text
FROM ai_readiness
UNION ALL
SELECT 
    'No Blacklist History',
    COUNT(CASE WHEN no_blacklist THEN 1 END),
    ROUND(COUNT(CASE WHEN no_blacklist THEN 1 END) * 100.0 / COUNT(*), 1)::text
FROM ai_readiness
UNION ALL
SELECT 
    'Reliable Source (≥0.7)',
    COUNT(CASE WHEN reliable_source THEN 1 END),
    ROUND(COUNT(CASE WHEN reliable_source THEN 1 END) * 100.0 / COUNT(*), 1)::text
FROM ai_readiness
UNION ALL
SELECT 
    'AI Training Ready',
    COUNT(CASE WHEN is_ai_ready THEN 1 END),
    ROUND(COUNT(CASE WHEN is_ai_ready THEN 1 END) * 100.0 / COUNT(*), 1)::text
FROM ai_readiness
ORDER BY count DESC;
EOF

# 9. Trust score stability check
log_step "9. Checking trust score stability over time..."
$PSQL_CMD "$DATABASE_URL" <<'EOF'
-- Analyze trust score changes in historical snapshots
WITH score_changes AS (
    SELECT 
        vhs.vessel_uuid,
        vhs.snapshot_date,
        (vhs.snapshot_data->>'trust_score')::numeric as historical_trust,
        vts.trust_score as current_trust,
        ABS((vhs.snapshot_data->>'trust_score')::numeric - vts.trust_score) as score_change
    FROM vessel_historical_snapshots vhs
    JOIN vessel_trust_scores vts ON vhs.vessel_uuid = vts.vessel_uuid
    WHERE vhs.snapshot_data->>'trust_score' IS NOT NULL
)
SELECT 
    CASE 
        WHEN score_change < 0.01 THEN 'Stable (<0.01)'
        WHEN score_change < 0.05 THEN 'Minor change (0.01-0.05)'
        WHEN score_change < 0.10 THEN 'Moderate change (0.05-0.10)'
        ELSE 'Large change (>0.10)'
    END as change_category,
    COUNT(*) as snapshot_count,
    ROUND(AVG(score_change)::numeric, 4) as avg_change,
    ROUND(MAX(score_change)::numeric, 4) as max_change
FROM score_changes
GROUP BY change_category
ORDER BY avg_change;
EOF

# 10. Generate final validation summary
log_step "10. Generating validation summary..."
$PSQL_CMD "$DATABASE_URL" <<'EOF'
-- Overall MDM health check
WITH mdm_stats AS (
    SELECT 
        COUNT(DISTINCT v.vessel_uuid) as total_vessels,
        COUNT(DISTINCT vts.vessel_uuid) as scored_vessels,
        COUNT(DISTINCT CASE WHEN vts.trust_score >= 0.7 THEN vts.vessel_uuid END) as high_trust_vessels,
        COUNT(DISTINCT CASE WHEN vts.trust_score >= 0.7 AND vts.data_completeness >= 0.6 THEN vts.vessel_uuid END) as ai_ready_vessels,
        COUNT(DISTINCT vs.source_id) as active_sources,
        COUNT(DISTINCT vhs.vessel_uuid) as vessels_with_history,
        COUNT(DISTINCT vre.vessel_uuid) as vessels_with_reputation_events
    FROM vessels v
    LEFT JOIN vessel_trust_scores vts ON v.vessel_uuid = vts.vessel_uuid
    LEFT JOIN vessel_sources vs ON v.vessel_uuid = vs.vessel_uuid
    LEFT JOIN vessel_historical_snapshots vhs ON v.vessel_uuid = vhs.vessel_uuid
    LEFT JOIN vessel_reputation_events vre ON v.vessel_uuid = vre.vessel_uuid
)
SELECT 
    'MDM System Health' as category,
    'Total Vessels' as metric,
    total_vessels::text as value
FROM mdm_stats
UNION ALL
SELECT 
    'MDM System Health',
    'Trust Scored Vessels',
    scored_vessels || ' (' || ROUND(scored_vessels * 100.0 / total_vessels, 1) || '%)'
FROM mdm_stats
UNION ALL
SELECT 
    'MDM System Health',
    'High Trust Vessels (≥0.7)',
    high_trust_vessels || ' (' || ROUND(high_trust_vessels * 100.0 / total_vessels, 1) || '%)'
FROM mdm_stats
UNION ALL
SELECT 
    'MDM System Health',
    'AI Ready Vessels',
    ai_ready_vessels || ' (' || ROUND(ai_ready_vessels * 100.0 / total_vessels, 1) || '%)'
FROM mdm_stats
UNION ALL
SELECT 
    'MDM System Health',
    'Active Sources',
    active_sources::text
FROM mdm_stats
UNION ALL
SELECT 
    'MDM System Health',
    'Vessels with History Tracking',
    vessels_with_history || ' (' || ROUND(vessels_with_history * 100.0 / total_vessels, 1) || '%)'
FROM mdm_stats
UNION ALL
SELECT 
    'MDM System Health',
    'Vessels with Reputation Events',
    vessels_with_reputation_events || ' (' || ROUND(vessels_with_reputation_events * 100.0 / total_vessels, 1) || '%)'
FROM mdm_stats;
EOF

log_success ""
log_success "✅ MDM Trust Scoring Validation Complete!"