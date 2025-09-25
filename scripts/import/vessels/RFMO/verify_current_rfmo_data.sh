#!/bin/bash
# Verify current RFMO data in the database
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/../../../core/logging.sh"

if [ -z "${DATABASE_URL:-}" ]; then
    log_error "DATABASE_URL not set"
    exit 1
fi

log_success "🔍 Verifying Current RFMO Data in Database"
log_success "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Check what RFMOs are in the database
psql "$DATABASE_URL" <<EOF
-- Summary of RFMO sources
WITH rfmo_summary AS (
    SELECT 
        osv.source_shortname as rfmo,
        osv.source_id,
        COUNT(DISTINCT vs.vessel_uuid) as vessel_count,
        COUNT(DISTINCT CASE WHEN vs.is_active THEN vs.vessel_uuid END) as active_vessels,
        COUNT(DISTINCT vts.vessel_uuid) as trust_scored,
        COUNT(DISTINCT CASE WHEN vts.trust_score >= 0.7 THEN vts.vessel_uuid END) as high_trust,
        ROUND(AVG(vts.trust_score)::numeric, 3) as avg_trust_score,
        ROUND(AVG(vts.data_completeness)::numeric, 3) as avg_completeness,
        MAX(vs.created_at) as last_import,
        COUNT(DISTINCT vhs.vessel_uuid) as with_history
    FROM original_sources_vessels osv
    LEFT JOIN vessel_sources vs ON osv.source_id = vs.source_id
    LEFT JOIN vessel_trust_scores vts ON vs.vessel_uuid = vts.vessel_uuid
    LEFT JOIN vessel_historical_snapshots vhs ON vs.vessel_uuid = vhs.vessel_uuid
    WHERE osv.source_type = 'RFMO'
    GROUP BY osv.source_shortname, osv.source_id
)
SELECT 
    rfmo,
    COALESCE(vessel_count, 0) as vessels,
    COALESCE(active_vessels, 0) as active,
    COALESCE(trust_scored, 0) as scored,
    COALESCE(high_trust, 0) as high_trust,
    COALESCE(avg_trust_score, 0) as avg_trust,
    COALESCE(avg_completeness, 0) as avg_complete,
    CASE 
        WHEN last_import IS NULL THEN 'Never imported'
        WHEN last_import::date = CURRENT_DATE THEN 'Today'
        WHEN last_import::date = CURRENT_DATE - 1 THEN 'Yesterday'
        ELSE last_import::date::text
    END as last_import
FROM rfmo_summary
ORDER BY vessel_count DESC;

-- Total unique vessels across all RFMOs
SELECT 
    '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━' as separator;
    
SELECT 
    'TOTAL UNIQUE VESSELS' as metric,
    COUNT(DISTINCT v.vessel_uuid)::text as value
FROM vessels v
WHERE EXISTS (
    SELECT 1 FROM vessel_sources vs
    JOIN original_sources_vessels osv ON vs.source_id = osv.source_id
    WHERE vs.vessel_uuid = v.vessel_uuid
    AND osv.source_type = 'RFMO'
);

-- Data quality metrics
WITH quality_metrics AS (
    SELECT 
        v.vessel_uuid,
        v.imo_number IS NOT NULL as has_imo,
        v.call_sign IS NOT NULL as has_ircs,
        v.mmsi IS NOT NULL as has_mmsi,
        v.vessel_flag IS NOT NULL as has_flag,
        v.vessel_type IS NOT NULL as has_type,
        vts.trust_score,
        vts.data_completeness
    FROM vessels v
    LEFT JOIN vessel_trust_scores vts ON v.vessel_uuid = vts.vessel_uuid
    WHERE EXISTS (
        SELECT 1 FROM vessel_sources vs
        JOIN original_sources_vessels osv ON vs.source_id = osv.source_id
        WHERE vs.vessel_uuid = v.vessel_uuid
        AND osv.source_type = 'RFMO'
    )
)
SELECT 
    'IMO Coverage' as metric,
    ROUND(COUNT(CASE WHEN has_imo THEN 1 END) * 100.0 / COUNT(*), 1) || '%' as value
FROM quality_metrics
UNION ALL
SELECT 
    'IRCS Coverage',
    ROUND(COUNT(CASE WHEN has_ircs THEN 1 END) * 100.0 / COUNT(*), 1) || '%'
FROM quality_metrics
UNION ALL
SELECT 
    'MMSI Coverage',
    ROUND(COUNT(CASE WHEN has_mmsi THEN 1 END) * 100.0 / COUNT(*), 1) || '%'
FROM quality_metrics
UNION ALL
SELECT 
    'Trust Scored',
    ROUND(COUNT(trust_score) * 100.0 / COUNT(*), 1) || '%'
FROM quality_metrics
UNION ALL
SELECT 
    'AI Ready (trust≥0.7, complete≥0.6)',
    ROUND(COUNT(CASE WHEN trust_score >= 0.7 AND data_completeness >= 0.6 THEN 1 END) * 100.0 / COUNT(*), 1) || '%'
FROM quality_metrics;
EOF

# Check available raw data files
log_success ""
log_success "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log_success "Available Raw Data Files"
log_success "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

REPO_ROOT="$(cd "$SCRIPT_DIR/../../../../../../" && pwd)"
DATA_ROOT="${EBISU_DATA_ROOT:-$REPO_ROOT/data/raw}"
RAW_DIR="$DATA_ROOT/vessels/vessel_data/RFMO/raw"
if [[ -d "$RAW_DIR" ]]; then
    for file in "$RAW_DIR"/*.csv; do
        if [[ -f "$file" ]]; then
            filename=$(basename "$file")
            rfmo=$(echo "$filename" | cut -d'_' -f1)
            size=$(du -h "$file" | cut -f1)
            lines=$(($(wc -l < "$file") - 1))
            log_success "$rfmo: $lines vessels ($size)"
        fi
    done
else
    log_error "Raw data directory not found: $RAW_DIR"
fi

# Check cleaned data status
log_success ""
log_success "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log_success "Cleaned Data Status"
log_success "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

CLEANED_DIR="$DATA_ROOT/vessels/vessel_data/RFMO/cleaned"
if [[ -d "$CLEANED_DIR" ]]; then
    for rfmo in ICCAT IOTC WCPFC IATTC CCSBT NAFO NEAFC NPFC SPRFMO; do
        rfmo_lower=$(echo "$rfmo" | tr '[:upper:]' '[:lower:]')
        cleaned_file="$CLEANED_DIR/${rfmo_lower}_vessels_cleaned.csv"
        if [[ -f "$cleaned_file" ]]; then
            lines=$(($(wc -l < "$cleaned_file") - 1))
            log_success "✓ $rfmo: $lines cleaned records"
        else
            log_error "✗ $rfmo: Not cleaned yet"
        fi
    done
else
    log_error "Cleaned data directory not found: $CLEANED_DIR"
fi
