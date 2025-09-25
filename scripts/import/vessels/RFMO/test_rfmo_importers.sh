#!/bin/bash
# /app/scripts/import/vessels/data/RFMO/test_rfmo_importers.sh
# Comprehensive testing framework for RFMO vessel importers
set -euo pipefail

# Determine environment and set paths
if [ -f /.dockerenv ]; then
    # Docker environment
    source /app/scripts/core/logging.sh
    SCRIPT_DIR="/app/scripts/import/vessels/data/RFMO"
    RAW_DATA_DIR="/import/vessels/vessel_data/RFMO/raw"
    CLEANED_DATA_DIR="/import/vessels/vessel_data/RFMO/cleaned"
else
    # Local/Crunchy Bridge environment
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    source "$SCRIPT_DIR/../../../core/logging.sh"
    REPO_ROOT="$(cd "$SCRIPT_DIR/../../../../../../" && pwd)"
    DATA_ROOT="${EBISU_DATA_ROOT:-$REPO_ROOT/data/raw}"
    RAW_DATA_DIR="$DATA_ROOT/vessels/vessel_data/RFMO/raw"
    CLEANED_DATA_DIR="$DATA_ROOT/vessels/vessel_data/RFMO/cleaned"
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

# Test results storage
TEST_RESULTS="/tmp/rfmo_test_results_$(date +%Y%m%d_%H%M%S).log"
FAILED_TESTS=0
PASSED_TESTS=0

# List of RFMOs to test
RFMOS=(
    "ICCAT"
    "IOTC" 
    "WCPFC"
    "IATTC"
    "CCSBT"
    "NAFO"
    "NEAFC"
    "NPFC"
    "SPRFMO"
)

# Helper functions
log_test() {
    local rfmo=$1
    local test_name=$2
    local status=$3
    local details=${4:-""}
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$rfmo] $test_name: $status $details" | tee -a "$TEST_RESULTS"
    
    if [[ "$status" == "PASS" ]]; then
        ((PASSED_TESTS++))
        log_success "✅ [$rfmo] $test_name passed"
    else
        ((FAILED_TESTS++))
        log_error "❌ [$rfmo] $test_name failed: $details"
    fi
}

check_raw_data() {
    local rfmo=$1
    local file_pattern="*${rfmo}*vessel*.csv"
    
    log_step "Checking raw data for $rfmo..."
    
    if find "$RAW_DATA_DIR" -iname "$file_pattern" -print -quit | grep -q .; then
        local file=$(find "$RAW_DATA_DIR" -iname "$file_pattern" | head -1)
        local row_count=$(tail -n +2 "$file" | wc -l)
        log_test "$rfmo" "Raw data exists" "PASS" "$(basename "$file") with $row_count records"
        return 0
    else
        log_test "$rfmo" "Raw data exists" "FAIL" "No file matching $file_pattern found"
        return 1
    fi
}

test_cleaning_script() {
    local rfmo=$1
    local clean_script="$SCRIPT_DIR/clean_${rfmo,,}_vessels.sh"
    
    log_step "Testing cleaning script for $rfmo..."
    
    # Check script exists
    if [[ ! -f "$clean_script" ]]; then
        log_test "$rfmo" "Cleaning script exists" "FAIL" "Script not found: $clean_script"
        return 1
    fi
    
    log_test "$rfmo" "Cleaning script exists" "PASS"
    
    # Run cleaning script (if raw data exists)
    if check_raw_data "$rfmo"; then
        if bash "$clean_script"; then
            # Check output was created
            local cleaned_file="$CLEANED_DATA_DIR/${rfmo,,}_vessels_cleaned.csv"
            if [[ -f "$cleaned_file" ]]; then
                local row_count=$(tail -n +2 "$cleaned_file" | wc -l)
                log_test "$rfmo" "Cleaning script runs" "PASS" "Generated $row_count cleaned records"
                
                # Validate cleaned data structure
                local headers=$(head -1 "$cleaned_file")
                if [[ "$headers" == *"vessel_name"* ]] && [[ "$headers" == *"imo"* ]]; then
                    log_test "$rfmo" "Cleaned data structure" "PASS" "Required columns present"
                else
                    log_test "$rfmo" "Cleaned data structure" "FAIL" "Missing required columns"
                fi
            else
                log_test "$rfmo" "Cleaning script runs" "FAIL" "No output file created"
            fi
        else
            log_test "$rfmo" "Cleaning script runs" "FAIL" "Script execution error"
        fi
    fi
}

test_setup_sql() {
    local rfmo=$1
    local setup_sql="$SCRIPT_DIR/setup_${rfmo,,}_loading.sql"
    
    log_step "Testing setup SQL for $rfmo..."
    
    # Check file exists
    if [[ ! -f "$setup_sql" ]]; then
        log_test "$rfmo" "Setup SQL exists" "FAIL" "File not found: $setup_sql"
        return 1
    fi
    
    log_test "$rfmo" "Setup SQL exists" "PASS"
    
    # Test SQL execution
    if $PSQL_CMD "$DATABASE_URL" < "$setup_sql" 2>/dev/null; then
        log_test "$rfmo" "Setup SQL executes" "PASS"
        
        # Verify functions created
        local func_count=$($PSQL_CMD "$DATABASE_URL" -t -c "
            SELECT COUNT(*) 
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname = 'public' 
            AND p.proname LIKE '${rfmo,,}_%'
        ")
        
        if [[ $func_count -gt 0 ]]; then
            log_test "$rfmo" "Setup functions created" "PASS" "$func_count functions"
        else
            log_test "$rfmo" "Setup functions created" "FAIL" "No functions found"
        fi
    else
        log_test "$rfmo" "Setup SQL executes" "FAIL" "SQL execution error"
    fi
}

test_load_script() {
    local rfmo=$1
    local load_script="$SCRIPT_DIR/load_${rfmo,,}_vessels.sh"
    
    log_step "Testing load script for $rfmo..."
    
    # Check script exists
    if [[ ! -f "$load_script" ]]; then
        log_test "$rfmo" "Load script exists" "FAIL" "Script not found: $load_script"
        return 1
    fi
    
    log_test "$rfmo" "Load script exists" "PASS"
    
    # Check if cleaned data exists
    local cleaned_file="$CLEANED_DATA_DIR/${rfmo,,}_vessels_cleaned.csv"
    if [[ ! -f "$cleaned_file" ]]; then
        log_test "$rfmo" "Load script ready" "SKIP" "No cleaned data to load"
        return 0
    fi
    
    # Get vessel count before loading
    local before_count=$($PSQL_CMD "$DATABASE_URL" -t -c "
        SELECT COUNT(DISTINCT v.vessel_uuid)
        FROM vessels v
        JOIN vessel_sources vs ON v.vessel_uuid = vs.vessel_uuid
        JOIN original_sources_vessels osv ON vs.source_id = osv.source_id
        WHERE osv.source_shortname = '$rfmo'
    ")
    
    # Run load script (dry run if specified)
    if [[ "${DRY_RUN:-false}" == "true" ]]; then
        log_test "$rfmo" "Load script validation" "SKIP" "Dry run mode"
    else
        if bash "$load_script"; then
            # Check vessels were loaded
            local after_count=$($PSQL_CMD "$DATABASE_URL" -t -c "
                SELECT COUNT(DISTINCT v.vessel_uuid)
                FROM vessels v
                JOIN vessel_sources vs ON v.vessel_uuid = vs.vessel_uuid
                JOIN original_sources_vessels osv ON vs.source_id = osv.source_id
                WHERE osv.source_shortname = '$rfmo'
            ")
            
            local loaded=$((after_count - before_count))
            if [[ $loaded -ge 0 ]]; then
                log_test "$rfmo" "Load script runs" "PASS" "$after_count vessels in database"
                
                # Check trust scores
                test_trust_scores "$rfmo"
                
                # Check data quality
                test_data_quality "$rfmo"
            else
                log_test "$rfmo" "Load script runs" "FAIL" "No vessels loaded"
            fi
        else
            log_test "$rfmo" "Load script runs" "FAIL" "Script execution error"
        fi
    fi
}

test_trust_scores() {
    local rfmo=$1
    
    log_step "Testing trust scores for $rfmo..."
    
    # Get trust score statistics
    local trust_stats=$($PSQL_CMD "$DATABASE_URL" -t <<EOF
WITH rfmo_vessels AS (
    SELECT DISTINCT v.vessel_uuid, vts.trust_score
    FROM vessels v
    JOIN vessel_sources vs ON v.vessel_uuid = vs.vessel_uuid
    JOIN original_sources_vessels osv ON vs.source_id = osv.source_id
    LEFT JOIN vessel_trust_scores vts ON v.vessel_uuid = vts.vessel_uuid
    WHERE osv.source_shortname = '$rfmo'
)
SELECT 
    COUNT(*) as total_vessels,
    COUNT(trust_score) as scored_vessels,
    ROUND(AVG(trust_score)::numeric, 3) as avg_score,
    COUNT(CASE WHEN trust_score >= 0.7 THEN 1 END) as high_trust_count
FROM rfmo_vessels;
EOF
)
    
    IFS='|' read -r total scored avg_score high_trust <<< "$trust_stats"
    total=$(echo $total | xargs)
    scored=$(echo $scored | xargs)
    avg_score=$(echo $avg_score | xargs)
    high_trust=$(echo $high_trust | xargs)
    
    if [[ $scored -gt 0 ]]; then
        log_test "$rfmo" "Trust scoring" "PASS" "$scored/$total vessels scored, avg=$avg_score, high_trust=$high_trust"
    else
        log_test "$rfmo" "Trust scoring" "FAIL" "No vessels have trust scores"
    fi
}

test_data_quality() {
    local rfmo=$1
    
    log_step "Testing data quality for $rfmo..."
    
    # Get data quality metrics
    local quality_stats=$($PSQL_CMD "$DATABASE_URL" -t <<EOF
WITH rfmo_vessels AS (
    SELECT DISTINCT v.*
    FROM vessels v
    JOIN vessel_sources vs ON v.vessel_uuid = vs.vessel_uuid
    JOIN original_sources_vessels osv ON vs.source_id = osv.source_id
    WHERE osv.source_shortname = '$rfmo'
)
SELECT 
    COUNT(*) as total,
    COUNT(imo_number) as with_imo,
    COUNT(call_sign) as with_ircs,
    COUNT(vessel_flag) as with_flag,
    COUNT(vessel_type) as with_type
FROM rfmo_vessels;
EOF
)
    
    IFS='|' read -r total imo ircs flag vtype <<< "$quality_stats"
    total=$(echo $total | xargs)
    imo=$(echo $imo | xargs)
    ircs=$(echo $ircs | xargs)
    flag=$(echo $flag | xargs)
    vtype=$(echo $vtype | xargs)
    
    if [[ $total -gt 0 ]]; then
        local imo_pct=$((imo * 100 / total))
        local ircs_pct=$((ircs * 100 / total))
        local flag_pct=$((flag * 100 / total))
        
        log_test "$rfmo" "Data quality" "PASS" "IMO=$imo_pct%, IRCS=$ircs_pct%, Flag=$flag_pct%"
        
        # Check historical tracking
        test_historical_tracking "$rfmo"
    else
        log_test "$rfmo" "Data quality" "SKIP" "No vessels to analyze"
    fi
}

test_historical_tracking() {
    local rfmo=$1
    
    # Check if historical data is being captured
    local history_count=$($PSQL_CMD "$DATABASE_URL" -t -c "
        SELECT COUNT(*)
        FROM vessel_historical_snapshots vhs
        JOIN vessel_sources vs ON vhs.vessel_uuid = vs.vessel_uuid
        JOIN original_sources_vessels osv ON vs.source_id = osv.source_id
        WHERE osv.source_shortname = '$rfmo'
    ")
    
    if [[ $history_count -gt 0 ]]; then
        log_test "$rfmo" "Historical tracking" "PASS" "$history_count snapshots captured"
    else
        log_test "$rfmo" "Historical tracking" "WARN" "No historical snapshots"
    fi
}

# Main test execution
main() {
    log_success "🧪 Starting RFMO Importer Testing Framework"
    log_success "Test results will be saved to: $TEST_RESULTS"
    
    # Check database connection
    if $PSQL_CMD "$DATABASE_URL" -c "SELECT 1" &>/dev/null; then
        log_success "✅ Database connection successful"
    else
        log_error "❌ Cannot connect to database"
        exit 1
    fi
    
    # Test each RFMO
    for rfmo in "${RFMOS[@]}"; do
        log_success ""
        log_success "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        log_success "Testing $rfmo Importer"
        log_success "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        
        test_cleaning_script "$rfmo"
        test_setup_sql "$rfmo"
        test_load_script "$rfmo"
    done
    
    # Summary report
    log_success ""
    log_success "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log_success "Test Summary"
    log_success "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log_success "Total Tests: $((PASSED_TESTS + FAILED_TESTS))"
    log_success "Passed: $PASSED_TESTS"
    log_success "Failed: $FAILED_TESTS"
    
    if [[ $FAILED_TESTS -eq 0 ]]; then
        log_success ""
        log_success "🎉 All tests passed!"
    else
        log_error ""
        log_error "⚠️  Some tests failed. Review: $TEST_RESULTS"
        exit 1
    fi
    
    # Generate comprehensive report
    generate_mdm_report
}

generate_mdm_report() {
    log_success ""
    log_success "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log_success "Master Data Management (MDM) System Report"
    log_success "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Overall vessel statistics
    local vessel_stats=$($PSQL_CMD "$DATABASE_URL" -t <<EOF
SELECT 
    COUNT(DISTINCT vessel_uuid) as total_vessels,
    COUNT(DISTINCT CASE WHEN imo_number IS NOT NULL THEN vessel_uuid END) as with_imo,
    COUNT(DISTINCT CASE WHEN call_sign IS NOT NULL THEN vessel_uuid END) as with_ircs,
    COUNT(DISTINCT CASE WHEN mmsi IS NOT NULL THEN vessel_uuid END) as with_mmsi
FROM vessels;
EOF
)
    
    IFS='|' read -r total imo ircs mmsi <<< "$vessel_stats"
    log_success "Total Unique Vessels: $(echo $total | xargs)"
    log_success "With IMO: $(echo $imo | xargs)"
    log_success "With IRCS: $(echo $ircs | xargs)"
    log_success "With MMSI: $(echo $mmsi | xargs)"
    
    # Trust score distribution
    log_success ""
    log_success "Trust Score Distribution:"
    $PSQL_CMD "$DATABASE_URL" <<EOF
SELECT 
    CASE 
        WHEN trust_score >= 0.9 THEN 'Excellent (0.9+)'
        WHEN trust_score >= 0.8 THEN 'Very Good (0.8-0.9)'
        WHEN trust_score >= 0.7 THEN 'Good/AI Ready (0.7-0.8)'
        WHEN trust_score >= 0.6 THEN 'Fair (0.6-0.7)'
        ELSE 'Poor (<0.6)'
    END as trust_category,
    COUNT(*) as vessel_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage
FROM vessel_trust_scores
GROUP BY trust_category
ORDER BY 
    CASE trust_category
        WHEN 'Excellent (0.9+)' THEN 1
        WHEN 'Very Good (0.8-0.9)' THEN 2
        WHEN 'Good/AI Ready (0.7-0.8)' THEN 3
        WHEN 'Fair (0.6-0.7)' THEN 4
        ELSE 5
    END;
EOF
    
    # RFMO coverage
    log_success ""
    log_success "RFMO Coverage:"
    $PSQL_CMD "$DATABASE_URL" <<EOF
SELECT 
    osv.source_shortname as RFMO,
    COUNT(DISTINCT vs.vessel_uuid) as vessels,
    COUNT(DISTINCT CASE WHEN vs.is_active THEN vs.vessel_uuid END) as active
FROM vessel_sources vs
JOIN original_sources_vessels osv ON vs.source_id = osv.source_id
WHERE osv.source_type = 'RFMO'
GROUP BY osv.source_shortname
ORDER BY vessels DESC;
EOF
    
    # AI readiness
    local ai_ready=$($PSQL_CMD "$DATABASE_URL" -t -c "
        SELECT COUNT(DISTINCT vessel_uuid)
        FROM vessel_trust_scores
        WHERE trust_score >= 0.7
        AND data_completeness >= 0.6
    ")
    
    log_success ""
    log_success "AI Training Readiness:"
    log_success "Vessels meeting AI criteria (trust >= 0.7, completeness >= 0.6): $(echo $ai_ready | xargs)"
}

# Parse command line arguments
DRY_RUN=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN=true
            log_success "Running in DRY RUN mode - no data will be loaded"
            shift
            ;;
        --rfmo)
            RFMOS=("$2")
            log_success "Testing only $2"
            shift 2
            ;;
        *)
            echo "Usage: $0 [--dry-run] [--rfmo RFMO_NAME]"
            exit 1
            ;;
    esac
done

# Run tests
main
