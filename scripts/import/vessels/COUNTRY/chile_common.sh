#!/bin/bash
# Common functions for Chile regional vessel imports
# Shared functionality to keep individual scripts under 400 lines

# =====================================================
# CHILE REGION MAPPING
# =====================================================
declare -A CHILE_REGIONS=(
    ["I"]="Tarapacá"
    ["II"]="Antofagasta"
    ["III"]="Atacama"
    ["IV"]="Coquimbo"
    ["V"]="Valparaíso"
    ["VI"]="O'Higgins"
    ["VII"]="Maule"
    ["VIII"]="Biobío"
    ["IX"]="La Araucanía"
    ["X"]="Los Lagos"
    ["XI"]="Aysén"
    ["XII"]="Magallanes"
    ["XIV"]="Los Ríos"
    ["XV"]="Arica y Parinacota"
    ["XVI"]="Ñuble"
    ["RM"]="Región Metropolitana"
)

# =====================================================
# STAGING TABLE CREATION
# =====================================================
create_chile_staging_table() {
    execute_sql "
DROP TABLE IF EXISTS chile_vessels_staging;
CREATE TABLE chile_vessels_staging (
    registro_pesquero_artesanal TEXT,
    rut TEXT,
    nombre TEXT,
    matricula TEXT,
    region TEXT,
    tipo TEXT,
    tamano TEXT,
    eslora TEXT,
    manga TEXT,
    puntal TEXT,
    ton_reg_grueso TEXT,
    capac_bodega_m3 TEXT,
    potencia_motor TEXT,
    artes TEXT,
    armador TEXT,
    rut_arm TEXT,
    tipo_arm TEXT,
    domicilio_armador TEXT,
    telefono_armador TEXT,
    puerto_desembarque TEXT,
    provincia TEXT,
    caleta TEXT
);
"
}

# =====================================================
# INTELLIGENCE REPORT CREATION
# =====================================================
create_chile_intelligence_reports() {
    local SOURCE_ID=$1
    local SOURCE_NAME=$2
    local BATCH_ID=$3
    local INPUT_FILE=$4
    local REGION_CODE=$5
    local REGION_NAME=$6
    
    execute_sql "
INSERT INTO intelligence_reports (
    source_id,
    rfmo_shortname,
    report_date,
    import_batch_id,
    raw_vessel_data,
    file_source,
    row_number,
    data_hash,
    valid_from,
    is_current
)
SELECT 
    '$SOURCE_ID'::uuid,
    '$SOURCE_NAME',
    '2025-09-08'::date,
    '$BATCH_ID'::uuid,
    jsonb_strip_nulls(jsonb_build_object(
        'vessel_name', NULLIF(nombre, ''),
        'registration_number', NULLIF(registro_pesquero_artesanal, ''),
        'matricula', NULLIF(matricula, ''),
        'vessel_flag_alpha3', 'CHL',
        'region', '$REGION_NAME',
        'region_code', '$REGION_CODE',
        'vessel_type', NULLIF(tipo, ''),
        'size_category', NULLIF(tamano, ''),
        'length', NULLIF(eslora, ''),
        'beam', NULLIF(manga, ''),
        'depth', NULLIF(puntal, ''),
        'gross_tonnage', NULLIF(ton_reg_grueso, ''),
        'hold_capacity_m3', NULLIF(capac_bodega_m3, ''),
        'engine_power', NULLIF(potencia_motor, ''),
        'gear_types', NULLIF(artes, ''),
        'owner_name', NULLIF(armador, ''),
        'owner_rut', NULLIF(rut_arm, ''),
        'owner_type', NULLIF(tipo_arm, ''),
        'owner_address', NULLIF(domicilio_armador, ''),
        'owner_phone', NULLIF(telefono_armador, ''),
        'landing_port', NULLIF(puerto_desembarque, ''),
        'province', NULLIF(provincia, ''),
        'caleta', NULLIF(caleta, ''),
        'original_source', '$SOURCE_NAME'
    )),
    '$(basename "$INPUT_FILE")',
    ROW_NUMBER() OVER (ORDER BY registro_pesquero_artesanal, nombre),
    md5(registro_pesquero_artesanal || COALESCE(nombre, '') || COALESCE(matricula, '')),
    CURRENT_DATE,
    TRUE
FROM chile_vessels_staging
WHERE nombre IS NOT NULL AND nombre != '';
"
}

# =====================================================
# VESSEL INTELLIGENCE EXTRACTION
# =====================================================
extract_chile_vessel_intelligence() {
    local BATCH_ID=$1
    
    execute_sql "
INSERT INTO vessel_intelligence (
    report_id,
    reported_vessel_name,
    reported_flag,
    rfmo_vessel_id,
    reported_vessel_type,
    reported_length,
    reported_tonnage,
    reported_engine_power,
    reported_gear_types,
    reported_port_registry,
    reported_owner_name,
    rfmo_specific_data,
    data_completeness_score,
    source_authority_level,
    valid_from,
    is_current
)
SELECT 
    ir.report_id,
    NULLIF(TRIM(ir.raw_vessel_data->>'vessel_name'), ''),
    'CHL',
    COALESCE(
        NULLIF(TRIM(ir.raw_vessel_data->>'registration_number'), ''),
        NULLIF(TRIM(ir.raw_vessel_data->>'matricula'), '')
    ),
    NULLIF(TRIM(ir.raw_vessel_data->>'vessel_type'), ''),
    CASE 
        WHEN ir.raw_vessel_data->>'length' ~ '^[0-9]+\.?[0-9]*$' 
        THEN (ir.raw_vessel_data->>'length')::numeric 
    END,
    CASE 
        WHEN ir.raw_vessel_data->>'gross_tonnage' ~ '^[0-9]+\.?[0-9]*$' 
        THEN (ir.raw_vessel_data->>'gross_tonnage')::numeric 
    END,
    CASE 
        WHEN ir.raw_vessel_data->>'engine_power' ~ '^[0-9]+\.?[0-9]*$' 
        THEN (ir.raw_vessel_data->>'engine_power')::numeric 
    END,
    NULLIF(TRIM(ir.raw_vessel_data->>'gear_types'), ''),
    NULLIF(TRIM(ir.raw_vessel_data->>'landing_port'), ''),
    NULLIF(TRIM(ir.raw_vessel_data->>'owner_name'), ''),
    jsonb_build_object(
        'registration_number', ir.raw_vessel_data->>'registration_number',
        'matricula', ir.raw_vessel_data->>'matricula',
        'region', ir.raw_vessel_data->>'region',
        'size_category', ir.raw_vessel_data->>'size_category',
        'beam', ir.raw_vessel_data->>'beam',
        'depth', ir.raw_vessel_data->>'depth',
        'hold_capacity_m3', ir.raw_vessel_data->>'hold_capacity_m3',
        'owner_details', jsonb_build_object(
            'rut', ir.raw_vessel_data->>'owner_rut',
            'type', ir.raw_vessel_data->>'owner_type',
            'address', ir.raw_vessel_data->>'owner_address',
            'phone', ir.raw_vessel_data->>'owner_phone'
        ),
        'location_details', jsonb_build_object(
            'province', ir.raw_vessel_data->>'province',
            'caleta', ir.raw_vessel_data->>'caleta'
        )
    ),
    -- Calculate data completeness
    (
        CASE WHEN ir.raw_vessel_data->>'vessel_name' IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN ir.raw_vessel_data->>'registration_number' IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN ir.raw_vessel_data->>'length' IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN ir.raw_vessel_data->>'gear_types' IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN ir.raw_vessel_data->>'owner_name' IS NOT NULL THEN 1 ELSE 0 END
    )::decimal / 5.0,
    'AUTHORITATIVE'::source_authority_level, -- Government registry
    CURRENT_DATE,
    TRUE
FROM intelligence_reports ir
WHERE ir.import_batch_id = '$BATCH_ID'::uuid;
"
}

# =====================================================
# SUMMARY REPORTING
# =====================================================
show_chile_import_summary() {
    local BATCH_ID=$1
    local INTELLIGENCE_COUNT=$2
    local REGION_NAME=$3
    local SOURCE_NAME=$4
    
    log_step "📊 Import Summary"
    log_success "✅ Chile $REGION_NAME Vessel Registry Import Complete"
    log_success "   Source: Servicio Nacional de Pesca - $REGION_NAME"
    log_success "   Records: $INTELLIGENCE_COUNT vessels"
    log_success "   Quality: Authoritative government source"
    log_success "   Coverage: Artisanal fishing vessels in $REGION_NAME"
    
    # Show vessel type distribution
    log_step "Vessel Type Distribution:"
    execute_sql "
WITH type_counts AS (
    SELECT 
        ir.raw_vessel_data->>'vessel_type' as vessel_type,
        COUNT(*) as count
    FROM intelligence_reports ir
    WHERE ir.import_batch_id = '$BATCH_ID'::uuid
    GROUP BY ir.raw_vessel_data->>'vessel_type'
    ORDER BY count DESC
    LIMIT 5
)
SELECT * FROM type_counts;
"
}