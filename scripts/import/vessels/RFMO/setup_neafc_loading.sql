-- /app/scripts/import/vessels/data/RFMO/setup_neafc_loading.sql
-- NEAFC Modular Setup - Comprehensive Functions Following NPFC Pattern (FIXED)

\echo '🌊 Setting up NEAFC vessel loading (Modular Pattern with Comprehensive Functions - FIXED)'

-- Verify source exists (consistent naming: 'NEAFC')
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM original_sources_vessels WHERE source_shortname = 'NEAFC') THEN
        RAISE EXCEPTION 'NEAFC source not found. Run load_sources_vessels.sh first!';
    END IF;
    RAISE NOTICE '✅ Found NEAFC source';
END $$;

-- Ensure NEAFC RFMO exists
INSERT INTO rfmos (rfmo_acronym, rfmo_name, rfmo_full_name, established_year, region_description)
VALUES ('NEAFC', 'NEAFC', 'North East Atlantic Fisheries Commission', 1963, 'North East Atlantic')
ON CONFLICT (rfmo_acronym) DO NOTHING;

-- ========================================
-- NEAFC MODULAR FUNCTIONS (Following NPFC Pattern)
-- ========================================

-- Country UUID resolution with NEAFC-specific mappings
CREATE OR REPLACE FUNCTION neafc_get_country_uuid(alpha3_code TEXT)
RETURNS UUID AS $$
DECLARE
    country_uuid UUID;
    clean_code TEXT;
BEGIN
    IF alpha3_code IS NULL OR trim(alpha3_code) = '' THEN
        RETURN NULL;
    END IF;
    
    clean_code := upper(trim(alpha3_code));
    
    -- Handle NEAFC-specific country codes
    CASE clean_code
        WHEN 'EUR' THEN clean_code := 'EUR'; -- European Union
        WHEN 'XEU' THEN clean_code := 'EUR'; -- Alt EU code
        WHEN 'EU27' THEN clean_code := 'EUR';
        WHEN 'EU' THEN clean_code := 'EUR';
        WHEN 'FRO' THEN clean_code := 'FRO'; -- Faroe Islands
        WHEN 'GRL' THEN clean_code := 'GRL'; -- Greenland
        WHEN 'NOR' THEN clean_code := 'NOR'; -- Norway
        WHEN 'ISL' THEN clean_code := 'ISL'; -- Iceland
        WHEN 'RUS' THEN clean_code := 'RUS'; -- Russia
        ELSE clean_code := clean_code;
    END CASE;
    
    -- Multi-field lookup using existing country_iso table
    SELECT id INTO country_uuid
    FROM country_iso 
    WHERE alpha_3_code = clean_code
       OR alpha_2_code = clean_code
       OR upper(short_name_en) = clean_code
    LIMIT 1;
    
    RETURN country_uuid;
END;
$$ LANGUAGE plpgsql STABLE;

-- Hierarchical vessel matching (following NPFC pattern)
CREATE OR REPLACE FUNCTION neafc_find_existing_vessel(
    p_imo TEXT, 
    p_ircs TEXT, 
    p_name TEXT, 
    p_flag_uuid UUID
) RETURNS UUID AS $$
DECLARE
    vessel_id UUID;
BEGIN
    -- Priority 1: IMO (strongest identifier)
    IF p_imo IS NOT NULL AND trim(p_imo) != '' AND length(trim(p_imo)) = 7 THEN
        SELECT vessel_uuid INTO vessel_id 
        FROM vessels 
        WHERE imo = trim(p_imo)
        LIMIT 1;
        
        IF vessel_id IS NOT NULL THEN 
            RETURN vessel_id; 
        END IF;
    END IF;
    
    -- Priority 2: IRCS (call sign)
    IF p_ircs IS NOT NULL AND trim(p_ircs) != '' THEN
        SELECT vessel_uuid INTO vessel_id 
        FROM vessels 
        WHERE ircs = trim(p_ircs)
        LIMIT 1;
        
        IF vessel_id IS NOT NULL THEN 
            RETURN vessel_id; 
        END IF;
    END IF;
    
    -- Priority 3: Name + Flag combination
    IF p_name IS NOT NULL AND p_flag_uuid IS NOT NULL THEN
        SELECT vessel_uuid INTO vessel_id 
        FROM vessels 
        WHERE vessel_name = trim(p_name)
          AND vessel_flag = p_flag_uuid 
        LIMIT 1;
        
        IF vessel_id IS NOT NULL THEN 
            RETURN vessel_id; 
        END IF;
    END IF;
    
    RETURN NULL;
END;
$$ LANGUAGE plpgsql STABLE;

-- Vessel type UUID resolution using existing vessel_types table
CREATE OR REPLACE FUNCTION neafc_get_vessel_type_uuid(type_code TEXT)
RETURNS UUID AS $$
DECLARE
    type_uuid UUID;
    clean_code TEXT;
BEGIN
    IF type_code IS NULL OR trim(type_code) = '' THEN
        RETURN NULL;
    END IF;
    
    clean_code := upper(trim(type_code));
    
    -- First try exact code matches
    SELECT id INTO type_uuid
    FROM vessel_types
    WHERE upper(vessel_type_isscfv_code) = clean_code
       OR upper(vessel_type_isscfv_alpha) = clean_code
    LIMIT 1;
    
    IF type_uuid IS NOT NULL THEN
        RETURN type_uuid;
    END IF;
    
    -- NEAFC-specific vessel type mapping
    SELECT id INTO type_uuid
    FROM vessel_types
    WHERE upper(vessel_type_cat) ILIKE '%' || 
        CASE clean_code
            WHEN 'LL' THEN 'LONGLINE'
            WHEN 'TTF' THEN 'TRAWL'
            WHEN 'PS' THEN 'PURSE'
            WHEN 'GN' THEN 'GILLNET'
            WHEN 'TR' THEN 'TRAWL'
            WHEN 'SN' THEN 'SEINE'
            ELSE clean_code
        END || '%'
    LIMIT 1;
    
    RETURN type_uuid;
END;
$$ LANGUAGE plpgsql STABLE;

-- Species harmonization (maps to harmonized_species table)
CREATE OR REPLACE FUNCTION neafc_get_species_ids(scientific_name TEXT)
RETURNS jsonb AS $$
DECLARE
    species_ids jsonb;
BEGIN
    IF scientific_name IS NULL OR trim(scientific_name) = '' THEN
        RETURN '[]'::jsonb;
    END IF;
    
    -- Look up in harmonized_species by scientific name
    SELECT jsonb_agg(harmonized_id) INTO species_ids
    FROM harmonized_species
    WHERE canonical_scientific_name ILIKE trim(scientific_name)
       OR alternative_names::text ILIKE '%' || trim(scientific_name) || '%'
       OR common_names::text ILIKE '%' || trim(scientific_name) || '%';
    
    -- Return empty array if nothing found
    RETURN COALESCE(species_ids, '[]'::jsonb);
END;
$$ LANGUAGE plpgsql STABLE;

-- Get NEAFC RFMO ID
CREATE OR REPLACE FUNCTION neafc_get_rfmo_id()
RETURNS UUID AS $$
DECLARE
    rfmo_id UUID;
BEGIN
    SELECT id INTO rfmo_id
    FROM rfmos 
    WHERE rfmo_acronym = 'NEAFC' 
    LIMIT 1;
    
    RETURN rfmo_id;
END;
$$ LANGUAGE plpgsql STABLE;

-- Process SENDER field (NEAFC-specific - reporting country)
CREATE OR REPLACE FUNCTION neafc_process_sender(sender_code TEXT)
RETURNS jsonb AS $$
DECLARE
    sender_uuid UUID;
    result jsonb;
    clean_sender TEXT;
BEGIN
    IF sender_code IS NULL OR trim(sender_code) = '' THEN
        RETURN '{}'::jsonb;
    END IF;
    
    clean_sender := upper(trim(sender_code));
    
    -- Get country UUID using NEAFC country mapping
    sender_uuid := neafc_get_country_uuid(clean_sender);
    
    IF sender_uuid IS NOT NULL THEN
        -- Return with UUID reference for proper FK relationship
        result := jsonb_build_object(
            'sender_country_id', sender_uuid,
            'sender_alpha3_code', clean_sender,
            'sender_description', 'Reporting contracting party'
        );
    ELSE
        -- Fallback to just alpha3 code
        result := jsonb_build_object(
            'sender_alpha3_code', clean_sender,
            'sender_description', 'Reporting contracting party (unmapped)'
        );
    END IF;
    
    RETURN result;
END;
$$ LANGUAGE plpgsql STABLE;

-- Validate vessel record completeness (following NPFC pattern)
CREATE OR REPLACE FUNCTION neafc_validate_vessel_record(
    p_vessel_name TEXT,
    p_imo TEXT,
    p_ircs TEXT,
    p_flag_uuid UUID
) RETURNS BOOLEAN AS $$
BEGIN
    -- Must have at least one strong identifier
    RETURN (
        (p_imo IS NOT NULL AND trim(p_imo) != '' AND trim(p_imo) ~ '^[0-9]{7}$') OR
        (p_ircs IS NOT NULL AND trim(p_ircs) != '' AND length(trim(p_ircs)) >= 2) OR
        (p_vessel_name IS NOT NULL AND trim(p_vessel_name) != '' AND p_flag_uuid IS NOT NULL)
    );
END;
$$ LANGUAGE plpgsql STABLE;

-- Update source status function (following NPFC pattern)
CREATE OR REPLACE FUNCTION neafc_update_source_status(
    p_vessels_count INTEGER,
    p_authorizations_count INTEGER,
    p_refresh_date DATE
) RETURNS VOID AS $$
BEGIN
    UPDATE original_sources_vessels 
    SET 
        status = 'LOADED',
        size_approx = p_vessels_count,
        refresh_date = p_refresh_date,
        last_updated = CURRENT_TIMESTAMP
    WHERE source_shortname = 'NEAFC';
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'NEAFC vessel source not found in original_sources_vessels';
    END IF;
    
    RAISE NOTICE '✅ NEAFC processing complete: % vessels, % authorizations', 
                 p_vessels_count, p_authorizations_count;
END;
$$ LANGUAGE plpgsql;

-- Enhanced function to map NEAFC vessel types to database enums
CREATE OR REPLACE FUNCTION neafc_map_vessel_type(type_code TEXT)
RETURNS TEXT AS $$
DECLARE
    mapped_type TEXT;
BEGIN
    IF type_code IS NULL OR trim(type_code) = '' THEN
        RETURN NULL;
    END IF;
    
    -- NEAFC vessel type mapping
    mapped_type := CASE upper(trim(type_code))
        WHEN 'LL' THEN 'Longline'
        WHEN 'TTF' THEN 'Trawler'
        WHEN 'PS' THEN 'Purse seiner'
        WHEN 'GN' THEN 'Gillnetter'
        WHEN 'TR' THEN 'Trawler'
        WHEN 'SN' THEN 'Seine netter'
        WHEN 'FV' THEN 'Fishing vessel'
        ELSE type_code
    END;
    
    RETURN mapped_type;
END;
$$ LANGUAGE plpgsql STABLE;

-- Function to validate NEAFC authorization dates
CREATE OR REPLACE FUNCTION neafc_validate_auth_dates(start_date DATE, end_date DATE)
RETURNS BOOLEAN AS $$
BEGIN
    -- Basic date validation for NEAFC authorizations
    RETURN (
        start_date IS NULL OR 
        end_date IS NULL OR 
        start_date <= end_date
    );
END;
$$ LANGUAGE plpgsql STABLE;

-- Enhanced function for NEAFC metric validation
CREATE OR REPLACE FUNCTION neafc_validate_metric(metric_value DECIMAL, metric_type TEXT)
RETURNS BOOLEAN AS $$
BEGIN
    IF metric_value IS NULL THEN
        RETURN TRUE; -- NULL values are acceptable
    END IF;
    
    -- NEAFC-specific metric validation
    RETURN CASE lower(metric_type)
        WHEN 'gross_tonnage' THEN metric_value > 0 AND metric_value < 1000000
        WHEN 'loa', 'length_overall' THEN metric_value > 0 AND metric_value < 500
        WHEN 'engine_power' THEN metric_value > 0 AND metric_value < 100000
        ELSE TRUE
    END;
END;
$$ LANGUAGE plpgsql STABLE;

-- Comprehensive setup verification
DO $$
DECLARE
    neafc_source_id UUID;
    neafc_rfmo_id UUID;
    function_count INTEGER;
BEGIN
    -- Check RFMO exists
    SELECT id INTO neafc_rfmo_id FROM rfmos WHERE rfmo_acronym = 'NEAFC';
    IF neafc_rfmo_id IS NULL THEN
        RAISE EXCEPTION 'NEAFC RFMO not found';
    END IF;
    
    -- Check source exists
    SELECT source_id INTO neafc_source_id FROM original_sources_vessels WHERE source_shortname = 'NEAFC';
    IF neafc_source_id IS NULL THEN
        RAISE EXCEPTION 'NEAFC vessel source not found';
    END IF;
    
    -- Check all modular functions exist
    SELECT COUNT(*) INTO function_count
    FROM pg_proc 
    WHERE proname IN (
        'neafc_get_country_uuid', 'neafc_find_existing_vessel', 'neafc_get_vessel_type_uuid',
        'neafc_get_species_ids', 'neafc_get_rfmo_id', 'neafc_process_sender',
        'neafc_validate_vessel_record', 'neafc_update_source_status', 'neafc_map_vessel_type',
        'neafc_validate_auth_dates', 'neafc_validate_metric'
    );
    
    IF function_count < 11 THEN
        RAISE EXCEPTION 'NEAFC modular setup verification failed: missing functions (found %, expected 11)', function_count;
    END IF;
    
    RAISE NOTICE 'NEAFC modular setup verification successful:';
    RAISE NOTICE '  - NEAFC RFMO ID: %', neafc_rfmo_id;
    RAISE NOTICE '  - NEAFC source ID: %', neafc_source_id;
    RAISE NOTICE '  - All modular functions: % created', function_count;
    RAISE NOTICE '  - Using existing vessel_types and country_iso tables';
    RAISE NOTICE '  - SENDER field processing with FK relationships';
    RAISE NOTICE '  - Species harmonization through harmonized_species table';
    RAISE NOTICE '  - Authorization support with date validation';
    RAISE NOTICE '  - Comprehensive metric validation';
    RAISE NOTICE '  - Ready for NEAFC data loading with modular pattern';
END;
$$;

\echo '✅ NEAFC modular setup completed with comprehensive functions'
\echo 'Functions created following NPFC pattern:'
\echo '  - neafc_get_country_uuid(): Country UUID resolution with NEAFC mappings'
\echo '  - neafc_find_existing_vessel(): Hierarchical vessel matching'
\echo '  - neafc_get_vessel_type_uuid(): Vessel type UUID resolution'
\echo '  - neafc_get_species_ids(): Species harmonization'
\echo '  - neafc_get_rfmo_id(): RFMO UUID retrieval'
\echo '  - neafc_process_sender(): SENDER field processing with FK relationships'
\echo '  - neafc_validate_vessel_record(): Vessel record validation'
\echo '  - neafc_update_source_status(): Source status management'
\echo '  - neafc_map_vessel_type(): Vessel type mapping'
\echo '  - neafc_validate_auth_dates(): Authorization date validation'
\echo '  - neafc_validate_metric(): Metric value validation'
\echo ''
\echo 'Features:'
\echo '  - Complete schema distribution support'
\echo '  - Authorization records with SENDER country mapping'
\echo '  - Species harmonization through existing tables'
\echo '  - Comprehensive validation and error handling'
\echo '  - Modular pattern following NPFC architecture'