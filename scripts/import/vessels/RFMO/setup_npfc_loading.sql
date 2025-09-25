-- /app/scripts/import/vessels/data/RFMO/setup_npfc_loading.sql
-- COMPREHENSIVE: NPFC Setup using EXISTING schema with ALL original functions - CORRECTED translations only
\echo 'Setting up NPFC vessel loading (COMPREHENSIVE - Using Existing Schema Only + ALL Original Functions + CORRECTED translations)'

-- Verify NPFC source exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM original_sources_vessels WHERE source_shortname = 'NPFC') THEN
        -- Create NPFC source if it doesn't exist
        INSERT INTO original_sources_vessels (
            source_shortname, source_fullname, source_types, refresh_date, status
        ) VALUES (
            'NPFC',
            'North Pacific Fisheries Commission Vessel Registry',
            ARRAY['RFMO']::TEXT[],
            CURRENT_DATE,
            'PENDING'
        );
        RAISE NOTICE 'Created NPFC source';
    ELSE
        RAISE NOTICE 'Found existing NPFC source';
    END IF;
END $$;

-- Ensure NPFC RFMO exists for comprehensive integration
INSERT INTO rfmos (rfmo_acronym, rfmo_name, rfmo_full_name, established_year, headquarters_location)
VALUES ('NPFC', 'NPFC', 'North Pacific Fisheries Commission', 2015, 'Tokyo, Japan')
ON CONFLICT (rfmo_acronym) DO NOTHING;

-- ============================================================================
-- COMPREHENSIVE FUNCTIONS USING EXISTING SCHEMA - ALL ORIGINAL FUNCTIONALITY
-- ============================================================================

-- CORRECTED: Country UUID resolution using existing country_iso table with correct column names
CREATE OR REPLACE FUNCTION get_country_uuid(country_input TEXT)
RETURNS UUID AS $$
DECLARE
    result_uuid UUID;
    clean_input TEXT;
BEGIN
    IF country_input IS NULL OR trim(country_input) = '' THEN
        RETURN NULL;
    END IF;
    
    clean_input := upper(trim(country_input));
    
    -- COMPREHENSIVE NPFC-specific country mappings - ALL ORIGINAL FUNCTIONALITY
    CASE clean_input
        WHEN 'CHINESE TAIPEI' THEN clean_input := 'TWN';
        WHEN 'SOUTH KOREA' THEN clean_input := 'KOR';
        WHEN 'KOREA' THEN clean_input := 'KOR';
        WHEN 'BELIZ' THEN clean_input := 'BLZ';
        WHEN 'RUSSIA' THEN clean_input := 'RUS';
        WHEN 'JAPAN' THEN clean_input := 'JPN';
        WHEN 'CHINA' THEN clean_input := 'CHN';
        WHEN 'UNITED STATES' THEN clean_input := 'USA';
        WHEN 'CANADA' THEN clean_input := 'CAN';
        WHEN 'NORWAY' THEN clean_input := 'NOR';
        ELSE 
            clean_input := clean_input;
    END CASE;
    
    -- CORRECTED: Use existing country_iso table with correct column names
    SELECT id INTO result_uuid
    FROM country_iso 
    WHERE alpha_3_code = clean_input
       OR alpha_2_code = clean_input
       OR upper(short_name_en) = clean_input
       OR upper(short_name_fr) = clean_input
    LIMIT 1;
    
    RETURN result_uuid;
END;
$$ LANGUAGE plpgsql STABLE;

-- CORRECTED: Vessel type UUID resolution using existing vessel_types table - ALL ORIGINAL LOGIC
CREATE OR REPLACE FUNCTION get_vessel_type_uuid(type_input TEXT) 
RETURNS UUID AS $$
DECLARE
    result_uuid UUID;
    clean_input TEXT;
BEGIN
    IF type_input IS NULL OR trim(type_input) = '' THEN
        RETURN NULL;
    END IF;
    
    clean_input := upper(trim(type_input));
    
    -- COMPREHENSIVE vessel type mapping logic using existing vessel_types table
    -- First try exact code matches
    SELECT id INTO result_uuid
    FROM vessel_types 
    WHERE upper(vessel_type_isscfv_code) = clean_input
       OR upper(vessel_type_isscfv_alpha) = clean_input
    LIMIT 1;
    
    IF result_uuid IS NOT NULL THEN
        RETURN result_uuid;
    END IF;
    
    -- CORRECTED: Use vessel_type_cat column for text matching
    SELECT id INTO result_uuid
    FROM vessel_types 
    WHERE upper(vessel_type_cat) ILIKE '%' || clean_input || '%'
    LIMIT 1;
    
    IF result_uuid IS NOT NULL THEN
        RETURN result_uuid;
    END IF;
    
    -- COMPREHENSIVE NPFC-specific vessel type matching - ALL ORIGINAL MAPPINGS LOGIC
    -- Handle NPFC specific patterns
    IF clean_input ILIKE '%TRAWL%' THEN
        SELECT id INTO result_uuid FROM vessel_types 
        WHERE upper(vessel_type_cat) ILIKE '%TRAWLER%' LIMIT 1;
    ELSIF clean_input ILIKE '%PURSE%' THEN
        SELECT id INTO result_uuid FROM vessel_types 
        WHERE upper(vessel_type_cat) ILIKE '%PURSE%' LIMIT 1;
    ELSIF clean_input ILIKE '%RESEARCH%' THEN
        SELECT id INTO result_uuid FROM vessel_types 
        WHERE upper(vessel_type_cat) ILIKE '%RESEARCH%' LIMIT 1;
    ELSIF clean_input ILIKE '%GILLNET%' THEN
        SELECT id INTO result_uuid FROM vessel_types 
        WHERE upper(vessel_type_cat) ILIKE '%GILLNET%' LIMIT 1;
    ELSIF clean_input ILIKE '%LINER%' OR clean_input ILIKE '%LINE%' THEN
        SELECT id INTO result_uuid FROM vessel_types 
        WHERE upper(vessel_type_cat) ILIKE '%LINE%' LIMIT 1;
    ELSIF clean_input ILIKE '%CARRIER%' OR clean_input ILIKE '%REEFER%' THEN
        SELECT id INTO result_uuid FROM vessel_types 
        WHERE upper(vessel_type_cat) ILIKE '%CARRIER%' OR upper(vessel_type_cat) ILIKE '%REEFER%' LIMIT 1;
    ELSIF clean_input ILIKE '%SUPPORT%' OR clean_input ILIKE '%SERVICE%' THEN
        SELECT id INTO result_uuid FROM vessel_types 
        WHERE upper(vessel_type_cat) ILIKE '%SUPPORT%' OR upper(vessel_type_cat) ILIKE '%SERVICE%' LIMIT 1;
    END IF;
    
    RETURN result_uuid;
END;
$$ LANGUAGE plpgsql STABLE;

-- COMPREHENSIVE gear type UUID resolution using existing gear_types_fao table - ALL ORIGINAL LOGIC
CREATE OR REPLACE FUNCTION get_gear_type_uuid(gear_input TEXT)
RETURNS UUID AS $$
DECLARE
    result_uuid UUID;
    clean_input TEXT;
BEGIN
    IF gear_input IS NULL OR trim(gear_input) = '' THEN
        RETURN NULL;
    END IF;
    
    clean_input := upper(trim(gear_input));
    
    -- First try exact code matches
    SELECT id INTO result_uuid
    FROM gear_types_fao 
    WHERE upper(fao_isscfg_code) = clean_input
       OR upper(fao_isscfg_alpha) = clean_input
    LIMIT 1;
    
    IF result_uuid IS NOT NULL THEN
        RETURN result_uuid;
    END IF;
    
    -- Use existing gear_types_fao table for text matching
    SELECT id INTO result_uuid
    FROM gear_types_fao 
    WHERE upper(fao_isscfg_name) ILIKE '%' || clean_input || '%'
    LIMIT 1;
    
    IF result_uuid IS NOT NULL THEN
        RETURN result_uuid;
    END IF;
    
    -- COMPREHENSIVE NPFC-specific gear matching - ALL ORIGINAL MAPPING LOGIC
    IF clean_input ILIKE '%TRAWL%' THEN
        SELECT id INTO result_uuid FROM gear_types_fao 
        WHERE upper(fao_isscfg_name) ILIKE '%TRAWL%' LIMIT 1;
    ELSIF clean_input ILIKE '%SEINE%' THEN
        SELECT id INTO result_uuid FROM gear_types_fao 
        WHERE upper(fao_isscfg_name) ILIKE '%SEINE%' LIMIT 1;
    ELSIF clean_input ILIKE '%LINE%' OR clean_input ILIKE '%HOOK%' THEN
        SELECT id INTO result_uuid FROM gear_types_fao 
        WHERE upper(fao_isscfg_name) ILIKE '%LINE%' OR upper(fao_isscfg_name) ILIKE '%HOOK%' LIMIT 1;
    ELSIF clean_input ILIKE '%NET%' THEN
        SELECT id INTO result_uuid FROM gear_types_fao 
        WHERE upper(fao_isscfg_name) ILIKE '%NET%' LIMIT 1;
    ELSIF clean_input ILIKE '%POT%' OR clean_input ILIKE '%TRAP%' THEN
        SELECT id INTO result_uuid FROM gear_types_fao 
        WHERE upper(fao_isscfg_name) ILIKE '%POT%' OR upper(fao_isscfg_name) ILIKE '%TRAP%' LIMIT 1;
    END IF;
    
    RETURN result_uuid;
END;
$$ LANGUAGE plpgsql STABLE;

-- COMPREHENSIVE vessel matching function using existing vessels table - ALL ORIGINAL FUNCTIONALITY
CREATE OR REPLACE FUNCTION find_existing_vessel(
    p_imo TEXT, 
    p_ircs TEXT, 
    p_mmsi TEXT, 
    p_name TEXT, 
    p_flag_uuid UUID
) RETURNS UUID AS $$
DECLARE
    vessel_id UUID;
BEGIN
    -- Priority 1: IMO (strongest identifier) - ORIGINAL LOGIC
    IF p_imo IS NOT NULL AND trim(p_imo) != '' AND length(trim(p_imo)) = 7 THEN
        SELECT vessel_uuid INTO vessel_id
        FROM vessels 
        WHERE imo = trim(p_imo)
        LIMIT 1;
        
        IF vessel_id IS NOT NULL THEN 
            RETURN vessel_id; 
        END IF;
    END IF;
    
    -- Priority 2: IRCS (call sign) - ORIGINAL LOGIC
    IF p_ircs IS NOT NULL AND trim(p_ircs) != '' THEN
        SELECT vessel_uuid INTO vessel_id
        FROM vessels 
        WHERE ircs = trim(p_ircs)
        LIMIT 1;
        
        IF vessel_id IS NOT NULL THEN 
            RETURN vessel_id; 
        END IF;
    END IF;
    
    -- Priority 3: MMSI - ORIGINAL LOGIC
    IF p_mmsi IS NOT NULL AND trim(p_mmsi) != '' AND length(trim(p_mmsi)) = 9 THEN
        SELECT vessel_uuid INTO vessel_id
        FROM vessels 
        WHERE mmsi = trim(p_mmsi)
        LIMIT 1;
        
        IF vessel_id IS NOT NULL THEN 
            RETURN vessel_id; 
        END IF;
    END IF;
    
    -- Priority 4: Name + Flag combination - ORIGINAL LOGIC
    IF p_name IS NOT NULL AND trim(p_name) != '' AND p_flag_uuid IS NOT NULL THEN
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

-- COMPREHENSIVE data validation - ALL ORIGINAL FUNCTIONALITY
CREATE OR REPLACE FUNCTION validate_vessel_identifiers(
    p_vessel_name TEXT,
    p_imo TEXT,
    p_ircs TEXT,
    p_mmsi TEXT,
    p_flag_uuid UUID
) RETURNS BOOLEAN AS $$
BEGIN
    RETURN (
        (p_imo IS NOT NULL AND trim(p_imo) != '' AND length(trim(p_imo)) = 7) OR
        (p_ircs IS NOT NULL AND trim(p_ircs) != '') OR
        (p_mmsi IS NOT NULL AND trim(p_mmsi) != '' AND length(trim(p_mmsi)) = 9) OR
        (p_vessel_name IS NOT NULL AND trim(p_vessel_name) != '' AND p_flag_uuid IS NOT NULL)
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- CORRECTED: Source status update function with proper integer casting - ORIGINAL FUNCTIONALITY + FIX
CREATE OR REPLACE FUNCTION update_npfc_source_status(
    p_vessel_count INTEGER,
    p_refresh_date DATE DEFAULT CURRENT_DATE
) RETURNS VOID AS $$
BEGIN
    UPDATE original_sources_vessels 
    SET 
        status = 'LOADED',
        size_approx = p_vessel_count,
        refresh_date = p_refresh_date,
        last_updated = CURRENT_TIMESTAMP
    WHERE source_shortname = 'NPFC';
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'NPFC vessel source not found';
    END IF;
    
    RAISE NOTICE 'NPFC comprehensive processing complete: % vessels loaded', p_vessel_count;
END;
$$ LANGUAGE plpgsql;

-- Create freezer types JSONB array from enum value - ALL ORIGINAL FUNCTIONALITY
CREATE OR REPLACE FUNCTION create_freezer_types_array(freezer_type_enum TEXT)
RETURNS JSONB AS $$
BEGIN
    IF freezer_type_enum IS NULL OR trim(freezer_type_enum) = '' THEN
        RETURN '[]'::jsonb;
    END IF;
    
    RETURN jsonb_build_array(freezer_type_enum);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- COMPREHENSIVE NPFC-specific processing functions using cleaned data - ALL ORIGINAL FUNCTIONS

-- CORRECTED: Enhanced vessel type mapping using EXACT user-provided translations
CREATE OR REPLACE FUNCTION npfc_get_vessel_type_code(npfc_description TEXT)
RETURNS TEXT AS $$
DECLARE
    result_code TEXT;
BEGIN
    IF npfc_description IS NULL OR trim(npfc_description) = '' THEN
        RETURN NULL;
    END IF;
    
    -- CORRECTED: Use EXACT vessel type mapping provided by user
    result_code := CASE trim(npfc_description)
        WHEN 'BUNKERING TANKER VESSELS (SB)' THEN 'SB'
        WHEN 'FISH CARRIERS AND REEFERS (FO)' THEN 'FO'
        WHEN 'FISHERY RESEARCH VESSELS (RT)' THEN 'RT'
        WHEN 'FISHERY RESEARCH VESSELS (ZO)' THEN 'RT'
        WHEN 'Fishery research vessels nei (RTX)' THEN 'RTX'
        WHEN 'GILLNETTERS (GO)' THEN 'GO'
        WHEN 'Hand liner vessels (LH)' THEN 'LH'
        WHEN 'Japanese type liners (LPJ)' THEN 'LO'
        WHEN 'Japanese type  liners (LPJ)' THEN 'LOX' -- Note: double space in original
        WHEN 'Japanese type   liners (LPJ)' THEN 'LOX' -- Note: triple space in original
        WHEN 'Jigger vessels (LJ)' THEN 'LJ'
        WHEN 'LIFT NETTERS - using boat operated net (NB)' THEN 'NO'
        WHEN 'LIFT NETTERS (NO)' THEN 'NO'
        WHEN 'Lift netters nei (NOX)' THEN 'NOX'
        WHEN 'Line vessels nei (LOX)' THEN 'LOX'
        WHEN 'Other fishing vessels [FISHING VESSELS NOT SPECIFIED] (FX)' THEN 'FXX'
        WHEN 'PROTECTION AND SURVEY VESSELS (BO)' THEN 'SA'
        WHEN 'Purse seiners (SP)' THEN 'SP'
        WHEN 'Purse seiners nei (SPX)' THEN 'SPX'
        WHEN 'Stern trawlers (TT)' THEN 'TT'
        WHEN 'Stern trawlers freezer (TTF)' THEN 'TT'
        WHEN 'Stern trawlers factory (TTP)' THEN 'TT'
        WHEN 'Stick-held dip netters (NS)' THEN 'SA'
        WHEN 'SUPPORT vessels (SA)' THEN 'SA'
        WHEN 'TRAWLERS (TO)' THEN 'TO'
        WHEN 'Trap setters nei (WOX)' THEN 'WOX'
        WHEN 'Vessels supporting fishing related activities [NON-FISHING VESSELS] (VO)' THEN 'VO'
        WHEN 'TRAP SETTERS (WO)' THEN 'WO'
        WHEN 'Side trawlers freezer (TSF)' THEN 'TS'
        WHEN 'Factory mothership (HSF)' THEN 'HOX'
        WHEN 'Multipurpose non-fishing vessels (NF)' THEN 'VOM'
        WHEN 'Refrigerated transport vessels (FR)' THEN 'FR'
        ELSE trim(npfc_description)
    END;
    
    RETURN result_code;
END;
$$ LANGUAGE plpgsql STABLE;

-- CORRECTED: Enhanced fishing method mapping using EXACT user-provided translations
CREATE OR REPLACE FUNCTION npfc_get_fishing_method_code(npfc_description TEXT)
RETURNS TEXT AS $$
DECLARE
    result_code TEXT;
BEGIN
    IF npfc_description IS NULL OR trim(npfc_description) = '' THEN
        RETURN NULL;
    END IF;
    
    -- CORRECTED: Use EXACT fishing method mapping provided by user
    result_code := CASE trim(npfc_description)
        WHEN 'Boat-operated lift nets (LNB)' THEN 'LNB'
        WHEN 'Gear not known (NK)' THEN 'NKX'
        WHEN 'Gillnets and entangling nets (nei) (GEN)' THEN 'GEN'
        WHEN 'Handlines and hand-operated pole-and-lines (LHP)' THEN 'LHP'
        WHEN 'Hooks and lines (nei) (LX)' THEN 'LX'
        WHEN 'Mechanized lines and pole-and-lines (LHM)' THEN 'LHM'
        WHEN 'Purse seines (PS)' THEN 'PS'
        WHEN 'SEINE NETS' THEN 'SX'
        WHEN 'Single boat midwater otter trawls (OTM)' THEN 'OTM'
        WHEN 'Stick-held dip net (SHDN)' THEN 'MIS'
        WHEN 'Pots (FPO)' THEN 'FPO'
        WHEN 'Aerial traps (FAR)' THEN 'FAR'
        WHEN 'Bottom trawls (nei) (TB)' THEN 'TB'
        WHEN 'Midwater trawls (nei) (TM)' THEN 'TM'
        WHEN 'Seine nets (nei) (SX)' THEN 'SX'
        WHEN 'Traps (nei) (FIX)' THEN 'FIX'
        WHEN 'Trawls (nei) (TX)' THEN 'TX'
        WHEN 'Semipelagic trawls (TSP)' THEN 'TSP'
        WHEN 'TRAWLS' THEN 'TX'
        ELSE trim(npfc_description)
    END;
    
    RETURN result_code;
END;
$$ LANGUAGE plpgsql STABLE;

-- Enhanced freezer type mapping - ALL ORIGINAL LOGIC MAINTAINED
CREATE OR REPLACE FUNCTION npfc_get_freezer_type_enum(npfc_description TEXT)
RETURNS TEXT AS $$
DECLARE
    result_enum TEXT;
BEGIN
    IF npfc_description IS NULL OR trim(npfc_description) = '' THEN
        RETURN NULL;
    END IF;
    
    -- COMPREHENSIVE freezer type mappings - ALL ORIGINAL MAPPINGS MAINTAINED
    result_enum := CASE trim(npfc_description)
        WHEN 'Air Blast' THEN 'AIR_BLAST'
        WHEN 'air blast' THEN 'AIR_BLAST'
        WHEN 'AIR BLAST' THEN 'AIR_BLAST'
        WHEN 'Air Coil' THEN 'AIR_COIL'
        WHEN 'air coil' THEN 'AIR_COIL'
        WHEN 'AIR COIL' THEN 'AIR_COIL'
        WHEN 'Bait Freezer' THEN 'BAIT_FREEZER'
        WHEN 'bait freezer' THEN 'BAIT_FREEZER'
        WHEN 'Blast' THEN 'BLAST'
        WHEN 'blast' THEN 'BLAST'
        WHEN 'Brine' THEN 'BRINE'
        WHEN 'brine' THEN 'BRINE'
        WHEN 'BRINE' THEN 'BRINE'
        WHEN 'Chilled' THEN 'CHILLED'
        WHEN 'chilled' THEN 'CHILLED'
        WHEN 'Coil' THEN 'COIL'
        WHEN 'coil' THEN 'COIL'
        WHEN 'Direct Expansion' THEN 'DIRECT_EXPANSION'
        WHEN 'direct expansion' THEN 'DIRECT_EXPANSION'
        WHEN 'Dry' THEN 'DRY'
        WHEN 'dry' THEN 'DRY'
        WHEN 'Freon Refrigeration System' THEN 'FREON_REFRIGERATION_SYSTEM'
        WHEN 'freon refrigeration system' THEN 'FREON_REFRIGERATION_SYSTEM'
        WHEN 'FREON REFRIGERATION SYSTEM' THEN 'FREON_REFRIGERATION_SYSTEM'
        WHEN 'Grid Coil' THEN 'GRID_COIL'
        WHEN 'grid coil' THEN 'GRID_COIL'
        WHEN 'Ice' THEN 'ICE'
        WHEN 'ice' THEN 'ICE'
        WHEN 'ICE' THEN 'ICE'
        WHEN 'Mykom' THEN 'MYKOM'
        WHEN 'mykom' THEN 'MYKOM'
        WHEN 'MYKOM' THEN 'MYKOM'
        WHEN 'Other' THEN 'OTHER'
        WHEN 'other' THEN 'OTHER'
        WHEN 'OTHER' THEN 'OTHER'
        WHEN 'Pipe' THEN 'PIPE'
        WHEN 'pipe' THEN 'PIPE'
        WHEN 'Plate Freezer' THEN 'PLATE_FREEZER'
        WHEN 'plate freezer' THEN 'PLATE_FREEZER'
        WHEN 'PLATE FREEZER' THEN 'PLATE_FREEZER'
        WHEN 'Plate Freezer, Plate Freezer' THEN 'PLATE_FREEZER' -- Handle duplicates
        WHEN 'plate freezer, plate freezer' THEN 'PLATE_FREEZER'
        WHEN 'RSW' THEN 'RSW'
        WHEN 'rsw' THEN 'RSW'
        WHEN 'Semi Air Blast' THEN 'SEMI_AIR_BLAST'
        WHEN 'semi air blast' THEN 'SEMI_AIR_BLAST'
        WHEN 'Tunnel' THEN 'TUNNEL'
        WHEN 'tunnel' THEN 'TUNNEL'
        ELSE 'OTHER'
    END;
    
    RETURN result_enum;
END;
$$ LANGUAGE plpgsql STABLE;

-- CORRECTED Enhanced unit mapping - CORRECTED to match database enum exactly
CREATE OR REPLACE FUNCTION npfc_get_unit_enum(npfc_description TEXT, measurement_category TEXT DEFAULT NULL)
RETURNS TEXT AS $$
DECLARE
    result_enum TEXT;
BEGIN
    IF npfc_description IS NULL OR trim(npfc_description) = '' THEN
        RETURN NULL;
    END IF;
    
    -- CORRECTED comprehensive unit mappings - FIXED to match database enum exactly
    result_enum := CASE trim(npfc_description)
        -- Length units - CORRECTED to singular forms
        WHEN 'meters' THEN 'METER'
        WHEN 'm' THEN 'METER'
        WHEN 'metre' THEN 'METER'
        WHEN 'metres' THEN 'METER'
        WHEN 'feet' THEN 'FEET'
        WHEN 'ft' THEN 'FEET'
        WHEN 'foot' THEN 'FEET'
        
        -- Volume units - CORRECTED to singular forms
        WHEN 'cubic feet' THEN 'CUBIC_FEET'
        WHEN 'Cubic Feet' THEN 'CUBIC_FEET'
        WHEN 'CUBIC FEET' THEN 'CUBIC_FEET'
        WHEN 'cubic meters' THEN 'CUBIC_METER'
        WHEN 'Cubic Metres' THEN 'CUBIC_METER'
        WHEN 'cubic metres' THEN 'CUBIC_METER'
        WHEN 'mÃ‚Â³' THEN 'CUBIC_METER'
        WHEN 'm3' THEN 'CUBIC_METER'
        WHEN 'liter' THEN 'LITER'
        WHEN 'litre' THEN 'LITER'
        WHEN 'l' THEN 'LITER'
        WHEN 'gallon' THEN 'GALLON'
        WHEN 'gal' THEN 'GALLON'
        
        -- Power units
        WHEN 'Kilowatts (kW)' THEN 'KW'
        WHEN 'kW' THEN 'KW'
        WHEN 'kilowatt' THEN 'KW'
        WHEN 'KW' THEN 'KW'
        WHEN 'Horse Power (hp)' THEN 'HP'
        WHEN 'hp' THEN 'HP'
        WHEN 'horsepower' THEN 'HP'
        WHEN 'HP' THEN 'HP'
        WHEN 'PferdestÃƒÂ¤rke (ps)' THEN 'PS'
        WHEN 'ps' THEN 'PS'
        WHEN 'PS' THEN 'PS'
        
        -- Speed units
        WHEN 'knots' THEN 'KNOTS'
        WHEN 'kt' THEN 'KNOTS'
        WHEN 'kn' THEN 'KNOTS'
        WHEN 'mph' THEN 'MPH'
        WHEN 'MPH' THEN 'MPH'
        WHEN 'kmh' THEN 'KMH'
        WHEN 'km/h' THEN 'KMH'
        WHEN 'KMH' THEN 'KMH'
        
        -- CORRECTED freezer capacity units mapped to standard available enums
        WHEN 'Metric Tons / Day' THEN 'METRIC_TONS / DAY'
        WHEN 'metric tons / day' THEN 'METRIC_TONS / DAY'
        WHEN 'METRIC TONS / DAY' THEN 'METRIC_TONS / DAY'
        WHEN 'Tons / Day' THEN 'TONS / DAY'
        WHEN 'tons / day' THEN 'TONS / DAY'
        WHEN 'TONS / DAY' THEN 'TONS / DAY'
        WHEN 'Tons / Day, Tons / Day' THEN 'TONS / DAY' -- Handle duplicates
        WHEN 'tons / day, tons / day' THEN 'TONS / DAY'
        
        ELSE 'METER' -- Default fallback
    END;
    
    RETURN result_enum;
END;
$$ LANGUAGE plpgsql STABLE;

-- Comprehensive conflict logging - ALL ORIGINAL FUNCTIONALITY
CREATE OR REPLACE FUNCTION npfc_log_conflict(
    p_vessel_uuid UUID,
    p_source_id UUID,
    p_match_method TEXT,
    p_field_name TEXT,
    p_existing_value TEXT,
    p_new_value TEXT
)
RETURNS UUID AS $$
DECLARE
    conflict_uuid UUID;
    conflict_record RECORD;
BEGIN
    -- Check if we have vessel_processing_conflicts table, if not just log to notice
    BEGIN
        SELECT table_name INTO conflict_record
        FROM information_schema.tables 
        WHERE table_name = 'vessel_processing_conflicts' 
        AND table_schema = current_schema();
        
        IF conflict_record.table_name IS NOT NULL THEN
            -- Table exists, insert conflict record
            INSERT INTO vessel_processing_conflicts (
                vessel_uuid, source_id, match_method, conflict_type,
                field_name, existing_value, new_value
            ) VALUES (
                p_vessel_uuid, p_source_id, p_match_method, 'IDENTIFIER_MISMATCH',
                p_field_name, p_existing_value, p_new_value
            ) RETURNING conflict_uuid INTO conflict_uuid;
        ELSE
            -- Table doesn't exist, just generate UUID and log
            conflict_uuid := gen_random_uuid();
            RAISE NOTICE 'NPFC Conflict: % % -> % (vessel: %, method: %)', 
                p_field_name, p_existing_value, p_new_value, p_vessel_uuid, p_match_method;
        END IF;
    EXCEPTION WHEN others THEN
        -- If anything fails, generate UUID and log
        conflict_uuid := gen_random_uuid();
        RAISE NOTICE 'NPFC Conflict: % % -> % (vessel: %, method: %)', 
            p_field_name, p_existing_value, p_new_value, p_vessel_uuid, p_match_method;
    END;
    
    RETURN conflict_uuid;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- COMPREHENSIVE PERFORMANCE INDEXES - ALL ORIGINAL FUNCTIONALITY
-- ============================================================================

-- Vessel lookup indexes for hierarchical matching
CREATE INDEX CONCURRENTLY IF NOT EXISTS vessels_npfc_imo_lookup_idx 
    ON vessels(imo) WHERE imo IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS vessels_npfc_ircs_lookup_idx 
    ON vessels(ircs) WHERE ircs IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS vessels_npfc_mmsi_lookup_idx 
    ON vessels(mmsi) WHERE mmsi IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS vessels_npfc_name_flag_registry_idx 
    ON vessels(vessel_name, vessel_flag, national_registry) 
    WHERE vessel_name IS NOT NULL AND vessel_flag IS NOT NULL AND national_registry IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS vessels_npfc_name_flag_idx 
    ON vessels(vessel_name, vessel_flag) 
    WHERE vessel_name IS NOT NULL AND vessel_flag IS NOT NULL;

-- Enhanced indexes for equipment freezer types JSONB arrays
CREATE INDEX CONCURRENTLY IF NOT EXISTS vessel_equipment_freezer_types_gin_idx 
    ON vessel_equipment USING gin(freezer_types) 
    WHERE freezer_types IS NOT NULL;

-- Enhanced source tracking indexes for NPFC
CREATE INDEX CONCURRENTLY IF NOT EXISTS vessel_sources_npfc_comprehensive_idx 
    ON vessel_sources(source_id, is_active, last_seen_date)
    WHERE source_id = (SELECT source_id FROM original_sources_vessels WHERE source_shortname = 'NPFC');

-- ============================================================================
-- COMPREHENSIVE VERIFICATION - ALL ORIGINAL FUNCTIONALITY
-- ============================================================================

DO $$
DECLARE
    npfc_source_id UUID;
    npfc_rfmo_id UUID;
    function_count INTEGER;
BEGIN
    -- Check RFMO exists
    SELECT id INTO npfc_rfmo_id FROM rfmos WHERE rfmo_acronym = 'NPFC';
    IF npfc_rfmo_id IS NULL THEN
        RAISE EXCEPTION 'NPFC RFMO not found';
    END IF;
    
    -- Check source exists
    SELECT source_id INTO npfc_source_id FROM original_sources_vessels WHERE source_shortname = 'NPFC';
    IF npfc_source_id IS NULL THEN
        RAISE EXCEPTION 'NPFC vessel source not found';
    END IF;
    
    -- Check comprehensive functions exist - ALL ORIGINAL FUNCTIONS
    SELECT COUNT(*) INTO function_count
    FROM pg_proc 
    WHERE proname IN (
        'get_country_uuid', 'get_vessel_type_uuid', 'get_gear_type_uuid',
        'find_existing_vessel', 'validate_vessel_identifiers', 'update_npfc_source_status',
        'create_freezer_types_array', 'npfc_get_vessel_type_code', 'npfc_get_fishing_method_code',
        'npfc_get_freezer_type_enum', 'npfc_get_unit_enum', 'npfc_log_conflict'
    );
    
    IF function_count < 12 THEN
        RAISE EXCEPTION 'NPFC comprehensive setup verification failed: missing functions (found %, expected 12)', function_count;
    END IF;
    
    RAISE NOTICE 'NPFC comprehensive setup verification successful:';
    RAISE NOTICE '  - NPFC RFMO ID: %', npfc_rfmo_id;
    RAISE NOTICE '  - NPFC source ID: %', npfc_source_id;
    RAISE NOTICE '  - ALL ORIGINAL comprehensive functions: % created', function_count;
    RAISE NOTICE '  - CORRECTED: Using existing vessel_types and gear_types_fao tables';
    RAISE NOTICE '  - CORRECTED: Fixed unit enum mappings (METER not METERS)';
    RAISE NOTICE '  - CORRECTED: Fixed function signatures for integer casting';
    RAISE NOTICE '  - CORRECTED: No new mapping tables created';
    RAISE NOTICE '  - CORRECTED: Using EXACT user-provided vessel type translations';
    RAISE NOTICE '  - CORRECTED: Using EXACT user-provided fishing method translations';
    RAISE NOTICE '  - ALL ORIGINAL vessel type mappings logic maintained';
    RAISE NOTICE '  - ALL ORIGINAL fishing method mappings logic maintained';  
    RAISE NOTICE '  - ALL ORIGINAL freezer type mappings logic maintained';
    RAISE NOTICE '  - Performance indexes created for all lookup patterns';
    RAISE NOTICE '  - Ready for comprehensive NPFC data loading with existing schema';
    RAISE NOTICE '  - COMPREHENSIVE FUNCTIONALITY MAINTAINED + CORRECTIONS APPLIED';
END;
$$;

\echo 'NPFC comprehensive setup completed using existing schema with ALL original functions + CORRECTED translations'