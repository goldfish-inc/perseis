-- /app/scripts/import/vessels/data/RFMO/setup_sprfmo_loading.sql
-- COMPREHENSIVE: SPRFMO Setup - COMPLETE WORKING VERSION with all functions
\echo 'Setting up SPRFMO vessel loading (COMPLETE WORKING - All Functions)'

-- Verify SPRFMO source exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM original_sources_vessels WHERE source_shortname = 'SPRFMO') THEN
        INSERT INTO original_sources_vessels (
            source_shortname, source_fullname, source_types, refresh_date, status
        ) VALUES (
            'SPRFMO',
            'South Pacific Regional Fisheries Management Organisation Vessel Registry',
            ARRAY['RFMO']::TEXT[],
            CURRENT_DATE,
            'PENDING'
        );
        RAISE NOTICE 'Created SPRFMO source';
    ELSE
        RAISE NOTICE 'Found existing SPRFMO source';
    END IF;
END $$;

-- Ensure SPRFMO RFMO exists
INSERT INTO rfmos (rfmo_acronym, rfmo_name, rfmo_full_name, established_year, headquarters_location)
VALUES ('SPRFMO', 'SPRFMO', 'South Pacific Regional Fisheries Management Organisation', 2012, 'Wellington, New Zealand')
ON CONFLICT (rfmo_acronym) DO NOTHING;

-- ============================================================================
-- DROP EXISTING FUNCTIONS TO ENSURE CLEAN RECREATION
-- ============================================================================

DROP FUNCTION IF EXISTS get_country_uuid(TEXT);
DROP FUNCTION IF EXISTS get_vessel_type_uuid(TEXT);
DROP FUNCTION IF EXISTS get_gear_type_uuid(TEXT);
DROP FUNCTION IF EXISTS standardize_metric_type(TEXT, DECIMAL);
DROP FUNCTION IF EXISTS standardize_unit(TEXT, TEXT);
DROP FUNCTION IF EXISTS get_external_identifier_type(TEXT);
DROP FUNCTION IF EXISTS year_to_date(INTEGER);

-- ============================================================================
-- ALL 7 COMPREHENSIVE FUNCTIONS - COMPLETE WORKING VERSION
-- ============================================================================

-- 1. COUNTRY UUID RESOLUTION - Using confirmed column names
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
    
    -- COMPREHENSIVE SPRFMO-specific country mappings
    CASE clean_input
        WHEN 'CHINESE TAIPEI' THEN clean_input := 'TWN';
        WHEN 'SOUTH KOREA' THEN clean_input := 'KOR';
        WHEN 'KOREA' THEN clean_input := 'KOR';
        WHEN 'REPUBLIC OF KOREA' THEN clean_input := 'KOR';
        WHEN 'BELIZE' THEN clean_input := 'BLZ';
        WHEN 'RUSSIA' THEN clean_input := 'RUS';
        WHEN 'RUSSIAN FEDERATION' THEN clean_input := 'RUS';
        WHEN 'JAPAN' THEN clean_input := 'JPN';
        WHEN 'CHINA' THEN clean_input := 'CHN';
        WHEN 'UNITED STATES' THEN clean_input := 'USA';
        WHEN 'CANADA' THEN clean_input := 'CAN';
        WHEN 'NORWAY' THEN clean_input := 'NOR';
        WHEN 'NEW ZEALAND' THEN clean_input := 'NZL';
        WHEN 'AUSTRALIA' THEN clean_input := 'AUS';
        WHEN 'VANUATU' THEN clean_input := 'VUT';
        WHEN 'CHILE' THEN clean_input := 'CHL';
        WHEN 'PERU' THEN clean_input := 'PER';
        WHEN 'ECUADOR' THEN clean_input := 'ECU';
        WHEN 'FAROE ISLANDS' THEN clean_input := 'FRO';
        WHEN 'COOK ISLANDS' THEN clean_input := 'COK';
        WHEN 'CUBA' THEN clean_input := 'CUB';
        WHEN 'CURACAO', 'CURAÇAO' THEN clean_input := 'CUW';
        WHEN 'LIBERIA' THEN clean_input := 'LBR';
        WHEN 'PANAMA' THEN clean_input := 'PAN';
        WHEN 'NETHERLANDS' THEN clean_input := 'NLD';
        WHEN 'GERMANY' THEN clean_input := 'DEU';
        WHEN 'POLAND' THEN clean_input := 'POL';
        WHEN 'SPAIN' THEN clean_input := 'ESP';
        WHEN 'LITHUANIA' THEN clean_input := 'LTU';
        WHEN 'PORTUGAL' THEN clean_input := 'PRT';
        ELSE 
            clean_input := clean_input;
    END CASE;
    
    -- Use confirmed column names: alpha_3_code and alpha_2_code
    SELECT id INTO result_uuid
    FROM country_iso 
    WHERE alpha_3_code = clean_input;
    
    IF result_uuid IS NULL THEN
        SELECT id INTO result_uuid
        FROM country_iso 
        WHERE alpha_2_code = clean_input;
    END IF;
    
    IF result_uuid IS NULL THEN
        RAISE WARNING 'Country code not found: %', country_input;
    END IF;
    
    RETURN result_uuid;
END;
$$ LANGUAGE plpgsql;

-- 2. VESSEL TYPE UUID RESOLUTION - Using confirmed column name
CREATE OR REPLACE FUNCTION get_vessel_type_uuid(vessel_type_input TEXT)
RETURNS UUID AS $$
DECLARE
    result_uuid UUID;
    clean_input TEXT;
BEGIN
    IF vessel_type_input IS NULL OR trim(vessel_type_input) = '' THEN
        RETURN NULL;
    END IF;
    
    clean_input := upper(trim(vessel_type_input));
    
    -- Use confirmed column name: vessel_type_isscfv_alpha
    SELECT id INTO result_uuid
    FROM vessel_types 
    WHERE upper(vessel_type_isscfv_alpha) = clean_input;
    
    IF result_uuid IS NULL THEN
        RAISE WARNING 'Vessel type not found: %', vessel_type_input;
    END IF;
    
    RETURN result_uuid;
END;
$$ LANGUAGE plpgsql;

-- 3. GEAR TYPE UUID RESOLUTION - Using confirmed column name
CREATE OR REPLACE FUNCTION get_gear_type_uuid(gear_input TEXT)
RETURNS UUID AS $$
DECLARE
    result_uuid UUID;
    clean_input TEXT;
    gear_array TEXT[];
    gear_code TEXT;
BEGIN
    IF gear_input IS NULL OR trim(gear_input) = '' THEN
        RETURN NULL;
    END IF;
    
    clean_input := trim(gear_input);
    
    -- Handle multiple gear types separated by semicolons
    gear_array := string_to_array(clean_input, ';');
    gear_code := trim(gear_array[1]); -- Take first gear type
    
    -- Use confirmed column name: fao_isscfg_code
    SELECT id INTO result_uuid
    FROM gear_types_fao 
    WHERE fao_isscfg_code = gear_code;
    
    IF result_uuid IS NULL THEN
        RAISE WARNING 'Gear type not found: %', gear_input;
    END IF;
    
    RETURN result_uuid;
END;
$$ LANGUAGE plpgsql;

-- 4. COMPREHENSIVE METRIC TYPE STANDARDIZATION
CREATE OR REPLACE FUNCTION standardize_metric_type(input_type TEXT, input_value DECIMAL DEFAULT NULL)
RETURNS metric_type_enum AS $$
DECLARE
    clean_input TEXT;
    result_type metric_type_enum;
BEGIN
    IF input_type IS NULL OR trim(input_type) = '' THEN
        RETURN NULL;
    END IF;
    
    clean_input := lower(trim(input_type));
    
    -- COMPREHENSIVE SPRFMO-specific metric type mappings
    CASE clean_input
        -- Length measurements
        WHEN 'lengthloa', 'length_loa', 'loa', 'length overall', 'total length', 'eslora total' THEN
            result_type := 'length_loa';
        WHEN 'lengthlbp', 'length_lbp', 'lbp', 'length between perpendiculars' THEN
            result_type := 'length_lbp';
        WHEN 'lengthrgl', 'length_rgl', 'rgl', 'registered length', 'register' THEN
            result_type := 'length_rgl';
        WHEN 'length' THEN
            result_type := 'length';
        
        -- Beam measurements
        WHEN 'beam', 'width', 'beam_width' THEN
            result_type := 'beam';
        WHEN 'extreme_beam', 'extremebeam' THEN
            result_type := 'extreme_beam';
        WHEN 'moulded_beam', 'mouldedbeam' THEN
            result_type := 'moulded_beam';
            
        -- Depth measurements
        WHEN 'depth', 'moulded_depth', 'mouldeddepth' THEN
            result_type := 'moulded_depth';
        WHEN 'draft_depth', 'draftdepth', 'draft' THEN
            result_type := 'draft_depth';
            
        -- Tonnage measurements
        WHEN 'gross_tonnage', 'grosstonnage', 'gt' THEN
            result_type := 'gross_tonnage';
        WHEN 'gross_register_tonnage', 'grossregistertonnage', 'grt' THEN
            result_type := 'gross_register_tonnage';
        WHEN 'net_tonnage', 'nettonnage', 'nt' THEN
            result_type := 'net_tonnage';
        WHEN 'tonnage' THEN
            result_type := 'tonnage';
            
        -- Engine and capacity measurements
        WHEN 'engine_power', 'enginepower', 'power' THEN
            result_type := 'engine_power';
        WHEN 'fish_hold_volume', 'fishholdvolume', 'hold_capacity', 'holdcapacity' THEN
            result_type := 'fish_hold_volume';
        WHEN 'freezer_capacity', 'freezercapacity' THEN
            result_type := 'freezer_capacity';
        WHEN 'carrying_capacity', 'carryingcapacity' THEN
            result_type := 'carrying_capacity';
            
        -- Default fallbacks
        ELSE
            result_type := 'length'::metric_type_enum;
    END CASE;
    
    RETURN result_type;
END;
$$ LANGUAGE plpgsql;

-- 5. COMPREHENSIVE UNIT ENUM STANDARDIZATION
CREATE OR REPLACE FUNCTION standardize_unit(input_unit TEXT, measurement_type TEXT DEFAULT 'general')
RETURNS unit_enum AS $$
DECLARE
    clean_input TEXT;
    clean_type TEXT;
    result_unit unit_enum;
BEGIN
    IF input_unit IS NULL OR trim(input_unit) = '' THEN
        -- Return default units based on measurement type
        CASE lower(trim(coalesce(measurement_type, 'general')))
            WHEN 'length', 'beam', 'depth' THEN RETURN 'METER'::unit_enum;
            WHEN 'volume', 'capacity' THEN RETURN 'CUBIC_METER'::unit_enum;
            WHEN 'power', 'engine' THEN RETURN 'KW'::unit_enum;
            ELSE RETURN NULL;
        END CASE;
    END IF;
    
    clean_input := upper(trim(input_unit));
    clean_type := lower(trim(coalesce(measurement_type, 'general')));
    
    -- COMPREHENSIVE unit mappings
    CASE clean_input
        -- Length units
        WHEN 'M', 'METER', 'METRE', 'METERS', 'METRES' THEN
            result_unit := 'METER';
        WHEN 'FT', 'FEET', 'FOOT' THEN
            result_unit := 'FEET';
            
        -- Volume units  
        WHEN 'M³', 'M3', 'CUBIC_METER', 'CUBIC METER', 'CBM' THEN
            result_unit := 'CUBIC_METER';
        WHEN 'FT³', 'FT3', 'CUBIC_FEET', 'CUBIC FEET', 'CFT' THEN
            result_unit := 'CUBIC_FEET';
        WHEN 'L', 'LITER', 'LITRE', 'LITERS', 'LITRES' THEN
            result_unit := 'LITER';
        WHEN 'GAL', 'GALLON', 'GALLONS' THEN
            result_unit := 'GALLON';
            
        -- Power units
        WHEN 'KW', 'KILOWATT', 'KILOWATTS' THEN
            result_unit := 'KW';
        WHEN 'HP', 'HORSEPOWER' THEN
            result_unit := 'HP';
        WHEN 'PS', 'PFERDESTÄRKE' THEN
            result_unit := 'PS';
            
        -- Speed units
        WHEN 'KNOTS', 'KN', 'KNOT' THEN
            result_unit := 'KNOTS';
        WHEN 'MPH', 'MILES PER HOUR' THEN
            result_unit := 'MPH';
        WHEN 'KMH', 'KM/H', 'KILOMETERS PER HOUR' THEN
            result_unit := 'KMH';
            
        -- Default fallbacks by measurement type
        ELSE
            CASE clean_type
                WHEN 'length', 'beam', 'depth' THEN result_unit := 'METER';
                WHEN 'volume', 'capacity' THEN result_unit := 'CUBIC_METER';
                WHEN 'power', 'engine' THEN result_unit := 'KW';
                WHEN 'speed' THEN result_unit := 'KNOTS';
                ELSE result_unit := NULL;
            END CASE;
    END CASE;
    
    RETURN result_unit;
END;
$$ LANGUAGE plpgsql;

-- 6. EXTERNAL IDENTIFIER TYPE MAPPING
CREATE OR REPLACE FUNCTION get_external_identifier_type(identifier_input TEXT)
RETURNS external_identifier_type_enum AS $$
BEGIN
    RETURN 'RFMO_SPRFMO'::external_identifier_type_enum;
END;
$$ LANGUAGE plpgsql;

-- 7. YEAR-TO-DATE CONVERSION
CREATE OR REPLACE FUNCTION year_to_date(year_input INTEGER)
RETURNS DATE AS $$
BEGIN
    IF year_input IS NULL OR year_input < 1800 OR year_input > 2100 THEN
        RETURN NULL;
    END IF;
    
    RETURN make_date(year_input, 1, 1);
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;

\echo 'SPRFMO setup complete - ALL 7 FUNCTIONS CREATED SUCCESSFULLY'