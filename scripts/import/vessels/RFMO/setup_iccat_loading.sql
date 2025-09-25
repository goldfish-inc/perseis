-- Setup functions for ICCAT vessel data loading
-- These functions handle ICCAT-specific mappings and transformations

-- Drop existing ICCAT functions if they exist
DROP FUNCTION IF EXISTS iccat_get_country_uuid CASCADE;
DROP FUNCTION IF EXISTS iccat_get_vessel_type_uuid CASCADE;
DROP FUNCTION IF EXISTS iccat_get_gear_type_uuid CASCADE;
DROP FUNCTION IF EXISTS iccat_map_vms_system CASCADE;

-- ICCAT country code mapping to UUID
CREATE OR REPLACE FUNCTION iccat_get_country_uuid(p_country_code TEXT)
RETURNS UUID AS $$
DECLARE
    v_uuid UUID;
    v_mapped_code TEXT;
BEGIN
    IF p_country_code IS NULL OR p_country_code = '' THEN
        RETURN NULL;
    END IF;
    
    -- ICCAT-specific country mappings
    v_mapped_code := CASE p_country_code
        -- EU countries (ICCAT uses EU-XXX format)
        WHEN 'EU-ESP' THEN 'ESP'
        WHEN 'EU-FRA' THEN 'FRA'
        WHEN 'EU-ITA' THEN 'ITA'
        WHEN 'EU-PRT' THEN 'PRT'
        WHEN 'EU-GRC' THEN 'GRC'
        WHEN 'EU-MLT' THEN 'MLT'
        WHEN 'EU-CYP' THEN 'CYP'
        WHEN 'EU-HRV' THEN 'HRV'
        WHEN 'EU-IRL' THEN 'IRL'
        WHEN 'EU-NLD' THEN 'NLD'
        WHEN 'EU-POL' THEN 'POL'
        WHEN 'EU-SVN' THEN 'SVN'
        WHEN 'EU-LTU' THEN 'LTU'
        WHEN 'EU-DEU' THEN 'DEU'
        WHEN 'EU-GBR' THEN 'GBR'  -- Historical
        WHEN 'EU' THEN 'EUE'       -- European Union entity
        
        -- Other mappings
        WHEN 'UK' THEN 'GBR'
        WHEN 'CHINESE TAIPEI' THEN 'TWN'
        WHEN 'KOREA REP' THEN 'KOR'
        WHEN 'S VINCENT' THEN 'VCT'
        WHEN 'S TOME PRN' THEN 'STP'
        WHEN 'T AND T' THEN 'TTO'
        WHEN 'C VERDE' THEN 'CPV'
        WHEN 'COTE D''IVOIRE' THEN 'CIV'
        WHEN 'FR ST P MQ' THEN 'SPM'  -- Saint Pierre and Miquelon
        
        -- Remove ICCAT-RMA (regional management, not a country)
        WHEN 'ICCAT-RMA' THEN NULL
        
        -- Otherwise use as-is if 3 chars
        ELSE CASE 
            WHEN LENGTH(p_country_code) = 3 THEN p_country_code
            ELSE NULL
        END
    END;
    
    -- Look up UUID
    IF v_mapped_code IS NOT NULL THEN
        SELECT id INTO v_uuid
        FROM country_iso
        WHERE alpha_3_code = v_mapped_code;
        
        -- Log unmapped country codes for investigation
        IF v_uuid IS NULL THEN
            RAISE NOTICE 'ICCAT: Unmapped country code: % (mapped to: %)', p_country_code, v_mapped_code;
        END IF;
    END IF;
    
    RETURN v_uuid;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ICCAT vessel type mapping to UUID
CREATE OR REPLACE FUNCTION iccat_get_vessel_type_uuid(p_vessel_type_code TEXT)
RETURNS UUID AS $$
DECLARE
    v_uuid UUID;
BEGIN
    IF p_vessel_type_code IS NULL OR p_vessel_type_code = '' THEN
        RETURN NULL;
    END IF;
    
    -- ICCAT uses standard ISSCFV codes, so direct lookup
    SELECT id INTO v_uuid
    FROM vessel_types
    WHERE vessel_type_isscfv_code = p_vessel_type_code
       OR vessel_type_isscfv_alpha = p_vessel_type_code;
    
    IF v_uuid IS NULL AND p_vessel_type_code IS NOT NULL THEN
        RAISE NOTICE 'ICCAT: Unmapped vessel type code: %', p_vessel_type_code;
    END IF;
    
    RETURN v_uuid;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ICCAT gear type mapping to UUID
CREATE OR REPLACE FUNCTION iccat_get_gear_type_uuid(p_gear_code TEXT)
RETURNS UUID AS $$
DECLARE
    v_uuid UUID;
    v_mapped_code TEXT;
BEGIN
    IF p_gear_code IS NULL OR p_gear_code = '' THEN
        RETURN NULL;
    END IF;
    
    -- ICCAT gear code mappings to FAO ISSCFG codes
    v_mapped_code := CASE p_gear_code
        WHEN 'BB' THEN '09.9.0'      -- Traps
        WHEN 'GILL' THEN '07.1.0'    -- Gillnets
        WHEN 'HAND' THEN '09.1.0'    -- Handlines
        WHEN 'HARP' THEN '10.1.0'    -- Harpoons
        WHEN 'LL' THEN '09.4.0'      -- Longlines
        WHEN 'LLD' THEN '09.4.1'     -- Drifting longlines
        WHEN 'LLP' THEN '09.4.2'     -- Set longlines
        WHEN 'LL?' THEN '09.4.0'     -- Longlines (unspecified)
        WHEN 'MWT' THEN '03.1.2'     -- Midwater trawls
        WHEN 'OTHER' THEN '20.0.0'   -- Other gear
        WHEN 'PS' THEN '01.1.0'      -- Purse seines
        WHEN 'RR' THEN '09.3.0'      -- Pole and line (rod and reel)
        WHEN 'SURF' THEN '09.9.0'    -- Surface
        WHEN 'TROL' THEN '03.1.0'    -- Trawls
        WHEN 'TROP' THEN '09.9.0'    -- Tropical
        WHEN 'UNCL' THEN '20.0.0'    -- Unclassified
        WHEN 'NAP' THEN '25.9.0'     -- No gear (support vessel)
        ELSE p_gear_code             -- Try direct match
    END;
    
    -- Look up UUID
    SELECT id INTO v_uuid
    FROM gear_types_fao
    WHERE fao_isscfg_code = v_mapped_code
       OR fao_isscfg_alpha = p_gear_code;
    
    IF v_uuid IS NULL AND p_gear_code IS NOT NULL THEN
        RAISE NOTICE 'ICCAT: Unmapped gear type code: % (mapped to: %)', p_gear_code, v_mapped_code;
    END IF;
    
    RETURN v_uuid;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Map ICCAT VMS system codes
CREATE OR REPLACE FUNCTION iccat_map_vms_system(p_vms_code TEXT)
RETURNS TEXT AS $$
BEGIN
    IF p_vms_code IS NULL OR p_vms_code = '' THEN
        RETURN NULL;
    END IF;
    
    -- ICCAT VMS system codes
    RETURN CASE UPPER(p_vms_code)
        WHEN 'ARGOS' THEN 'ARGOS'
        WHEN 'INMARSAT' THEN 'INMARSAT-C'
        WHEN 'INMARSAT-C' THEN 'INMARSAT-C'
        WHEN 'IRIDIUM' THEN 'IRIDIUM'
        WHEN 'SATLINK' THEN 'SATLINK'
        WHEN 'THORAYA' THEN 'THURAYA'
        WHEN 'THURAYA' THEN 'THURAYA'
        WHEN 'ORBCOMM' THEN 'ORBCOMM'
        WHEN 'OTHER' THEN 'OTHER'
        ELSE p_vms_code
    END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Create comments for documentation
COMMENT ON FUNCTION iccat_get_country_uuid IS 'Maps ICCAT country codes (including EU-XXX format) to country_iso UUIDs';
COMMENT ON FUNCTION iccat_get_vessel_type_uuid IS 'Maps ICCAT vessel type codes (ISSCFV) to vessel_types UUIDs';
COMMENT ON FUNCTION iccat_get_gear_type_uuid IS 'Maps ICCAT gear codes to gear_types_fao UUIDs';
COMMENT ON FUNCTION iccat_map_vms_system IS 'Standardizes ICCAT VMS system codes';