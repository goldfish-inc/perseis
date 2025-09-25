-- Setup functions for CCSBT vessel data loading
-- These functions handle CCSBT-specific data structures

-- Helper function to process CCSBT authorization status
CREATE OR REPLACE FUNCTION ccsbt_process_authorization(
    p_vessel_uuid UUID,
    p_source_id UUID,
    p_authorization_status TEXT,
    p_auth_from_date DATE,
    p_auth_to_date DATE,
    p_target_species TEXT,
    p_area_of_operation TEXT
) RETURNS VOID AS $$
BEGIN
    -- Store authorization information
    IF p_authorization_status IS NOT NULL THEN
        -- Update vessel_sources with authorization status
        UPDATE vessel_sources
        SET is_active = (p_authorization_status IN ('AUTHORIZED', 'ACTIVE', 'CURRENT', 'VALID')),
            data_governance_notes = COALESCE(data_governance_notes, '') || 
                E'\nAuthorization Status: ' || p_authorization_status ||
                CASE WHEN p_auth_from_date IS NOT NULL OR p_auth_to_date IS NOT NULL
                     THEN E'\nAuthorization Period: ' || 
                          COALESCE(p_auth_from_date::text, 'Unknown') || ' to ' || 
                          COALESCE(p_auth_to_date::text, 'Current')
                     ELSE ''
                END ||
                CASE WHEN p_target_species IS NOT NULL 
                     THEN E'\nTarget Species: ' || p_target_species
                     ELSE ''
                END ||
                CASE WHEN p_area_of_operation IS NOT NULL 
                     THEN E'\nArea of Operation: ' || p_area_of_operation
                     ELSE ''
                END
        WHERE vessel_uuid = p_vessel_uuid
          AND source_id = p_source_id;
    END IF;

    -- Record authorization period
    IF p_auth_from_date IS NOT NULL OR p_auth_to_date IS NOT NULL THEN
        -- Store authorization period in vessel_attributes as vessel_reported_history 
        -- doesn't have the columns for this type of data
        INSERT INTO vessel_attributes (
            vessel_uuid,
            source_id,
            attributes,
            last_updated
        ) VALUES (
            p_vessel_uuid,
            p_source_id,
            jsonb_build_object(
                'authorization_period', jsonb_build_object(
                    'from_date', p_auth_from_date,
                    'to_date', p_auth_to_date,
                    'recorded_date', CURRENT_DATE
                )
            ),
            NOW()
        ) ON CONFLICT (vessel_uuid, source_id) DO UPDATE
        SET attributes = vessel_attributes.attributes || EXCLUDED.attributes,
            last_updated = NOW();
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to process CCSBT vessel metrics
CREATE OR REPLACE FUNCTION ccsbt_process_vessel_metrics(
    p_vessel_uuid UUID,
    p_source_id UUID,
    p_gross_tonnage NUMERIC,
    p_length_value NUMERIC,
    p_length_unit TEXT
) RETURNS VOID AS $$
BEGIN
    -- Insert gross tonnage
    IF p_gross_tonnage IS NOT NULL AND p_gross_tonnage > 0 THEN
        -- Intelligence principle: Multiple measurements are valuable data points
        INSERT INTO vessel_metrics (
            vessel_uuid,
            source_id,
            metric_type,
            value,
            unit,
            created_at
        ) VALUES (
            p_vessel_uuid,
            p_source_id,
            'gross_tonnage'::metric_type_enum,
            p_gross_tonnage,
            'DIMENSIONLESS'::unit_enum,
            NOW()
        );
        -- No ON CONFLICT - each measurement is a data point
    END IF;
    
    -- Insert length
    IF p_length_value IS NOT NULL AND p_length_value > 0 THEN
        -- Intelligence principle: Multiple measurements are valuable data points
        INSERT INTO vessel_metrics (
            vessel_uuid,
            source_id,
            metric_type,
            value,
            unit,
            created_at
        ) VALUES (
            p_vessel_uuid,
            p_source_id,
            'length_loa'::metric_type_enum,
            p_length_value,
            COALESCE(p_length_unit, 'METER')::unit_enum,
            NOW()
        );
        -- No ON CONFLICT - each measurement is a data point
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to handle CCSBT vessel operators
CREATE OR REPLACE FUNCTION ccsbt_process_vessel_operators(
    p_vessel_uuid UUID,
    p_source_id UUID,
    p_owner_name TEXT,
    p_operator_name TEXT
) RETURNS VOID AS $$
DECLARE
    v_owner_id UUID;
    v_operator_id UUID;
BEGIN
    -- Process owner
    IF p_owner_name IS NOT NULL AND p_owner_name != '' THEN
        -- Get or create owner
        v_owner_id := get_or_create_associate(
            p_owner_name,
            'OWNER',
            NULL,
            NULL
        );
        
        -- Link to vessel
        PERFORM link_vessel_associate(
            p_vessel_uuid,
            v_owner_id,
            p_source_id
        );
    END IF;
    
    -- Process operator (if different from owner)
    IF p_operator_name IS NOT NULL AND p_operator_name != '' 
       AND p_operator_name != p_owner_name THEN
        -- Get or create operator
        v_operator_id := get_or_create_associate(
            p_operator_name,
            'OPERATOR',
            NULL,
            NULL
        );
        
        -- Link to vessel
        PERFORM link_vessel_associate(
            p_vessel_uuid,
            v_operator_id,
            p_source_id
        );
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to record CCSBT target species information
CREATE OR REPLACE FUNCTION ccsbt_record_target_species(
    p_vessel_uuid UUID,
    p_source_id UUID,
    p_target_species TEXT
) RETURNS VOID AS $$
DECLARE
    v_species_code TEXT;
BEGIN
    IF p_target_species IS NOT NULL AND p_target_species != '' THEN
        -- Map common CCSBT species names to FAO codes
        v_species_code := CASE 
            WHEN p_target_species ILIKE '%southern bluefin%' THEN 'SBF'
            WHEN p_target_species ILIKE '%SBT%' THEN 'SBF'
            WHEN p_target_species ILIKE '%bluefin%' THEN 'SBF'
            WHEN p_target_species ILIKE '%albacore%' THEN 'ALB'
            WHEN p_target_species ILIKE '%bigeye%' THEN 'BET'
            WHEN p_target_species ILIKE '%yellowfin%' THEN 'YFT'
            WHEN p_target_species ILIKE '%skipjack%' THEN 'SKJ'
            ELSE p_target_species
        END;
        
        -- Store in vessel_sources notes for now
        -- In future, this could link to a species table
        UPDATE vessel_sources
        SET data_quality_notes = COALESCE(data_quality_notes, '') || 
            E'\nTarget Species: ' || v_species_code || ' (' || p_target_species || ')'
        WHERE vessel_uuid = p_vessel_uuid
          AND source_id = p_source_id;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to determine CCSBT vessel type from multiple indicators
CREATE OR REPLACE FUNCTION ccsbt_determine_vessel_type(
    p_vessel_type_code TEXT,
    p_gear_type_code TEXT
) RETURNS TEXT AS $$
BEGIN
    -- Use vessel type if available
    IF p_vessel_type_code IS NOT NULL THEN
        RETURN p_vessel_type_code;
    END IF;
    
    -- Otherwise infer from gear type
    IF p_gear_type_code IS NOT NULL THEN
        RETURN CASE 
            WHEN p_gear_type_code = '09.4.0' THEN 'LL'  -- Longline
            WHEN p_gear_type_code = '01.1.0' THEN 'PS'  -- Purse seine
            WHEN p_gear_type_code = '09.3.0' THEN 'BB'  -- Pole and line
            WHEN p_gear_type_code = '03.1.0' THEN 'TO'  -- Trawl
            WHEN p_gear_type_code = '07.1.0' THEN 'GO'  -- Gillnet
            WHEN p_gear_type_code = '07.2.1' THEN 'GD'  -- Driftnet
            ELSE 'NO'  -- Other
        END;
    END IF;
    
    -- Default to unknown
    RETURN 'NO';
END;
$$ LANGUAGE plpgsql;

-- Comments
COMMENT ON FUNCTION ccsbt_process_authorization IS 'Handles CCSBT vessel authorization status and periods';
COMMENT ON FUNCTION ccsbt_process_vessel_metrics IS 'Processes CCSBT tonnage and length measurements';
COMMENT ON FUNCTION ccsbt_process_vessel_operators IS 'Links owner and operator companies to vessels';
COMMENT ON FUNCTION ccsbt_record_target_species IS 'Records target species information for CCSBT vessels';
COMMENT ON FUNCTION ccsbt_determine_vessel_type IS 'Determines vessel type from type code or gear type';