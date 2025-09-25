-- Setup functions for IATTC vessel data loading
-- These functions handle IATTC-specific data structures

-- Helper function to process IATTC vessel status
CREATE OR REPLACE FUNCTION iattc_process_vessel_status(
    p_vessel_uuid UUID,
    p_source_id UUID,
    p_register_status TEXT,
    p_confirmation_date DATE,
    p_notes TEXT
) RETURNS VOID AS $$
BEGIN
    -- Store status information
    IF p_register_status IS NOT NULL THEN
        -- Update vessel_sources with status
        UPDATE vessel_sources
        SET is_active = (p_register_status = 'Active'),
            data_governance_notes = COALESCE(data_governance_notes, '') || 
                E'\nStatus: ' || p_register_status ||
                CASE WHEN p_confirmation_date IS NOT NULL 
                     THEN ' (Confirmed: ' || p_confirmation_date::text || ')' 
                     ELSE '' 
                END ||
                CASE WHEN p_notes IS NOT NULL 
                     THEN E'\nNotes: ' || p_notes 
                     ELSE '' 
                END
        WHERE vessel_uuid = p_vessel_uuid
          AND source_id = p_source_id;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to process IATTC vessel metrics including carrying capacity
CREATE OR REPLACE FUNCTION iattc_process_vessel_metrics(
    p_vessel_uuid UUID,
    p_source_id UUID,
    p_gross_tonnage NUMERIC,
    p_fish_hold_volume NUMERIC,
    p_carrying_capacity NUMERIC
) RETURNS VOID AS $$
BEGIN
    -- Insert gross tonnage (allowing multiple measurements as intelligence)
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
    
    -- Insert fish hold volume (allowing multiple measurements as intelligence)
    IF p_fish_hold_volume IS NOT NULL AND p_fish_hold_volume > 0 THEN
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
            'fish_hold_capacity'::metric_type_enum,
            p_fish_hold_volume,
            'CUBIC_METER'::unit_enum,
            NOW()
        );
        -- No ON CONFLICT - each measurement is a data point
    END IF;
    
    -- Store carrying capacity in notes (no standard metric for this)
    IF p_carrying_capacity IS NOT NULL THEN
        UPDATE vessel_sources
        SET data_governance_notes = COALESCE(data_governance_notes, '') || 
            E'\nCarrying Capacity: ' || p_carrying_capacity::text || ' MT'
        WHERE vessel_uuid = p_vessel_uuid
          AND source_id = p_source_id;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to handle IATTC previous vessel information
CREATE OR REPLACE FUNCTION iattc_record_previous_identity(
    p_vessel_uuid UUID,
    p_source_id UUID,
    p_previous_name TEXT,
    p_previous_flag TEXT,
    p_last_modification DATE DEFAULT NULL
) RETURNS VOID AS $$
DECLARE
    v_previous_flag_id UUID;
    v_event_date DATE;
BEGIN
    -- Use last modification date or current date
    v_event_date := COALESCE(p_last_modification, CURRENT_DATE);
    
    -- Record previous name if provided
    IF p_previous_name IS NOT NULL AND p_previous_name != '' THEN
        INSERT INTO vessel_reported_history (
            vessel_uuid,
            source_id,
            reported_history_type,
            identifier_value,
            created_at
        ) VALUES (
            p_vessel_uuid,
            p_source_id,
            'VESSEL_NAME_CHANGE'::reported_history_enum,
            p_previous_name,
            NOW()
        );
        
        -- Record reputation event for name change
        PERFORM record_reputation_event(
            p_vessel_uuid,
            v_event_date,
            'NAME_CHANGE',
            p_source_id,
            p_previous_name,
            (SELECT vessel_name FROM vessels WHERE vessel_uuid = p_vessel_uuid)
        );
    END IF;
    
    -- Record previous flag if provided
    IF p_previous_flag IS NOT NULL AND p_previous_flag != '' THEN
        -- Get country ID for previous flag
        SELECT id INTO v_previous_flag_id
        FROM country_iso
        WHERE alpha_3_code = p_previous_flag;
        
        IF v_previous_flag_id IS NOT NULL THEN
            INSERT INTO vessel_reported_history (
                vessel_uuid,
                source_id,
                reported_history_type,
                flag_country_id,
                created_at
            ) VALUES (
                p_vessel_uuid,
                p_source_id,
                'FLAG_CHANGE'::reported_history_enum,
                v_previous_flag_id,
                NOW()
            );
            
            -- Record reputation event for flag change
            PERFORM record_reputation_event(
                p_vessel_uuid,
                v_event_date,
                'FLAG_CHANGE',
                p_source_id,
                p_previous_flag,
                (SELECT c.alpha_3_code 
                 FROM vessels v 
                 JOIN country_iso c ON v.vessel_flag = c.id 
                 WHERE v.vessel_uuid = p_vessel_uuid)
            );
        END IF;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to process IATTC company information
CREATE OR REPLACE FUNCTION iattc_process_company_info(
    p_vessel_uuid UUID,
    p_source_id UUID,
    p_company_name TEXT,
    p_company_address TEXT
) RETURNS VOID AS $$
BEGIN
    -- Store company information in vessel attributes
    -- Associates table doesn't exist yet
    IF p_company_name IS NOT NULL OR p_company_address IS NOT NULL THEN
        INSERT INTO vessel_attributes (
            vessel_uuid,
            source_id,
            attributes,
            last_updated
        ) VALUES (
            p_vessel_uuid,
            p_source_id,
            jsonb_build_object(
                'company_info', jsonb_build_object(
                    'name', p_company_name,
                    'address', p_company_address
                )
            ),
            NOW()
        )
        ON CONFLICT (vessel_uuid, source_id) DO UPDATE
        SET attributes = vessel_attributes.attributes || EXCLUDED.attributes,
            last_updated = NOW();
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to determine IATTC vessel type from gear
CREATE OR REPLACE FUNCTION iattc_infer_vessel_type(p_gear_code TEXT)
RETURNS TEXT AS $$
BEGIN
    -- Map gear codes to vessel types
    RETURN CASE 
        WHEN p_gear_code = '01.1.0' THEN 'PS'  -- Purse seine
        WHEN p_gear_code = '09.4.0' THEN 'LL'  -- Longline
        WHEN p_gear_code = '09.3.0' THEN 'BB'  -- Pole and line
        WHEN p_gear_code = '03.1.0' THEN 'TO'  -- Trawl
        WHEN p_gear_code = '07.1.0' THEN 'GO'  -- Gillnet
        WHEN p_gear_code = '09.6.0' THEN 'LX'  -- Troll
        ELSE 'NO'  -- Other
    END;
END;
$$ LANGUAGE plpgsql;

-- Comments
COMMENT ON FUNCTION iattc_process_vessel_status IS 'Handles IATTC vessel registration status and confirmation dates';
COMMENT ON FUNCTION iattc_process_vessel_metrics IS 'Processes IATTC tonnage, volume, and carrying capacity';
COMMENT ON FUNCTION iattc_record_previous_identity IS 'Records previous vessel names and flags for history tracking';
COMMENT ON FUNCTION iattc_process_company_info IS 'Links company information to vessels';
COMMENT ON FUNCTION iattc_infer_vessel_type IS 'Infers vessel type from gear type when not explicitly provided';