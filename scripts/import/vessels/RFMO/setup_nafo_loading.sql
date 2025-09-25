-- Setup functions for NAFO vessel data loading
-- These functions handle NAFO-specific data structures

-- Helper function to process NAFO authorization and divisions
CREATE OR REPLACE FUNCTION nafo_process_authorization(
    p_vessel_uuid UUID,
    p_source_id UUID,
    p_authorization_status TEXT,
    p_nafo_divisions TEXT,
    p_notification_date DATE
) RETURNS VOID AS $$
BEGIN
    -- Store authorization information
    IF p_authorization_status IS NOT NULL THEN
        -- Update vessel_sources with authorization status
        UPDATE vessel_sources
        SET is_active = (p_authorization_status IN ('AUTHORIZED', 'ACTIVE', 'NOTIFIED', 'VALID')),
            data_governance_notes = COALESCE(data_governance_notes, '') || 
                E'\nAuthorization Status: ' || p_authorization_status ||
                CASE WHEN p_nafo_divisions IS NOT NULL 
                     THEN E'\nNAFO Divisions: ' || p_nafo_divisions
                     ELSE ''
                END ||
                CASE WHEN p_notification_date IS NOT NULL 
                     THEN E'\nNotification Date: ' || p_notification_date::text
                     ELSE ''
                END
        WHERE vessel_uuid = p_vessel_uuid
          AND source_id = p_source_id;
    END IF;

    -- Record NAFO divisions as area of operation
    IF p_nafo_divisions IS NOT NULL THEN
        INSERT INTO vessel_reported_history (
            vessel_uuid,
            source_id,
            change_type,
            previous_value,
            new_value,
            reported_date,
            created_at
        ) VALUES (
            p_vessel_uuid,
            p_source_id,
            'AREA_OF_OPERATION',
            'NAFO Divisions',
            p_nafo_divisions,
            COALESCE(p_notification_date, CURRENT_DATE),
            NOW()
        )
        ON CONFLICT (vessel_uuid, source_id, change_type, reported_date) 
        DO UPDATE SET
            new_value = EXCLUDED.new_value,
            updated_at = NOW();
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to process NAFO species quotas
CREATE OR REPLACE FUNCTION nafo_process_species_quotas(
    p_vessel_uuid UUID,
    p_source_id UUID,
    p_species_quotas TEXT
) RETURNS VOID AS $$
DECLARE
    v_species_array TEXT[];
    v_species TEXT;
    v_fao_code TEXT;
BEGIN
    IF p_species_quotas IS NOT NULL AND p_species_quotas != '' THEN
        -- Parse species list (comma or semicolon separated)
        v_species_array := string_to_array(
            regexp_replace(p_species_quotas, '[;,]', ',', 'g'), 
            ','
        );
        
        -- Process each species
        FOREACH v_species IN ARRAY v_species_array
        LOOP
            v_species := TRIM(v_species);
            
            -- Map common NAFO species to FAO codes
            v_fao_code := CASE 
                -- Groundfish
                WHEN v_species ILIKE '%cod%' THEN 'COD'
                WHEN v_species ILIKE '%haddock%' THEN 'HAD'
                WHEN v_species ILIKE '%redfish%' THEN 'RED'
                WHEN v_species ILIKE '%yellowtail%' THEN 'YEL'
                WHEN v_species ILIKE '%american plaice%' THEN 'PLA'
                WHEN v_species ILIKE '%witch flounder%' THEN 'WIT'
                WHEN v_species ILIKE '%greenland halibut%' THEN 'GHL'
                WHEN v_species ILIKE '%turbot%' THEN 'GHL'
                WHEN v_species ILIKE '%skate%' THEN 'SKA'
                WHEN v_species ILIKE '%white hake%' THEN 'HKW'
                -- Shrimp
                WHEN v_species ILIKE '%shrimp%' THEN 'PRA'
                WHEN v_species ILIKE '%prawn%' THEN 'PRA'
                -- Pelagics
                WHEN v_species ILIKE '%capelin%' THEN 'CAP'
                WHEN v_species ILIKE '%herring%' THEN 'HER'
                WHEN v_species ILIKE '%mackerel%' THEN 'MAC'
                -- Other
                WHEN v_species ILIKE '%squid%' THEN 'SQI'
                ELSE v_species
            END;
            
            -- Store in vessel_sources notes
            UPDATE vessel_sources
            SET data_quality_notes = COALESCE(data_quality_notes, '') || 
                E'\nQuota Species: ' || v_fao_code || 
                CASE WHEN v_fao_code != v_species 
                     THEN ' (' || v_species || ')' 
                     ELSE '' 
                END
            WHERE vessel_uuid = p_vessel_uuid
              AND source_id = p_source_id;
        END LOOP;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to process NAFO vessel metrics
CREATE OR REPLACE FUNCTION nafo_process_vessel_metrics(
    p_vessel_uuid UUID,
    p_source_id UUID,
    p_gross_tonnage NUMERIC,
    p_length_value NUMERIC,
    p_engine_power NUMERIC,
    p_engine_power_unit TEXT
) RETURNS VOID AS $$
BEGIN
    -- Insert gross tonnage
    IF p_gross_tonnage IS NOT NULL AND p_gross_tonnage > 0 THEN
        INSERT INTO vessel_metrics (
            vessel_uuid,
            source_id,
            metric_type,
            value,
            unit_enum,
            created_at
        ) VALUES (
            p_vessel_uuid,
            p_source_id,
            'gross_tonnage',
            p_gross_tonnage,
            'GT',
            NOW()
        )
        ON CONFLICT (vessel_uuid, source_id, metric_type) 
        DO UPDATE SET
            value = EXCLUDED.value,
            updated_at = NOW();
    END IF;
    
    -- Insert length
    IF p_length_value IS NOT NULL AND p_length_value > 0 THEN
        INSERT INTO vessel_measurements (
            vessel_uuid,
            source_id,
            metric_type,
            value,
            unit_enum,
            created_at
        ) VALUES (
            p_vessel_uuid,
            p_source_id,
            'length_loa',
            p_length_value,
            'METER',
            NOW()
        )
        ON CONFLICT (vessel_uuid, source_id, metric_type) 
        DO UPDATE SET
            value = EXCLUDED.value,
            unit_enum = EXCLUDED.unit_enum,
            updated_at = NOW();
    END IF;
    
    -- Insert engine power
    IF p_engine_power IS NOT NULL AND p_engine_power > 0 THEN
        INSERT INTO vessel_metrics (
            vessel_uuid,
            source_id,
            metric_type,
            value,
            unit_enum,
            created_at
        ) VALUES (
            p_vessel_uuid,
            p_source_id,
            'engine_power',
            p_engine_power,
            COALESCE(p_engine_power_unit, 'KW'),
            NOW()
        )
        ON CONFLICT (vessel_uuid, source_id, metric_type) 
        DO UPDATE SET
            value = EXCLUDED.value,
            unit_enum = EXCLUDED.unit_enum,
            updated_at = NOW();
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to handle NAFO vessel operators
CREATE OR REPLACE FUNCTION nafo_process_vessel_operators(
    p_vessel_uuid UUID,
    p_source_id UUID,
    p_owner_name TEXT,
    p_manager_name TEXT
) RETURNS VOID AS $$
DECLARE
    v_owner_id UUID;
    v_manager_id UUID;
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
    
    -- Process manager/operator (if different from owner)
    IF p_manager_name IS NOT NULL AND p_manager_name != '' 
       AND p_manager_name != p_owner_name THEN
        -- Get or create manager
        v_manager_id := get_or_create_associate(
            p_manager_name,
            'MANAGER',
            NULL,
            NULL
        );
        
        -- Link to vessel
        PERFORM link_vessel_associate(
            p_vessel_uuid,
            v_manager_id,
            p_source_id
        );
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to determine NAFO vessel type from gear and type
CREATE OR REPLACE FUNCTION nafo_determine_vessel_type(
    p_vessel_type_code TEXT,
    p_gear_type_code TEXT
) RETURNS TEXT AS $$
BEGIN
    -- Use vessel type if available and valid
    IF p_vessel_type_code IS NOT NULL AND p_vessel_type_code != 'NO' THEN
        RETURN p_vessel_type_code;
    END IF;
    
    -- Otherwise infer from gear type
    IF p_gear_type_code IS NOT NULL THEN
        RETURN CASE 
            WHEN p_gear_type_code IN ('03.1.1', '03.1.2', '03.1.0') THEN 'TO'  -- Trawler
            WHEN p_gear_type_code = '09.4.0' THEN 'LL'  -- Longliner
            WHEN p_gear_type_code = '07.1.0' THEN 'GO'  -- Gillnetter
            WHEN p_gear_type_code IN ('01.1.0', '01.2.0') THEN 'SN'  -- Seiner
            WHEN p_gear_type_code = '04.1.0' THEN 'DO'  -- Dredger
            WHEN p_gear_type_code = '08.2.0' THEN 'FPO'  -- Pot/trap vessel
            ELSE 'NO'  -- Other
        END;
    END IF;
    
    -- Default to unknown
    RETURN 'NO';
END;
$$ LANGUAGE plpgsql;

-- Comments
COMMENT ON FUNCTION nafo_process_authorization IS 'Handles NAFO vessel authorization status and fishing divisions';
COMMENT ON FUNCTION nafo_process_species_quotas IS 'Processes NAFO species quota information';
COMMENT ON FUNCTION nafo_process_vessel_metrics IS 'Processes NAFO tonnage, length, and engine power';
COMMENT ON FUNCTION nafo_process_vessel_operators IS 'Links owner and manager companies to vessels';
COMMENT ON FUNCTION nafo_determine_vessel_type IS 'Determines vessel type from type code or gear type';