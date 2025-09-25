-- Setup functions for IOTC vessel data loading
-- These functions handle IOTC-specific data structures and mappings

-- Helper function to get or create IOTC gear type
CREATE OR REPLACE FUNCTION iotc_get_gear_type_id(p_gear_codes TEXT)
RETURNS UUID[] AS $$
DECLARE
    v_gear_ids UUID[] := '{}';
    v_gear_code TEXT;
    v_gear_id UUID;
BEGIN
    IF p_gear_codes IS NULL OR p_gear_codes = '' THEN
        RETURN NULL;
    END IF;
    
    -- IOTC uses semicolon-separated gear codes
    FOREACH v_gear_code IN ARRAY string_to_array(p_gear_codes, ';')
    LOOP
        v_gear_code := TRIM(v_gear_code);
        IF v_gear_code != '' THEN
            -- Get or create gear type
            SELECT gear_type_id INTO v_gear_id
            FROM gear_types
            WHERE isscfg_code = v_gear_code;
            
            IF v_gear_id IS NOT NULL THEN
                v_gear_ids := array_append(v_gear_ids, v_gear_id);
            END IF;
        END IF;
    END LOOP;
    
    RETURN CASE WHEN array_length(v_gear_ids, 1) > 0 THEN v_gear_ids ELSE NULL END;
END;
$$ LANGUAGE plpgsql;

-- Helper function to get vessel type ID for IOTC
CREATE OR REPLACE FUNCTION iotc_get_vessel_type_id(p_type_code TEXT)
RETURNS UUID AS $$
DECLARE
    v_type_id UUID;
BEGIN
    IF p_type_code IS NULL OR p_type_code = '' THEN
        RETURN NULL;
    END IF;
    
    SELECT vessel_type_id INTO v_type_id
    FROM vessel_types
    WHERE type_code = p_type_code;
    
    RETURN v_type_id;
END;
$$ LANGUAGE plpgsql;

-- Function to handle IOTC authorization periods
CREATE OR REPLACE FUNCTION iotc_process_vessel_authorization(
    p_vessel_uuid UUID,
    p_source_id UUID,
    p_auth_from DATE,
    p_auth_to DATE,
    p_range_code TEXT DEFAULT 'EEZ'
) RETURNS VOID AS $$
DECLARE
    v_status authorization_status_enum;
BEGIN
    -- Skip if no dates
    IF p_auth_from IS NULL AND p_auth_to IS NULL THEN
        RETURN;
    END IF;
    
    -- Determine authorization status
    IF p_auth_to IS NOT NULL AND p_auth_to < CURRENT_DATE THEN
        v_status := 'EXPIRED';
    ELSIF p_auth_from IS NOT NULL AND p_auth_from > CURRENT_DATE THEN
        v_status := 'PENDING';
    ELSE
        v_status := 'ACTIVE';
    END IF;
    
    -- Insert authorization record
    INSERT INTO vessel_authorizations (
        vessel_uuid,
        source_id,
        authorization_type,
        authorized_from,
        authorized_to,
        authorization_status,
        authorization_details,
        created_at,
        updated_at
    ) VALUES (
        p_vessel_uuid,
        p_source_id,
        'FISHING',
        p_auth_from,
        p_auth_to,
        v_status,
        jsonb_build_object(
            'range', p_range_code,
            'region', 'IOTC',
            'ocean', 'Indian Ocean'
        ),
        NOW(),
        NOW()
    )
    ON CONFLICT (vessel_uuid, source_id, authorization_type, authorized_from) 
    DO UPDATE SET
        authorized_to = EXCLUDED.authorized_to,
        authorization_status = EXCLUDED.authorization_status,
        authorization_details = EXCLUDED.authorization_details,
        updated_at = NOW();
END;
$$ LANGUAGE plpgsql;

-- Function to process IOTC ownership structure
CREATE OR REPLACE FUNCTION iotc_process_vessel_ownership(
    p_vessel_uuid UUID,
    p_source_id UUID,
    p_owner_name TEXT,
    p_owner_address TEXT,
    p_operator_name TEXT,
    p_operator_address TEXT,
    p_operating_company TEXT,
    p_operating_company_address TEXT,
    p_operating_company_reg_num TEXT,
    p_beneficial_owner TEXT,
    p_beneficial_owner_address TEXT
) RETURNS VOID AS $$
DECLARE
    v_owner_id UUID;
    v_operator_id UUID;
    v_operating_company_id UUID;
    v_beneficial_owner_id UUID;
BEGIN
    -- Process owner
    IF p_owner_name IS NOT NULL AND p_owner_name != '' THEN
        v_owner_id := get_or_create_associate(
            p_owner_name, 
            'OWNER', 
            NULL, 
            p_owner_address
        );
        
        PERFORM link_vessel_associate(
            p_vessel_uuid,
            v_owner_id,
            p_source_id
        );
    END IF;
    
    -- Process operator (if different from owner)
    IF p_operator_name IS NOT NULL AND p_operator_name != '' 
       AND p_operator_name != p_owner_name THEN
        v_operator_id := get_or_create_associate(
            p_operator_name,
            'OPERATOR',
            NULL,
            p_operator_address
        );
        
        PERFORM link_vessel_associate(
            p_vessel_uuid,
            v_operator_id,
            p_source_id
        );
    END IF;
    
    -- Process operating company
    IF p_operating_company IS NOT NULL AND p_operating_company != '' THEN
        v_operating_company_id := get_or_create_associate(
            p_operating_company,
            'MANAGER',  -- Using MANAGER for operating company
            NULL,
            p_operating_company_address,
            jsonb_build_object('registration_number', p_operating_company_reg_num)
        );
        
        PERFORM link_vessel_associate(
            p_vessel_uuid,
            v_operating_company_id,
            p_source_id
        );
    END IF;
    
    -- Process beneficial owner (ultimate owner)
    IF p_beneficial_owner IS NOT NULL AND p_beneficial_owner != '' THEN
        v_beneficial_owner_id := get_or_create_associate(
            p_beneficial_owner,
            'BENEFICIAL_OWNER',
            NULL,
            p_beneficial_owner_address,
            jsonb_build_object('ownership_type', 'ultimate_beneficial_owner')
        );
        
        PERFORM link_vessel_associate(
            p_vessel_uuid,
            v_beneficial_owner_id,
            p_source_id
        );
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to process vessel metrics for IOTC
CREATE OR REPLACE FUNCTION iotc_process_vessel_metrics(
    p_vessel_uuid UUID,
    p_source_id UUID,
    p_gt_value NUMERIC,
    p_grt_value NUMERIC,
    p_volume_value NUMERIC,
    p_cc_value NUMERIC
) RETURNS VOID AS $$
BEGIN
    -- Insert GT (Gross Tonnage)
    IF p_gt_value IS NOT NULL AND p_gt_value > 0 THEN
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
            p_gt_value,
            'GT',
            NOW()
        )
        ON CONFLICT (vessel_uuid, source_id, metric_type) 
        DO UPDATE SET
            value = EXCLUDED.value,
            updated_at = NOW();
    END IF;
    
    -- Insert GRT (Gross Register Tonnage)
    IF p_grt_value IS NOT NULL AND p_grt_value > 0 THEN
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
            'gross_register_tonnage',
            p_grt_value,
            'GRT',
            NOW()
        )
        ON CONFLICT (vessel_uuid, source_id, metric_type) 
        DO UPDATE SET
            value = EXCLUDED.value,
            updated_at = NOW();
    END IF;
    
    -- Insert Total Volume
    IF p_volume_value IS NOT NULL AND p_volume_value > 0 THEN
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
            'fish_hold_volume',
            p_volume_value,
            'CUBIC_METER',
            NOW()
        )
        ON CONFLICT (vessel_uuid, source_id, metric_type) 
        DO UPDATE SET
            value = EXCLUDED.value,
            updated_at = NOW();
    END IF;
    
    -- Insert Cold Storage Capacity
    IF p_cc_value IS NOT NULL AND p_cc_value > 0 THEN
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
            'cold_storage_capacity',
            p_cc_value,
            'MT',
            NOW()
        )
        ON CONFLICT (vessel_uuid, source_id, metric_type) 
        DO UPDATE SET
            value = EXCLUDED.value,
            updated_at = NOW();
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to store IOTC photo URLs for future use
CREATE OR REPLACE FUNCTION iotc_store_vessel_photos(
    p_vessel_uuid UUID,
    p_source_id UUID,
    p_starboard_photo TEXT,
    p_portside_photo TEXT,
    p_bow_photo TEXT
) RETURNS VOID AS $$
DECLARE
    v_photos JSONB;
BEGIN
    -- Build photos JSON
    v_photos := jsonb_build_object(
        'starboard', p_starboard_photo,
        'portside', p_portside_photo,
        'bow', p_bow_photo,
        'last_updated', NOW()
    );
    
    -- Remove null values
    v_photos := v_photos - ARRAY(
        SELECT key 
        FROM jsonb_each(v_photos) 
        WHERE value::text = 'null' OR value IS NULL
    );
    
    -- Only store if we have photos
    IF jsonb_typeof(v_photos) = 'object' AND v_photos != '{}'::jsonb THEN
        -- Store in vessel_sources as additional data
        UPDATE vessel_sources
        SET data_governance_notes = COALESCE(data_governance_notes, '') || 
            E'\nPhoto URLs: ' || v_photos::text
        WHERE vessel_uuid = p_vessel_uuid
          AND source_id = p_source_id;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Comments for documentation
COMMENT ON FUNCTION iotc_get_gear_type_id IS 'Processes IOTC semicolon-separated gear codes';
COMMENT ON FUNCTION iotc_process_vessel_authorization IS 'Handles IOTC fishing authorization periods';
COMMENT ON FUNCTION iotc_process_vessel_ownership IS 'Processes complex IOTC ownership structure including beneficial owners';
COMMENT ON FUNCTION iotc_process_vessel_metrics IS 'Handles IOTC tonnage and capacity measurements';
COMMENT ON FUNCTION iotc_store_vessel_photos IS 'Stores vessel photo URLs for future verification use';