-- Setup functions for WCPFC vessel data loading
-- These functions handle WCPFC-specific data structures

-- Associate functions commented out - associates table doesn't exist yet
-- TODO: Uncomment when associates table is available
/*
CREATE OR REPLACE FUNCTION get_or_create_associate(
    p_name TEXT,
    p_type associate_type_enum,
    p_country_id UUID,
    p_address TEXT
) RETURNS UUID AS $$
DECLARE
    v_associate_id UUID;
BEGIN
    -- Return NULL for empty names
    IF p_name IS NULL OR trim(p_name) = '' THEN
        RETURN NULL;
    END IF;
    
    -- Try to find existing associate
    SELECT associate_id INTO v_associate_id
    FROM associates
    WHERE associate_name = trim(p_name)
      AND associate_type = p_type
    LIMIT 1;
    
    -- Create if not exists
    IF v_associate_id IS NULL THEN
        INSERT INTO associates (
            associate_id,
            associate_name,
            associate_type,
            country_id,
            address,
            created_at
        ) VALUES (
            gen_random_uuid(),
            trim(p_name),
            p_type,
            p_country_id,
            p_address,
            NOW()
        )
        RETURNING associate_id INTO v_associate_id;
    END IF;
    
    RETURN v_associate_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION link_vessel_associate(
    p_vessel_uuid UUID,
    p_associate_id UUID,
    p_source_id UUID
) RETURNS VOID AS $$
BEGIN
    IF p_vessel_uuid IS NULL OR p_associate_id IS NULL THEN
        RETURN;
    END IF;
    
    INSERT INTO vessel_associates (
        vessel_uuid,
        associate_id,
        source_id,
        created_at
    ) VALUES (
        p_vessel_uuid,
        p_associate_id,
        p_source_id,
        NOW()
    )
    ON CONFLICT (vessel_uuid, associate_id, source_id) DO UPDATE
    SET updated_at = NOW();
END;
$$ LANGUAGE plpgsql;
*/

-- Helper function to process WCPFC authorization details
CREATE OR REPLACE FUNCTION wcpfc_process_authorization(
    p_vessel_uuid UUID,
    p_source_id UUID,
    p_auth_form TEXT,
    p_auth_number TEXT,
    p_auth_areas TEXT,
    p_auth_species TEXT,
    p_auth_from_date DATE,
    p_auth_to_date DATE,
    p_tranship_high_seas TEXT,
    p_tranship_at_sea TEXT
) RETURNS VOID AS $$
BEGIN
    -- Store authorization information
    IF p_auth_form IS NOT NULL OR p_auth_number IS NOT NULL THEN
        -- Update vessel_sources with authorization details
        UPDATE vessel_sources
        SET is_active = (p_auth_to_date IS NULL OR p_auth_to_date >= CURRENT_DATE),
            data_governance_notes = COALESCE(data_governance_notes, '') || 
                CASE WHEN p_auth_form IS NOT NULL 
                     THEN E'\nAuthorization Form: ' || p_auth_form
                     ELSE ''
                END ||
                CASE WHEN p_auth_number IS NOT NULL 
                     THEN E'\nAuthorization Number: ' || p_auth_number
                     ELSE ''
                END ||
                CASE WHEN p_auth_areas IS NOT NULL 
                     THEN E'\nAuthorized Areas: ' || p_auth_areas
                     ELSE ''
                END ||
                CASE WHEN p_auth_species IS NOT NULL 
                     THEN E'\nAuthorized Species: ' || p_auth_species
                     ELSE ''
                END ||
                CASE WHEN p_auth_from_date IS NOT NULL OR p_auth_to_date IS NOT NULL
                     THEN E'\nAuthorization Period: ' || 
                          COALESCE(p_auth_from_date::text, 'Unknown') || ' to ' || 
                          COALESCE(p_auth_to_date::text, 'Current')
                     ELSE ''
                END ||
                CASE WHEN p_tranship_high_seas IS NOT NULL 
                     THEN E'\nTransshipment High Seas: ' || p_tranship_high_seas
                     ELSE ''
                END ||
                CASE WHEN p_tranship_at_sea IS NOT NULL 
                     THEN E'\nTransshipment at Sea: ' || p_tranship_at_sea
                     ELSE ''
                END
        WHERE vessel_uuid = p_vessel_uuid
          AND source_id = p_source_id;
    END IF;

    -- Store authorization period in vessel_attributes instead
    IF p_auth_from_date IS NOT NULL OR p_auth_to_date IS NOT NULL THEN
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
                    'active', p_auth_to_date IS NULL OR p_auth_to_date >= CURRENT_DATE
                )
            ),
            NOW()
        )
        ON CONFLICT (vessel_uuid, source_id) DO UPDATE SET
            attributes = vessel_attributes.attributes || EXCLUDED.attributes,
            last_updated = NOW();
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to process WCPFC vessel measurements and capacity
CREATE OR REPLACE FUNCTION wcpfc_process_vessel_metrics(
    p_vessel_uuid UUID,
    p_source_id UUID,
    p_length_value NUMERIC,
    p_length_metric_type TEXT,
    p_length_unit TEXT,
    p_depth_value NUMERIC,
    p_depth_unit TEXT,
    p_beam_value NUMERIC,
    p_beam_unit TEXT,
    p_tonnage_value NUMERIC,
    p_tonnage_metric_type TEXT,
    p_engine_power NUMERIC,
    p_engine_power_unit TEXT,
    p_fish_hold_capacity NUMERIC,
    p_fish_hold_capacity_unit TEXT
) RETURNS VOID AS $$
BEGIN
    -- Insert length
    IF p_length_value IS NOT NULL AND p_length_value > 0 THEN
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
            COALESCE(p_length_metric_type, 'length_loa')::metric_type_enum,
            p_length_value,
            COALESCE(p_length_unit, 'METER')::unit_enum,
            NOW()
        );
    END IF;

    -- Insert depth
    IF p_depth_value IS NOT NULL AND p_depth_value > 0 THEN
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
            'moulded_depth',
            p_depth_value,
            COALESCE(p_depth_unit, 'METER')::unit_enum,
            NOW()
        );
    END IF;

    -- Insert beam
    IF p_beam_value IS NOT NULL AND p_beam_value > 0 THEN
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
            'beam',
            p_beam_value,
            COALESCE(p_beam_unit, 'METER')::unit_enum,
            NOW()
        );
    END IF;

    -- Insert tonnage
    IF p_tonnage_value IS NOT NULL AND p_tonnage_value > 0 THEN
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
            COALESCE(p_tonnage_metric_type, 'gross_tonnage')::metric_type_enum,
            p_tonnage_value,
            'DIMENSIONLESS'::unit_enum,
            NOW()
        );
    END IF;

    -- Insert engine power
    IF p_engine_power IS NOT NULL AND p_engine_power > 0 THEN
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
            'engine_power',
            p_engine_power,
            COALESCE(p_engine_power_unit, 'HP')::unit_enum,
            NOW()
        );
    END IF;

    -- Insert fish hold capacity
    IF p_fish_hold_capacity IS NOT NULL AND p_fish_hold_capacity > 0 THEN
        -- Map unit to standard enum
        DECLARE
            v_unit TEXT;
        BEGIN
            v_unit := CASE p_fish_hold_capacity_unit
                WHEN 'M3' THEN 'CUBIC_METER'
                WHEN 'CUBIC METERS' THEN 'CUBIC_METER'
                WHEN 'MT' THEN 'MT'
                WHEN 'METRIC TONS' THEN 'MT'
                WHEN 'TONS' THEN 'MT'
                ELSE 'CUBIC_METER'  -- Default
            END;

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
                'fish_hold_capacity',
                p_fish_hold_capacity,
                v_unit::unit_enum,
                NOW()
            );
        END;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to process WCPFC freezer information
CREATE OR REPLACE FUNCTION wcpfc_process_freezer_info(
    p_vessel_uuid UUID,
    p_source_id UUID,
    p_freezer_types TEXT,
    p_freezing_capacity NUMERIC,
    p_freezing_capacity_unit TEXT,
    p_freezer_units NUMERIC
) RETURNS VOID AS $$
BEGIN
    -- Store freezer information in notes for now
    -- In future, could have dedicated freezer capacity table
    IF p_freezer_types IS NOT NULL OR p_freezing_capacity IS NOT NULL OR p_freezer_units IS NOT NULL THEN
        UPDATE vessel_sources
        SET data_governance_notes = COALESCE(data_governance_notes, '') || 
            CASE WHEN p_freezer_types IS NOT NULL 
                 THEN E'\nFreezer Types: ' || p_freezer_types
                 ELSE ''
            END ||
            CASE WHEN p_freezing_capacity IS NOT NULL 
                 THEN E'\nFreezing Capacity: ' || p_freezing_capacity::text || 
                      COALESCE(' ' || p_freezing_capacity_unit, '')
                 ELSE ''
            END ||
            CASE WHEN p_freezer_units IS NOT NULL 
                 THEN E'\nFreezer Units: ' || p_freezer_units::text
                 ELSE ''
            END
        WHERE vessel_uuid = p_vessel_uuid
          AND source_id = p_source_id;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to process WCPFC crew information
CREATE OR REPLACE FUNCTION wcpfc_process_crew_info(
    p_vessel_uuid UUID,
    p_source_id UUID,
    p_crew_complement NUMERIC,
    p_master_name TEXT,
    p_master_nationality TEXT
) RETURNS VOID AS $$
DECLARE
    v_master_country_id UUID;
BEGIN
    -- Store crew complement
    IF p_crew_complement IS NOT NULL AND p_crew_complement > 0 THEN
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
            'crew_complement',
            p_crew_complement,
            'PERSON'::unit_enum,
            NOW()
        );
    END IF;

    -- Store master information
    IF p_master_name IS NOT NULL OR p_master_nationality IS NOT NULL THEN
        -- Get country ID for master nationality
        IF p_master_nationality IS NOT NULL THEN
            SELECT id INTO v_master_country_id
            FROM country_iso
            WHERE alpha_3_code = p_master_nationality;
        END IF;

        UPDATE vessel_sources
        SET data_governance_notes = COALESCE(data_governance_notes, '') || 
            CASE WHEN p_master_name IS NOT NULL 
                 THEN E'\nMaster Name: ' || p_master_name
                 ELSE ''
            END ||
            CASE WHEN p_master_nationality IS NOT NULL 
                 THEN E'\nMaster Nationality: ' || p_master_nationality
                 ELSE ''
            END
        WHERE vessel_uuid = p_vessel_uuid
          AND source_id = p_source_id;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to handle WCPFC vessel relationships (owner, charter, CCM)
CREATE OR REPLACE FUNCTION wcpfc_process_vessel_relationships(
    p_vessel_uuid UUID,
    p_source_id UUID,
    p_owner_name TEXT,
    p_owner_address TEXT,
    p_charterer_name TEXT,
    p_charterer_address TEXT,
    p_charter_start DATE,
    p_charter_end DATE,
    p_submitted_by_ccm TEXT,
    p_host_ccm TEXT
) RETURNS VOID AS $$
DECLARE
    v_owner_id UUID;
    v_charterer_id UUID;
BEGIN
    -- Process owner
    IF p_owner_name IS NOT NULL AND p_owner_name != '' THEN
        -- Skip associate processing for now
        -- TODO: Implement when associates table is available
    END IF;

    -- Process charterer
    IF p_charterer_name IS NOT NULL AND p_charterer_name != '' THEN
        -- Skip associate processing for now
        -- TODO: Implement when associates table is available
    END IF;

    -- Store CCM information
    IF p_submitted_by_ccm IS NOT NULL OR p_host_ccm IS NOT NULL THEN
        UPDATE vessel_sources
        SET data_governance_notes = COALESCE(data_governance_notes, '') || 
            CASE WHEN p_submitted_by_ccm IS NOT NULL 
                 THEN E'\nSubmitted by CCM: ' || p_submitted_by_ccm
                 ELSE ''
            END ||
            CASE WHEN p_host_ccm IS NOT NULL 
                 THEN E'\nHost CCM: ' || p_host_ccm
                 ELSE ''
            END
        WHERE vessel_uuid = p_vessel_uuid
          AND source_id = p_source_id;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to process WCPFC vessel history (previous names and flags)
CREATE OR REPLACE FUNCTION wcpfc_process_vessel_history(
    p_vessel_uuid UUID,
    p_source_id UUID,
    p_previous_names TEXT,
    p_previous_flag TEXT
) RETURNS VOID AS $$
DECLARE
    v_name_array TEXT[];
    v_prev_name TEXT;
    v_previous_flag_id UUID;
BEGIN
    -- Process previous names (may be comma-separated list)
    IF p_previous_names IS NOT NULL AND p_previous_names != '' THEN
        -- Split by common delimiters
        v_name_array := string_to_array(
            regexp_replace(p_previous_names, '[;,/]', ',', 'g'), 
            ','
        );
        
        -- Record each previous name
        FOREACH v_prev_name IN ARRAY v_name_array
        LOOP
            v_prev_name := TRIM(v_prev_name);
            IF v_prev_name != '' THEN
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
                    v_prev_name,
                    NOW()
                )
                ON CONFLICT DO NOTHING;
                
                -- Record reputation event
                PERFORM record_reputation_event(
                    p_vessel_uuid,
                    CURRENT_DATE,
                    'NAME_CHANGE',
                    p_source_id,
                    v_prev_name,
                    (SELECT vessel_name FROM vessels WHERE vessel_uuid = p_vessel_uuid)
                );
            END IF;
        END LOOP;
    END IF;

    -- Process previous flag
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
            )
            ON CONFLICT DO NOTHING;
            
            -- Record reputation event
            PERFORM record_reputation_event(
                p_vessel_uuid,
                CURRENT_DATE,
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

-- Helper function to record reputation events
CREATE OR REPLACE FUNCTION record_reputation_event(
    p_vessel_uuid UUID,
    p_event_date DATE,
    p_event_type TEXT,
    p_source_id UUID,
    p_previous_value TEXT,
    p_new_value TEXT
) RETURNS VOID AS $$
BEGIN
    -- For now, store in vessel_reported_history
    -- Store reputation event based on type
    INSERT INTO vessel_reported_history (
        vessel_uuid,
        source_id,
        reported_history_type,
        identifier_value,
        created_at
    ) VALUES (
        p_vessel_uuid,
        p_source_id,
        CASE p_event_type
            WHEN 'NAME_CHANGE' THEN 'VESSEL_NAME_CHANGE'
            WHEN 'FLAG_CHANGE' THEN 'FLAG_CHANGE'
            ELSE 'OTHER_CHANGE'
        END::reported_history_enum,
        COALESCE(p_previous_value || ' -> ' || p_new_value, p_previous_value, p_new_value),
        NOW()
    )
    ON CONFLICT DO NOTHING;
END;
$$ LANGUAGE plpgsql;

-- Helper function to capture vessel snapshot
CREATE OR REPLACE FUNCTION capture_vessel_snapshot(
    p_vessel_uuid UUID,
    p_source_id UUID,
    p_snapshot_date DATE DEFAULT CURRENT_DATE,
    p_import_run_id UUID DEFAULT NULL
) RETURNS TABLE (snapshot_id UUID) AS $$
BEGIN
    -- For now, just return a dummy UUID
    -- In production, this would capture actual vessel state
    RETURN QUERY SELECT gen_random_uuid() AS snapshot_id;
END;
$$ LANGUAGE plpgsql;

-- Comments
COMMENT ON FUNCTION wcpfc_process_authorization IS 'Handles WCPFC vessel authorization details including areas, species, and transshipment';
COMMENT ON FUNCTION wcpfc_process_vessel_metrics IS 'Processes WCPFC vessel measurements and capacity metrics';
COMMENT ON FUNCTION wcpfc_process_freezer_info IS 'Handles WCPFC freezer types and capacity information';
COMMENT ON FUNCTION wcpfc_process_crew_info IS 'Processes WCPFC crew complement and master information';
COMMENT ON FUNCTION wcpfc_process_vessel_relationships IS 'Links owners, charterers, and CCM relationships to vessels';
COMMENT ON FUNCTION wcpfc_process_vessel_history IS 'Records previous vessel names and flag changes';