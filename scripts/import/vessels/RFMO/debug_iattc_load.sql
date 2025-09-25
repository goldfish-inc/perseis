-- Debug IATTC loading to understand why 0 vessels are processed

-- First, let's check what's in staging
SELECT 'Staging table contents:' as info;
SELECT COUNT(*) as total_records FROM staging_iattc_vessels;

-- Check for NULL/empty values that might cause issues
SELECT 
    'Records with key identifier issues:' as info,
    COUNT(*) FILTER (WHERE vessel_name IS NULL OR vessel_name = '') as null_names,
    COUNT(*) FILTER (WHERE vessel_flag_alpha3 IS NULL OR vessel_flag_alpha3 = '') as null_flags,
    COUNT(*) FILTER (WHERE imo IS NULL OR imo = '') as null_imo,
    COUNT(*) FILTER (WHERE ircs IS NULL OR ircs = '') as null_ircs,
    COUNT(*) FILTER (WHERE mmsi IS NULL OR mmsi = '') as null_mmsi
FROM staging_iattc_vessels;

-- Check the DISTINCT ON query that's used in the processing loop
SELECT 'Distinct vessels to process:' as info;
SELECT COUNT(*) FROM (
    SELECT DISTINCT ON (
        COALESCE(imo, ''), 
        COALESCE(ircs, ''), 
        COALESCE(vessel_name, ''),
        COALESCE(vessel_flag_alpha3, '')
    ) *
    FROM staging_iattc_vessels
) t;

-- Sample a few records to see what would be processed
SELECT 'Sample records:' as info;
SELECT 
    vessel_name,
    imo,
    ircs,
    mmsi,
    vessel_flag_alpha3,
    iattc_vessel_number,
    gear_type_code
FROM (
    SELECT DISTINCT ON (
        COALESCE(imo, ''), 
        COALESCE(ircs, ''), 
        COALESCE(vessel_name, ''),
        COALESCE(vessel_flag_alpha3, '')
    ) *
    FROM staging_iattc_vessels
    ORDER BY 
        COALESCE(imo, ''), 
        COALESCE(ircs, ''), 
        COALESCE(vessel_name, ''),
        COALESCE(vessel_flag_alpha3, ''),
        CASE 
            WHEN imo IS NOT NULL AND imo != '' THEN 1
            WHEN ircs IS NOT NULL AND ircs != '' THEN 2
            WHEN mmsi IS NOT NULL AND mmsi != '' THEN 3
            ELSE 4
        END,
        last_modification DESC NULLS LAST
) t
LIMIT 10;

-- Test processing a single vessel manually
DO $$
DECLARE
    v_vessel_uuid UUID;
    v_flag_id UUID;
    v_vessel_type_id UUID;
    v_source_id UUID := (SELECT source_id FROM original_sources_vessels WHERE source_shortname = 'IATTC');
    r RECORD;
BEGIN
    -- Get first record
    SELECT * INTO r
    FROM staging_iattc_vessels
    WHERE vessel_name IS NOT NULL 
    LIMIT 1;
    
    RAISE NOTICE 'Testing vessel: % (IMO: %, IRCS: %, Flag: %)', 
        r.vessel_name, r.imo, r.ircs, r.vessel_flag_alpha3;
    
    -- Get flag country ID
    v_flag_id := NULL;
    IF r.vessel_flag_alpha3 IS NOT NULL THEN
        SELECT id INTO v_flag_id
        FROM country_iso
        WHERE alpha_3_code = r.vessel_flag_alpha3;
        RAISE NOTICE 'Flag ID: %', v_flag_id;
    END IF;
    
    -- Determine vessel type from gear if not specified
    v_vessel_type_id := NULL;
    IF r.gear_type_code IS NOT NULL THEN
        -- Use the IATTC function to infer vessel type
        SELECT id INTO v_vessel_type_id
        FROM vessel_types
        WHERE vessel_type_isscfv_code = iattc_infer_vessel_type(r.gear_type_code);
        RAISE NOTICE 'Vessel type ID: %', v_vessel_type_id;
    END IF;
    
    -- Try to create vessel
    BEGIN
        SELECT vessel_uuid INTO v_vessel_uuid 
        FROM find_or_create_vessel_with_trust(
            r.vessel_name,
            r.imo,
            r.ircs,
            r.mmsi,
            r.vessel_flag_alpha3,
            v_source_id,
            NULL::UUID,
            COALESCE(r.source_date::DATE, CURRENT_DATE)
        );
        
        RAISE NOTICE 'Created/found vessel: %', v_vessel_uuid;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Error creating vessel: %', SQLERRM;
    END;
END $$;

-- Check if any vessels were created in this session
SELECT 'IATTC vessels in database:' as info;
SELECT COUNT(DISTINCT vs.vessel_uuid) as vessel_count
FROM vessel_sources vs
JOIN original_sources_vessels osv ON vs.source_id = osv.source_id
WHERE osv.source_shortname = 'IATTC';