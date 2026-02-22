-- ============================================================================
-- Post-Ingestion Optimizations for Buildings Table
-- ============================================================================

-- Remove small buildings (less than 100 sq meters)
DELETE FROM osm_buildings WHERE ST_Area(way) <= 100;

-- Create spatial index on buildings geometry
CREATE INDEX osm_buildings_way_idx ON osm_buildings USING GIST (way) WITH (FILLFACTOR = 100);

-- Update table statistics for optimal query planning
VACUUM FULL ANALYZE osm_buildings;

-- Note: CLUSTER command removed for faster import. Run manually if needed:
-- CLUSTER osm_buildings USING osm_buildings_way_idx;

-- ============================================================================
-- Summary Statistics
-- ============================================================================

DO $$
DECLARE
    total_buildings bigint;
    buildings_with_levels bigint;
    avg_levels numeric;
    max_levels numeric;
BEGIN
    SELECT COUNT(*), 
           COUNT(levels),
           ROUND(AVG(levels)::numeric, 2),
           MAX(levels)
    INTO total_buildings, buildings_with_levels, avg_levels, max_levels
    FROM osm_buildings;
    
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'BUILDINGS IMPORT STATISTICS';
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'Total buildings:           %', total_buildings;
    RAISE NOTICE 'Buildings with levels:     % (%.1f%%)', 
        buildings_with_levels, 
        (buildings_with_levels::numeric / NULLIF(total_buildings, 0) * 100);
    RAISE NOTICE 'Average levels:            %', avg_levels;
    RAISE NOTICE 'Maximum levels:            %', max_levels;
    RAISE NOTICE '============================================================================';
END $$;
