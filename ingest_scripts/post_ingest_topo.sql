-- ============================================================================
-- Post-Ingestion Optimizations for Buildings Table
-- ============================================================================

-- Remove small buildings (< 100 sq meters) using a table-swap instead of
-- DELETE + VACUUM FULL. A straight DELETE leaves ~40% dead tuples requiring
-- a full table rewrite via VACUUM FULL. Building a clean table from scratch
-- and renaming it is 3–5× faster and avoids all bloat.
CREATE TABLE osm_buildings_filtered AS
    SELECT
        -- NULL out clearly corrupt level values (Burj Khalifa = 163 floors)
        CASE WHEN levels <= 200 THEN levels END AS levels,
        way
    FROM osm_buildings
    WHERE ST_Area(way) > 100;

DROP TABLE osm_buildings;
ALTER TABLE osm_buildings_filtered RENAME TO osm_buildings;

-- Create spatial index on the clean, compactly-stored table
CREATE INDEX osm_buildings_way_idx ON osm_buildings USING GIST (way) WITH (FILLFACTOR = 100);

-- Plain ANALYZE is sufficient: the new table has no dead tuples
ANALYZE osm_buildings;

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
    RAISE NOTICE 'Buildings with levels:     % (%%)' ,
        buildings_with_levels,
        ROUND(buildings_with_levels::numeric / NULLIF(total_buildings, 0) * 100, 1);
    RAISE NOTICE 'Average levels:            %', avg_levels;
    RAISE NOTICE 'Maximum levels:            %', max_levels;
    RAISE NOTICE '============================================================================';
END $$;
