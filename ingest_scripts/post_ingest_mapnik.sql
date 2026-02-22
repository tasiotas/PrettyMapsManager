-- ============================================================================
-- Post-ingestion optimizations for Mapnik tables
-- ============================================================================

-- Clean up rows with invalid geometries to keep rendering stable.
DELETE FROM osm_roads WHERE way IS NULL OR ST_IsEmpty(way);
DELETE FROM osm_railways WHERE way IS NULL OR ST_IsEmpty(way);
DELETE FROM osm_water WHERE way IS NULL OR ST_IsEmpty(way);
DELETE FROM osm_boundaries WHERE way IS NULL OR ST_IsEmpty(way);
DELETE FROM countries WHERE wkb_geometry IS NULL OR ST_IsEmpty(wkb_geometry);

-- Spatial indexes for render queries and border assembly.
CREATE INDEX osm_roads_way_idx ON osm_roads USING GIST (way) WITH (FILLFACTOR = 100);
CREATE INDEX osm_railways_way_idx ON osm_railways USING GIST (way) WITH (FILLFACTOR = 100);
CREATE INDEX osm_water_way_idx ON osm_water USING GIST (way) WITH (FILLFACTOR = 100);
CREATE INDEX osm_boundaries_way_idx ON osm_boundaries USING GIST (way) WITH (FILLFACTOR = 100);
CREATE INDEX countries_way_idx ON countries USING GIST (wkb_geometry) WITH (FILLFACTOR = 100);

-- Roads are filtered by highway type in SQL, so keep a simple btree index.
CREATE INDEX osm_roads_highway_idx ON osm_roads (highway);

-- Create final borders table from imported country polygons + OSM boundary labels.
DROP TABLE IF EXISTS borders;
CREATE TABLE borders AS
SELECT
    c.wkb_geometry AS way,
    l.iso_3166_1
FROM
    countries c
JOIN
    osm_boundaries l
ON
    ST_Intersects(c.wkb_geometry, l.way)
WHERE
    l.iso_3166_1 IS NOT NULL;

CREATE INDEX borders_iso_idx ON borders (iso_3166_1) WITH (FILLFACTOR = 100);
CREATE INDEX borders_way_idx ON borders USING GIST (way) WITH (FILLFACTOR = 100);
CLUSTER borders USING borders_way_idx;

DROP TABLE IF EXISTS countries;
DROP TABLE IF EXISTS osm_boundaries;

-- Keep planner statistics fresh.
ANALYZE osm_roads;
ANALYZE osm_railways;
ANALYZE osm_water;
ANALYZE borders;

-- ============================================================================
-- Summary
-- ============================================================================
DO $$
DECLARE
    roads_count bigint;
    railways_count bigint;
    water_count bigint;
    borders_count bigint;
BEGIN
    SELECT COUNT(*) INTO roads_count FROM osm_roads;
    SELECT COUNT(*) INTO railways_count FROM osm_railways;
    SELECT COUNT(*) INTO water_count FROM osm_water;
    SELECT COUNT(*) INTO borders_count FROM borders;

    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'MAPNIK IMPORT STATISTICS';
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'Road segments:              %', roads_count;
    RAISE NOTICE 'Railway segments:           %', railways_count;
    RAISE NOTICE 'Water features:             %', water_count;
    RAISE NOTICE 'Border polygons:            %', borders_count;
    RAISE NOTICE '============================================================================';
END $$;
