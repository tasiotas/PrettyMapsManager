-- OSM Buildings Import Script for osm2pgsql
-- Imports building polygons with levels information

local buildings_table = osm2pgsql.define_table {
    name = 'osm_buildings',
    columns = {
        { column = 'levels', type = 'real' },
        { column = 'way', type = 'geometry', projection = 3857, not_null = true }
    },
    cluster = 'no',
    indexes = {}
}

-- Helper function to extract levels
local function get_levels(tags)
    if tags['building:levels'] then
        local levels = tonumber(tags['building:levels'])
        if levels then
            return levels
        end
    end
    
    return nil
end

-- Process ways (most buildings are ways)
function osm2pgsql.process_way(object)
    local tags = object.tags
    
    -- Only process if it has a building tag
    if not tags.building then
        return
    end
    
    -- Must be a closed way to be a valid building
    if not object.is_closed then
        return
    end
    
    local row = {
        levels = get_levels(tags),
        way = object:as_polygon()
    }
    
    buildings_table:insert(row)
end

-- Process relations (some buildings are multipolygons)
function osm2pgsql.process_relation(object)
    local tags = object.tags
    
    -- Only process multipolygon relations with building tag
    if tags.type ~= 'multipolygon' or not tags.building then
        return
    end
    
    local row = {
        levels = get_levels(tags),
        way = object:as_multipolygon()
    }
    
    buildings_table:insert(row)
end
