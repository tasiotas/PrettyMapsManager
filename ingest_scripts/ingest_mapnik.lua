local ROAD_TYPES = {
    motorway = true,
    trunk = true,
    primary = true,
    secondary = true,
    tertiary = true,
}

local roads_table = osm2pgsql.define_table {
    name = "osm_roads",
    columns = {
        { column = "highway", type = "text", not_null = true },
        { column = "way", type = "geometry", projection = 3857, not_null = true }
    },
    cluster = "no",
    indexes = {}
}

local railways_table = osm2pgsql.define_table {
    name = "osm_railways",
    columns = {
        { column = "way", type = "geometry", projection = 3857, not_null = true }
    },
    cluster = "no",
    indexes = {}
}

local water_table = osm2pgsql.define_table {
    name = "osm_water",
    columns = {
        { column = "way", type = "geometry", projection = 3857, not_null = true }
    },
    cluster = "no",
    indexes = {}
}

local boundaries_table = osm2pgsql.define_table {
    name = "osm_boundaries",
    columns = {
        { column = "iso_3166_1", type = "text" },
        { column = "way", type = "geometry", projection = 3857, not_null = true }
    },
    cluster = "no",
    indexes = {}
}

local function insert_road(object, highway_type)
    local geom = object:as_linestring()
    if not geom then
        return
    end

    roads_table:insert({
        highway = highway_type,
        way = geom
    })
end

local function insert_railway(object)
    local geom = object:as_linestring()
    if not geom then
        return
    end

    railways_table:insert({
        way = geom
    })
end

local function insert_water(object, natural_type)
    local geom = nil

    if object.tags and object.tags.type == "multipolygon" then
        geom = object:as_multipolygon()
    elseif natural_type == "water" and object.is_closed then
        geom = object:as_polygon()
    else
        local osm_type = object.object_type or "way"
        if osm_type == "way" then
            geom = object:as_linestring()
        else
            geom = object:as_multipolygon()
        end
    end

    if not geom then
        return
    end

    water_table:insert({
        way = geom
    })
end

local function insert_boundary(object)
    local geom = object:as_multipolygon()
    if not geom then
        return
    end

    boundaries_table:insert({
        iso_3166_1 = object.tags["ISO3166-1"] or object.tags["ISO3166-1:alpha2"] or object.tags["iso3166-1:alpha2"],
        way = geom
    })
end

function osm2pgsql.process_node(object)
    return nil
end

function osm2pgsql.process_way(object)
    local tags = object.tags

    if tags.highway and ROAD_TYPES[tags.highway] then
        insert_road(object, tags.highway)
        return
    end

    if tags.railway == "rail" then
        insert_railway(object)
        return
    end

    if tags.waterway == "river" or tags.natural == "water" then
        insert_water(object, tags.natural)
        return
    end
end

function osm2pgsql.process_relation(object)
    local tags = object.tags

    if not (tags.type == "multipolygon" or tags.type == "boundary") then
        return
    end

    if tags.waterway == "river" or tags.natural == "water" then
        insert_water(object, tags.natural)
        return
    end

    if tags.boundary == "administrative" and tags.admin_level == "2" then
        insert_boundary(object)
        return
    end
end
