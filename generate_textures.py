#!/usr/bin/env python3
"""OSM highway + border renderer from PostGIS using GDAL rasterization or Mapnik.

Supports two rendering engines:
1. GDAL rasterization (default) for vector-to-raster conversion with PostGIS SQL queries
2. Mapnik rendering engine for high-quality cartographic output

Both engines support:
- ST_Buffer for road line styling (based on highway type)  
- Supersampling for anti-aliasing (e.g., 4x resolution)
- oiiotool for high-quality downsampling with Lanczos filtering

Features:
  * Fetches OSM data from a PostGIS database for a given bbox
  * Highways (motorway, trunk, primary, secondary, tertiary) with scaled buffers
  * National borders, water features, railways
  * GDAL: Uses SQL with ST_Buffer for road width styling
  * Mapnik: Uses Python mapnik module for native styling with LineSymbolizer and PolygonSymbolizer
  * Parallel rendering of different layer types

Usage:
  # GDAL rendering (default)
  python generate_textures.py \
      --bbox min_lon,min_lat,max_lon,max_lat \
      --output output/vancouver_test.png \
      --width 2048 --height 1536 \
      --scale-factor 4

  # Mapnik rendering (requires python-mapnik)
  python generate_textures.py \
      --bbox min_lon,min_lat,max_lon,max_lat \
      --output output/vancouver_test.png \
      --renderer mapnik \
      --width 2048 --height 1536 \
      --scale-factor 4

Dependencies:
  psycopg3, gdal (gdal_rasterize), oiiotool (OpenImageIO)
  Optional: python-mapnik (for Mapnik rendering)

Notes:
  - Assumes osm2pgsql-style schema (osm_roads, borders, etc.)
  - Database credentials should be handled securely (e.g., env vars)
  - Road buffer sizes are scaled by the supersampling factor
  - Elevation rendering always uses GDAL (gdalwarp) regardless of renderer choice
"""

import argparse
import json

# Check if python-mapnik module is available
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from pyproj import Transformer

# Try to import mapnik module
try:
    import mapnik

    MAPNIK_AVAILABLE = True
except ImportError:
    mapnik = None
    MAPNIK_AVAILABLE = False


def setup_mapnik_plugins():
    """Setup Mapnik plugins directory - optimized with hardcoded paths"""
    if not MAPNIK_AVAILABLE:
        return None

    try:
        # Check if PostGIS plugin is already available
        try:
            plugins = mapnik.DatasourceCache.plugin_names()
            if "postgis" in plugins:
                return "already_registered"
        except Exception:
            pass

        # Hardcoded paths in order of preference (fastest first)
        preferred_paths = [
            "/Users/tas/Documents/code/PrettyMaps/mapnik_plugins",  # Local copy
            "/opt/homebrew/lib/mapnik/input",  # Homebrew on macOS
        ]

        for plugins_dir in preferred_paths:
            plugins_path = Path(plugins_dir)
            if plugins_path.exists() and (plugins_path / "postgis.input").exists():
                try:
                    success = mapnik.DatasourceCache.register_datasources(
                        str(plugins_path), True
                    )
                    if success:
                        # Verify PostGIS is now available
                        plugins = mapnik.DatasourceCache.plugin_names()
                        if "postgis" in plugins:
                            return f"registered: {plugins_path}"
                except Exception:
                    continue

        return "no_plugins_found"
    except Exception as e:
        print(f"Warning: Could not setup Mapnik plugins: {e}", file=sys.stderr)
        return None


# Create a transformer object for WGS84 to Web Mercator conversion
# 'EPSG:4326' is the code for WGS84 (lon, lat).
# 'EPSG:3857' is the code for Web Mercator (x, y).
transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857")


def wgs84_to_web_mercator(lon, lat):
    """Convert WGS84 coordinates to Web Mercator (EPSG:3857) using pyproj."""
    # Perform the transformation.
    x, y = transformer.transform(lat, lon)  # Note: pyproj expects (lat, lon) order
    return x, y


# Database configuration
DB_HOST = "192.168.1.3"
DB_NAME = "osm_db"
DB_USER = "postgres"
DB_PASSWORD = None

HIGHWAY_TAGS = [
    "motorway",
    "trunk",
    # "primary",
    # "secondary",
    # "tertiary",
]

# Buffer sizes for different feature types (in final output pixels, will be scaled by supersampling factor)
HIGHWAY_BUFFERS = {
    "motorway": 32.0,
    "trunk": 16.0,
    "primary": 12.0,
    "secondary": 4.0,
    "tertiary": 4.0,
}

# Global buffer sizes for single-type layers (in final output pixels)
RAILWAY_BUFFER = 32.0
WATERWAY_BUFFER = 4.0

# Default supersampling scale factor for anti-aliasing
SCALE_FACTOR = 4

# Elevation dataset configuration
ELEVATION_VRT_PATH = (
    "F:/ElevationMaps/Datasets/AW3D30/aw3d30.vrt"  # Default path, can be overridden
)

# Mapnik styling configuration
MAPNIK_STYLES = {
    "roads": {
        "motorway": {"color": "#FFFFFF", "width": 8.0},
        "trunk": {"color": "#FFFFFF", "width": 4.0},
        "primary": {"color": "#FFFFFF", "width": 3.0},
        "secondary": {"color": "#FFFFFF", "width": 1.0},
        "tertiary": {"color": "#FFFFFF", "width": 1.0},
    },
    "railways": {"color": "#444444", "width": 8.0},
    "water": {"color": "#0077FF", "width": 1.0},
    "borders": {"color": "#000000", "width": 0.5, "fill": "#888888"},
}


class Config:
    def __init__(
        self,
        bbox,
        width,
        height,
        output,
        render_types=None,
        verbose=False,
        geojson_polygon=None,
        geojson_file_path=None,
        scale_factor=SCALE_FACTOR,
        max_workers=4,
        elevation_vrt_path=ELEVATION_VRT_PATH,
        renderer="gdal",
    ):
        self.bbox = bbox  # (min_lon, min_lat, max_lon, max_lat)
        self.width = width
        self.height = height
        self.output = output if isinstance(output, Path) else Path(output)
        self.render_types = render_types or ["roads", "borders"]
        self.verbose = verbose
        self.geojson_polygon = (
            geojson_polygon  # GeoJSON polygon geometry for precise filtering
        )
        self.geojson_file_path = geojson_file_path  # Path to original GeoJSON file
        self.scale_factor = scale_factor
        self.max_workers = max_workers
        self.elevation_vrt_path = elevation_vrt_path
        self.renderer = renderer  # "gdal" or "mapnik"

        # Calculate supersampled dimensions
        self.super_width = self.width * self.scale_factor
        self.super_height = self.height * self.scale_factor


# ---------------------- Utility / IO ------------------------------------ #


def parse_bbox(bbox_str):
    parts = bbox_str.split(",")
    if len(parts) != 4:
        raise ValueError(
            "BBox must have 4 comma-separated numbers: min_lon,min_lat,max_lon,max_lat"
        )
    min_lon, min_lat, max_lon, max_lat = map(float, parts)
    if not (min_lon < max_lon and min_lat < max_lat):
        raise ValueError("Invalid bbox coordinate ordering or extents")
    return (min_lon, min_lat, max_lon, max_lat)


def parse_geojson_region(geojson_path):
    """Parse GeoJSON file and extract bounding box and polygon geometry from the first polygon feature"""
    try:
        with open(geojson_path, "r") as f:
            geojson_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        raise ValueError(f"Failed to read or parse GeoJSON file '{geojson_path}': {e}")

    # Extract coordinates from the first feature
    features = geojson_data.get("features", [])
    if not features:
        raise ValueError("No features found in GeoJSON file")

    first_feature = features[0]
    geometry = first_feature.get("geometry", {})
    geom_type = geometry.get("type", "")
    coordinates = geometry.get("coordinates", [])

    if geom_type != "Polygon":
        raise ValueError(f"Expected Polygon geometry, found {geom_type}")

    if not coordinates or not coordinates[0]:
        raise ValueError("Invalid polygon coordinates in GeoJSON")

    # Extract exterior ring coordinates (first array in coordinates)
    exterior_ring = coordinates[0]

    # Calculate bounding box from all coordinates
    lons = [coord[0] for coord in exterior_ring]
    lats = [coord[1] for coord in exterior_ring]

    min_lon, max_lon = min(lons), max(lons)
    min_lat, max_lat = min(lats), max(lats)

    if not (min_lon < max_lon and min_lat < max_lat):
        raise ValueError("Invalid polygon extents")

    # Return both bbox and the full geometry for precise spatial queries
    return (min_lon, min_lat, max_lon, max_lat), geometry


def log(msg, cfg, *, force=False):
    if cfg.verbose or force:
        print(msg, file=sys.stderr)


def log_timing(start_time, operation, cfg):
    """Log timing information in verbose mode"""
    if cfg.verbose:
        elapsed = time.time() - start_time
        if elapsed < 1:
            print(f"[TIMING] {operation}: {elapsed * 1000:.1f}ms", file=sys.stderr)
        else:
            print(f"[TIMING] {operation}: {elapsed:.2f}s", file=sys.stderr)


# ---------------------- Query Construction ------------------------------ #


def make_spatial_filter(bbox, geojson_polygon=None):
    """Create spatial filter clause - use polygon geometry if available, otherwise bbox"""
    if geojson_polygon:
        # Convert GeoJSON polygon to WKT format for PostGIS
        coords = geojson_polygon["coordinates"][0]  # exterior ring
        wkt_coords = ", ".join([f"{lon} {lat}" for lon, lat in coords])
        polygon_wkt = f"POLYGON(({wkt_coords}))"
        return f"ST_Intersects(way, ST_Transform(ST_GeomFromText('{polygon_wkt}', 4326), 3857))"
    else:
        # Fall back to bounding box
        min_lon, min_lat, max_lon, max_lat = bbox
        return f"way && ST_Transform(ST_MakeEnvelope({min_lon}, {min_lat}, {max_lon}, {max_lat}, 4326), 3857)"


# ---------------------- GDAL Rendering Functions ---------------------- #


def create_oiiotool_downsample_cmd(input_path, output_path, scale_factor):
    """Create oiiotool command for downsampling"""
    downsample_percent = int(100 / scale_factor)

    output_ext = str(output_path).lower()

    if output_ext.endswith(".exr"):
        # EXR format with specific options
        cmd = [
            "oiiotool",
            str(input_path),
            "--resize:filter=cubic",
            f"{downsample_percent}%",
            "-d",
            "half",
            "--compression",
            "zip",
            "-otex",
            str(output_path),
        ]
    else:
        # Standard formats (PNG, TIFF, etc.)
        cmd = [
            "oiiotool",
            str(input_path),
            "--resize:filter=box",
            f"{downsample_percent}%",
            "-o",
            str(output_path),
        ]

    return cmd


def render_elevation_layer(cfg, temp_supersampled, final_output):
    """Render elevation layer using gdalwarp instead of gdal_rasterize"""
    start_time = time.time()

    log("[GDAL] Rendering elevation with gdalwarp...", cfg)

    # Build gdalwarp command for elevation
    # Use final dimensions, not supersampled dimensions for elevation data
    cmd = [
        "gdalwarp",
        "-t_srs",
        "EPSG:3857",
        "-ot",
        "Float32",
        "-ts",
        str(cfg.width),  # Use final width, not supersampled
        str(cfg.height),  # Use explicit height to match borders layer
        "-r",
        "lanczos",
    ]

    if cfg.geojson_file_path:
        # When using cutline, we need to specify target extent to ensure consistent dimensions
        min_lon, min_lat, max_lon, max_lat = cfg.bbox
        cmd.extend(
            [
                "-te_srs",
                "EPSG:4326",
                "-te",
                str(min_lon),
                str(min_lat),
                str(max_lon),
                str(max_lat),
                "-cutline",
                str(cfg.geojson_file_path),
                "-crop_to_cutline",
            ]
        )
    else:
        min_lon, min_lat, max_lon, max_lat = cfg.bbox
        cmd.extend(
            [
                "-te_srs",
                "EPSG:4326",
                "-te",
                str(min_lon),
                str(min_lat),
                str(max_lon),
                str(max_lat),
            ]
        )

    # Add source and destination
    cmd.extend([cfg.elevation_vrt_path, "-overwrite", str(temp_supersampled)])

    if cfg.verbose:
        print(f"[GDAL Command] {' '.join(cmd)}", file=sys.stderr)

    if cfg.verbose:
        # In verbose mode, show gdalwarp progress output
        result = subprocess.run(cmd, text=True)
    else:
        # In non-verbose mode, capture output as before
        result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        if cfg.verbose:
            raise RuntimeError(
                f"gdalwarp failed for elevation with return code {result.returncode}"
            )
        else:
            raise RuntimeError(f"gdalwarp failed for elevation: {result.stderr}")

    log_timing(start_time, "GDAL elevation rendering", cfg)

    # For elevation, we still need oiiotool processing but skip downsampling
    # Apply format conversion and compression
    if temp_supersampled.exists():
        oiio_start = time.time()

        # Create oiiotool command for elevation processing
        output_ext = str(final_output).lower()

        if output_ext.endswith(".exr"):
            # EXR format with specific options for elevation data
            cmd = [
                "oiiotool",
                str(temp_supersampled),
                "-d",
                "half",
                "-compression",
                "zip",
                "-otex",
                str(final_output),
            ]
        else:
            # Standard formats (PNG, TIFF, etc.) - just convert format
            cmd = [
                "oiiotool",
                str(temp_supersampled),
                "-o",
                str(final_output),
            ]

        log(f"[oiiotool] Processing elevation format for {final_output}...", cfg)

        if cfg.verbose:
            print(f"[OIIO Command] {' '.join(cmd)}", file=sys.stderr)

        if cfg.verbose:
            # In verbose mode, show oiiotool progress output
            result = subprocess.run(cmd, text=True)
        else:
            # In non-verbose mode, capture output as before
            result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            if cfg.verbose:
                raise RuntimeError(
                    f"oiiotool failed for elevation with return code {result.returncode}"
                )
            else:
                raise RuntimeError(f"oiiotool failed for elevation: {result.stderr}")

        log_timing(oiio_start, "oiiotool elevation processing", cfg)
        log(f"[Elevation] Processed elevation data to: {final_output}", cfg)

    log_timing(start_time, "Total elevation rendering", cfg)
    return final_output


def render_single_layer_gdal(cfg, layer_name, spatial_filter):
    """Render a single layer using GDAL rasterization + oiiotool downsampling"""

    start_time = time.time()
    log(f"[Render] Starting GDAL rendering for {layer_name}...", cfg)

    # Create temporary supersampled output path in the same directory as output
    temp_dir = cfg.output.parent / "temp"
    temp_dir.mkdir(exist_ok=True)
    temp_supersampled = temp_dir / f"{cfg.output.stem}_{layer_name}_supersampled.tif"

    # Create final output path - use same extension as specified in output argument
    output_ext = cfg.output.suffix if cfg.output.suffix else ".png"
    final_output = cfg.output.parent / f"{layer_name}{output_ext}"

    try:
        # Handle elevation rendering with gdalwarp instead of gdal_rasterize
        if layer_name == "elevation":
            return render_elevation_layer(cfg, temp_supersampled, final_output)

        # Generate appropriate SQL query based on layer type
        if layer_name == "roads":
            # For roads, create complex SQL with buffers for different highway types
            highway_conditions = []
            for highway_type in HIGHWAY_TAGS:
                # Convert pixel width to buffer radius in supersampled space
                # Buffer radius = (pixel_width / 2) * scale_factor
                pixel_width = HIGHWAY_BUFFERS.get(highway_type, 2.0)
                buffer_radius = (pixel_width / 2.0) * cfg.scale_factor
                highway_conditions.append(
                    f"WHEN highway = '{highway_type}' THEN ST_Buffer(way, {buffer_radius})"
                )
            sql_query = f"""
                SELECT CASE {" ".join(highway_conditions)} END AS geom
                FROM public.osm_roads 
                WHERE {spatial_filter} 
                AND highway IN ({", ".join([f"'{ht}'" for ht in HIGHWAY_TAGS])})
            """
        elif layer_name == "railways":
            # For railways, use a single global buffer for all railway features
            # Convert pixel width to buffer radius in supersampled space
            pixel_width = RAILWAY_BUFFER
            buffer_radius = (pixel_width / 2.0) * cfg.scale_factor
            sql_query = f"""
                SELECT ST_Buffer(way, {buffer_radius}) AS geom
                FROM public.osm_railways 
                WHERE {spatial_filter}
            """
        elif layer_name == "water":
            # For water, use a single global buffer for all waterway features (rivers only)
            # Convert pixel width to buffer radius in supersampled space
            pixel_width = WATERWAY_BUFFER
            buffer_radius = (pixel_width / 2.0) * cfg.scale_factor
            sql_query = f"""
                SELECT ST_Buffer(way, {buffer_radius}) AS geom
                FROM public.osm_water 
                WHERE {spatial_filter}
            """
        elif layer_name == "borders":
            sql_query = (
                f"""SELECT way FROM public.borders WHERE {spatial_filter}"""
            )
        else:
            raise ValueError(f"Unknown layer type: {layer_name}")

        # Build unified GDAL command for all vector layers
        cmd = [
            "gdal_rasterize",
            "-ot",
            "Byte",
            "-a_nodata",
            "0",
            "-burn",
            "255",
            "-ts",
            str(cfg.super_width),
            str(cfg.super_height),
            "-co",
            "COMPRESS=LZW",
            "-sql",
            sql_query,
            f"PG:host={DB_HOST} port=5432 user={DB_USER} dbname={DB_NAME}",
            str(temp_supersampled),
        ]

        # Add consistent extent for all layers
        min_lon, min_lat, max_lon, max_lat = cfg.bbox
        # Convert WGS84 coordinates to Web Mercator (EPSG:3857) to match PostGIS data
        min_x, min_y = wgs84_to_web_mercator(min_lon, min_lat)
        max_x, max_y = wgs84_to_web_mercator(max_lon, max_lat)
        cmd.extend(["-te", str(min_x), str(min_y), str(max_x), str(max_y)])

        log(f"[GDAL] Rasterizing {layer_name}...", cfg)

        if cfg.verbose:
            print(f"[GDAL Command] {' '.join(cmd)}", file=sys.stderr)

        if cfg.verbose:
            # In verbose mode, show gdal_rasterize progress output
            result = subprocess.run(cmd, text=True)
        else:
            # In non-verbose mode, capture output as before
            result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            if cfg.verbose:
                raise RuntimeError(
                    f"gdal_rasterize failed for {layer_name} with return code {result.returncode}"
                )
            else:
                raise RuntimeError(
                    f"gdal_rasterize failed for {layer_name}: {result.stderr}"
                )

        log_timing(start_time, f"GDAL rasterization for {layer_name}", cfg)

        # Downsample using oiiotool
        if temp_supersampled.exists():
            downsample_start = time.time()
            cmd = create_oiiotool_downsample_cmd(
                temp_supersampled, final_output, cfg.scale_factor
            )
            log(f"[oiiotool] Downsampling {layer_name}...", cfg)

            if cfg.verbose:
                print(f"[OIIO Command] {' '.join(cmd)}", file=sys.stderr)

            if cfg.verbose:
                # In verbose mode, show oiiotool progress output
                result = subprocess.run(cmd, text=True)
            else:
                # In non-verbose mode, capture output as before
                result = subprocess.run(cmd, capture_output=True, text=True)

            if result.returncode != 0:
                if cfg.verbose:
                    raise RuntimeError(
                        f"oiiotool failed for {layer_name} with return code {result.returncode}"
                    )
                else:
                    raise RuntimeError(f"oiiotool failed: {result.stderr}")

            log_timing(downsample_start, f"oiiotool downsampling for {layer_name}", cfg)

        log_timing(start_time, f"Total rendering for {layer_name}", cfg)
        return final_output

    except Exception as e:
        # Clean up on error - only clean up files for this specific layer
        # temp_dir = Path(tempfile.gettempdir()) / "osm_render"
        # if temp_dir.exists():
        #     temp_file = temp_dir / f"{cfg.output.stem}_{layer_name}_supersampled.tif"
        #     if temp_file.exists():
        #         temp_file.unlink()
        raise e


def render_single_layer_mapnik(cfg, layer_name, spatial_filter):
    """Render a single layer using Python mapnik module"""

    if not MAPNIK_AVAILABLE:
        raise RuntimeError(
            "python-mapnik module is not available. Please install python-mapnik or use GDAL rendering."
        )

    start_time = time.time()
    log(f"[Render] Starting Mapnik rendering for {layer_name}...", cfg)

    # Setup plugins
    setup_mapnik_plugins()

    # Create final output path - use same extension as specified in output argument
    output_ext = cfg.output.suffix if cfg.output.suffix else ".png"
    final_output = cfg.output.parent / f"{layer_name}{output_ext}"

    try:
        # Create temporary directory
        temp_dir = cfg.output.parent / "temp"
        temp_dir.mkdir(exist_ok=True)

        # Create temporary supersampled output
        temp_supersampled = (
            temp_dir / f"{cfg.output.stem}_{layer_name}_mapnik_supersampled.png"
        )

        # Convert WGS84 bbox to Web Mercator
        min_lon, min_lat, max_lon, max_lat = cfg.bbox
        min_x, min_y = wgs84_to_web_mercator(min_lon, min_lat)
        max_x, max_y = wgs84_to_web_mercator(max_lon, max_lat)

        # Create Map object
        m = mapnik.Map(cfg.super_width, cfg.super_height)
        m.background = mapnik.Color("black")

        # Set the spatial reference system
        m.srs = "+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs +over"

        # Create datasource parameters
        ds_params = {
            "type": "postgis",
            "host": DB_HOST,
            "port": "5432",
            "user": DB_USER,
            "dbname": DB_NAME,
            "srid": "3857",
        }
        if DB_PASSWORD:
            ds_params["password"] = DB_PASSWORD

        # Create layer and style based on layer type
        if layer_name == "roads":
            # Create style for roads
            style = mapnik.Style()

            # Add rules for each highway type
            for highway_type in HIGHWAY_TAGS:
                highway_style = MAPNIK_STYLES["roads"].get(
                    highway_type, {"color": "#FFFFFF", "width": 1.0}
                )

                rule = mapnik.Rule()
                rule.filter = mapnik.Expression(f"[highway] = '{highway_type}'")

                line_sym = mapnik.LineSymbolizer()
                line_sym.stroke = mapnik.Color(highway_style["color"])
                line_sym.stroke_width = highway_style["width"]
                line_sym.stroke_linecap = mapnik.stroke_linecap.ROUND_CAP
                line_sym.stroke_linejoin = mapnik.stroke_linejoin.ROUND_JOIN

                rule.symbolizers.append(line_sym)
                style.rules.append(rule)

            m.append_style("roads_style", style)

            # Create layer
            highway_list = "', '".join(HIGHWAY_TAGS)
            table_query = f"(SELECT way, highway FROM public.osm_roads WHERE {spatial_filter} AND highway IN ('{highway_list}')) as roads"
            ds_params["table"] = table_query
            ds_params["geometry_field"] = "way"

            layer = mapnik.Layer("roads")
            layer.srs = "+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs +over"
            layer.datasource = mapnik.Datasource(**ds_params)
            layer.styles.append("roads_style")

        elif layer_name == "railways":
            # Create style for railways
            style = mapnik.Style()
            rule = mapnik.Rule()

            railway_style = MAPNIK_STYLES["railways"]
            line_sym = mapnik.LineSymbolizer()
            line_sym.stroke = mapnik.Color(railway_style["color"])
            line_sym.stroke_width = railway_style["width"]
            line_sym.stroke_linecap = mapnik.stroke_linecap.ROUND_CAP

            rule.symbolizers.append(line_sym)
            style.rules.append(rule)
            m.append_style("railways_style", style)

            # Create layer
            table_query = f"(SELECT way FROM public.osm_railways WHERE {spatial_filter}) as railways"
            ds_params["table"] = table_query
            ds_params["geometry_field"] = "way"

            layer = mapnik.Layer("railways")
            layer.srs = "+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs +over"
            layer.datasource = mapnik.Datasource(**ds_params)
            layer.styles.append("railways_style")

        elif layer_name == "water":
            # Create style for water
            style = mapnik.Style()
            rule = mapnik.Rule()

            water_style = MAPNIK_STYLES["water"]
            line_sym = mapnik.LineSymbolizer()
            line_sym.stroke = mapnik.Color(water_style["color"])
            line_sym.stroke_width = water_style["width"]

            rule.symbolizers.append(line_sym)
            style.rules.append(rule)
            m.append_style("water_style", style)

            # Create layer
            table_query = (
                f"(SELECT way FROM public.osm_water WHERE {spatial_filter}) as water"
            )
            ds_params["table"] = table_query
            ds_params["geometry_field"] = "way"

            layer = mapnik.Layer("water")
            layer.srs = "+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs +over"
            layer.datasource = mapnik.Datasource(**ds_params)
            layer.styles.append("water_style")

        elif layer_name == "borders":
            # Create style for borders
            style = mapnik.Style()
            rule = mapnik.Rule()

            border_style = MAPNIK_STYLES["borders"]

            # Add polygon fill
            poly_sym = mapnik.PolygonSymbolizer()
            poly_sym.fill = mapnik.Color(border_style["fill"])
            poly_sym.fill_opacity = 0.3
            rule.symbolizers.append(poly_sym)

            # Add border line
            line_sym = mapnik.LineSymbolizer()
            line_sym.stroke = mapnik.Color(border_style["color"])
            line_sym.stroke_width = border_style["width"]
            rule.symbolizers.append(line_sym)

            style.rules.append(rule)
            m.append_style("borders_style", style)

            # Create layer
            table_query = (
                f"(SELECT way FROM public.borders WHERE {spatial_filter}) as borders"
            )
            ds_params["table"] = table_query
            ds_params["geometry_field"] = "way"

            layer = mapnik.Layer("borders")
            layer.srs = "+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs +over"
            layer.datasource = mapnik.Datasource(**ds_params)
            layer.styles.append("borders_style")

        else:
            raise ValueError(f"Unknown layer type: {layer_name}")

        # Add layer to map
        m.layers.append(layer)

        # Set the geographic extent
        bbox = mapnik.Box2d(min_x, min_y, max_x, max_y)
        m.zoom_to_box(bbox)

        log(
            f"[Mapnik] Rendering {layer_name} at {cfg.super_width}x{cfg.super_height}...",
            cfg,
        )

        # Render the map
        im = mapnik.Image(cfg.super_width, cfg.super_height)
        mapnik.render(m, im)

        # Save the image
        im.save(str(temp_supersampled), "png")

        log_timing(start_time, f"Mapnik rendering for {layer_name}", cfg)

        # Downsample using oiiotool if scale factor > 1
        if cfg.scale_factor > 1 and temp_supersampled.exists():
            downsample_start = time.time()
            cmd = create_oiiotool_downsample_cmd(
                temp_supersampled, final_output, cfg.scale_factor
            )
            log(f"[oiiotool] Downsampling {layer_name}...", cfg)

            if cfg.verbose:
                print(f"[OIIO Command] {' '.join(cmd)}", file=sys.stderr)

            if cfg.verbose:
                result = subprocess.run(cmd, text=True)
            else:
                result = subprocess.run(cmd, capture_output=True, text=True)

            if result.returncode != 0:
                if cfg.verbose:
                    raise RuntimeError(
                        f"oiiotool failed for {layer_name} with return code {result.returncode}"
                    )
                else:
                    raise RuntimeError(f"oiiotool failed: {result.stderr}")

            log_timing(downsample_start, f"oiiotool downsampling for {layer_name}", cfg)
        elif temp_supersampled.exists():
            # No downsampling needed, just copy/convert format if necessary
            if str(final_output) != str(temp_supersampled):
                cmd = ["oiiotool", str(temp_supersampled), "-o", str(final_output)]
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode != 0:
                    raise RuntimeError(
                        f"oiiotool format conversion failed: {result.stderr}"
                    )

        log_timing(start_time, f"Total Mapnik rendering for {layer_name}", cfg)
        return final_output

    except Exception as e:
        raise e


def render_mapnik(cfg):
    """Render all requested layers using Mapnik rendering engine"""

    if not MAPNIK_AVAILABLE:
        raise RuntimeError(
            "python-mapnik module is not available. Please install python-mapnik or use GDAL rendering with --renderer gdal"
        )

    # # Setup plugins
    # plugins_result = setup_mapnik_plugins()
    # if plugins_result:
    #     log(f"[Mapnik] Plugins setup: {plugins_result}", cfg)
    # else:
    #     log(
    #         "[WARNING] Could not setup Mapnik plugins. PostGIS support may not be available.",
    #         cfg,
    #         force=True,
    #     )

    # Create spatial filter for the bbox or geojson polygon
    spatial_filter = make_spatial_filter(cfg.bbox, cfg.geojson_polygon)

    # Prepare rendering tasks
    render_tasks = []
    for layer_name in cfg.render_types:
        # Skip elevation for Mapnik rendering (use GDAL for elevation)
        if layer_name == "elevation":
            log(
                "[Mapnik] Skipping elevation layer - use GDAL renderer for elevation",
                cfg,
            )
            continue
        render_tasks.append((layer_name, spatial_filter))

    log(
        f"[Render] Starting {len(render_tasks)} Mapnik rendering tasks...",
        cfg,
        force=True,
    )

    # Render layers in parallel
    output_paths = []
    if render_tasks:
        with ThreadPoolExecutor(
            max_workers=min(len(render_tasks), cfg.max_workers)
        ) as executor:
            # Submit all rendering tasks
            future_to_layer = {}
            for layer_name, spatial_filter in render_tasks:
                log(f"[Render] Submitting {layer_name} Mapnik rendering task...", cfg)
                future = executor.submit(
                    render_single_layer_mapnik, cfg, layer_name, spatial_filter
                )
                future_to_layer[future] = layer_name

            # Collect results as they complete
            completed_count = 0
            for future in as_completed(future_to_layer):
                layer_name = future_to_layer[future]
                try:
                    output_path = future.result()
                    output_paths.append(output_path)
                    completed_count += 1
                    log(
                        f"[Render] Completed {layer_name} layer ({completed_count}/{len(render_tasks)}): {output_path}",
                        cfg,
                        force=True,
                    )
                except Exception as e:
                    log(f"[ERROR] Failed to render {layer_name}: {e}", cfg, force=True)

    # # Clean up temporary files after all layers are complete
    # temp_dir = cfg.output.parent / "temp"
    # if temp_dir.exists():
    #     try:
    #         for temp_file in temp_dir.glob("*mapnik*"):
    #             if temp_file.exists():
    #                 temp_file.unlink()
    #         for temp_file in temp_dir.glob("*_style.xml"):
    #             if temp_file.exists():
    #                 temp_file.unlink()
    #         log("[Cleanup] Removed Mapnik temporary files", cfg)
    #     except OSError as e:
    #         log(f"[WARNING] Could not clean up Mapnik temp files: {e}", cfg)

    return output_paths


def render_gdal(cfg):
    """Render all requested layers using GDAL + oiiotool pipeline"""

    # Create spatial filter for the bbox or geojson polygon
    spatial_filter = make_spatial_filter(cfg.bbox, cfg.geojson_polygon)

    # Prepare rendering tasks
    render_tasks = []
    for layer_name in cfg.render_types:
        render_tasks.append((layer_name, spatial_filter))

    log(
        f"[Render] Starting {len(render_tasks)} GDAL rendering tasks...",
        cfg,
        force=True,
    )

    # Render layers in parallel
    output_paths = []
    if render_tasks:
        with ThreadPoolExecutor(
            max_workers=min(len(render_tasks), cfg.max_workers)
        ) as executor:
            # Submit all rendering tasks
            future_to_layer = {}
            for layer_name, spatial_filter in render_tasks:
                log(f"[Render] Submitting {layer_name} GDAL rendering task...", cfg)
                future = executor.submit(
                    render_single_layer_gdal, cfg, layer_name, spatial_filter
                )
                future_to_layer[future] = layer_name

            # Collect results as they complete
            completed_count = 0
            for future in as_completed(future_to_layer):
                layer_name = future_to_layer[future]
                try:
                    output_path = future.result()
                    output_paths.append(output_path)
                    completed_count += 1
                    log(
                        f"[Render] Completed {layer_name} layer ({completed_count}/{len(render_tasks)}): {output_path}",
                        cfg,
                        force=True,
                    )
                except Exception as e:
                    log(f"[ERROR] Failed to render {layer_name}: {e}", cfg, force=True)

    # Clean up temporary files after all layers are complete
    temp_dir = cfg.output.parent / "temp"
    if temp_dir.exists():
        try:
            for temp_file in temp_dir.glob("*"):
                if temp_file.exists():
                    temp_file.unlink()
            temp_dir.rmdir()
            log("[Cleanup] Removed temporary files", cfg)
        except OSError as e:
            log(f"[WARNING] Could not clean up temp directory: {e}", cfg)

    return output_paths


# ---------------------- Main Flow --------------------------------------- #


def run(cfg):
    total_start = time.time()
    log(f"BBox: {cfg.bbox}", cfg, force=True)
    log(f"Output: {cfg.output}", cfg)
    log(f"Rendering: {', '.join(cfg.render_types)}", cfg, force=True)
    log(f"Renderer: {cfg.renderer}", cfg, force=True)
    log(f"Final dimensions: {cfg.width}x{cfg.height}", cfg, force=True)
    log(f"Scale factor: {cfg.scale_factor}", cfg, force=True)
    log(
        f"Supersampled dimensions: {cfg.super_width}x{cfg.super_height}",
        cfg,
        force=True,
    )
    log(
        f"Supersampling: {cfg.scale_factor}x ({cfg.super_width}x{cfg.super_height} -> {cfg.width}x{cfg.height})",
        cfg,
        force=True,
    )

    # Choose rendering method based on configuration
    if cfg.renderer == "mapnik":
        log("[Render] Starting Mapnik-based rendering...", cfg)
        render_start = time.time()
        out_paths = render_mapnik(cfg)
        log_timing(render_start, "Total Mapnik rendering", cfg)
    else:
        log("[Render] Starting GDAL-based rendering...", cfg)
        render_start = time.time()
        out_paths = render_gdal(cfg)
        log_timing(render_start, "Total GDAL rendering", cfg)

    log_timing(total_start, "TOTAL EXECUTION TIME", cfg)

    if out_paths:
        log(
            f"Rendered {len(out_paths)} layer(s) (size {cfg.width}x{cfg.height}):",
            cfg,
            force=True,
        )
        for path in out_paths:
            log(f"  - {path}", cfg, force=True)
    else:
        log("No layers were rendered (no data found)", cfg, force=True)

    return out_paths


# ---------------------- CLI --------------------------------------------- #


def build_arg_parser():
    epilog = (
        "Notes: If your --bbox value starts with a negative number you must either quote it "
        "('--bbox \"-123.3,49.1,-122.9,49.35\"') or use the equals form (--bbox=-123.3,49.1,-122.9,49.35).\n"
        "You can also use --geojson to specify a polygon region from a GeoJSON file (overrides --bbox).\n\n"
        "Render types: roads, borders, water, railways, elevation (all are optional, default is roads, borders, water, railways)\n"
        "Elevation rendering uses gdalwarp with a cutline from the bbox/geojson polygon."
    )
    p = argparse.ArgumentParser(
        description="Render OSM highways + borders using GDAL rasterization from PostGIS",
        epilog=epilog,
    )
    p.add_argument(
        "--bbox",
        required=False,
        type=str,
        # default="130.36,32.49,130.85,32.98",
        default="129.97,32.60,130.79,33.33",
        help="Combined bbox 'min_lon,min_lat,max_lon,max_lat' (use equals sign for negative values: --bbox=-123.30,49.10,-122.90,49.35)",
    )
    p.add_argument(
        "--geojson",
        required=False,
        type=str,
        help="Path to GeoJSON file containing a polygon region to render (overrides --bbox if provided)",
    )
    p.add_argument(
        "--output",
        required=False,
        default="output/kumamoto.png",
        help="Output file path (supports .png, .tif, etc.)",
    )
    p.add_argument(
        "--width",
        type=int,
        default=2048,
        help="Final image width in pixels (default: 2048)",
    )
    p.add_argument(
        "--height",
        type=int,
        default=None,
        help="Final image height in pixels. If not provided, will be calculated from width based on bbox/geojson aspect ratio",
    )
    p.add_argument(
        "--scale-factor",
        type=int,
        default=SCALE_FACTOR,
        help=f"Supersampling scale factor for anti-aliasing (default: {SCALE_FACTOR})",
    )
    p.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Maximum number of concurrent rendering threads (default: 4)",
    )
    p.add_argument(
        "--elevation-vrt",
        type=str,
        default=ELEVATION_VRT_PATH,
        help=f"Path to elevation VRT file for elevation rendering (default: {ELEVATION_VRT_PATH})",
    )
    p.add_argument(
        "--render",
        nargs="*",
        choices=["roads", "borders", "water", "railways", "elevation"],
        default=["roads", "borders", "water", "railways", "elevation"],
        help="What to render (default: roads borders). Can specify multiple: --render roads water borders elevation",
    )
    p.add_argument(
        "--renderer",
        choices=["gdal", "mapnik"],
        default="gdal",
        help="Rendering engine to use: gdal (default) or mapnik. Mapnik provides better styling but requires python-mapnik.",
    )
    p.add_argument("--verbose", action="store_true")
    return p


def main(argv=None):
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    # Determine bbox source - prioritize GeoJSON if provided
    bbox = None
    geojson_polygon = None

    if args.geojson:
        try:
            bbox, geojson_polygon = parse_geojson_region(args.geojson)
            print(f"Using region from GeoJSON file: {args.geojson}", file=sys.stderr)
            print(f"Extracted bbox: {bbox}", file=sys.stderr)
            print(
                "Using precise polygon geometry for spatial filtering", file=sys.stderr
            )
        except ValueError as e:
            parser.error(str(e))
    else:
        # Fall back to bbox argument
        try:
            bbox = parse_bbox(args.bbox)
        except ValueError as e:  # noqa: BLE001
            parser.error(str(e))

    # Calculate height based on bbox aspect ratio if not provided
    width = args.width
    height = args.height

    if height is None:
        # Calculate height based on bbox aspect ratio in EPSG:3857 (Web Mercator)
        min_lon, min_lat, max_lon, max_lat = bbox

        # Transform coordinates to EPSG:3857 to get metric units
        min_x, min_y = wgs84_to_web_mercator(min_lon, min_lat)
        max_x, max_y = wgs84_to_web_mercator(max_lon, max_lat)

        # Calculate width and height in meters
        bbox_width_meters = max_x - min_x
        bbox_height_meters = max_y - min_y

        # Calculate aspect ratio using metric units
        aspect_ratio = bbox_height_meters / bbox_width_meters
        height = int(width * aspect_ratio)
        print(
            f"Calculated height from bbox aspect ratio: {height} pixels (aspect ratio: {aspect_ratio:.3f})",
            file=sys.stderr,
        )
        print(
            f"Bbox dimensions in EPSG:3857: {bbox_width_meters:.0f}m x {bbox_height_meters:.0f}m",
            file=sys.stderr,
        )

    cfg = Config(
        bbox=bbox,
        width=width,
        height=height,
        output=Path(args.output),
        render_types=args.render,
        verbose=args.verbose,
        geojson_polygon=geojson_polygon,
        geojson_file_path=args.geojson,  # Pass the original file path
        scale_factor=args.scale_factor,
        max_workers=args.max_workers,
        elevation_vrt_path=args.elevation_vrt,
        renderer=args.renderer,
    )

    try:
        run(cfg)
    except Exception as e:  # noqa: BLE001
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
    raise SystemExit(main())
