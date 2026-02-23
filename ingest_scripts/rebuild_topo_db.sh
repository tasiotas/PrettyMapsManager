#!/bin/bash
# ============================================================================
# OSM Topo Database Rebuild Script
# Rebuilds osm_topo_app and ingests:
#   - Buildings (for 3D generation)
#   - Mapnik layers (roads / railways / water / borders)
#   - Country polygons from land_polygons.shp
#
# Usage:
#   ./rebuild_topo_db.sh [dev|staging|prod] [--skip-buildings] [--skip-mapnik] [--recreate-db]
# ============================================================================

set -u

usage() {
    echo "Usage: $0 [dev|staging|prod] [--skip-buildings] [--skip-mapnik] [--recreate-db]"
    echo "  dev             localhost:5432, japan-latest.osm.pbf"
    echo "  staging         postgis.staging.taslab.lan:5444, japan-latest.osm.pbf"
    echo "  prod            postgis.prod.taslab.lan:5444, planet.osm.pbf"
    echo
    echo "Options:"
    echo "  --skip-buildings   Skip buildings ingest and post-ingest SQL"
    echo "  --skip-mapnik      Skip mapnik ingest and post-ingest SQL"
    echo "  --recreate-db      Drop and recreate the whole database before ingest"
    echo "  -h, --help         Show this help"
}

MODE=""
SKIP_BUILDINGS=0
SKIP_MAPNIK=0
RECREATE_DB=0

for arg in "$@"; do
    case "$arg" in
        dev|staging|prod)
            if [ -n "$MODE" ]; then
                echo "ERROR: Multiple modes provided: '$MODE' and '$arg'"
                usage
                exit 1
            fi
            MODE="$arg"
            ;;
        --skip-buildings)
            SKIP_BUILDINGS=1
            ;;
        --skip-mapnik)
            SKIP_MAPNIK=1
            ;;
        --recreate-db)
            RECREATE_DB=1
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "ERROR: Unknown argument '$arg'"
            usage
            exit 1
            ;;
    esac
done

if [ -z "$MODE" ]; then
    echo "ERROR: Missing mode argument"
    usage
    exit 1
fi

if [ "$SKIP_BUILDINGS" -eq 1 ] && [ "$SKIP_MAPNIK" -eq 1 ]; then
    echo "ERROR: Both --skip-buildings and --skip-mapnik are set; nothing to run."
    exit 1
fi

echo "Starting OSM topo database rebuild in $MODE mode..."
echo "Skip buildings ingest: $([ "$SKIP_BUILDINGS" -eq 1 ] && echo yes || echo no)"
echo "Skip mapnik ingest:   $([ "$SKIP_MAPNIK" -eq 1 ] && echo yes || echo no)"
echo "Recreate database:    $([ "$RECREATE_DB" -eq 1 ] && echo yes || echo no)"
echo

# Initialize timing variables
SCRIPT_START=$(date +%s)
DB_SETUP_TIME="skipped"
BUILDINGS_IMPORT_TIME="skipped"
BUILDINGS_SQL_TIME="skipped"
MAPNIK_IMPORT_TIME="skipped"
COUNTRIES_IMPORT_TIME="skipped"
MAPNIK_SQL_TIME="skipped"

# Detect OS and set base path
if [[ "$OSTYPE" == "darwin"* ]] || [ -d "/Volumes/Vault" ]; then
    # macOS
    BASE_PATH="/Volumes/Vault/PrettyMaps"
else
    # Windows (Git Bash, Cygwin, WSL) - use Windows UNC path format
    BASE_PATH="\\\\TRUENAS\\Vault\\PrettyMaps"
fi

echo "Detected base path: $BASE_PATH"
echo

# Database connection settings
if [ "$MODE" = "dev" ]; then
    PGHOST=localhost
    PGPORT=5432
elif [ "$MODE" = "staging" ]; then
    PGHOST=staging.taslab.lan
    PGPORT=5443
else  # prod
    PGHOST=prod.taslab.lan
    PGPORT=5443
fi

PGUSER=SRoVfQiHuQsItlaWsCBKrbAoRERIqWTU
PGDATABASE=osm_topo_app

export PGPASSWORD=hi5pw9Fry7Bg8vzsdtCRUuepwpsIWwosU4j5hUUgpi4Gllgi6ygmCTZHQZUq67m4

# Set file path based on mode
if [ "$MODE" = "dev" ] || [ "$MODE" = "staging" ]; then
    OSM_FILE="$BASE_PATH/Datasets/OSM_Geofabrik/japan-latest.osm.pbf"
else  # prod
    OSM_FILE="D:\PrettyMaps\planet.osm.pbf"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

resolve_script_path() {
    local filename="$1"
    local local_path="$SCRIPT_DIR/$filename"
    local synced_path="$BASE_PATH/synced_scripts/topo/$filename"

    if [ -f "$local_path" ]; then
        echo "$local_path"
        return 0
    fi

    if [ -f "$synced_path" ]; then
        echo "$synced_path"
        return 0
    fi

    echo "ERROR: Required script file not found: $filename" >&2
    echo "Checked:" >&2
    echo "  $local_path" >&2
    echo "  $synced_path" >&2
    return 1
}

BUILDINGS_LUA="$(resolve_script_path "ingest_topo.lua")" || exit 1
BUILDINGS_SQL="$(resolve_script_path "post_ingest_topo.sql")" || exit 1
MAPNIK_LUA="$(resolve_script_path "ingest_mapnik.lua")" || exit 1
MAPNIK_SQL="$(resolve_script_path "post_ingest_mapnik.sql")" || exit 1

TOTAL_STEPS=1
if [ "$SKIP_BUILDINGS" -eq 0 ]; then
    TOTAL_STEPS=$((TOTAL_STEPS + 2))
fi
if [ "$SKIP_MAPNIK" -eq 0 ]; then
    TOTAL_STEPS=$((TOTAL_STEPS + 3))
fi
STEP_NO=1

# ============================================================================
# Database setup - optional full recreate, always ensure required extensions
# ============================================================================
echo "[$STEP_NO/$TOTAL_STEPS] Preparing database..."
STEP_START=$(date +%s)

if [ "$RECREATE_DB" -eq 1 ]; then
    if ! psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -c "SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = '$PGDATABASE' AND pid <> pg_backend_pid();" \
        || ! psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -c "DROP DATABASE IF EXISTS $PGDATABASE;" \
        || ! psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -c "CREATE DATABASE $PGDATABASE;"; then
        echo "ERROR: Database recreate failed!"
        exit 1
    fi
else
    DB_EXISTS=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = '$PGDATABASE';")

    if [ $? -ne 0 ]; then
        echo "ERROR: Failed checking whether database exists!"
        exit 1
    fi

    if [ "$DB_EXISTS" != "1" ]; then
        if ! psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -c "CREATE DATABASE $PGDATABASE;"; then
            echo "ERROR: Failed creating missing database $PGDATABASE!"
            exit 1
        fi
    fi
fi

if ! psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -c "CREATE EXTENSION IF NOT EXISTS postgis;" \
    || ! psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -c "CREATE EXTENSION IF NOT EXISTS hstore;"; then
    echo "ERROR: Failed enabling required extensions!"
    exit 1
fi

STEP_END=$(date +%s)
DB_SETUP_TIME="$((STEP_END - STEP_START))s"
echo "Database prep completed successfully in $DB_SETUP_TIME"
echo
STEP_NO=$((STEP_NO + 1))

# ============================================================================
# Buildings ingest
# ============================================================================
if [ "$SKIP_BUILDINGS" -eq 0 ]; then
    if ! psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -c "DROP TABLE IF EXISTS osm_buildings CASCADE;" > /dev/null; then
        echo "ERROR: Failed dropping existing buildings tables!"
        exit 1
    fi

    echo "[$STEP_NO/$TOTAL_STEPS] Importing OSM buildings data into PostGIS..."
    STEP_START=$(date +%s)

    osm2pgsql -H "$PGHOST" -P "$PGPORT" -U "$PGUSER" \
      -O flex -S "$BUILDINGS_LUA" \
      -d "$PGDATABASE" --number-processes 6 \
      "$OSM_FILE"

    if [ $? -ne 0 ]; then
        echo "ERROR: OSM buildings import failed!"
        exit 1
    fi

    STEP_END=$(date +%s)
    BUILDINGS_IMPORT_TIME="$((STEP_END - STEP_START))s"
    echo "OSM buildings import completed successfully in $BUILDINGS_IMPORT_TIME"
    echo
    STEP_NO=$((STEP_NO + 1))

    echo "[$STEP_NO/$TOTAL_STEPS] Running buildings post-ingestion SQL..."
    STEP_START=$(date +%s)

    psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -f "$BUILDINGS_SQL"

    if [ $? -ne 0 ]; then
        echo "ERROR: Buildings post-ingestion SQL failed!"
        exit 1
    fi

    STEP_END=$(date +%s)
    BUILDINGS_SQL_TIME="$((STEP_END - STEP_START))s"
    echo "Buildings post-ingestion SQL completed successfully in $BUILDINGS_SQL_TIME"
    echo
    STEP_NO=$((STEP_NO + 1))
fi

# ============================================================================
# Mapnik ingest (roads / railways / water / borders)
# ============================================================================
if [ "$SKIP_MAPNIK" -eq 0 ]; then
    if ! psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -c "DROP TABLE IF EXISTS osm_roads, osm_railways, osm_water, osm_boundaries, countries, borders CASCADE;" > /dev/null; then
        echo "ERROR: Failed dropping existing mapnik tables!"
        exit 1
    fi

    echo "[$STEP_NO/$TOTAL_STEPS] Importing OSM mapnik layers into PostGIS..."
    STEP_START=$(date +%s)

    osm2pgsql -H "$PGHOST" -P "$PGPORT" -U "$PGUSER" \
      -O flex -S "$MAPNIK_LUA" \
      -d "$PGDATABASE" --number-processes 6 \
      "$OSM_FILE"

    if [ $? -ne 0 ]; then
        echo "ERROR: OSM mapnik import failed!"
        exit 1
    fi

    STEP_END=$(date +%s)
    MAPNIK_IMPORT_TIME="$((STEP_END - STEP_START))s"
    echo "OSM mapnik import completed successfully in $MAPNIK_IMPORT_TIME"
    echo
    STEP_NO=$((STEP_NO + 1))

    # ============================================================================
    # Country polygons import (filtered to Japan bbox in dev/staging)
    # ============================================================================
    echo "[$STEP_NO/$TOTAL_STEPS] Importing country polygons..."
    STEP_START=$(date +%s)

    # Japan bbox in EPSG:3857 (Web Mercator): 13580000,2700000,17140000,5780000
    TEMP_SQL="${TMPDIR:-/tmp}/countries_import.sql"

    if [ "$MODE" = "dev" ] || [ "$MODE" = "staging" ]; then
        ogr2ogr -f "PGDUMP" "$TEMP_SQL" \
          "$BASE_PATH/Datasets/OSMData/land-polygons-split-3857/land_polygons.shp" \
          -nln countries -lco DROP_TABLE=IF_EXISTS -nlt MULTIPOLYGON \
          -spat 13580000 2700000 17140000 5780000
    else  # prod
        ogr2ogr -f "PGDUMP" "$TEMP_SQL" \
          "$BASE_PATH/Datasets/OSMData/land-polygons-split-3857/land_polygons.shp" \
          -nln countries -lco DROP_TABLE=IF_EXISTS -nlt MULTIPOLYGON
    fi

    if [ $? -ne 0 ]; then
        echo "ERROR: Country polygons SQL generation failed!"
        exit 1
    fi

    psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -f "$TEMP_SQL" > /dev/null

    if [ $? -ne 0 ]; then
        echo "ERROR: Country polygons import failed!"
        rm -f "$TEMP_SQL"
        exit 1
    fi

    rm -f "$TEMP_SQL"

    STEP_END=$(date +%s)
    COUNTRIES_IMPORT_TIME="$((STEP_END - STEP_START))s"
    echo "Country polygons import completed successfully in $COUNTRIES_IMPORT_TIME"
    echo
    STEP_NO=$((STEP_NO + 1))

    echo "[$STEP_NO/$TOTAL_STEPS] Running mapnik post-ingestion SQL..."
    STEP_START=$(date +%s)

    psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -f "$MAPNIK_SQL"

    if [ $? -ne 0 ]; then
        echo "ERROR: Mapnik post-ingestion SQL failed!"
        exit 1
    fi

    STEP_END=$(date +%s)
    MAPNIK_SQL_TIME="$((STEP_END - STEP_START))s"
    echo "Mapnik post-ingestion SQL completed successfully in $MAPNIK_SQL_TIME"
    echo
    STEP_NO=$((STEP_NO + 1))
fi

# ============================================================================
# Summary Statistics
# ============================================================================
SCRIPT_END=$(date +%s)
TOTAL_TIME=$((SCRIPT_END - SCRIPT_START))

echo "============================================================================"
echo "REBUILD COMPLETE - Summary Statistics"
echo "============================================================================"
echo "Mode:                         $MODE"
echo "Database setup:               $DB_SETUP_TIME"
echo "Buildings import:             $BUILDINGS_IMPORT_TIME"
echo "Buildings post-ingest SQL:    $BUILDINGS_SQL_TIME"
echo "Mapnik import:                $MAPNIK_IMPORT_TIME"
echo "Country polygons import:      $COUNTRIES_IMPORT_TIME"
echo "Mapnik post-ingest SQL:       $MAPNIK_SQL_TIME"
echo "----------------------------------------------------------------------------"
echo "Total time:                   ${TOTAL_TIME}s ($((TOTAL_TIME / 60))m $((TOTAL_TIME % 60))s)"
echo "============================================================================"
echo

if [ -t 0 ]; then
    read -r -p "Press Enter to exit..."
fi
