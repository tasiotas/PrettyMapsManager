#!/usr/bin/env python3
"""Generate GLB building tiles from PostGIS osm_buildings."""

from __future__ import annotations

import argparse
import base64
import json
import math
import multiprocessing
import shutil
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

MISSING_REQUIRED_DEPS: list[str] = []

try:
    import numpy as np
except ModuleNotFoundError:
    np = None  # type: ignore[assignment]
    MISSING_REQUIRED_DEPS.append("numpy")

try:
    import psycopg2
    import psycopg2.extras
except ModuleNotFoundError:
    psycopg2 = None  # type: ignore[assignment]
    MISSING_REQUIRED_DEPS.append("psycopg2-binary")

try:
    from pygltflib import ARRAY_BUFFER
    from pygltflib import ELEMENT_ARRAY_BUFFER
    from pygltflib import FLOAT
    from pygltflib import UNSIGNED_INT
    from pygltflib import Accessor
    from pygltflib import Buffer
    from pygltflib import BufferView
    from pygltflib import GLTF2
    from pygltflib import Material
    from pygltflib import Mesh
    from pygltflib import Node
    from pygltflib import PbrMetallicRoughness
    from pygltflib import Primitive
    from pygltflib import Scene
except ModuleNotFoundError:
    ARRAY_BUFFER = ELEMENT_ARRAY_BUFFER = FLOAT = UNSIGNED_INT = 0
    Accessor = Buffer = BufferView = GLTF2 = Material = Mesh = Node = object  # type: ignore[assignment]
    PbrMetallicRoughness = Primitive = Scene = object  # type: ignore[assignment]
    MISSING_REQUIRED_DEPS.append("pygltflib")

try:
    import mapbox_earcut as earcut

    HAS_EARCUT = True
except ImportError:
    HAS_EARCUT = False


@dataclass(slots=True, frozen=True)
class TileInfo:
    row: int
    col: int
    minx: float
    miny: float
    maxx: float
    maxy: float

    @property
    def bbox(self) -> tuple[float, float, float, float]:
        return (self.minx, self.miny, self.maxx, self.maxy)

    @property
    def filename(self) -> str:
        return f"tile_r{self.row}_c{self.col}.glb"


def parse_bbox(value: str) -> tuple[float, float, float, float]:
    parts = [item.strip() for item in value.split(",")]
    if len(parts) != 4:
        raise argparse.ArgumentTypeError(
            "Expected 4 comma-separated values for --bbox: minx,miny,maxx,maxy"
        )
    try:
        bbox = tuple(float(item) for item in parts)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Invalid numeric value in --bbox") from exc
    return bbox  # type: ignore[return-value]


def parse_tile(value: str) -> tuple[int, int]:
    parts = [item.strip() for item in value.split(",")]
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(
            "Expected 2 comma-separated values for --tile: row,col"
        )
    try:
        row = int(parts[0])
        col = int(parts[1])
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Invalid integer value in --tile") from exc
    return (row, col)


def subdivide_bbox_4x4(minx: float, miny: float, maxx: float, maxy: float) -> list[TileInfo]:
    width = (maxx - minx) / 4.0
    height = (maxy - miny) / 4.0
    tiles: list[TileInfo] = []
    for row in range(4):
        for col in range(4):
            tile_minx = minx + col * width
            tile_miny = miny + row * height
            tile_maxx = tile_minx + width
            tile_maxy = tile_miny + height
            tiles.append(
                TileInfo(
                    row=row,
                    col=col,
                    minx=tile_minx,
                    miny=tile_miny,
                    maxx=tile_maxx,
                    maxy=tile_maxy,
                )
            )
    return tiles


def simple_fan_triangulation(coords: list[list[float]]) -> list[int]:
    indices: list[int] = []
    for i in range(1, len(coords) - 1):
        indices.extend([0, i, i + 1])
    return indices


def triangulate_polygon(
    exterior: list[list[float]], holes: list[list[list[float]]] | None = None
) -> list[int]:
    if not HAS_EARCUT:
        return simple_fan_triangulation(exterior)

    all_vertices: list[list[float]] = [[coord[0], coord[1]] for coord in exterior]
    ring_end_indices = [len(all_vertices)]
    if holes:
        for hole in holes:
            for coord in hole:
                all_vertices.append([coord[0], coord[1]])
            ring_end_indices.append(len(all_vertices))

    vertices_np = np.array(all_vertices, dtype=np.float64)
    ring_end_indices_np = np.array(ring_end_indices, dtype=np.uint32)
    triangles = earcut.triangulate_float64(vertices_np, ring_end_indices_np)
    return triangles.tolist()


def clean_ring(ring: list[list[float]]) -> list[list[float]]:
    if len(ring) > 1 and ring[0] == ring[-1]:
        return ring[:-1]
    return ring


def create_building_geometry(
    polygon_rings: list[list[list[float]]],
    height: float,
    origin: tuple[float, float],
) -> tuple[np.ndarray, np.ndarray]:
    if not polygon_rings:
        raise ValueError("Polygon has no rings")

    exterior_ring = clean_ring(polygon_rings[0])
    holes = [clean_ring(ring) for ring in polygon_rings[1:]]

    all_coords = list(exterior_ring)
    for hole in holes:
        all_coords.extend(hole)

    n = len(all_coords)
    if n < 3:
        raise ValueError("Polygon has fewer than 3 unique vertices")

    origin_x, origin_y = origin
    vertices = np.zeros((n * 2, 3), dtype=np.float32)

    local_exterior: list[list[float]] = []
    local_holes: list[list[list[float]]] = []

    for i, coord in enumerate(exterior_ring):
        x = coord[0] - origin_x
        y = coord[1] - origin_y
        vertices[i] = [x, 0.0, -y]
        local_exterior.append([x, y])

    offset = len(exterior_ring)
    for hole in holes:
        local_hole: list[list[float]] = []
        for coord in hole:
            x = coord[0] - origin_x
            y = coord[1] - origin_y
            vertices[offset] = [x, 0.0, -y]
            local_hole.append([x, y])
            offset += 1
        local_holes.append(local_hole)

    vertices[n:] = vertices[:n].copy()
    vertices[n:, 1] = height

    indices: list[int] = []
    triangle_indices = triangulate_polygon(
        local_exterior, local_holes if local_holes else None
    )

    for i in range(0, len(triangle_indices), 3):
        indices.extend(
            [triangle_indices[i + 2], triangle_indices[i + 1], triangle_indices[i]]
        )

    for i in range(0, len(triangle_indices), 3):
        indices.extend(
            [
                n + triangle_indices[i],
                n + triangle_indices[i + 1],
                n + triangle_indices[i + 2],
            ]
        )

    for i in range(len(exterior_ring)):
        next_i = (i + 1) % len(exterior_ring)
        indices.extend([i, next_i, n + next_i])
        indices.extend([i, n + next_i, n + i])

    hole_offset = len(exterior_ring)
    for hole in holes:
        for i in range(len(hole)):
            next_i = (i + 1) % len(hole)
            v1 = hole_offset + i
            v2 = hole_offset + next_i
            indices.extend([v1, n + v1, n + v2])
            indices.extend([v1, n + v2, v2])
        hole_offset += len(hole)

    return vertices, np.array(indices, dtype=np.uint32)


def connect_postgis(host: str, port: int, database: str, user: str, password: str):
    return psycopg2.connect(
        host=host, port=port, database=database, user=user, password=password
    )


def query_osm_buildings(
    conn, bbox: tuple[float, float, float, float], srid: int = 4326
) -> list[dict[str, Any]]:
    minx, miny, maxx, maxy = bbox
    bbox_wkt = (
        f"SRID={srid};POLYGON(({minx} {miny}, {maxx} {miny}, "
        f"{maxx} {maxy}, {minx} {maxy}, {minx} {miny}))"
    )
    query = """
    SELECT
        ST_AsGeoJSON(way) AS geometry,
        levels
    FROM osm_buildings
    WHERE ST_Intersects(way, ST_Transform(ST_GeomFromText(%s), 3857))
    """

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        cursor.execute(query, (bbox_wkt,))
        rows = cursor.fetchall()

    features: list[dict[str, Any]] = []
    for row in rows:
        features.append(
            {
                "type": "Feature",
                "geometry": json.loads(row["geometry"]),
                "properties": {"levels": row.get("levels")},
            }
        )
    return features


def calculate_height_from_levels(
    levels: Any, default_height: float = 10.0, meters_per_level: float = 3.0
) -> float:
    if levels is None:
        return default_height
    try:
        return float(levels) * meters_per_level
    except (ValueError, TypeError):
        return default_height


def wgs84_to_mercator(lon: float, lat: float) -> tuple[float, float]:
    x = lon * 20037508.34 / 180.0
    y = math.log(math.tan((90 + lat) * math.pi / 360.0)) * 20037508.34 / math.pi
    return (x, y)


def calculate_tile_origin(bbox: tuple[float, float, float, float]) -> tuple[float, float]:
    minx, miny, maxx, maxy = bbox
    if abs(minx) < 180 and abs(maxx) < 180 and abs(miny) < 90 and abs(maxy) < 90:
        min_mercator = wgs84_to_mercator(minx, miny)
        max_mercator = wgs84_to_mercator(maxx, maxy)
        return (
            (min_mercator[0] + max_mercator[0]) / 2.0,
            (min_mercator[1] + max_mercator[1]) / 2.0,
        )
    return ((minx + maxx) / 2.0, (miny + maxy) / 2.0)


def buildings_to_glb(
    buildings: list[dict[str, Any]],
    output_path: Path,
    origin: tuple[float, float],
) -> bool:
    if not buildings:
        return False

    all_vertices: list[np.ndarray] = []
    all_indices: list[np.ndarray] = []
    vertex_offset = 0
    building_count = 0

    for feature in buildings:
        geometry = feature.get("geometry", {})
        geom_type = geometry.get("type")
        properties = feature.get("properties", {})
        height = calculate_height_from_levels(properties.get("levels"))

        polygons: list[list[list[list[float]]]]
        if geom_type == "Polygon":
            polygons = [geometry["coordinates"]]
        elif geom_type == "MultiPolygon":
            polygons = geometry["coordinates"]
        else:
            continue

        for polygon_rings in polygons:
            if not polygon_rings or len(polygon_rings[0]) < 4:
                continue
            try:
                vertices, indices = create_building_geometry(
                    polygon_rings=polygon_rings,
                    height=height,
                    origin=origin,
                )
            except Exception:
                continue

            all_vertices.append(vertices)
            all_indices.append(indices + vertex_offset)
            vertex_offset += len(vertices)
            building_count += 1

    if building_count == 0:
        return False

    vertices = np.vstack(all_vertices)
    indices = np.hstack(all_indices)

    gltf = GLTF2()

    vertices_bytes = vertices.tobytes()
    indices_bytes = indices.tobytes()
    buffer_data = vertices_bytes + indices_bytes

    buffer = Buffer()
    buffer.byteLength = len(buffer_data)
    buffer.uri = (
        "data:application/octet-stream;base64,"
        f"{base64.b64encode(buffer_data).decode('ascii')}"
    )
    gltf.buffers.append(buffer)

    vertex_view = BufferView()
    vertex_view.buffer = 0
    vertex_view.byteOffset = 0
    vertex_view.byteLength = len(vertices_bytes)
    vertex_view.target = ARRAY_BUFFER
    gltf.bufferViews.append(vertex_view)

    index_view = BufferView()
    index_view.buffer = 0
    index_view.byteOffset = len(vertices_bytes)
    index_view.byteLength = len(indices_bytes)
    index_view.target = ELEMENT_ARRAY_BUFFER
    gltf.bufferViews.append(index_view)

    position_accessor = Accessor()
    position_accessor.bufferView = 0
    position_accessor.byteOffset = 0
    position_accessor.componentType = FLOAT
    position_accessor.count = len(vertices)
    position_accessor.type = "VEC3"
    position_accessor.min = vertices.min(axis=0).tolist()
    position_accessor.max = vertices.max(axis=0).tolist()
    gltf.accessors.append(position_accessor)

    index_accessor = Accessor()
    index_accessor.bufferView = 1
    index_accessor.byteOffset = 0
    index_accessor.componentType = UNSIGNED_INT
    index_accessor.count = len(indices)
    index_accessor.type = "SCALAR"
    gltf.accessors.append(index_accessor)

    material = Material()
    material.pbrMetallicRoughness = PbrMetallicRoughness()
    material.pbrMetallicRoughness.baseColorFactor = [0.8, 0.8, 0.8, 1.0]
    material.pbrMetallicRoughness.metallicFactor = 0.0
    material.pbrMetallicRoughness.roughnessFactor = 0.9
    gltf.materials.append(material)

    primitive = Primitive()
    primitive.attributes.POSITION = 0
    primitive.indices = 1
    primitive.material = 0

    mesh = Mesh()
    mesh.primitives.append(primitive)
    gltf.meshes.append(mesh)

    node = Node()
    node.mesh = 0
    gltf.nodes.append(node)

    scene = Scene()
    scene.nodes.append(0)
    gltf.scenes.append(scene)
    gltf.scene = 0

    output_path.parent.mkdir(parents=True, exist_ok=True)
    gltf.save_binary(str(output_path))
    return True


def process_tile(
    args: tuple[
        TileInfo, dict[str, Any], tuple[float, float], Path, bool
    ]
) -> dict[str, Any]:
    tile, db_config, origin, output_dir, use_tile_origin = args
    started = time.time()
    result: dict[str, Any] = {
        "tile": f"r{tile.row}_c{tile.col}",
        "success": False,
        "building_count": 0,
        "error": None,
        "time": 0.0,
    }

    conn = None
    try:
        conn = connect_postgis(**db_config)
        buildings = query_osm_buildings(conn, tile.bbox)
        result["building_count"] = len(buildings)

        if not buildings:
            result["success"] = True
            result["skipped"] = True
            return result

        tile_origin = calculate_tile_origin(tile.bbox) if use_tile_origin else origin
        output_path = output_dir / tile.filename
        ok = buildings_to_glb(buildings, output_path, tile_origin)
        result["success"] = ok
        result["output_path"] = str(output_path) if ok else None
    except Exception as exc:
        result["error"] = str(exc)
    finally:
        if conn is not None:
            conn.close()
        result["time"] = time.time() - started

    return result


def generate_tiles_parallel(
    bbox: tuple[float, float, float, float],
    output_dir: Path,
    db_config: dict[str, Any],
    num_workers: int = 24,
    single_tile: tuple[int, int] | None = None,
) -> dict[str, Any]:
    started = time.time()

    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    global_origin = calculate_tile_origin(bbox)
    all_tiles = subdivide_bbox_4x4(*bbox)

    use_tile_origin = False
    if single_tile is not None:
        row, col = single_tile
        tiles = [tile for tile in all_tiles if tile.row == row and tile.col == col]
        if not tiles:
            raise ValueError(
                f"Tile ({row}, {col}) not found. Valid range is row 0-3 and col 0-3."
            )
        print(f"Processing single tile: row={row}, col={col}")
        use_tile_origin = True
    else:
        tiles = all_tiles
        print(f"Processing {len(tiles)} tiles in 4x4 grid with {num_workers} workers...")

    print(f"Global origin: {global_origin}")
    print(f"Output directory: {output_dir}")
    print()

    worker_args = [
        (tile, db_config, global_origin, output_dir, use_tile_origin) for tile in tiles
    ]

    results: list[dict[str, Any]] = []
    total_tiles = len(tiles)
    with multiprocessing.Pool(processes=num_workers) as pool:
        for i, result in enumerate(pool.imap_unordered(process_tile, worker_args), start=1):
            tile_name = result["tile"]
            tile_time = float(result.get("time", 0.0))
            if result["success"]:
                if result.get("skipped"):
                    print(
                        f"[{i}/{total_tiles}] Tile {tile_name}: skipped (no buildings) ({tile_time:.2f}s)"
                    )
                else:
                    count = int(result.get("building_count", 0))
                    print(
                        f"[{i}/{total_tiles}] Tile {tile_name}: ok {count} buildings ({tile_time:.2f}s)"
                    )
            else:
                error = str(result.get("error") or "Unknown error")
                print(
                    f"[{i}/{total_tiles}] Tile {tile_name}: failed: {error} ({tile_time:.2f}s)"
                )
            results.append(result)

    successful = sum(1 for item in results if item["success"] and not item.get("skipped"))
    skipped = sum(1 for item in results if item.get("skipped"))
    failed = sum(1 for item in results if not item["success"])
    total_buildings = sum(int(item.get("building_count", 0)) for item in results)
    total_time = time.time() - started
    avg_time = (
        sum(float(item.get("time", 0.0)) for item in results) / len(results)
        if results
        else 0.0
    )

    print()
    print("=" * 60)
    print(f"Completed: {successful} tiles generated")
    print(f"Skipped:   {skipped} tiles (no buildings)")
    print(f"Failed:    {failed} tiles")
    print(f"Buildings: {total_buildings}")
    print(f"Total:     {total_time:.2f}s")
    print(f"Avg/tile:  {avg_time:.2f}s")
    if total_time > 0:
        print(f"Bldg/sec:  {total_buildings / total_time:.1f}")
    print("=" * 60)

    return {
        "results": results,
        "successful": successful,
        "skipped": skipped,
        "failed": failed,
        "total_buildings": total_buildings,
    }


def run_osm_tiles(args: argparse.Namespace) -> int:
    if MISSING_REQUIRED_DEPS:
        joined = ", ".join(sorted(set(MISSING_REQUIRED_DEPS)))
        raise RuntimeError(
            "Missing dependencies for building generation: "
            f"{joined}. Install project dependencies first."
        )

    db_config = {
        "host": args.db_host,
        "port": args.db_port,
        "database": args.db_name,
        "user": args.db_user,
        "password": args.db_password,
    }

    print("Generating GLB tiles from osm_buildings...")
    print(f"Bounding box: {args.bbox}")
    if args.tile is not None:
        print(f"Single tile mode: row={args.tile[0]}, col={args.tile[1]}")
    print(f"Database: {args.db_host}:{args.db_port}/{args.db_name}")
    print(f"Workers: {args.workers}")
    print()

    generate_tiles_parallel(
        bbox=args.bbox,
        output_dir=Path(args.output),
        db_config=db_config,
        num_workers=args.workers,
        single_tile=args.tile,
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Building generation tools for PrettyMapsManager"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    osm_tiles = subparsers.add_parser(
        "osm-tiles",
        help="Generate 4x4 GLB tiles from PostGIS osm_buildings data",
    )
    osm_tiles.add_argument(
        "--bbox",
        type=parse_bbox,
        required=True,
        help="Bounding box in WGS84: minx,miny,maxx,maxy",
    )
    osm_tiles.add_argument(
        "--output",
        type=Path,
        default=Path("buildings_output"),
        help="Output directory for generated .glb tiles",
    )
    osm_tiles.add_argument(
        "--workers",
        type=int,
        default=24,
        help="Parallel worker count",
    )
    osm_tiles.add_argument(
        "--tile",
        type=parse_tile,
        default=None,
        help="Generate only one tile row,col for debugging",
    )
    osm_tiles.add_argument("--db-host", type=str, default="staging.taslab.lan")
    osm_tiles.add_argument("--db-port", type=int, default=5443)
    osm_tiles.add_argument("--db-name", type=str, default="osm_topo_app")
    osm_tiles.add_argument("--db-user", type=str, default="prettymaps")
    osm_tiles.add_argument("--db-password", type=str, default="pass")

    return parser


def main(argv: list[str] | None = None) -> int:
    multiprocessing.freeze_support()
    parser = build_parser()
    try:
        args = parser.parse_args(argv)

        if args.command == "osm-tiles":
            return run_osm_tiles(args)

        parser.error(f"Unsupported command: {args.command}")
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
