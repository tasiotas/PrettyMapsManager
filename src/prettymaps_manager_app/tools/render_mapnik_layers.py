#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from xml.sax.saxutils import escape as xml_escape

from pyproj import Transformer

WEB_MERCATOR_SRS = (
    "+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 "
    "+x_0=0.0 +y_0=0.0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs +over"
)

ROAD_WIDTHS = {
    "motorway": 8.0,
    "trunk": 4.0,
    "primary": 3.0,
    "secondary": 1.0,
    "tertiary": 1.0,
}

LAYER_CHOICES = ("roads", "railways", "water", "borders")
DEFAULT_LAYERS = ("roads", "railways", "water", "borders")
DEFAULT_DB_PORT = 5444

TO_WEB_MERCATOR = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)


@dataclass(slots=True)
class RenderConfig:
    bbox: tuple[float, float, float, float]
    geometry: dict | None
    output: Path
    width: int
    height: int
    scale_factor: int
    max_workers: int
    render_layers: tuple[str, ...]
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    plugins_dir: Path | None
    verbose: bool

    @property
    def super_width(self) -> int:
        return self.width * self.scale_factor

    @property
    def super_height(self) -> int:
        return self.height * self.scale_factor


def _log(message: str, cfg: RenderConfig, *, force: bool = False) -> None:
    if cfg.verbose or force:
        print(message, file=sys.stderr)


def _run_command(cmd: list[str], cfg: RenderConfig) -> None:
    if cfg.verbose:
        print("$ " + " ".join(cmd), file=sys.stderr)
        result = subprocess.run(cmd, text=True)
    else:
        result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        detail = result.stderr.strip() if result.stderr else "unknown error"
        raise RuntimeError(
            f"Command failed ({result.returncode}): {' '.join(cmd)}\n{detail}"
        )


def _iter_lon_lat(node: object) -> Iterable[tuple[float, float]]:
    if isinstance(node, (list, tuple)):
        if (
            len(node) >= 2
            and isinstance(node[0], (int, float))
            and isinstance(node[1], (int, float))
        ):
            yield float(node[0]), float(node[1])
            return
        for child in node:
            yield from _iter_lon_lat(child)


def _parse_bbox(value: str) -> tuple[float, float, float, float]:
    parts = [chunk.strip() for chunk in value.split(",")]
    if len(parts) != 4:
        raise ValueError(
            "BBox must have 4 comma-separated values: min_lon,min_lat,max_lon,max_lat"
        )

    min_lon, min_lat, max_lon, max_lat = [float(part) for part in parts]
    if not (min_lon < max_lon and min_lat < max_lat):
        raise ValueError("Invalid bbox extents.")
    return min_lon, min_lat, max_lon, max_lat


def _extract_geometry(data: dict) -> dict:
    obj_type = data.get("type")
    if obj_type == "FeatureCollection":
        features = data.get("features", [])
        if not features:
            raise ValueError("GeoJSON FeatureCollection has no features.")
        geometry = features[0].get("geometry")
    elif obj_type == "Feature":
        geometry = data.get("geometry")
    else:
        geometry = data

    if not isinstance(geometry, dict):
        raise ValueError("GeoJSON geometry is missing or invalid.")

    geom_type = geometry.get("type")
    if geom_type not in {"Polygon", "MultiPolygon"}:
        raise ValueError(f"Unsupported GeoJSON geometry type: {geom_type}")
    return geometry


def _parse_geojson_region(path: Path) -> tuple[tuple[float, float, float, float], dict]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ValueError(f"Could not read GeoJSON: {path}\n{exc}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid GeoJSON: {path}\n{exc}") from exc

    geometry = _extract_geometry(data)
    coords = list(_iter_lon_lat(geometry.get("coordinates")))
    if not coords:
        raise ValueError("GeoJSON geometry has no coordinate pairs.")

    min_lon = min(lon for lon, _ in coords)
    min_lat = min(lat for _, lat in coords)
    max_lon = max(lon for lon, _ in coords)
    max_lat = max(lat for _, lat in coords)
    if not (min_lon < max_lon and min_lat < max_lat):
        raise ValueError("GeoJSON geometry has invalid extents.")

    return (min_lon, min_lat, max_lon, max_lat), geometry


def _calc_height(width: int, bbox: tuple[float, float, float, float]) -> int:
    min_lon, min_lat, max_lon, max_lat = bbox
    min_x, min_y = TO_WEB_MERCATOR.transform(min_lon, min_lat)
    max_x, max_y = TO_WEB_MERCATOR.transform(max_lon, max_lat)
    width_m = max_x - min_x
    height_m = max_y - min_y
    if width_m <= 0 or height_m <= 0:
        raise ValueError("Could not calculate valid output height from bbox.")
    return max(1, int(round(width * (height_m / width_m))))


def _make_spatial_filter(
    bbox: tuple[float, float, float, float],
    geometry: dict | None,
) -> str:
    if geometry is not None:
        geojson_text = json.dumps(geometry, separators=(",", ":"), ensure_ascii=True)
        geojson_text = geojson_text.replace("'", "''")
        return (
            "ST_Intersects("
            "way, "
            "ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON("
            f"'{geojson_text}'"
            "), 4326), 3857)"
            ")"
        )

    min_lon, min_lat, max_lon, max_lat = bbox
    return (
        "way && ST_Transform("
        f"ST_MakeEnvelope({min_lon}, {min_lat}, {max_lon}, {max_lat}, 4326), "
        "3857"
        ")"
    )


def _mapnik_plugins_candidates(explicit: Path | None) -> list[Path]:
    candidates: list[Path] = []

    if explicit is not None:
        candidates.append(explicit)

    env_keys = ("MAPNIK_INPUT_PLUGINS", "MAPNIK_PLUGINS_DIR")
    for key in env_keys:
        value = str(os.environ.get(key, "")).strip()
        if value:
            candidates.append(Path(value))

    candidates.extend(
        [
            Path("/opt/homebrew/lib/mapnik/input"),
            Path("/usr/local/lib/mapnik/input"),
            Path("/usr/lib/mapnik/input"),
            Path("C:/Program Files/mapnik/input"),
        ]
    )

    unique: list[Path] = []
    seen: set[str] = set()
    for path in candidates:
        key = str(path)
        if key not in seen:
            seen.add(key)
            unique.append(path)
    return unique


def _find_plugins_dir(explicit: Path | None) -> Path | None:
    for candidate in _mapnik_plugins_candidates(explicit):
        if not candidate.exists():
            continue
        # Homebrew mapnik commonly ships postgis+pgraster.input instead of postgis.input.
        if (candidate / "postgis.input").exists():
            return candidate
        if list(candidate.glob("postgis*.input")):
            return candidate
    return None


def _require_mapnik_render() -> str:
    executable = shutil.which("mapnik-render")
    if executable:
        return executable
    raise RuntimeError(
        "mapnik-render CLI not found in PATH. Install Mapnik CLI (Homebrew on macOS) "
        "and ensure `mapnik-render` is available."
    )


def _sql_for_layer(layer_name: str, spatial_filter: str) -> str:
    if layer_name == "roads":
        road_types = "', '".join(ROAD_WIDTHS.keys())
        return (
            "(SELECT way, highway FROM public.osm_roads "
            f"WHERE {spatial_filter} AND highway IN ('{road_types}')) AS roads"
        )
    if layer_name == "railways":
        return (
            f"(SELECT way FROM public.osm_railways WHERE {spatial_filter}) AS railways"
        )
    if layer_name == "water":
        return f"(SELECT way FROM public.osm_water WHERE {spatial_filter}) AS water"
    if layer_name == "borders":
        return f"(SELECT way FROM public.borders WHERE {spatial_filter}) AS borders"
    raise ValueError(f"Unsupported layer: {layer_name}")


def _style_xml_for_layer(layer_name: str) -> str:
    if layer_name == "roads":
        rules: list[str] = []
        for highway_type, width in ROAD_WIDTHS.items():
            rules.append(
                """
    <Rule>
      <Filter>[highway] = '{highway}'</Filter>
      <LineSymbolizer stroke="#ffffff" stroke-width="{width}" stroke-linecap="round" stroke-linejoin="round" />
    </Rule>""".strip().format(highway=highway_type, width=width)
            )
        return "\n".join(rules)

    if layer_name == "railways":
        return (
            "<Rule>"
            '<LineSymbolizer stroke="#444444" stroke-width="8.0" stroke-linecap="round" />'
            "</Rule>"
        )

    if layer_name == "water":
        return (
            "<Rule>"
            '<LineSymbolizer stroke="#0077ff" stroke-width="1.0" stroke-linecap="round" />'
            "</Rule>"
        )

    if layer_name == "borders":
        return (
            "<Rule>"
            '<PolygonSymbolizer fill="#888888" fill-opacity="0.3" />'
            '<LineSymbolizer stroke="#000000" stroke-width="0.5" />'
            "</Rule>"
        )

    raise ValueError(f"Unsupported layer: {layer_name}")


def _datasource_xml(cfg: RenderConfig, layer_name: str, spatial_filter: str) -> str:
    sql = _sql_for_layer(layer_name, spatial_filter)
    params = {
        "type": "postgis",
        "host": cfg.db_host,
        "port": str(cfg.db_port),
        "dbname": cfg.db_name,
        "user": cfg.db_user,
        "srid": "3857",
        "table": sql,
        "geometry_field": "way",
    }
    if cfg.db_password:
        params["password"] = cfg.db_password

    lines = []
    for key, value in params.items():
        lines.append(
            f'      <Parameter name="{xml_escape(key)}">{xml_escape(value)}</Parameter>'
        )
    return "\n".join(lines)


def _mapnik_xml(cfg: RenderConfig, layer_name: str, spatial_filter: str) -> str:
    style_name = f"{layer_name}_style"
    style_xml = _style_xml_for_layer(layer_name)
    datasource_xml = _datasource_xml(cfg, layer_name, spatial_filter)

    return f"""<?xml version=\"1.0\" encoding=\"utf-8\"?>
<Map srs=\"{xml_escape(WEB_MERCATOR_SRS)}\" background-color=\"black\">
  <Style name=\"{xml_escape(style_name)}\">
{style_xml}
  </Style>
  <Layer name=\"{xml_escape(layer_name)}\" srs=\"{xml_escape(WEB_MERCATOR_SRS)}\">
    <StyleName>{xml_escape(style_name)}</StyleName>
    <Datasource>
{datasource_xml}
    </Datasource>
  </Layer>
</Map>
"""


def _bbox_mercator(
    bbox: tuple[float, float, float, float],
) -> tuple[float, float, float, float]:
    min_lon, min_lat, max_lon, max_lat = bbox
    min_x, min_y = TO_WEB_MERCATOR.transform(min_lon, min_lat)
    max_x, max_y = TO_WEB_MERCATOR.transform(max_lon, max_lat)
    return min_x, min_y, max_x, max_y


def _write_output_image(
    cfg: RenderConfig,
    super_path: Path,
    final_path: Path,
) -> None:
    if cfg.scale_factor <= 1:
        if final_path.suffix.lower() == ".png":
            shutil.copyfile(super_path, final_path)
            return
        _run_command(["oiiotool", str(super_path), "-o", str(final_path)], cfg)
        return

    cmd = [
        "oiiotool",
        str(super_path),
        "--resize",
        f"{cfg.width}x{cfg.height}",
    ]
    if final_path.suffix.lower() == ".exr":
        cmd.extend(["-d", "half", "-compression", "zip", "-otex", str(final_path)])
    else:
        cmd.extend(["-o", str(final_path)])
    _run_command(cmd, cfg)


def _render_single_layer(
    cfg: RenderConfig,
    layer_name: str,
    spatial_filter: str,
    *,
    mapnik_render_bin: str,
    plugins_dir: Path | None,
) -> Path:
    temp_dir = cfg.output.parent / "temp"
    temp_dir.mkdir(parents=True, exist_ok=True)

    output_ext = cfg.output.suffix or ".png"
    final_output = cfg.output.parent / f"{layer_name}{output_ext}"
    temp_super = temp_dir / f"{cfg.output.stem}_{layer_name}_mapnik_super.png"
    temp_xml = temp_dir / f"{cfg.output.stem}_{layer_name}.xml"

    _log(
        f"[render] {layer_name}: {cfg.super_width}x{cfg.super_height} -> {cfg.width}x{cfg.height}",
        cfg,
    )

    xml_text = _mapnik_xml(cfg, layer_name, spatial_filter)
    temp_xml.write_text(xml_text, encoding="utf-8")

    min_x, min_y, max_x, max_y = _bbox_mercator(cfg.bbox)
    bbox_arg = f"{min_x},{min_y},{max_x},{max_y}"

    render_cmd = [
        mapnik_render_bin,
        "--xml",
        str(temp_xml),
        "--img",
        str(temp_super),
        "--map-width",
        str(cfg.super_width),
        "--map-height",
        str(cfg.super_height),
        "--bbox",
        bbox_arg,
    ]
    if plugins_dir is not None:
        render_cmd.extend(["--plugins-dir", str(plugins_dir)])

    try:
        _run_command(render_cmd, cfg)
        _write_output_image(cfg, temp_super, final_output)
    finally:
        for tmp in (temp_super, temp_xml):
            try:
                if tmp.exists():
                    tmp.unlink()
            except OSError:
                pass

    return final_output


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render roads/railways/water/borders with mapnik-render from PostGIS."
    )
    region = parser.add_mutually_exclusive_group(required=True)
    region.add_argument(
        "--bbox",
        type=str,
        help="min_lon,min_lat,max_lon,max_lat",
    )
    region.add_argument(
        "--geojson",
        type=Path,
        help="GeoJSON polygon/multipolygon file used for bbox and precise filtering",
    )
    parser.add_argument("--output", type=Path, required=True, help="Base output path")
    parser.add_argument("--width", type=int, default=4096)
    parser.add_argument(
        "--height",
        type=int,
        default=0,
        help="Target height. Set 0 to auto-calculate from bbox aspect ratio.",
    )
    parser.add_argument("--scale-factor", type=int, default=2)
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument(
        "--render",
        nargs="*",
        choices=LAYER_CHOICES,
        default=list(DEFAULT_LAYERS),
        help="Layers to render",
    )
    parser.add_argument(
        "--db-host",
        type=str,
        default="postgis.staging.taslab.lan",
    )
    parser.add_argument("--db-port", type=int, default=DEFAULT_DB_PORT)
    parser.add_argument("--db-name", type=str, default="osm_topo_app")
    parser.add_argument("--db-user", type=str, default="prettymaps")
    parser.add_argument("--db-password", type=str, default="pass")
    parser.add_argument(
        "--plugins-dir",
        type=Path,
        default=None,
        help="Mapnik input plugins directory (contains postgis.input)",
    )
    parser.add_argument("--verbose", action="store_true")
    return parser


def _build_config(args: argparse.Namespace) -> RenderConfig:
    if args.geojson:
        bbox, geometry = _parse_geojson_region(args.geojson)
    else:
        bbox = _parse_bbox(args.bbox)
        geometry = None

    width = int(args.width)
    if width <= 0:
        raise ValueError("Width must be greater than 0.")

    height = int(args.height)
    if height <= 0:
        height = _calc_height(width, bbox)

    scale_factor = int(args.scale_factor)
    if scale_factor <= 0:
        raise ValueError("Scale factor must be greater than 0.")

    max_workers = int(args.max_workers)
    if max_workers <= 0:
        raise ValueError("Max workers must be greater than 0.")

    layers = tuple(args.render)
    if not layers:
        raise ValueError("At least one layer must be selected.")

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    return RenderConfig(
        bbox=bbox,
        geometry=geometry,
        output=output,
        width=width,
        height=height,
        scale_factor=scale_factor,
        max_workers=max_workers,
        render_layers=layers,
        db_host=str(args.db_host),
        db_port=int(args.db_port),
        db_name=str(args.db_name),
        db_user=str(args.db_user),
        db_password=str(args.db_password),
        plugins_dir=args.plugins_dir,
        verbose=bool(args.verbose),
    )


def run(cfg: RenderConfig) -> list[Path]:
    mapnik_render_bin = _require_mapnik_render()
    plugins_dir = _find_plugins_dir(cfg.plugins_dir)
    if plugins_dir is not None:
        _log(f"[mapnik-render] plugins: {plugins_dir}", cfg, force=True)
    else:
        _log(
            "[mapnik-render] plugins dir not detected; relying on mapnik-render defaults",
            cfg,
            force=True,
        )

    spatial_filter = _make_spatial_filter(cfg.bbox, cfg.geometry)

    _log(f"BBox: {cfg.bbox}", cfg, force=True)
    _log(f"Output base: {cfg.output}", cfg, force=True)
    _log(f"Layers: {', '.join(cfg.render_layers)}", cfg, force=True)
    _log(f"Size: {cfg.width}x{cfg.height}", cfg, force=True)
    _log(f"Scale factor: {cfg.scale_factor}", cfg, force=True)

    results: list[Path] = []
    failures: list[str] = []

    worker_count = min(cfg.max_workers, len(cfg.render_layers))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {
            executor.submit(
                _render_single_layer,
                cfg,
                layer,
                spatial_filter,
                mapnik_render_bin=mapnik_render_bin,
                plugins_dir=plugins_dir,
            ): layer
            for layer in cfg.render_layers
        }
        for future in as_completed(future_map):
            layer = future_map[future]
            try:
                output_path = future.result()
                results.append(output_path)
                _log(f"[done] {layer}: {output_path}", cfg, force=True)
            except Exception as exc:  # noqa: BLE001
                failures.append(f"{layer}: {exc}")

    temp_dir = cfg.output.parent / "temp"
    if temp_dir.exists():
        try:
            temp_dir.rmdir()
        except OSError:
            pass

    if failures:
        detail = "\n".join(failures)
        raise RuntimeError(f"One or more layers failed:\n{detail}")
    return results


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        cfg = _build_config(args)
        outputs = run(cfg)
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    for path in sorted(outputs):
        print(path)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
