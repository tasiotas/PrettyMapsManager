from __future__ import annotations

import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path


DEFAULT_CONFIG_NAME = "prettymaps_manager.toml"
RENDER_SCRIPT_REL_PATH = "src/prettymaps_manager_app/tools/render_mapnik_layers.py"
BUILDINGS_SCRIPT_REL_PATH = "src/prettymaps_manager_app/tools/generate_buildings_glb.py"


@dataclass(slots=True)
class DatabaseDefaults:
    host: str
    port: int
    name: str
    user: str
    password: str


@dataclass(slots=True)
class AppConfig:
    path: Path
    master_projects_dir: Path
    elevation_vrt_path: Path
    render_script_path: Path
    buildings_script_path: Path
    db: DatabaseDefaults
    elevation_width: int
    map_width: int
    map_scale_factor: int
    map_max_workers: int
    buildings_workers: int


def _default_config_text() -> str:
    return """# PrettyMapsManager configuration
# Edit these paths for your machine. Keep both mac and windows paths filled.

[paths]
master_projects_dir_mac = "/Volumes/Vault/PrettyMaps/Projects"
master_projects_dir_windows = "F:/PrettyMaps/Projects"
elevation_vrt_mac = "/Volumes/Vault/PrettyMaps/Datasets/AW3D30/aw3d30.vrt"
elevation_vrt_windows = "F:/PrettyMaps/Datasets/AW3D30/aw3d30.vrt"

[database]
host = "postgis.staging.taslab.lan"
port = 5444
name = "osm_topo_app"
user = "prettymaps"
password = "pass"

[defaults]
elevation_width = 28800
map_width = 4096
map_scale_factor = 2
map_max_workers = 4
buildings_workers = 24
"""


def _ensure_config_file(config_path: Path) -> None:
    if config_path.exists():
        return
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(_default_config_text(), encoding="utf-8")


def _platform_value(raw: dict, key: str, default_mac: str, default_windows: str) -> str:
    mac_key = f"{key}_mac"
    windows_key = f"{key}_windows"
    mac_value = str(raw.get(mac_key, default_mac))
    windows_value = str(raw.get(windows_key, default_windows))

    if sys.platform.startswith("win"):
        return windows_value or mac_value
    return mac_value or windows_value


def _resolve_path(path_value: str, base_dir: Path) -> Path:
    value = str(path_value).strip()
    if not value:
        return base_dir
    if len(value) >= 3 and value[1] == ":" and value[2] in ("/", "\\"):
        return Path(value)
    path = Path(value).expanduser()
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


def load_config(project_root: Path, config_path: Path | None = None) -> AppConfig:
    cfg_path = (config_path or project_root / DEFAULT_CONFIG_NAME).resolve()
    _ensure_config_file(cfg_path)

    raw = tomllib.loads(cfg_path.read_text(encoding="utf-8"))
    paths = raw.get("paths", {})
    database = raw.get("database", {})
    defaults = raw.get("defaults", {})

    master_projects_value = _platform_value(
        paths,
        "master_projects_dir",
        "/Volumes/Vault/PrettyMaps/Projects",
        "F:/PrettyMaps/Projects",
    )
    elevation_vrt_value = _platform_value(
        paths,
        "elevation_vrt",
        "/Volumes/Vault/PrettyMaps/Datasets/AW3D30/aw3d30.vrt",
        "F:/PrettyMaps/Datasets/AW3D30/aw3d30.vrt",
    )

    db_defaults = DatabaseDefaults(
        host=str(database.get("host", "postgis.staging.taslab.lan")),
        port=int(database.get("port", 5444)),
        name=str(database.get("name", "osm_topo_app")),
        user=str(database.get("user", "prettymaps")),
        password=str(database.get("password", "pass")),
    )

    return AppConfig(
        path=cfg_path,
        master_projects_dir=_resolve_path(master_projects_value, project_root),
        elevation_vrt_path=_resolve_path(elevation_vrt_value, project_root),
        render_script_path=_resolve_path(RENDER_SCRIPT_REL_PATH, project_root),
        buildings_script_path=_resolve_path(BUILDINGS_SCRIPT_REL_PATH, project_root),
        db=db_defaults,
        elevation_width=int(defaults.get("elevation_width", 28800)),
        map_width=int(defaults.get("map_width", 4096)),
        map_scale_factor=int(defaults.get("map_scale_factor", 2)),
        map_max_workers=int(defaults.get("map_max_workers", 4)),
        buildings_workers=int(defaults.get("buildings_workers", 24)),
    )
