# PrettyMapsManager

Desktop GUI for map-driven terrain and OSM processing workflows.

## Features

- Interactive OSM/OpenTopo map with pan/zoom.
- Left-side project manager panel that lists folders in your master projects directory.
- Create sequential project folders (e.g. `1_Japan`, `2_Poland`).
- Rectangle or polygon selection with live bbox/coordinates.
- Step 1: Run `gdalwarp` (and optional `oiiotool`) from selected geometry.
- Step 2: Run mapnik rendering (roads, railways, water, borders) through `render_mapnik_layers.py`.
- Step 3: Run PostGIS -> GLB buildings export through `generate_buildings_glb.py`.
- Script paths are fixed in code (not config-driven).

## Run

```bash
uv run prettymaps-gui
```

Optional:

```bash
uv run prettymaps-gui --project-root /absolute/path/to/PrettyMapsManager
```

Optional explicit config path:

```bash
uv run prettymaps-gui --config /absolute/path/to/prettymaps_manager.toml
```

## Notes

- External tools must be installed and available in `PATH` for runtime:
  - `gdalwarp`
  - `oiiotool` (if enabled)
- Step 2 additionally requires the `mapnik-render` CLI and Mapnik PostGIS input plugin.
- Step 2 uses `src/prettymaps_manager_app/tools/render_mapnik_layers.py` from this repo.
- Step 3 uses `src/prettymaps_manager_app/tools/generate_buildings_glb.py` from this repo.
- Config file: `/Users/tas/Documents/code/PrettyMapsManager/prettymaps_manager.toml`
  - Stores both macOS and Windows path values for master projects and elevation VRT.
  - App auto-selects the correct platform values at runtime.
  - Edit this file to hardcode your environment paths.
