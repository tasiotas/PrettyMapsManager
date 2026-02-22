from __future__ import annotations

import json
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class SelectionGeometry:
    geometry: dict[str, Any]
    bbox: tuple[float, float, float, float]
    selection_type: str = "polygon"

    @property
    def bbox_csv(self) -> str:
        min_lon, min_lat, max_lon, max_lat = self.bbox
        return f"{min_lon},{min_lat},{max_lon},{max_lat}"

    def to_feature_collection(self) -> dict[str, Any]:
        return {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"selection_type": self.selection_type},
                    "geometry": self.geometry,
                }
            ],
        }


@dataclass(slots=True)
class CommandSpec:
    program: str
    args: list[str]
    cwd: Path | None = None
    label: str | None = None

    def to_argv(self) -> list[str]:
        return [self.program, *self.args]

    def display(self) -> str:
        return " ".join(shlex.quote(part) for part in self.to_argv())


@dataclass(slots=True)
class RunPlan:
    title: str
    commands: list[CommandSpec]
    selection_geojson_path: Path | None = None
    selection_geojson_data: dict[str, Any] | None = None


def write_geojson(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2)

