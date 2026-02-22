from __future__ import annotations

import sys
from pathlib import Path
from typing import Callable

from PySide6 import QtCore, QtWidgets

from ..models import CommandSpec, RunPlan, SelectionGeometry


class MapRenderPanel(QtWidgets.QWidget):
    settings_changed = QtCore.Signal()

    def __init__(
        self,
        project_root: Path,
        default_script_path: Path,
        default_width: int,
        default_scale_factor: int,
        default_max_workers: int,
        default_db_host: str,
        default_db_port: int,
        default_db_name: str,
        default_db_user: str,
        default_db_password: str,
        get_selection: Callable[[], SelectionGeometry | None],
        run_plan_callback: Callable[[RunPlan], None],
        parent: QtWidgets.QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._project_root = project_root
        self._script_path = default_script_path
        self._default_width = default_width
        self._default_scale_factor = default_scale_factor
        self._default_max_workers = default_max_workers
        self._db_host = default_db_host
        self._db_port = default_db_port
        self._db_name = default_db_name
        self._db_user = default_db_user
        self._db_password = default_db_password
        self._project_dir: Path | None = None
        self._get_selection = get_selection
        self._run_plan_callback = run_plan_callback

        self._build_ui()
        self._wire()
        self.refresh_preview()

    def _build_ui(self) -> None:
        root = QtWidgets.QVBoxLayout(self)
        form = QtWidgets.QFormLayout()

        self.width = QtWidgets.QSpinBox()
        self.width.setRange(256, 100_000)
        self.width.setValue(self._default_width)
        form.addRow("Width:", self.width)

        self.height = QtWidgets.QSpinBox()
        self.height.setRange(0, 100_000)
        self.height.setValue(0)
        self.height.setSpecialValueText("Auto")
        form.addRow("Height:", self.height)

        self.scale_factor = QtWidgets.QSpinBox()
        self.scale_factor.setRange(1, 16)
        self.scale_factor.setValue(self._default_scale_factor)
        form.addRow("Scale Factor:", self.scale_factor)

        self.max_workers = QtWidgets.QSpinBox()
        self.max_workers.setRange(1, 64)
        self.max_workers.setValue(self._default_max_workers)
        form.addRow("Max Workers:", self.max_workers)

        layers_widget = QtWidgets.QWidget()
        layers_layout = QtWidgets.QHBoxLayout(layers_widget)
        layers_layout.setContentsMargins(0, 0, 0, 0)
        self.layer_roads = QtWidgets.QCheckBox("roads")
        self.layer_railways = QtWidgets.QCheckBox("railways")
        self.layer_water = QtWidgets.QCheckBox("water")
        self.layer_borders = QtWidgets.QCheckBox("borders")
        for cb in (
            self.layer_roads,
            self.layer_railways,
            self.layer_water,
            self.layer_borders,
        ):
            cb.setChecked(True)
            layers_layout.addWidget(cb)
        form.addRow("Layers:", layers_widget)

        self.verbose = QtWidgets.QCheckBox("Verbose")
        form.addRow("", self.verbose)

        root.addLayout(form)

        self.preview = QtWidgets.QPlainTextEdit()
        self.preview.setReadOnly(True)
        self.preview.setMinimumHeight(120)
        root.addWidget(QtWidgets.QLabel("Command Preview:"))
        root.addWidget(self.preview, stretch=1)

        self.run_btn = QtWidgets.QPushButton("Run Step 2 (Mapnik Render)")
        root.addWidget(self.run_btn)

    def _wire(self) -> None:
        self.run_btn.clicked.connect(self._run_requested)

        watch = [
            self.width.valueChanged,
            self.height.valueChanged,
            self.scale_factor.valueChanged,
            self.max_workers.valueChanged,
            self.layer_roads.toggled,
            self.layer_railways.toggled,
            self.layer_water.toggled,
            self.layer_borders.toggled,
            self.verbose.toggled,
        ]
        for signal in watch:
            signal.connect(self._on_inputs_changed)

    def _selected_layers(self) -> list[str]:
        layers: list[str] = []
        if self.layer_roads.isChecked():
            layers.append("roads")
        if self.layer_railways.isChecked():
            layers.append("railways")
        if self.layer_water.isChecked():
            layers.append("water")
        if self.layer_borders.isChecked():
            layers.append("borders")
        return layers

    def update_selection(self, _: SelectionGeometry | None) -> None:
        self.refresh_preview()

    def set_project_context(self, project_dir: Path | None) -> None:
        self._project_dir = project_dir
        self.refresh_preview()

    @QtCore.Slot()
    def _on_inputs_changed(self) -> None:
        self.refresh_preview()
        self.settings_changed.emit()

    def export_settings(self) -> dict[str, object]:
        return {
            "width": self.width.value(),
            "height": self.height.value(),
            "scale_factor": self.scale_factor.value(),
            "max_workers": self.max_workers.value(),
            "layers": self._selected_layers(),
            "verbose": self.verbose.isChecked(),
        }

    def apply_settings(self, settings: dict[str, object]) -> None:
        blockers = [
            QtCore.QSignalBlocker(self.width),
            QtCore.QSignalBlocker(self.height),
            QtCore.QSignalBlocker(self.scale_factor),
            QtCore.QSignalBlocker(self.max_workers),
            QtCore.QSignalBlocker(self.layer_roads),
            QtCore.QSignalBlocker(self.layer_railways),
            QtCore.QSignalBlocker(self.layer_water),
            QtCore.QSignalBlocker(self.layer_borders),
            QtCore.QSignalBlocker(self.verbose),
        ]
        try:
            width = settings.get("width")
            if width is not None:
                self.width.setValue(int(width))

            height = settings.get("height")
            if height is not None:
                self.height.setValue(int(height))

            scale_factor = settings.get("scale_factor")
            if scale_factor is not None:
                self.scale_factor.setValue(int(scale_factor))

            max_workers = settings.get("max_workers")
            if max_workers is not None:
                self.max_workers.setValue(int(max_workers))

            layers = settings.get("layers")
            if isinstance(layers, list):
                selected = {str(layer) for layer in layers}
                self.layer_roads.setChecked("roads" in selected)
                self.layer_railways.setChecked("railways" in selected)
                self.layer_water.setChecked("water" in selected)
                self.layer_borders.setChecked("borders" in selected)

            verbose = settings.get("verbose")
            if verbose is not None:
                self.verbose.setChecked(bool(verbose))
        except Exception:
            pass
        finally:
            del blockers

        self.refresh_preview()

    def _build_plan(self) -> RunPlan:
        selection = self._get_selection()
        if not selection:
            raise ValueError("Draw a rectangle or polygon on the map first.")
        if self._project_dir is None:
            raise ValueError("Select a project in the left panel first.")

        script = self._script_path
        if not script.exists():
            raise ValueError(f"Renderer script not found: {script}")

        layers = self._selected_layers()
        if not layers:
            raise ValueError("Enable at least one render layer.")

        output = self._project_dir / "textures" / "vector_layers.png"
        output.parent.mkdir(parents=True, exist_ok=True)
        selection_geojson = output.parent / f"{output.stem}_selection.geojson"
        commands: list[CommandSpec] = []

        args = [
            str(script),
            "--geojson",
            str(selection_geojson),
            "--output",
            str(output),
            "--width",
            str(self.width.value()),
            "--scale-factor",
            str(self.scale_factor.value()),
            "--max-workers",
            str(self.max_workers.value()),
            "--db-host",
            self._db_host,
            "--db-port",
            str(self._db_port),
            "--db-name",
            self._db_name,
            "--db-user",
            self._db_user,
            "--db-password",
            self._db_password,
            "--render",
            *layers,
        ]

        if self.height.value() > 0:
            args.extend(["--height", str(self.height.value())])

        if self.verbose.isChecked():
            args.append("--verbose")

        commands.append(
            CommandSpec(
                program=sys.executable,
                args=args,
                cwd=self._project_root,
                label=self._script_path.name,
            )
        )

        return RunPlan(
            title="Step 2: Mapnik roads/rail/water/borders render",
            commands=commands,
            selection_geojson_path=selection_geojson,
            selection_geojson_data=selection.to_feature_collection(),
        )

    @QtCore.Slot()
    def _run_requested(self) -> None:
        try:
            plan = self._build_plan()
        except ValueError as exc:
            QtWidgets.QMessageBox.warning(self, "Step 2 validation", str(exc))
            return
        self._run_plan_callback(plan)

    @QtCore.Slot()
    def refresh_preview(self) -> None:
        try:
            plan = self._build_plan()
        except ValueError as exc:
            self.preview.setPlainText(str(exc))
            return

        text = [plan.title]
        for i, command in enumerate(plan.commands, start=1):
            text.append(f"\n[{i}] {command.display()}")
        self.preview.setPlainText("\n".join(text))
