from __future__ import annotations

import sys
from pathlib import Path
from typing import Callable

from PySide6 import QtCore, QtWidgets

from ..models import CommandSpec, RunPlan, SelectionGeometry


class BuildingsPanel(QtWidgets.QWidget):
    settings_changed = QtCore.Signal()

    def __init__(
        self,
        project_root: Path,
        default_script_path: Path,
        default_workers: int,
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
        self._db_host = default_db_host
        self._db_port = default_db_port
        self._db_name = default_db_name
        self._db_user = default_db_user
        self._db_password = default_db_password
        self._project_dir: Path | None = None
        self._get_selection = get_selection
        self._run_plan_callback = run_plan_callback

        self._build_ui()
        self.workers.setValue(default_workers)
        self._wire()
        self.refresh_preview()

    def _build_ui(self) -> None:
        root = QtWidgets.QVBoxLayout(self)
        form = QtWidgets.QFormLayout()

        self.workers = QtWidgets.QSpinBox()
        self.workers.setRange(1, 128)
        self.workers.setValue(24)
        form.addRow("Workers:", self.workers)

        self.single_tile = QtWidgets.QCheckBox("Single tile debug mode")
        form.addRow("", self.single_tile)

        tile_widget = QtWidgets.QWidget()
        tile_row = QtWidgets.QHBoxLayout(tile_widget)
        tile_row.setContentsMargins(0, 0, 0, 0)
        self.tile_row = QtWidgets.QSpinBox()
        self.tile_row.setRange(0, 3)
        self.tile_col = QtWidgets.QSpinBox()
        self.tile_col.setRange(0, 3)
        tile_row.addWidget(QtWidgets.QLabel("row"))
        tile_row.addWidget(self.tile_row)
        tile_row.addWidget(QtWidgets.QLabel("col"))
        tile_row.addWidget(self.tile_col)
        form.addRow("Tile:", tile_widget)

        root.addLayout(form)

        self.preview = QtWidgets.QPlainTextEdit()
        self.preview.setReadOnly(True)
        self.preview.setMinimumHeight(120)
        root.addWidget(QtWidgets.QLabel("Command Preview:"))
        root.addWidget(self.preview, stretch=1)

        self.warning_label = QtWidgets.QLabel(
            "Note: the importer clears the output directory before writing tiles."
        )
        self.warning_label.setWordWrap(True)
        root.addWidget(self.warning_label)

        self.run_btn = QtWidgets.QPushButton("Run Step 3 (Buildings to GLB)")
        root.addWidget(self.run_btn)

        self._set_tile_controls_enabled(False)

    def _wire(self) -> None:
        self.single_tile.toggled.connect(self._set_tile_controls_enabled)
        self.run_btn.clicked.connect(self._run_requested)

        watch = [
            self.workers.valueChanged,
            self.single_tile.toggled,
            self.tile_row.valueChanged,
            self.tile_col.valueChanged,
        ]
        for signal in watch:
            signal.connect(self._on_inputs_changed)

    @QtCore.Slot(bool)
    def _set_tile_controls_enabled(self, enabled: bool) -> None:
        self.tile_row.setEnabled(enabled)
        self.tile_col.setEnabled(enabled)

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
            "workers": self.workers.value(),
            "single_tile": self.single_tile.isChecked(),
            "tile_row": self.tile_row.value(),
            "tile_col": self.tile_col.value(),
        }

    def apply_settings(self, settings: dict[str, object]) -> None:
        blockers = [
            QtCore.QSignalBlocker(self.workers),
            QtCore.QSignalBlocker(self.single_tile),
            QtCore.QSignalBlocker(self.tile_row),
            QtCore.QSignalBlocker(self.tile_col),
        ]
        try:
            workers = settings.get("workers")
            if workers is not None:
                self.workers.setValue(int(workers))

            single_tile = settings.get("single_tile")
            if single_tile is not None:
                self.single_tile.setChecked(bool(single_tile))

            tile_row = settings.get("tile_row")
            if tile_row is not None:
                self.tile_row.setValue(int(tile_row))

            tile_col = settings.get("tile_col")
            if tile_col is not None:
                self.tile_col.setValue(int(tile_col))
        except Exception:
            pass
        finally:
            del blockers

        self._set_tile_controls_enabled(self.single_tile.isChecked())
        self.refresh_preview()

    def _build_plan(self) -> RunPlan:
        selection = self._get_selection()
        if not selection:
            raise ValueError("Draw a rectangle or polygon on the map first.")
        if self._project_dir is None:
            raise ValueError("Select a project in the left panel first.")

        script = self._script_path
        if not script.exists():
            raise ValueError(f"Importer script not found: {script}")

        output_dir = self._project_dir / "buildings_output"
        output_dir.mkdir(parents=True, exist_ok=True)

        args = [
            str(script),
            "osm-tiles",
            "--bbox",
            selection.bbox_csv,
            "--output",
            str(output_dir),
            "--workers",
            str(self.workers.value()),
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
        ]

        if self.single_tile.isChecked():
            args.extend(["--tile", f"{self.tile_row.value()},{self.tile_col.value()}"])

        return RunPlan(
            title="Step 3: PostGIS buildings -> GLB",
            commands=[
                CommandSpec(
                    program=sys.executable,
                    args=args,
                    cwd=self._project_root,
                    label=f"{self._script_path.name} osm-tiles",
                )
            ],
        )

    @QtCore.Slot()
    def _run_requested(self) -> None:
        try:
            plan = self._build_plan()
        except ValueError as exc:
            QtWidgets.QMessageBox.warning(self, "Step 3 validation", str(exc))
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
