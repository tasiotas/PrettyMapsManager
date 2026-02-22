from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from PySide6 import QtCore, QtGui, QtWidgets, QtWebChannel, QtWebEngineCore
from PySide6.QtWebEngineWidgets import QWebEngineView

from ..command_runner import CommandQueueRunner
from ..config import AppConfig
from ..map_bridge import MapBridge
from ..models import RunPlan, SelectionGeometry, write_geojson
from ..workflows.buildings import BuildingsPanel
from ..workflows.elevation import ElevationPanel
from ..workflows.map_render import MapRenderPanel
from .project_panel import ProjectManagerPanel


class CopyValueLabel(QtWidgets.QLabel):
    copy_requested = QtCore.Signal(str)

    def __init__(self, prefix: str, text: str, parent: QtWidgets.QWidget | None = None):
        super().__init__(text, parent)
        self._prefix = prefix
        self.setCursor(QtCore.Qt.CursorShape.PointingHandCursor)

    def _value_text(self) -> str:
        raw = self.text().strip()
        prefix = f"{self._prefix}:"
        if raw.startswith(prefix):
            raw = raw[len(prefix) :].strip()
        return raw

    def mousePressEvent(self, event: QtGui.QMouseEvent) -> None:
        if event.button() == QtCore.Qt.MouseButton.LeftButton:
            text = self._value_text()
            if text and text != "-":
                self.copy_requested.emit(text)
        super().mousePressEvent(event)


class MainWindow(QtWidgets.QMainWindow):
    def __init__(
        self,
        project_root: Path,
        app_config: AppConfig,
        parent: QtWidgets.QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._project_root = project_root
        self._app_config = app_config
        self._selection: SelectionGeometry | None = None
        self._current_project_dir: Path | None = None
        self._project_state_filename = ".prettymaps_project.json"
        self._map_ready = False
        self._pending_selection_restore: dict[str, Any] | None = None
        self._suspend_project_state_io = False

        self._runner = CommandQueueRunner(self)
        self._bridge = MapBridge(self)

        self._build_ui()
        self._wire()
        self._load_map()
        initial_project = self.project_panel.selected_project_path()
        self._on_project_selected(str(initial_project) if initial_project else "")

    def _build_ui(self) -> None:
        self.setWindowTitle("PrettyMaps Manager")
        self.resize(1680, 1020)

        central = QtWidgets.QWidget()
        self.setCentralWidget(central)
        layout = QtWidgets.QVBoxLayout(central)
        layout.setContentsMargins(0, 0, 0, 0)

        vertical_splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Vertical)
        layout.addWidget(vertical_splitter, stretch=1)

        top_splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Horizontal)
        vertical_splitter.addWidget(top_splitter)

        self.project_panel = ProjectManagerPanel(
            master_projects_dir=self._app_config.master_projects_dir
        )
        top_splitter.addWidget(self.project_panel)

        map_panel = QtWidgets.QWidget()
        map_layout = QtWidgets.QVBoxLayout(map_panel)
        map_layout.setContentsMargins(0, 0, 0, 0)

        info_bar = QtWidgets.QHBoxLayout()
        self.project_label = QtWidgets.QLabel("Project: none")
        self.selection_label = QtWidgets.QLabel("Selection: none")
        self.bbox_label = CopyValueLabel("BBox", "BBox: -")
        self.click_label = CopyValueLabel("Click", "Click: -")
        info_bar.addWidget(self.project_label)
        info_bar.addWidget(self.selection_label)
        info_bar.addStretch(1)
        info_bar.addWidget(self.bbox_label)
        info_bar.addWidget(self.click_label)
        map_layout.addLayout(info_bar)

        actions = QtWidgets.QHBoxLayout()
        self.clear_selection_btn = QtWidgets.QPushButton("Clear Selection")
        self.export_selection_btn = QtWidgets.QPushButton("Export Selection GeoJSON")
        actions.addWidget(self.clear_selection_btn)
        actions.addWidget(self.export_selection_btn)
        actions.addStretch(1)
        map_layout.addLayout(actions)

        self.map_view = QWebEngineView()
        settings = self.map_view.settings()
        settings.setAttribute(
            QtWebEngineCore.QWebEngineSettings.WebAttribute.LocalContentCanAccessRemoteUrls,
            True,
        )
        settings.setAttribute(
            QtWebEngineCore.QWebEngineSettings.WebAttribute.LocalContentCanAccessFileUrls,
            True,
        )
        map_layout.addWidget(self.map_view, stretch=1)

        top_splitter.addWidget(map_panel)

        controls_panel = QtWidgets.QWidget()
        controls_layout = QtWidgets.QVBoxLayout(controls_panel)
        controls_layout.setContentsMargins(0, 0, 0, 0)

        self.tabs = QtWidgets.QTabWidget()
        self.elevation_panel = ElevationPanel(
            project_root=self._project_root,
            default_dataset_path=self._app_config.elevation_vrt_path,
            default_width=self._app_config.elevation_width,
            get_selection=self.get_selection,
            run_plan_callback=self._start_plan,
        )
        self.map_render_panel = MapRenderPanel(
            project_root=self._project_root,
            default_script_path=self._app_config.render_script_path,
            default_width=self._app_config.map_width,
            default_scale_factor=self._app_config.map_scale_factor,
            default_max_workers=self._app_config.map_max_workers,
            default_db_host=self._app_config.db.host,
            default_db_port=self._app_config.db.port,
            default_db_name=self._app_config.db.name,
            default_db_user=self._app_config.db.user,
            default_db_password=self._app_config.db.password,
            get_selection=self.get_selection,
            run_plan_callback=self._start_plan,
        )
        self.buildings_panel = BuildingsPanel(
            project_root=self._project_root,
            default_script_path=self._app_config.buildings_script_path,
            default_workers=self._app_config.buildings_workers,
            default_db_host=self._app_config.db.host,
            default_db_port=self._app_config.db.port,
            default_db_name=self._app_config.db.name,
            default_db_user=self._app_config.db.user,
            default_db_password=self._app_config.db.password,
            get_selection=self.get_selection,
            run_plan_callback=self._start_plan,
        )

        self.tabs.addTab(self.elevation_panel, "Step 1 Elevation")
        self.tabs.addTab(self.map_render_panel, "Step 2 Mapnik Render")
        self.tabs.addTab(self.buildings_panel, "Step 3 Buildings")
        controls_layout.addWidget(self.tabs, stretch=1)

        self.running_state = QtWidgets.QLabel("Runner: idle")
        controls_layout.addWidget(self.running_state)

        buttons = QtWidgets.QHBoxLayout()
        self.stop_btn = QtWidgets.QPushButton("Stop")
        self.stop_btn.setEnabled(False)
        buttons.addWidget(self.stop_btn)
        buttons.addStretch(1)
        controls_layout.addLayout(buttons)

        top_splitter.addWidget(controls_panel)
        top_splitter.setStretchFactor(0, 3)
        top_splitter.setStretchFactor(1, 8)
        top_splitter.setStretchFactor(2, 5)
        top_splitter.setSizes([360, 900, 620])

        log_panel = QtWidgets.QWidget()
        log_layout = QtWidgets.QVBoxLayout(log_panel)
        log_layout.setContentsMargins(0, 0, 0, 0)
        log_actions = QtWidgets.QHBoxLayout()
        log_actions.addWidget(QtWidgets.QLabel("Logs:"))
        log_actions.addStretch(1)
        self.copy_log_btn = QtWidgets.QPushButton("Copy Log")
        log_actions.addWidget(self.copy_log_btn)
        log_layout.addLayout(log_actions)
        self.log_view = QtWidgets.QPlainTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setMinimumHeight(180)
        log_layout.addWidget(self.log_view, stretch=1)
        vertical_splitter.addWidget(log_panel)
        vertical_splitter.setStretchFactor(0, 5)
        vertical_splitter.setStretchFactor(1, 2)
        vertical_splitter.setSizes([760, 260])

    def _wire(self) -> None:
        self._runner.output.connect(self._append_log)
        self._runner.command_started.connect(self._on_command_started)
        self._runner.finished.connect(self._on_runner_finished)
        self._runner.running_changed.connect(self._on_running_changed)

        self._bridge.selection_payload.connect(self._on_selection_payload)
        self._bridge.map_clicked.connect(self._on_map_clicked)

        self.stop_btn.clicked.connect(self._runner.stop)
        self.clear_selection_btn.clicked.connect(self._clear_selection)
        self.export_selection_btn.clicked.connect(self._export_selection_geojson)
        self.copy_log_btn.clicked.connect(self._copy_log_to_clipboard)
        self.project_panel.project_selected.connect(self._on_project_selected)
        self.bbox_label.copy_requested.connect(self._copy_text_to_clipboard)
        self.click_label.copy_requested.connect(self._copy_text_to_clipboard)
        self.elevation_panel.settings_changed.connect(self._on_step_settings_changed)
        self.map_render_panel.settings_changed.connect(self._on_step_settings_changed)
        self.buildings_panel.settings_changed.connect(self._on_step_settings_changed)
        self.map_view.loadFinished.connect(self._on_map_load_finished)

    def _load_map(self) -> None:
        channel = QtWebChannel.QWebChannel(self.map_view.page())
        channel.registerObject("pyBridge", self._bridge)
        self.map_view.page().setWebChannel(channel)

        html_path = (
            Path(__file__).resolve().parent.parent / "web" / "map.html"
        ).resolve()
        self.map_view.load(QtCore.QUrl.fromLocalFile(str(html_path)))

    def get_selection(self) -> SelectionGeometry | None:
        return self._selection

    def _project_state_path(self, project_dir: Path) -> Path:
        return project_dir / self._project_state_filename

    def _selection_to_dict(self) -> dict[str, Any] | None:
        if self._selection is None:
            return None
        return {
            "selection_type": self._selection.selection_type,
            "bbox": list(self._selection.bbox),
            "geometry": self._selection.geometry,
        }

    def _selection_from_dict(self, data: dict[str, Any]) -> SelectionGeometry | None:
        geometry = data.get("geometry")
        bbox = data.get("bbox")
        if geometry is None or not isinstance(bbox, list) or len(bbox) != 4:
            return None
        try:
            return SelectionGeometry(
                geometry=geometry,
                bbox=(
                    float(bbox[0]),
                    float(bbox[1]),
                    float(bbox[2]),
                    float(bbox[3]),
                ),
                selection_type=str(data.get("selection_type", "polygon")),
            )
        except Exception:
            return None

    def _collect_project_state(self) -> dict[str, Any]:
        return {
            "version": 1,
            "selection": self._selection_to_dict(),
            "steps": {
                "elevation": self.elevation_panel.export_settings(),
                "map_render": self.map_render_panel.export_settings(),
                "buildings": self.buildings_panel.export_settings(),
            },
        }

    def _save_project_state(self, project_dir: Path) -> None:
        if self._suspend_project_state_io:
            return
        try:
            payload = self._collect_project_state()
            path = self._project_state_path(project_dir)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        except Exception as exc:  # noqa: BLE001
            self._append_log(f"[project] Failed to save state: {exc}")

    def _save_current_project_state(self) -> None:
        if self._current_project_dir is None:
            return
        self._save_project_state(self._current_project_dir)

    def _load_project_state(self, project_dir: Path) -> None:
        path = self._project_state_path(project_dir)
        if not path.exists():
            return
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            self._append_log(f"[project] Failed to load state: {exc}")
            return

        steps = raw.get("steps", {})
        selection_state = raw.get("selection")

        previous_suspend = self._suspend_project_state_io
        self._suspend_project_state_io = True
        try:
            if isinstance(steps, dict):
                elevation_state = steps.get("elevation")
                if isinstance(elevation_state, dict):
                    self.elevation_panel.apply_settings(elevation_state)

                map_render_state = steps.get("map_render")
                if isinstance(map_render_state, dict):
                    self.map_render_panel.apply_settings(map_render_state)

                buildings_state = steps.get("buildings")
                if isinstance(buildings_state, dict):
                    self.buildings_panel.apply_settings(buildings_state)

            selection = (
                self._selection_from_dict(selection_state)
                if isinstance(selection_state, dict)
                else None
            )
            self._selection = selection
            self._set_selection_labels()
            self._notify_selection_changed()
            self._apply_selection_to_map(selection)
        finally:
            self._suspend_project_state_io = previous_suspend

    def _apply_selection_to_map(self, selection: SelectionGeometry | None) -> None:
        if selection is None:
            self._pending_selection_restore = None
            if self._map_ready:
                self.map_view.page().runJavaScript(
                    "if (window.clearSelection) { window.clearSelection(); }"
                )
            return

        payload = {
            "selectionType": selection.selection_type,
            "bbox": list(selection.bbox),
            "geometry": selection.geometry,
        }
        self._pending_selection_restore = payload
        if not self._map_ready:
            return
        payload_json = json.dumps(payload)
        self.map_view.page().runJavaScript(
            f"if (window.setSelectionFromPayload) {{ window.setSelectionFromPayload({payload_json}); }}"
        )
        self._pending_selection_restore = None

    @QtCore.Slot()
    def _clear_selection(self) -> None:
        self._pending_selection_restore = None
        self.map_view.page().runJavaScript(
            "if (window.clearSelection) { window.clearSelection(); }"
        )
        self._selection = None
        self._set_selection_labels()
        self._notify_selection_changed()
        self._save_current_project_state()

    @QtCore.Slot()
    def _export_selection_geojson(self) -> None:
        if not self._selection:
            QtWidgets.QMessageBox.information(
                self, "Selection", "No selection available to export."
            )
            return

        base_dir = self._current_project_dir or self._project_root

        path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            "Export selection as GeoJSON",
            str(base_dir / "selection.geojson"),
            "GeoJSON (*.geojson *.json)",
        )
        if not path:
            return

        write_geojson(Path(path), self._selection.to_feature_collection())
        self._append_log(f"[selection] Exported: {path}")

    def _notify_selection_changed(self) -> None:
        self.elevation_panel.update_selection(self._selection)
        self.map_render_panel.update_selection(self._selection)
        self.buildings_panel.update_selection(self._selection)

    def _notify_project_changed(self) -> None:
        self.elevation_panel.set_project_context(self._current_project_dir)
        self.map_render_panel.set_project_context(self._current_project_dir)
        self.buildings_panel.set_project_context(self._current_project_dir)

    @QtCore.Slot(str)
    def _on_project_selected(self, path_str: str) -> None:
        selected_path = Path(path_str) if path_str else None
        previous_project = self._current_project_dir
        if previous_project and previous_project != selected_path:
            self._save_project_state(previous_project)

        if not path_str:
            self._current_project_dir = None
            self.project_label.setText("Project: none")
            self._notify_project_changed()
            return

        project_dir = Path(path_str)
        self._current_project_dir = project_dir
        self.project_label.setText(f"Project: {project_dir.name}")
        self._append_log(f"[project] Selected: {project_dir}")
        previous_suspend = self._suspend_project_state_io
        self._suspend_project_state_io = True
        try:
            self._notify_project_changed()
            self._selection = None
            self._set_selection_labels()
            self._notify_selection_changed()
            self._apply_selection_to_map(None)
            self._load_project_state(project_dir)
        finally:
            self._suspend_project_state_io = previous_suspend

    @QtCore.Slot(str)
    def _on_selection_payload(self, payload_json: str) -> None:
        try:
            payload = json.loads(payload_json)
            selection_type = str(payload.get("selectionType", "polygon"))
            geometry = payload.get("geometry")
            if geometry is None or selection_type == "none":
                self._selection = None
                self._append_log("[selection] cleared")
            else:
                bbox = tuple(payload["bbox"])
                if len(bbox) != 4:
                    raise ValueError("bbox must contain four coordinates")
                self._selection = SelectionGeometry(
                    geometry=geometry,
                    bbox=(
                        float(bbox[0]),
                        float(bbox[1]),
                        float(bbox[2]),
                        float(bbox[3]),
                    ),
                    selection_type=selection_type,
                )
                self._append_log(
                    f"[selection] {selection_type} bbox={self._selection.bbox_csv}"
                )
            self._set_selection_labels()
            self._notify_selection_changed()
            self._save_current_project_state()
        except Exception as exc:  # noqa: BLE001
            self._append_log(f"[map] Invalid selection payload: {exc}")

    def _set_selection_labels(self) -> None:
        if not self._selection:
            self.selection_label.setText("Selection: none")
            self.bbox_label.setText("BBox: -")
            return
        min_lon, min_lat, max_lon, max_lat = self._selection.bbox
        self.selection_label.setText(f"Selection: {self._selection.selection_type}")
        self.bbox_label.setText(
            f"BBox: {min_lon:.6f}, {min_lat:.6f}, {max_lon:.6f}, {max_lat:.6f}"
        )

    @QtCore.Slot(float, float)
    def _on_map_clicked(self, lat: float, lon: float) -> None:
        self.click_label.setText(f"Click: {lon:.6f}, {lat:.6f}")

    @QtCore.Slot(str)
    def _copy_text_to_clipboard(self, value: str) -> None:
        QtWidgets.QApplication.clipboard().setText(value)
        self._append_log(f"[clipboard] Copied: {value}")

    @QtCore.Slot()
    def _copy_log_to_clipboard(self) -> None:
        QtWidgets.QApplication.clipboard().setText(self.log_view.toPlainText())

    @QtCore.Slot()
    def _on_step_settings_changed(self) -> None:
        self._save_current_project_state()

    @QtCore.Slot(bool)
    def _on_map_load_finished(self, success: bool) -> None:
        self._map_ready = bool(success)
        if not self._map_ready:
            return
        if self._pending_selection_restore is not None:
            payload_json = json.dumps(self._pending_selection_restore)
            self.map_view.page().runJavaScript(
                f"if (window.setSelectionFromPayload) {{ window.setSelectionFromPayload({payload_json}); }}"
            )
            self._pending_selection_restore = None

    def _start_plan(self, plan: RunPlan) -> None:
        if self._runner.is_running():
            QtWidgets.QMessageBox.warning(
                self,
                "Runner busy",
                "A command queue is already running. Wait for completion or stop it.",
            )
            return

        if plan.selection_geojson_path and plan.selection_geojson_data:
            write_geojson(plan.selection_geojson_path, plan.selection_geojson_data)
            self._append_log(
                f"[selection] Wrote cutline: {plan.selection_geojson_path}"
            )

        self._append_log(f"\n=== {plan.title} ===")
        for idx, command in enumerate(plan.commands, start=1):
            self._append_log(f"[{idx}] {command.display()}")

        self._runner.run(plan.commands)

    @QtCore.Slot(str)
    def _on_command_started(self, command: str) -> None:
        self._append_log(f"\n$ {command}")

    @QtCore.Slot(bool)
    def _on_running_changed(self, running: bool) -> None:
        self.stop_btn.setEnabled(running)
        self.running_state.setText("Runner: running" if running else "Runner: idle")

    @QtCore.Slot(bool, str)
    def _on_runner_finished(self, success: bool, message: str) -> None:
        if success:
            self._append_log(f"[done] {message}")
        else:
            self._append_log(f"[error] {message}")
            QtWidgets.QMessageBox.warning(self, "Command execution", message)

    @QtCore.Slot(str)
    def _append_log(self, text: str) -> None:
        if not text:
            return
        self.log_view.appendPlainText(text.rstrip())
        scrollbar = self.log_view.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())
