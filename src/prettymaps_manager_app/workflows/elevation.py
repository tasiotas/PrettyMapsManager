from __future__ import annotations

from pathlib import Path
from typing import Callable

from PySide6 import QtCore, QtWidgets

from ..models import CommandSpec, RunPlan, SelectionGeometry


class ElevationPanel(QtWidgets.QWidget):
    settings_changed = QtCore.Signal()

    def __init__(
        self,
        project_root: Path,
        default_dataset_path: Path,
        default_width: int,
        get_selection: Callable[[], SelectionGeometry | None],
        run_plan_callback: Callable[[RunPlan], None],
        parent: QtWidgets.QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._project_root = project_root
        self._dataset_path = default_dataset_path
        self._default_width = default_width
        self._project_dir: Path | None = None
        self._get_selection = get_selection
        self._run_plan_callback = run_plan_callback

        self._build_ui()
        self._wire()
        self.refresh_preview()

    def _build_ui(self) -> None:
        root = QtWidgets.QVBoxLayout(self)
        form = QtWidgets.QFormLayout()

        self.output_stem = QtWidgets.QLineEdit("elevation")
        form.addRow("Output Stem:", self.output_stem)

        self.width = QtWidgets.QSpinBox()
        self.width.setRange(256, 100_000)
        self.width.setValue(self._default_width)
        form.addRow("Target Width:", self.width)

        self.height = QtWidgets.QSpinBox()
        self.height.setRange(0, 100_000)
        self.height.setValue(0)
        self.height.setSpecialValueText("Auto")
        form.addRow("Target Height:", self.height)

        self.resampling = QtWidgets.QComboBox()
        self.resampling.addItems(["lanczos", "cubic", "bilinear", "nearest"])
        form.addRow("Resampling:", self.resampling)

        self.compression = QtWidgets.QComboBox()
        self.compression.addItems(["DEFLATE", "LZW", "None"])
        form.addRow("TIFF Compression:", self.compression)

        self.convert_to_exr = QtWidgets.QCheckBox(
            "Convert TIFF to 16-bit EXR with oiiotool"
        )
        self.convert_to_exr.setChecked(True)
        form.addRow("", self.convert_to_exr)

        root.addLayout(form)

        self.preview = QtWidgets.QPlainTextEdit()
        self.preview.setReadOnly(True)
        self.preview.setMinimumHeight(120)
        root.addWidget(QtWidgets.QLabel("Command Preview:"))
        root.addWidget(self.preview, stretch=1)

        self.run_btn = QtWidgets.QPushButton("Run Step 1 (Elevation)")
        root.addWidget(self.run_btn)

    def _wire(self) -> None:
        self.run_btn.clicked.connect(self._run_requested)

        watch = [
            self.output_stem.textChanged,
            self.width.valueChanged,
            self.height.valueChanged,
            self.resampling.currentTextChanged,
            self.compression.currentTextChanged,
            self.convert_to_exr.toggled,
        ]
        for signal in watch:
            signal.connect(self._on_inputs_changed)

    def update_selection(self, _: SelectionGeometry | None) -> None:
        self.refresh_preview()

    def set_project_context(self, project_dir: Path | None) -> None:
        self._project_dir = project_dir
        if project_dir is not None:
            suffix = (
                project_dir.name.split("_", 1)[1]
                if "_" in project_dir.name
                else project_dir.name
            )
            self.output_stem.setText(f"{suffix.lower()}_elevation")
        self.refresh_preview()

    @QtCore.Slot()
    def _on_inputs_changed(self) -> None:
        self.refresh_preview()
        self.settings_changed.emit()

    def export_settings(self) -> dict[str, object]:
        return {
            "output_stem": self.output_stem.text(),
            "width": self.width.value(),
            "height": self.height.value(),
            "resampling": self.resampling.currentText(),
            "compression": self.compression.currentText(),
            "convert_to_exr": self.convert_to_exr.isChecked(),
        }

    def apply_settings(self, settings: dict[str, object]) -> None:
        blockers = [
            QtCore.QSignalBlocker(self.output_stem),
            QtCore.QSignalBlocker(self.width),
            QtCore.QSignalBlocker(self.height),
            QtCore.QSignalBlocker(self.resampling),
            QtCore.QSignalBlocker(self.compression),
            QtCore.QSignalBlocker(self.convert_to_exr),
        ]
        try:
            output_stem = settings.get("output_stem")
            if isinstance(output_stem, str):
                self.output_stem.setText(output_stem)

            width = settings.get("width")
            if width is not None:
                self.width.setValue(int(width))

            height = settings.get("height")
            if height is not None:
                self.height.setValue(int(height))

            resampling = settings.get("resampling")
            if isinstance(resampling, str):
                index = self.resampling.findText(resampling)
                if index >= 0:
                    self.resampling.setCurrentIndex(index)

            compression = settings.get("compression")
            if isinstance(compression, str):
                index = self.compression.findText(compression)
                if index >= 0:
                    self.compression.setCurrentIndex(index)

            convert_to_exr = settings.get("convert_to_exr")
            if convert_to_exr is not None:
                self.convert_to_exr.setChecked(bool(convert_to_exr))
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

        dataset = self._dataset_path
        if not dataset.exists():
            raise ValueError(f"Elevation dataset not found: {dataset}")

        out_dir = self._project_dir / "textures"
        out_dir.mkdir(parents=True, exist_ok=True)

        stem = self.output_stem.text().strip() or "elevation"
        width = self.width.value()
        height = self.height.value()
        resampling = self.resampling.currentText()
        compression = self.compression.currentText()

        selection_geojson = out_dir / f"{stem}_selection.geojson"
        tiff_output = out_dir / f"{stem}_{width}_32.tif"
        commands: list[CommandSpec] = []

        gdal_args = [
            "-cutline",
            str(selection_geojson),
            "-crop_to_cutline",
            "-ot",
            "Float32",
        ]

        if compression != "None":
            gdal_args.extend(["-co", f"COMPRESS={compression}"])

        gdal_args.extend(
            [
                "-ts",
                str(width),
                str(height),
                "-r",
                resampling,
                str(dataset),
                "-overwrite",
                str(tiff_output),
            ]
        )

        commands.append(
            CommandSpec(
                program="gdalwarp",
                args=gdal_args,
                cwd=self._project_root,
                label="gdalwarp elevation cutline",
            )
        )

        if self.convert_to_exr.isChecked():
            exr_output = out_dir / f"{stem}_{width}_16.exr"
            commands.append(
                CommandSpec(
                    program="oiiotool",
                    args=[
                        str(tiff_output),
                        "-d",
                        "half",
                        "-compression",
                        "zip",
                        "-otex",
                        str(exr_output),
                    ],
                    cwd=self._project_root,
                    label="oiiotool convert to EXR",
                )
            )

        return RunPlan(
            title="Step 1: Elevation texture generation",
            commands=commands,
            selection_geojson_path=selection_geojson,
            selection_geojson_data=selection.to_feature_collection(),
        )

    @QtCore.Slot()
    def _run_requested(self) -> None:
        try:
            plan = self._build_plan()
        except ValueError as exc:
            QtWidgets.QMessageBox.warning(self, "Step 1 validation", str(exc))
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
