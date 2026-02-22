from __future__ import annotations

import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from PySide6 import QtCore, QtGui, QtWidgets


_PROJECT_NAME_RE = re.compile(r"^(\d+)_(.+)$")
_INVALID_CHARS_RE = re.compile(r'[<>:"/\\|?*]+')


@dataclass(slots=True)
class ProjectEntry:
    path: Path
    number: int | None
    label: str


class ProjectManagerPanel(QtWidgets.QWidget):
    project_selected = QtCore.Signal(str)

    def __init__(
        self,
        master_projects_dir: Path,
        parent: QtWidgets.QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._master_projects_dir = master_projects_dir
        self._entries: list[ProjectEntry] = []
        self._building_list = False

        self._build_ui()
        self._wire()
        self.refresh_projects(select_first=True)

    @property
    def master_projects_dir(self) -> Path:
        return self._master_projects_dir

    def selected_project_path(self) -> Path | None:
        item = self.projects_list.currentItem()
        if item is None:
            return None
        raw = item.data(QtCore.Qt.ItemDataRole.UserRole)
        if not raw:
            return None
        return Path(str(raw))

    def _build_ui(self) -> None:
        root = QtWidgets.QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)

        root.addWidget(QtWidgets.QLabel("Project Manager"))
        root.addWidget(QtWidgets.QLabel(f"Master: {self._master_projects_dir}"))

        actions = QtWidgets.QHBoxLayout()
        self.refresh_btn = QtWidgets.QPushButton("Refresh")
        self.new_btn = QtWidgets.QPushButton("New Project")
        actions.addWidget(self.refresh_btn)
        actions.addWidget(self.new_btn)
        actions.addStretch(1)
        root.addLayout(actions)

        self.projects_list = QtWidgets.QListWidget()
        self.projects_list.setSelectionMode(
            QtWidgets.QAbstractItemView.SelectionMode.SingleSelection
        )
        self.projects_list.setContextMenuPolicy(
            QtCore.Qt.ContextMenuPolicy.CustomContextMenu
        )
        root.addWidget(self.projects_list, stretch=1)

        root.addWidget(QtWidgets.QLabel("Project Contents"))
        self.contents_tree = QtWidgets.QTreeWidget()
        self.contents_tree.setColumnCount(2)
        self.contents_tree.setHeaderLabels(["Name", "Size"])
        self.contents_tree.setUniformRowHeights(True)
        self.contents_tree.setContextMenuPolicy(
            QtCore.Qt.ContextMenuPolicy.CustomContextMenu
        )
        root.addWidget(self.contents_tree, stretch=2)

        self.info_label = QtWidgets.QLabel("")
        self.info_label.setWordWrap(True)
        root.addWidget(self.info_label)

    def _wire(self) -> None:
        self.refresh_btn.clicked.connect(self.refresh_projects)
        self.new_btn.clicked.connect(self._create_project)
        self.projects_list.currentItemChanged.connect(self._on_selection_changed)
        self.projects_list.customContextMenuRequested.connect(
            self._on_projects_context_menu
        )
        self.contents_tree.customContextMenuRequested.connect(
            self._on_contents_context_menu
        )

    def _parse_entry(self, folder: Path) -> ProjectEntry:
        match = _PROJECT_NAME_RE.match(folder.name)
        if not match:
            return ProjectEntry(path=folder, number=None, label=folder.name)
        number = int(match.group(1))
        suffix = match.group(2)
        return ProjectEntry(path=folder, number=number, label=f"{number}_{suffix}")

    def _sorted_entries(self, folders: list[Path]) -> list[ProjectEntry]:
        entries = [self._parse_entry(folder) for folder in folders]

        def sort_key(entry: ProjectEntry) -> tuple[int, int, str]:
            if entry.number is None:
                return (1, 0, entry.label.lower())
            return (0, entry.number, entry.label.lower())

        return sorted(entries, key=sort_key)

    def _sorted_children(self, folder: Path) -> list[Path]:
        try:
            children = list(folder.iterdir())
        except OSError:
            return []
        return sorted(children, key=lambda child: (child.is_file(), child.name.lower()))

    def _format_size(self, size_bytes: int) -> str:
        units = ["B", "KB", "MB", "GB", "TB"]
        size = float(max(size_bytes, 0))
        for idx, unit in enumerate(units):
            if size < 1024 or idx == len(units) - 1:
                if idx == 0:
                    return f"{int(size)} {unit}"
                return f"{size:.1f} {unit}"
            size /= 1024
        return "0 B"

    def _entry_size(self, path: Path) -> int:
        try:
            if path.is_symlink():
                return path.lstat().st_size
            return path.stat().st_size
        except OSError:
            return 0

    def _add_tree_children(
        self,
        parent_item: QtWidgets.QTreeWidgetItem,
        folder: Path,
    ) -> int:
        total_size = 0
        for child in self._sorted_children(folder):
            label = f"{child.name}/" if child.is_dir() else child.name
            item = QtWidgets.QTreeWidgetItem([label, ""])
            item.setData(0, QtCore.Qt.ItemDataRole.UserRole, str(child))
            parent_item.addChild(item)

            if child.is_dir() and not child.is_symlink():
                child_size = self._add_tree_children(item, child)
            else:
                child_size = self._entry_size(child)
            item.setText(1, self._format_size(child_size))
            total_size += child_size
        return total_size

    def _populate_contents_tree(self, path: Path | None) -> None:
        self.contents_tree.clear()
        if path is None or not path.exists():
            return

        root_item = QtWidgets.QTreeWidgetItem([f"{path.name}/", ""])
        root_item.setData(0, QtCore.Qt.ItemDataRole.UserRole, str(path))
        self.contents_tree.addTopLevelItem(root_item)
        total_size = self._add_tree_children(root_item, path)
        root_item.setText(1, self._format_size(total_size))
        root_item.setExpanded(True)
        self.contents_tree.resizeColumnToContents(0)
        self.contents_tree.resizeColumnToContents(1)

    def _file_manager_name(self) -> str:
        if sys.platform == "darwin":
            return "Finder"
        if sys.platform.startswith("win"):
            return "Explorer"
        return "File Manager"

    def _open_in_file_manager(self, path: Path) -> None:
        if not path.exists():
            QtWidgets.QMessageBox.warning(
                self,
                "Open path",
                f"Path does not exist:\n{path}",
            )
            return

        try:
            if sys.platform == "darwin":
                if path.is_file():
                    subprocess.Popen(["open", "-R", str(path)])
                else:
                    subprocess.Popen(["open", str(path)])
            elif sys.platform.startswith("win"):
                if path.is_file():
                    subprocess.Popen(["explorer", f"/select,{path}"])
                else:
                    subprocess.Popen(["explorer", str(path)])
            else:
                target = path if path.is_dir() else path.parent
                QtGui.QDesktopServices.openUrl(
                    QtCore.QUrl.fromLocalFile(str(target))
                )
        except Exception as exc:  # noqa: BLE001
            QtWidgets.QMessageBox.warning(
                self,
                "Open path",
                f"Could not open path:\n{path}\n\n{exc}",
            )

    def _show_open_menu(self, global_pos: QtCore.QPoint, path: Path) -> None:
        menu = QtWidgets.QMenu(self)
        open_action = menu.addAction(f"Open in {self._file_manager_name()}")
        chosen = menu.exec(global_pos)
        if chosen == open_action:
            self._open_in_file_manager(path)

    @QtCore.Slot(QtCore.QPoint)
    def _on_projects_context_menu(self, pos: QtCore.QPoint) -> None:
        item = self.projects_list.itemAt(pos)
        if item is None:
            return
        path_raw = item.data(QtCore.Qt.ItemDataRole.UserRole)
        if not path_raw:
            return
        self._show_open_menu(
            self.projects_list.viewport().mapToGlobal(pos),
            Path(str(path_raw)),
        )

    @QtCore.Slot(QtCore.QPoint)
    def _on_contents_context_menu(self, pos: QtCore.QPoint) -> None:
        item = self.contents_tree.itemAt(pos)
        if item is None:
            return
        path_raw = item.data(0, QtCore.Qt.ItemDataRole.UserRole)
        if not path_raw:
            return
        self._show_open_menu(
            self.contents_tree.viewport().mapToGlobal(pos),
            Path(str(path_raw)),
        )

    def _ensure_master_dir(self) -> bool:
        master = self._master_projects_dir
        if master.exists() and master.is_dir():
            return True

        reply = QtWidgets.QMessageBox.question(
            self,
            "Create master folder",
            f"Master folder does not exist:\n{master}\n\nCreate it now?",
            QtWidgets.QMessageBox.StandardButton.Yes
            | QtWidgets.QMessageBox.StandardButton.No,
        )
        if reply != QtWidgets.QMessageBox.StandardButton.Yes:
            return False
        master.mkdir(parents=True, exist_ok=True)
        return True

    @QtCore.Slot()
    def refresh_projects(self, select_first: bool = False) -> None:
        self.projects_list.clear()
        self._entries = []

        if not self._ensure_master_dir():
            self.info_label.setText("Master folder is missing.")
            self._populate_contents_tree(None)
            self.project_selected.emit("")
            return

        folders = [p for p in self._master_projects_dir.iterdir() if p.is_dir()]
        self._entries = self._sorted_entries(folders)

        self._building_list = True
        try:
            for entry in self._entries:
                item = QtWidgets.QListWidgetItem(entry.label)
                item.setData(QtCore.Qt.ItemDataRole.UserRole, str(entry.path))
                self.projects_list.addItem(item)
        finally:
            self._building_list = False

        count = len(self._entries)
        self.info_label.setText(f"{count} project folder(s)")

        if count == 0:
            self._populate_contents_tree(None)
            self.project_selected.emit("")
            return

        if select_first:
            self.projects_list.setCurrentRow(0)
        elif self.projects_list.currentItem() is None:
            self.projects_list.setCurrentRow(0)

    def _next_project_number(self) -> int:
        numbered = [entry.number for entry in self._entries if entry.number is not None]
        if not numbered:
            return 1
        return max(numbered) + 1

    def _sanitize_name(self, value: str) -> str:
        cleaned = _INVALID_CHARS_RE.sub("", value.strip())
        cleaned = cleaned.replace(" ", "_")
        cleaned = re.sub(r"_+", "_", cleaned)
        return cleaned.strip("_")

    @QtCore.Slot()
    def _create_project(self) -> None:
        if not self._ensure_master_dir():
            return

        name, ok = QtWidgets.QInputDialog.getText(
            self,
            "New project",
            "Project name (without number prefix):",
        )
        if not ok:
            return

        suffix = self._sanitize_name(name)
        if not suffix:
            QtWidgets.QMessageBox.warning(
                self,
                "Invalid name",
                "Project name cannot be empty.",
            )
            return

        number = self._next_project_number()
        folder_name = f"{number}_{suffix}"
        path = self._master_projects_dir / folder_name
        if path.exists():
            QtWidgets.QMessageBox.warning(
                self,
                "Project exists",
                f"Folder already exists:\n{path}",
            )
            return

        (path / "textures").mkdir(parents=True, exist_ok=True)
        (path / "buildings_output").mkdir(parents=True, exist_ok=True)

        self.refresh_projects(select_first=False)
        self._select_project_path(path)

    def _select_project_path(self, path: Path) -> None:
        for i in range(self.projects_list.count()):
            item = self.projects_list.item(i)
            item_path = Path(item.data(QtCore.Qt.ItemDataRole.UserRole))
            if item_path == path:
                self.projects_list.setCurrentItem(item)
                break

    @QtCore.Slot(QtWidgets.QListWidgetItem, QtWidgets.QListWidgetItem)
    def _on_selection_changed(
        self,
        current: QtWidgets.QListWidgetItem | None,
        _: QtWidgets.QListWidgetItem | None,
    ) -> None:
        if self._building_list:
            return
        if current is None:
            self._populate_contents_tree(None)
            self.project_selected.emit("")
            return
        path_str = current.data(QtCore.Qt.ItemDataRole.UserRole)
        path = Path(str(path_str))
        self._populate_contents_tree(path)
        self.project_selected.emit(str(path))
