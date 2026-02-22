from __future__ import annotations

from PySide6 import QtCore


class MapBridge(QtCore.QObject):
    selection_payload = QtCore.Signal(str)
    cursor_moved = QtCore.Signal(float, float)
    map_clicked = QtCore.Signal(float, float)

    @QtCore.Slot(str)
    def selectionChanged(self, payload: str) -> None:
        self.selection_payload.emit(payload)

    @QtCore.Slot(float, float)
    def cursorMoved(self, lat: float, lon: float) -> None:
        self.cursor_moved.emit(lat, lon)

    @QtCore.Slot(float, float)
    def mapClicked(self, lat: float, lon: float) -> None:
        self.map_clicked.emit(lat, lon)

