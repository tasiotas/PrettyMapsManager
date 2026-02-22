from __future__ import annotations

from pathlib import Path

from PySide6 import QtCore

from .models import CommandSpec


class CommandQueueRunner(QtCore.QObject):
    output = QtCore.Signal(str)
    command_started = QtCore.Signal(str)
    finished = QtCore.Signal(bool, str)
    running_changed = QtCore.Signal(bool)

    def __init__(self, parent: QtCore.QObject | None = None) -> None:
        super().__init__(parent)
        self._commands: list[CommandSpec] = []
        self._index = 0
        self._running = False

        self._process = QtCore.QProcess(self)
        self._process.readyReadStandardOutput.connect(self._read_stdout)
        self._process.readyReadStandardError.connect(self._read_stderr)
        self._process.errorOccurred.connect(self._on_error)
        self._process.finished.connect(self._on_finished)

    def is_running(self) -> bool:
        return self._running

    def stop(self) -> None:
        if self._process.state() != QtCore.QProcess.ProcessState.NotRunning:
            self.output.emit("[runner] Terminating active process...")
            self._process.kill()

    def run(self, commands: list[CommandSpec]) -> None:
        if self._running:
            self.finished.emit(False, "Another process queue is already running.")
            return
        if not commands:
            self.finished.emit(True, "No commands queued.")
            return

        self._commands = commands
        self._index = 0
        self._set_running(True)
        self._start_current()

    def _set_running(self, value: bool) -> None:
        if self._running == value:
            return
        self._running = value
        self.running_changed.emit(value)

    def _start_current(self) -> None:
        if self._index >= len(self._commands):
            self._set_running(False)
            self.finished.emit(True, "All commands completed successfully.")
            return

        spec = self._commands[self._index]
        if spec.cwd:
            self._process.setWorkingDirectory(str(spec.cwd))
        else:
            self._process.setWorkingDirectory(str(Path.cwd()))

        self.command_started.emit(spec.display())
        self._process.start(spec.program, spec.args)

        if not self._process.waitForStarted(3000):
            self._fail(f"Failed to start command: {spec.program}")

    def _read_stdout(self) -> None:
        data = bytes(self._process.readAllStandardOutput()).decode(
            "utf-8", errors="replace"
        )
        if data:
            self.output.emit(data.rstrip())

    def _read_stderr(self) -> None:
        data = bytes(self._process.readAllStandardError()).decode(
            "utf-8", errors="replace"
        )
        if data:
            self.output.emit(data.rstrip())

    def _on_error(self, error: QtCore.QProcess.ProcessError) -> None:
        if not self._running:
            return
        if error == QtCore.QProcess.ProcessError.UnknownError:
            return
        self._fail(f"Process error: {error.name}")

    def _on_finished(
        self,
        exit_code: int,
        exit_status: QtCore.QProcess.ExitStatus,
    ) -> None:
        if not self._running:
            return
        if exit_status != QtCore.QProcess.ExitStatus.NormalExit or exit_code != 0:
            self._fail(f"Command failed with exit code {exit_code}.")
            return
        self._index += 1
        self._start_current()

    def _fail(self, message: str) -> None:
        self._set_running(False)
        self.finished.emit(False, message)
