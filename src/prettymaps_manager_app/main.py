from __future__ import annotations

import argparse
import sys
from pathlib import Path

from PySide6 import QtWidgets

from .config import load_config
from .ui.main_window import MainWindow


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="PrettyMapsManager GUI - map selection and processing workflows"
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=Path.cwd(),
        help="Project directory containing app scripts and tools",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to PrettyMapsManager TOML config file",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    project_root = args.project_root.resolve()
    app_config = load_config(project_root=project_root, config_path=args.config)

    app = QtWidgets.QApplication(sys.argv)
    app.setApplicationName("PrettyMapsManager")
    app.setOrganizationName("PrettyMaps")

    window = MainWindow(project_root=project_root, app_config=app_config)
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
