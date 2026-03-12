from __future__ import annotations

from pathlib import Path

from lte_pm_platform.utils.paths import archive_dir, rejected_dir


def move_by_status(file_path: Path, status: str) -> tuple[str, Path]:
    if status in {"SUCCESS", "SKIPPED_DUPLICATE"}:
        destination_dir = archive_dir()
        action = "archived"
    elif status == "FAILED":
        destination_dir = rejected_dir()
        action = "rejected"
    else:
        return "left_in_place", file_path

    destination_dir.mkdir(parents=True, exist_ok=True)
    destination = unique_destination(destination_dir / file_path.name)
    file_path.replace(destination)
    return action, destination


def unique_destination(destination: Path) -> Path:
    if not destination.exists():
        return destination

    counter = 1
    while True:
        candidate = destination.with_name(f"{destination.stem}_{counter}{destination.suffix}")
        if not candidate.exists():
            return candidate
        counter += 1
