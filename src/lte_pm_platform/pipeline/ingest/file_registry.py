from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RegisteredFile:
    source_file: str
    file_hash: str
