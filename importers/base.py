"""
Interfaz común para todos los importers.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
import time
from typing import Any


@dataclass
class CredField:
    key: str
    label: str
    type: str = "text"            # text | password | textarea | file
    placeholder: str = ""
    default: str = ""
    required: bool = True
    hint: str = ""


@dataclass
class ImportState:
    """Estado mutable compartido entre el thread del job y el endpoint /poll."""
    status: str = "idle"          # idle | running | waiting_auth | done | error
    message: str = ""
    user_code: str | None = None
    verification_uri: str | None = None
    tables_imported: dict[str, int] = field(default_factory=dict)  # table_name → row_count
    created_at: float = field(default_factory=time.time)
    extra: dict[str, Any] = field(default_factory=dict)

    def as_dict(self):
        return {
            "status": self.status,
            "message": self.message,
            "user_code": self.user_code,
            "verification_uri": self.verification_uri,
            "tables_imported": self.tables_imported,
            "elapsed": round(time.time() - self.created_at, 1),
        }


class BaseImporter:
    name: str = "base"
    label: str = "Base"
    description: str = ""
    credential_fields: list[CredField] = []

    def run(self, report: Any, tables: list, creds: dict, state: ImportState, data_dir: Path):
        """
        Override esto. Debe:
          - actualizar state.status / state.message en cada paso
          - escribir 1 parquet por tabla bajo data_dir/<table>.parquet
          - poblar state.tables_imported = {table_name: row_count}
        """
        raise NotImplementedError
