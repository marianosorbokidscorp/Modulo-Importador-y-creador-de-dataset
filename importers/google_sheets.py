"""
Stub para Google Sheets.
"""
from __future__ import annotations
from pathlib import Path
from .base import BaseImporter, CredField, ImportState


class GoogleSheetsImporter(BaseImporter):
    name = "google_sheets"
    label = "Google Sheets"
    description = "Importer de Google Sheets — requiere OAuth o service account. Pendiente de implementación."
    credential_fields = [
        CredField(key="sheet_url", label="URL de la sheet"),
        CredField(key="auth_method", label="Método auth", default="oauth", hint="oauth | service_account"),
    ]

    def run(self, report, tables, creds, state: ImportState, data_dir: Path):
        state.status = "error"
        state.message = "Importer Google Sheets pendiente. Si querés lo prioritizo y lo armo."
