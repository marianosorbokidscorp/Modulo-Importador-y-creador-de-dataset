"""
Stub para SQL — recibe credenciales pero todavía no ejecuta queries reales.
Devuelve un error claro indicando que está pendiente.
"""
from __future__ import annotations
from pathlib import Path
from .base import BaseImporter, CredField, ImportState


class SQLDatabaseImporter(BaseImporter):
    name = "sql_database"
    label = "SQL Database (Snowflake / SQL Server / Postgres / Redshift)"
    description = "Conectar al warehouse upstream. Por ahora solo guarda los credenciales — implementación pendiente."
    credential_fields = [
        CredField(key="dialect", label="Dialecto", default="snowflake",
                  hint="snowflake | sqlserver | postgres | redshift"),
        CredField(key="host", label="Host / Account",
                  placeholder="xy12345.us-east-1.aws.snowflakecomputing.com"),
        CredField(key="database", label="Database"),
        CredField(key="schema",   label="Schema", default="PUBLIC", required=False),
        CredField(key="warehouse",label="Warehouse / Compute", required=False, hint="Solo para Snowflake"),
        CredField(key="user",     label="Usuario"),
        CredField(key="password", label="Password", type="password"),
    ]

    def run(self, report, tables, creds, state: ImportState, data_dir: Path):
        state.status = "error"
        state.message = (
            "Importer SQL todavía no implementado. "
            "Pasame el dialecto exacto y un par de queries de ejemplo y te lo armo."
        )
