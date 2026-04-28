"""
Registro de importers disponibles por tipo de source.

Cada importer expone:
  - name: identificador
  - label: nombre user-friendly
  - credential_fields: list[CredField] que el frontend usa para armar el form
  - run(report, table_filter, creds, state) → llena state.message + escribe parquets
"""
from .synthetic import SyntheticImporter
from .pbi_dataflow import PBIDataflowImporter
from .csv_upload import CSVUploadImporter
from .sql_database import SQLDatabaseImporter
from .google_sheets import GoogleSheetsImporter

# orden = orden de presentación en UI
IMPORTERS = {
    "synthetic":     SyntheticImporter(),
    "pbi_dataflow":  PBIDataflowImporter(),
    "csv":           CSVUploadImporter(),
    "sql_database":  SQLDatabaseImporter(),
    "google_sheets": GoogleSheetsImporter(),
}


def get_importer(source: str):
    return IMPORTERS.get(source)
