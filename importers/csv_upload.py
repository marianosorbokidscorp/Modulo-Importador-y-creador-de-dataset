"""
Importer CSV — el user sube uno o más CSVs y los matcheamos a tablas por nombre de archivo.
"""
from __future__ import annotations
from pathlib import Path
import re
import pandas as pd
from .base import BaseImporter, CredField, ImportState


class CSVUploadImporter(BaseImporter):
    name = "csv"
    label = "CSV / Excel local"
    description = "Subir uno o más archivos. El nombre de archivo (sin extensión) debe matchear el nombre de la tabla."
    credential_fields = [
        CredField(key="files", label="Archivos CSV/XLSX", type="file", required=True,
                  hint="Drag & drop. El nombre debe coincidir con el de la tabla (ej: VIEW_REPORTING_MARGENES.csv)."),
    ]

    def run(self, report, tables, creds, state: ImportState, data_dir: Path):
        try:
            uploads: list[tuple[str, bytes]] = creds.get("_files") or []
            if not uploads:
                state.status = "error"; state.message = "No se subieron archivos."
                return

            state.status = "running"
            data_dir.mkdir(parents=True, exist_ok=True)

            wanted = {t.name.lower(): t for t in tables}
            for filename, content in uploads:
                base = Path(filename).stem
                # match insensitive
                t = wanted.get(base.lower())
                if not t:
                    continue
                state.message = f"Cargando {filename} → {t.name}"
                buf = bytes(content) if isinstance(content, (bytes, bytearray)) else content
                if filename.lower().endswith((".xlsx", ".xls")):
                    df = pd.read_excel(buf)
                else:
                    import io
                    df = pd.read_csv(io.BytesIO(buf))
                out = data_dir / f"{_safe(t.name)}.parquet"
                df.to_parquet(out, index=False)
                state.tables_imported[t.name] = len(df)

            if not state.tables_imported:
                state.status = "error"
                state.message = "Ningún archivo matcheó con tablas del reporte."
                return
            state.status = "done"
            state.message = f"OK. {len(state.tables_imported)} tablas cargadas."
        except Exception as e:
            state.status = "error"
            state.message = f"{type(e).__name__}: {e}"


def _safe(n: str) -> str:
    return re.sub(r"[^A-Za-z0-9_\-]", "_", n)
