"""
Parsea el catálogo de metadata Power BI (Kc Reports_MERGED.xlsx) y devuelve
la estructura completa de cada reporte: tablas, columnas, tipo de origen.

Detecta el source type leyendo el M code del Partition QueryDefinition.
"""
from __future__ import annotations
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any
import re
import pandas as pd

DEFAULT_XLSX = Path(r"C:/Users/Mariano Sorbo/Downloads/Kc Reports_MERGED (1).xlsx")

# Regex de detección del source type basado en el M code de la partition
SOURCE_PATTERNS = [
    (r"PowerPlatform\.Dataflows",      "pbi_dataflow"),
    (r"Snowflake\.Databases",          "snowflake"),
    (r"Sql\.Database",                 "sql_server"),
    (r"PostgreSQL\.Database",          "postgres"),
    (r"AmazonRedshift\.Database",      "redshift"),
    (r"GoogleSheets\.Contents",        "google_sheets"),
    (r"Excel\.Workbook",               "excel"),
    (r"Csv\.Document",                 "csv"),
    (r"Web\.Contents",                 "http"),
    (r"AnalysisServices\.Database",    "analysis_services"),
    (r"OData\.Feed",                   "odata"),
    (r"Folder\.Files",                 "folder"),
]
DAX_HINTS = ("CALENDAR(", "CALENDARAUTO(", "SUMMARIZE", "FILTER(", "ROW(", "VAR ", "ADDCOLUMNS")
MANUAL_HINTS = ("Table.FromRows", "#table(")


def detect_source(query: str | None) -> str:
    if not query or not isinstance(query, str):
        return "unknown"
    q = query.strip()
    for pat, name in SOURCE_PATTERNS:
        if re.search(pat, q):
            return name
    if any(h in q for h in MANUAL_HINTS):
        return "manual"
    if any(q.lstrip().startswith(h) for h in DAX_HINTS) or any(h in q for h in DAX_HINTS):
        return "dax_calculated"
    return "other"


def slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


@dataclass
class Column:
    name: str
    data_type: str | int | None = None
    is_hidden: bool = False
    is_calculated: bool = False
    expression: str | None = None


@dataclass
class TableInfo:
    name: str
    table_id: int
    is_hidden: bool = False
    source: str = "unknown"
    query_definition: str | None = None
    columns: list[Column] = field(default_factory=list)


@dataclass
class ReportInfo:
    name: str
    slug: str
    tables: list[TableInfo] = field(default_factory=list)

    @property
    def visible_tables(self) -> list[TableInfo]:
        return [t for t in self.tables if not t.is_hidden and not _is_pbi_internal(t.name)]

    @property
    def sources(self) -> list[str]:
        s = sorted({t.source for t in self.visible_tables})
        return s


def _is_pbi_internal(name: str) -> bool:
    """Tablas auto-generadas de PBI que no son reales (LocalDateTable, DateTableTemplate)."""
    return bool(re.match(r"(LocalDateTable_|DateTableTemplate_)", name))


_cache: dict[str, list[ReportInfo]] = {}


def load_catalog(xlsx_path: Path | str = DEFAULT_XLSX) -> list[ReportInfo]:
    key = str(xlsx_path)
    if key in _cache:
        return _cache[key]

    path = Path(xlsx_path)
    if not path.exists():
        raise FileNotFoundError(f"No existe el catálogo: {path}")

    tablas = pd.read_excel(path, sheet_name="Tablas")
    parts  = pd.read_excel(path, sheet_name="Partitions")
    cols   = pd.read_excel(path, sheet_name="Columnas Y Metricas")

    reports: dict[str, ReportInfo] = {}

    for _, row in tablas.iterrows():
        rep = row["Reporte"]
        if rep not in reports:
            reports[rep] = ReportInfo(name=rep, slug=slugify(rep))
        rep_obj = reports[rep]

        # source desde Partitions
        part = parts[(parts["Reporte"] == rep) & (parts["TableID"] == row["ID"])]
        query_def = None
        source = "unknown"
        if len(part) > 0:
            query_def = part.iloc[0].get("QueryDefinition")
            source = detect_source(query_def)

        table = TableInfo(
            name=str(row["Name"]),
            table_id=int(row["ID"]),
            is_hidden=bool(row.get("IsHidden", False)),
            source=source,
            query_definition=query_def if isinstance(query_def, str) else None,
        )

        # columnas de esa tabla
        col_rows = cols[(cols["Reporte"] == rep) & (cols["TableID"] == row["ID"])]
        for _, c in col_rows.iterrows():
            table.columns.append(Column(
                name=str(c["Name"]),
                data_type=str(c.get("DataType")) if pd.notna(c.get("DataType")) else None,
                is_hidden=bool(c.get("IsHidden", False)),
                is_calculated=bool(pd.notna(c.get("Expression"))),
                expression=str(c["Expression"]) if pd.notna(c.get("Expression")) else None,
            ))

        rep_obj.tables.append(table)

    out = sorted(reports.values(), key=lambda r: r.name)
    _cache[key] = out
    return out


def report_to_dict(rep: ReportInfo) -> dict[str, Any]:
    return {
        "name": rep.name,
        "slug": rep.slug,
        "tables_total": len(rep.tables),
        "tables_visible": len(rep.visible_tables),
        "sources": rep.sources,
        "tables": [
            {
                "name": t.name,
                "is_hidden": t.is_hidden,
                "source": t.source,
                "n_columns": len([c for c in t.columns if not c.is_hidden]),
                "columns": [
                    {"name": c.name, "data_type": c.data_type, "is_calculated": c.is_calculated}
                    for c in t.columns if not c.is_hidden
                ],
            }
            for t in rep.visible_tables
        ],
    }


if __name__ == "__main__":
    import json
    reps = load_catalog()
    for r in reps:
        print(f"{r.name}  ({r.slug})  tables={len(r.visible_tables)}  sources={r.sources}")
