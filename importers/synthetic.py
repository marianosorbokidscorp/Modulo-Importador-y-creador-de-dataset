"""
Synthetic importer — genera dummy data que respeta el schema del reporte.

Util para probar el flujo sin acceso a la fuente real, o como fallback cuando
el source declarado del reporte no está implementado.
"""
from __future__ import annotations
from pathlib import Path
import random
import numpy as np
import pandas as pd
from datetime import date, timedelta
from .base import BaseImporter, CredField, ImportState


class SyntheticImporter(BaseImporter):
    name = "synthetic"
    label = "Sintético (dummy data)"
    description = "Genera datos de prueba que respetan el schema declarado. No requiere credenciales — siempre funciona."
    credential_fields = [
        CredField(key="rows_per_table", label="Filas por tabla", type="text",
                  default="1000", required=False,
                  hint="Cantidad de filas a generar por tabla (10 a 100000)"),
        CredField(key="seed", label="Random seed", type="text",
                  default="42", required=False,
                  hint="Para resultados reproducibles."),
    ]

    def run(self, report, tables, creds, state: ImportState, data_dir: Path):
        try:
            n_rows = int(creds.get("rows_per_table") or 1000)
            seed = int(creds.get("seed") or 42)
            n_rows = max(10, min(100000, n_rows))

            rng = np.random.default_rng(seed)
            random.seed(seed)

            state.status = "running"
            data_dir.mkdir(parents=True, exist_ok=True)

            for t in tables:
                state.message = f"Generando {t.name} ({n_rows} filas)..."
                df = _generate_table(t, n_rows, rng)
                out = data_dir / f"{_safe(t.name)}.parquet"
                df.to_parquet(out, index=False)
                state.tables_imported[t.name] = len(df)

            state.status = "done"
            state.message = f"OK. {len(state.tables_imported)} tablas generadas."
        except Exception as e:
            state.status = "error"
            state.message = f"{type(e).__name__}: {e}"


def _safe(n: str) -> str:
    import re
    return re.sub(r"[^A-Za-z0-9_\-]", "_", n)


def _generate_table(table, n_rows: int, rng) -> pd.DataFrame:
    """Genera un DataFrame con N filas según los nombres y dtypes de las columnas."""
    cols = [c for c in table.columns if not c.is_hidden]
    if not cols:
        return pd.DataFrame()
    data = {}
    for col in cols:
        data[col.name] = _generate_column(col.name, col.data_type, n_rows, rng)
    return pd.DataFrame(data)


def _generate_column(name: str, dtype, n: int, rng) -> np.ndarray | list:
    """Generación heurística según nombre + tipo declarado."""
    n_lower = name.lower()
    s = str(dtype) if dtype is not None else ""

    # categóricos primero (para evitar que "mes" en "trimestre" lo capture el date check)
    if "trimestre" in n_lower or "quarter" in n_lower:
        return rng.choice(["Q1 2024", "Q2 2024", "Q3 2024", "Q4 2024",
                           "Q1 2025", "Q2 2025", "Q3 2025", "Q4 2025"], n)

    # numéricos por nombre típico
    if any(k in n_lower for k in ["impressions", "impresiones", "views", "clicks", "transactions"]):
        return rng.integers(1000, 5_000_000, n)
    if any(k in n_lower for k in ["spent", "cost", "revenue", "margin", "billing", "amount", "usd", "monto",
                                   "diff", "margen", "committed"]):
        return np.round(rng.lognormal(mean=8, sigma=1.5, size=n), 2)
    if any(k in n_lower for k in ["pct", "rate", "ratio", "kick_back", "incentive", "margen_actual", "benchmark"]):
        return np.round(rng.normal(0.25, 0.15, n).clip(-0.1, 0.7), 4)
    if any(k in n_lower for k in ["cpm", "cpv", "cpc"]):
        return np.round(rng.normal(4, 2, n).clip(0.1, 30), 4)
    if "id" == n_lower or n_lower.endswith("_id"):
        return rng.integers(100000, 999999, n).astype(str)

    # fechas — usar word boundaries con regex para no matchear "mes" en "trimestre"
    import re as _re
    is_date = (
        _re.search(r"(?:^|_|\b)(fecha|date|month|time|inicio|fin)(?:$|_|\b)", n_lower)
        or n_lower in ("mes", "mesaño", "mesano", "mes_ano", "mes_anio")
        or n_lower.startswith("mes_") or n_lower.startswith("fecha_")
    )
    if is_date:
        base = date.today().replace(day=1)
        offsets = rng.integers(-720, 0, n)
        return [base + timedelta(days=int(o)) for o in offsets]

    # categóricos por nombre
    if "advertiser" in n_lower:
        return rng.choice(["Sony Pictures", "Lego", "Mattel", "Disney", "Warner Bros.",
                           "Cartoon Network", "Nickelodeon", "Hasbro", "Nintendo",
                           "Roblox Corp", "Coca-Cola", "Kellogg"], n)
    if any(k in n_lower for k in ["country", "pais"]):
        return rng.choice(["Brasil", "Argentina", "Chile", "Colombia", "México", "Perú", "Uruguay"], n)
    if any(k in n_lower for k in ["mercado", "market"]):
        return rng.choice(["Brasil", "LATAM", "Cono Sur", "Andina"], n)
    if any(k in n_lower for k in ["business_model", "model"]):
        return rng.choice(["Dynamics", "Programmatic", "Direct", "Self-Service"], n)
    if any(k in n_lower for k in ["format", "formato"]):
        return rng.choice(["Video", "In-Stream Skippable", "In-Stream Non Skippable",
                           "Bumper Ad", "Interstitial", "Display", "Native"], n)
    if any(k in n_lower for k in ["offer_type", "offer"]):
        return rng.choice(["CPM", "CPV", "CPC", "CPA"], n)
    if any(k in n_lower for k in ["bonificated", "bonif"]):
        return rng.choice(["True", "False"], n, p=[0.1, 0.9])
    if any(k in n_lower for k in ["age", "rango_etario", "edad"]):
        return rng.choice(["4-12", "7-12", "7-18", "4-18", "10-18"], n)
    if "trimestre" in n_lower or "quarter" in n_lower:
        return rng.choice(["Q1 2024", "Q2 2024", "Q3 2024", "Q4 2024",
                           "Q1 2025", "Q2 2025", "Q3 2025", "Q4 2025"], n)
    if "owner" in n_lower or "user" in n_lower or "usuario" in n_lower:
        return rng.choice(["Ana García", "Bruno López", "Camila Pérez",
                           "Diego Sánchez", "Elena Torres", "Fer Rojas"], n)
    if "status" in n_lower or "estado" in n_lower:
        return rng.choice(["Active", "Paused", "Closed", "Pending"], n)

    # tipo declarado (DataType numérico de PBI)
    if s in ("integer", "int", "Int64"):
        return rng.integers(0, 10000, n)
    if s in ("decimal", "float", "double", "Double", "Decimal"):
        return np.round(rng.normal(100, 50, n), 2)
    if s in ("dateTime", "date"):
        base = date.today().replace(day=1)
        offsets = rng.integers(-720, 0, n)
        return [base + timedelta(days=int(o)) for o in offsets]
    if s in ("boolean", "bool"):
        return rng.choice([True, False], n)

    # default: string genérico
    return [f"{name}_{i:05d}" for i in range(n)]
