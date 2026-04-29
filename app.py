"""
Flask app — importador y previsualizador de datasets.

Endpoints:
  GET  /                            → SPA (sirve templates/index.html)
  GET  /api/catalog                 → lista de reportes con su estructura
  GET  /api/report/<slug>           → detalle del reporte (tablas + sources)
  GET  /api/importers               → importers disponibles + sus campos
  POST /api/import/<slug>           → arranca un import job, devuelve job_id
  GET  /api/import/poll/<job_id>    → estado del job
  GET  /api/dataset/<slug>/tables   → tablas materializadas + row counts
  GET  /api/dataset/<slug>/data     → datos paginados de una tabla
                                      ?table=<name>&page=0&size=50&sort=col&dir=asc
"""
from __future__ import annotations
import os
import secrets
import threading
import webbrowser
from dataclasses import asdict
from pathlib import Path
import duckdb
import pandas as pd
from flask import Flask, request, jsonify, render_template, send_from_directory

# Cargar .env si existe — pre-llena los defaults de los importers (PBI_USERNAME, etc.)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass  # python-dotenv opcional

import catalog
from importers import IMPORTERS, get_importer
from importers.base import ImportState

ROOT = Path(__file__).resolve().parent
DATA_ROOT = ROOT / "data"
DATA_ROOT.mkdir(exist_ok=True)
PORT = int(os.getenv("PORT", "8766"))

app = Flask(__name__, template_folder=str(ROOT / "templates"), static_folder=str(ROOT / "static"))
_jobs: dict[str, ImportState] = {}


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/catalog")
def api_catalog():
    reps = catalog.load_catalog()
    return jsonify([catalog.report_to_dict(r) for r in reps])


@app.route("/api/report/<slug>")
def api_report(slug):
    reps = catalog.load_catalog()
    rep = next((r for r in reps if r.slug == slug), None)
    if not rep:
        return jsonify({"error": "report not found"}), 404
    return jsonify(catalog.report_to_dict(rep))


@app.route("/api/importers")
def api_importers():
    return jsonify({
        name: {
            "name": imp.name,
            "label": imp.label,
            "description": imp.description,
            "fields": [asdict(f) for f in imp.credential_fields],
        }
        for name, imp in IMPORTERS.items()
    })


@app.route("/api/import/<slug>", methods=["POST"])
def api_import(slug):
    reps = catalog.load_catalog()
    rep = next((r for r in reps if r.slug == slug), None)
    if not rep:
        return jsonify({"ok": False, "error": "report not found"}), 404

    importer_name = (request.form.get("importer") or
                     (request.get_json(silent=True) or {}).get("importer"))
    if not importer_name:
        return jsonify({"ok": False, "error": "importer requerido"}), 400
    imp = get_importer(importer_name)
    if not imp:
        return jsonify({"ok": False, "error": f"importer {importer_name} desconocido"}), 400

    # Credenciales: vienen en form (si hay archivos) o en JSON
    if request.files:
        creds = {k: v for k, v in request.form.items()}
        creds["_files"] = [(f.filename, f.read()) for f in request.files.getlist("files")]
    else:
        body = request.get_json(silent=True) or {}
        creds = {k: v for k, v in body.items() if k != "importer"}

    # Filtrado de tablas (opcional)
    table_names = creds.pop("_tables", None)
    if table_names:
        tables = [t for t in rep.visible_tables if t.name in table_names]
    else:
        tables = rep.visible_tables

    state = ImportState()
    job_id = secrets.token_hex(6)
    _jobs[job_id] = state
    state.extra["report_slug"] = slug
    state.extra["importer"] = importer_name

    data_dir = DATA_ROOT / slug
    data_dir.mkdir(parents=True, exist_ok=True)

    # los importers que usan threads ya inician su propio; los simples corren sync en otro thread
    if importer_name == "pbi_dataflow":
        # ya inicia su thread interno
        imp.run(rep, tables, creds, state, data_dir)
    else:
        threading.Thread(target=imp.run, args=(rep, tables, creds, state, data_dir), daemon=True).start()

    return jsonify({"ok": True, "job_id": job_id, **state.as_dict()})


@app.route("/api/import/poll/<job_id>")
def api_import_poll(job_id):
    state = _jobs.get(job_id)
    if not state:
        return jsonify({"ok": False, "error": "job not found"}), 404
    return jsonify({"ok": True, **state.as_dict()})


@app.route("/api/dataset/<slug>/tables")
def api_dataset_tables(slug):
    """Lista los parquets materializados + row count + lista de columnas."""
    data_dir = DATA_ROOT / slug
    if not data_dir.exists():
        return jsonify([])
    out = []
    for p in sorted(data_dir.glob("*.parquet")):
        try:
            md = duckdb.execute(f"SELECT * FROM read_parquet('{p.as_posix()}') LIMIT 0").description
            row_count = duckdb.execute(f"SELECT COUNT(*) FROM read_parquet('{p.as_posix()}')").fetchone()[0]
            out.append({
                "name": p.stem,
                "rows": int(row_count),
                "cols": len(md),
                "columns": [c[0] for c in md],
                "size_kb": p.stat().st_size // 1024,
            })
        except Exception as e:
            out.append({"name": p.stem, "error": str(e)})
    return jsonify(out)


@app.route("/api/dataset/<slug>/data")
def api_dataset_data(slug):
    """Devuelve datos paginados de una tabla."""
    table = request.args.get("table")
    if not table:
        return jsonify({"error": "table requerido"}), 400
    page = max(0, int(request.args.get("page", 0)))
    size = max(1, min(500, int(request.args.get("size", 50))))
    sort = request.args.get("sort")
    direction = (request.args.get("dir") or "asc").lower()
    if direction not in ("asc", "desc"):
        direction = "asc"

    data_dir = DATA_ROOT / slug
    # buscar archivo case-insensitive
    matches = [p for p in data_dir.glob("*.parquet") if p.stem.lower() == table.lower()]
    if not matches:
        return jsonify({"error": f"tabla {table} no encontrada"}), 404
    path = matches[0]

    con = duckdb.connect(":memory:")
    try:
        cols = con.execute(f"SELECT * FROM read_parquet('{path.as_posix()}') LIMIT 0").description
        col_names = [c[0] for c in cols]
        col_types = [str(c[1]) for c in cols]

        order_clause = ""
        if sort and sort in col_names:
            order_clause = f' ORDER BY "{sort}" {direction.upper()} NULLS LAST'

        total = con.execute(f"SELECT COUNT(*) FROM read_parquet('{path.as_posix()}')").fetchone()[0]
        offset = page * size
        rows = con.execute(f"""
            SELECT * FROM read_parquet('{path.as_posix()}')
            {order_clause}
            LIMIT {size} OFFSET {offset}
        """).fetchdf()
    finally:
        con.close()

    return jsonify({
        "columns": col_names,
        "types": col_types,
        "rows": rows.fillna("").astype(str).values.tolist(),
        "total": int(total),
        "page": page,
        "size": size,
        "pages": (int(total) + size - 1) // size,
    })


def main():
    print(f"[serve] http://localhost:{PORT}")
    webbrowser.open(f"http://localhost:{PORT}")
    app.run(host="127.0.0.1", port=PORT, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
