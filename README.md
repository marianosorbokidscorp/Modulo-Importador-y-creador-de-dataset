# Módulo Importador y Creador de Datasets

App web que toma el catálogo de metadata Power BI (`Kc Reports_MERGED.xlsx`), te deja elegir cualquiera de los 9 reportes, conectarte a su origen real (o usar dummy data como fallback) y previsualizar las tablas resultantes en una vista tipo **Power BI Data view**.

> **No es un dashboard con gráficos.** Es un constructor de datasets + previsualizador de tablas crudas. La idea es replicar lo que hacés en PBI Desktop cuando entrás a la "Vista de tabla".

## Quick start

```bash
cd "C:/Users/Mariano Sorbo/Documents/Reportes Python/Modulo Importador y creador de dataset"

# 1. Dependencias (una sola vez)
pip install -r requirements.txt

# 2. Levantar la app
python app.py
```

Se abre solo en `http://localhost:8766`.

Para usar otro puerto: `PORT=9000 python app.py`.

## Flujo de uso

```
[1. Catálogo]  →  [2. Setup]  →  [3. Vista de datos]
   click          form de         sidebar tablas
   reporte        credenciales    + grid paginado
                  + import
```

### Paso 1 — Catálogo
Lista de los 9 reportes leídos del Excel. Cada card muestra:
- Cantidad de tablas visibles
- Tags con los **orígenes detectados** (`pbi_dataflow`, `dax_calculated`, `manual`, `google_sheets`, etc.) — auto-detectados parseando el M code de las Partitions.

### Paso 2 — Setup (configurar import)
- Muestra las tablas a importar (con conteo de columnas y tag de origen)
- **Tabs de importer** — el recomendado según el origen tiene una ★
- Form de credenciales que cambia por importer
- Botón "Importar" → ejecuta + muestra progreso en vivo

### Paso 3 — Vista de datos (estilo Power BI)
- **Sidebar derecha:** lista de tablas materializadas con row count + col count + tamaño
- **Grid central:** datos paginados, sortables por header, tipos de columna debajo del nombre, numeración sticky a la izquierda

## Importers disponibles

| Importer | Estado | Uso |
|---|---|---|
| **Sintético** | ✅ | Genera dummy data realista respetando schema. Heurísticas por nombre + tipo (impressions → int, fecha → date, advertiser → categórico kids/teens, etc.). Siempre funciona, no requiere creds. |
| **Power BI Dataflow** | ✅ | Device code flow contra Microsoft. Pega el código en el browser, autenticás, baja partitions del CDM. Requiere workspace Premium o "Bring your own data lake". |
| **CSV / Excel local** | ✅ | Drag & drop de uno o más archivos. Matchea por nombre de archivo (case-insensitive) con la tabla del reporte. |
| **SQL Database** | 🟡 stub | Form completo (Snowflake/SQL Server/Postgres/Redshift) pero el ejecutor está pendiente. |
| **Google Sheets** | 🟡 stub | Pendiente — necesita decisión OAuth vs Service Account. |

## Arquitectura

```
Modulo Importador y creador de dataset/
├── app.py                       Flask + endpoints REST
├── catalog.py                   parser del Excel → ReportInfo[TableInfo[Column]]
├── importers/
│   ├── base.py                  interfaz CredField + ImportState
│   ├── synthetic.py             dummy data por heurística
│   ├── pbi_dataflow.py          MSAL device flow + descarga partitions
│   ├── csv_upload.py            multi-file upload
│   ├── sql_database.py          stub
│   └── google_sheets.py         stub
├── templates/index.html         SPA single-page
├── static/
│   ├── app.css                  estilos (theme Kidscorp morado)
│   └── app.js                   3 vistas + polling de jobs
├── data/                        parquets materializados por reporte
│   └── {report_slug}/
│       └── {table}.parquet
└── requirements.txt
```

### Pipeline de un import

```
1. POST /api/import/<slug>           → arranca thread del importer, devuelve job_id
2. GET  /api/import/poll/<job_id>    → frontend poll cada 1.5s
3. importer escribe data/<slug>/<table>.parquet por cada tabla
4. al status=done, frontend cambia a vista de datos
5. GET  /api/dataset/<slug>/tables   → lista parquets + row counts
6. GET  /api/dataset/<slug>/data     → datos paginados (DuckDB read_parquet)
```

### Auto-detección del origen

El parser (`catalog.py`) lee la columna `QueryDefinition` de la hoja **Partitions** y aplica regex contra el M code:

| Patrón M | Origen detectado |
|---|---|
| `PowerPlatform.Dataflows` | `pbi_dataflow` |
| `Snowflake.Databases` | `snowflake` |
| `Sql.Database` | `sql_server` |
| `GoogleSheets.Contents` | `google_sheets` |
| `Excel.Workbook` | `excel` |
| `Csv.Document` | `csv` |
| Empieza con `CALENDAR(`, `SUMMARIZE`, `VAR `... | `dax_calculated` |
| Contiene `Table.FromRows` o `#table(` | `manual` |
| (otra cosa) | `other` / `unknown` |

## API REST (referencia)

| Método | Endpoint | Descripción |
|---|---|---|
| GET | `/` | SPA |
| GET | `/api/catalog` | Lista de reportes con sources |
| GET | `/api/report/<slug>` | Detalle de un reporte (tablas + columnas) |
| GET | `/api/importers` | Importers + sus campos de credenciales |
| POST | `/api/import/<slug>` | Arranca import job. Body JSON `{importer, ...creds}` o multipart si hay archivos. Devuelve `job_id`. |
| GET | `/api/import/poll/<job_id>` | Estado del job (`status`, `message`, `user_code`, `tables_imported`...) |
| GET | `/api/dataset/<slug>/tables` | Tablas materializadas (parquets) con row/col count |
| GET | `/api/dataset/<slug>/data?table=X&page=0&size=50&sort=col&dir=asc` | Datos paginados de una tabla |

## Cómo agregar un nuevo importer

1. Crear `importers/mi_importer.py` con una clase que herede de `BaseImporter`:
   ```python
   from .base import BaseImporter, CredField, ImportState

   class MyImporter(BaseImporter):
       name = "my_importer"
       label = "Mi Source"
       description = "Lee datos de X"
       credential_fields = [
           CredField(key="host", label="Host"),
           CredField(key="token", label="API Token", type="password"),
       ]

       def run(self, report, tables, creds, state: ImportState, data_dir):
           state.status = "running"
           # ... lógica de descarga ...
           # por cada tabla, escribir data_dir/{table}.parquet
           # actualizar state.tables_imported = {nombre: row_count}
           state.status = "done"
   ```

2. Registrar en `importers/__init__.py`:
   ```python
   from .my_importer import MyImporter
   IMPORTERS["my_importer"] = MyImporter()
   ```

3. **Bonus**: hacer que `catalog.py:detect_source` retorne `"my_importer"` para algún patrón M específico, así aparece como recomendado (★) cuando un reporte usa esa fuente.

## Troubleshooting

| Síntoma | Causa probable | Fix |
|---|---|---|
| `ModuleNotFoundError` al importar | Falta dependencia | `pip install -r requirements.txt` |
| El browser no abre solo | webbrowser bloqueado en Windows | Abrí manualmente `http://localhost:8766` |
| Encoding raro en console (`MesA�o`) | cp1252 en cmd Windows | `set PYTHONIOENCODING=utf-8` antes de correr |
| PBI: `Entity sin partition URLs` | Workspace no Premium / no expone CDM | Usar **Sintético** o ir al SQL upstream |
| PBI: `AADSTS70011` o auth bloqueada | Tenant restringe el client de Azure CLI | Registrar app pública propia y poner el ID en "Client ID" |
| Import dice `done` pero la grilla está vacía | Filename del CSV no matchea nombre de tabla | Renombrar archivo a `<TableName>.csv` (case-insensitive) |

## Storage

Cada reporte tiene su carpeta en `data/<slug>/` con un parquet por tabla:

```
data/
└── dashboard_margins/
    ├── Calendar.parquet
    ├── VIEW_REPORTING_MARGENES.parquet
    └── VIEW_REPORTING_MARGENES_POR_LI.parquet
```

Si re-importás el mismo reporte, los parquets se sobreescriben.

Para borrar un dataset: simplemente eliminá la carpeta `data/<slug>/`.

## Stack

- **Backend:** Flask 3, pandas, DuckDB (read_parquet)
- **Auth PBI:** MSAL Python (device code flow)
- **Frontend:** vanilla JS + CSS (sin frameworks ni build step)
- **Storage:** Parquet por tabla (lectura via DuckDB on-demand para paginación)

## Roadmap sugerido

- [ ] Implementar SQL real (priorizar Snowflake si esa es la fuente upstream de Kidscorp)
- [ ] Filtros por columna en el grid (input por header tipo Excel)
- [ ] Export del dataset completo (zip con parquets o un .duckdb)
- [ ] Persistir job history (cuándo se importó cada tabla, con qué importer, qué creds usadas — sin guardar passwords)
- [ ] Diff entre dos imports del mismo reporte (qué cambió)
- [ ] Sub-selección de tablas a importar (no siempre querés bajar todo)
