"""
Importer Power BI Dataflow — device code flow + descarga de partitions.
"""
from __future__ import annotations
import io
import threading
from pathlib import Path
import pandas as pd
import requests
from .base import BaseImporter, CredField, ImportState

PBI_BASE = "https://api.powerbi.com/v1.0/myorg"
SCOPES = ["https://analysis.windows.net/powerbi/api/.default"]
DEFAULT_CLIENT_ID = "1950a258-227b-4e31-a9cf-717495945fc2"


class PBIDataflowImporter(BaseImporter):
    name = "pbi_dataflow"
    label = "Power BI Dataflow"
    description = "Autenticación delegada (device code) contra Microsoft Power BI."
    credential_fields = [
        CredField(key="tenant_id",   label="Tenant ID", default="common",
                  hint="Directorio Azure AD de tu organización. Usá 'common' si no sabés."),
        CredField(key="workspace_id", label="Workspace ID",
                  placeholder="00000000-0000-0000-0000-000000000000"),
        CredField(key="dataflow_id",  label="Dataflow ID",
                  placeholder="00000000-0000-0000-0000-000000000000"),
        CredField(key="client_id",    label="Client ID (opcional)", required=False,
                  placeholder="vacío = Azure CLI public client",
                  hint="Solo si tu tenant bloquea el client default."),
    ]

    def run(self, report, tables, creds, state: ImportState, data_dir: Path):
        # Esto bloquea el thread esperando el device code → corremos en otro thread
        # que llena state. El Flask endpoint hace el polling.
        th = threading.Thread(target=self._do, args=(report, tables, creds, state, data_dir), daemon=True)
        th.start()

    def _do(self, report, tables, creds, state: ImportState, data_dir: Path):
        try:
            import msal
            tenant = creds.get("tenant_id") or "common"
            client = creds.get("client_id") or DEFAULT_CLIENT_ID
            ws = creds["workspace_id"]; df = creds["dataflow_id"]

            app = msal.PublicClientApplication(
                client_id=client,
                authority=f"https://login.microsoftonline.com/{tenant}",
            )
            flow = app.initiate_device_flow(scopes=SCOPES)
            if "user_code" not in flow:
                state.status = "error"; state.message = f"Device flow init failed: {flow}"
                return
            state.status = "waiting_auth"
            state.user_code = flow["user_code"]
            state.verification_uri = flow["verification_uri"]
            state.message = f"Pegá el código {flow['user_code']} en {flow['verification_uri']}"

            result = app.acquire_token_by_device_flow(flow)
            if "access_token" not in result:
                state.status = "error"; state.message = f"Auth failed: {result.get('error_description')}"
                return

            token = result["access_token"]
            state.status = "running"
            state.message = "Autenticado. Bajando model.json del dataflow..."

            s = requests.Session()
            s.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/json"})
            r = s.get(f"{PBI_BASE}/groups/{ws}/dataflows/{df}", timeout=60)
            if not r.ok:
                state.status = "error"; state.message = f"PBI {r.status_code}: {r.text[:300]}"
                return
            model = r.json()
            entities = model.get("entities") or model.get("Entities") or []

            data_dir.mkdir(parents=True, exist_ok=True)
            for t in tables:
                state.message = f"Bajando {t.name}..."
                ent = next((e for e in entities if (e.get("name") or e.get("Name")) == t.name), None)
                if not ent:
                    state.message = f"Entity '{t.name}' no encontrada — salteo"
                    continue
                partitions = ent.get("partitions") or ent.get("Partitions") or []
                if not partitions:
                    state.status = "error"
                    state.message = (f"Entity '{t.name}' sin partition URLs. "
                                     "El workspace probablemente no es Premium. "
                                     "Usá Sintético como alternativa.")
                    return
                frames = []
                for p in partitions:
                    loc = p.get("location") or p.get("Location")
                    if not loc:
                        continue
                    rr = s.get(loc, timeout=120)
                    if not rr.ok:
                        state.status = "error"; state.message = f"GET partition {rr.status_code}"
                        return
                    frames.append(pd.read_csv(io.BytesIO(rr.content), header=None))
                df_data = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
                attrs = ent.get("attributes") or ent.get("Attributes") or []
                if attrs and len(attrs) == df_data.shape[1]:
                    df_data.columns = [a.get("name") or a.get("Name") for a in attrs]
                out = data_dir / f"{_safe(t.name)}.parquet"
                df_data.to_parquet(out, index=False)
                state.tables_imported[t.name] = len(df_data)

            state.status = "done"
            state.message = f"OK. {len(state.tables_imported)} tablas descargadas."
        except Exception as e:
            state.status = "error"
            state.message = f"{type(e).__name__}: {e}"


def _safe(n: str) -> str:
    import re
    return re.sub(r"[^A-Za-z0-9_\-]", "_", n)
