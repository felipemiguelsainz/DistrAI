"""Router PDV: upload CSV/Excel, geocoding, CRUD."""

from __future__ import annotations

import asyncio
import io
import uuid
from datetime import datetime, timezone
from typing import Optional

import json

import pandas as pd
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from sse_starlette.sse import EventSourceResponse
from supabase import Client

from core.auth import UserContext, get_current_user, require_roles
from core.logging_config import get_logger
from db.supabase import get_supabase
from services import column_mapper
from services.geocoding import geocode_pending

logger = get_logger("pdv")

router = APIRouter()

# ── In-memory progress store (per upload job) ──────────────────────
_jobs: dict[str, dict] = {}

# Column mapping: CSV/Excel header (lowercase, stripped) → DB column
_COL_MAP_RAW = {
    "fecha alta": "fecha_alta",
    "fecha de alta": "fecha_alta",
    "última vta": "ultima_vta",
    "ultima vta": "ultima_vta",
    "última venta": "ultima_vta",
    "ultima venta": "ultima_vta",
    "pdv codigo": "pdv_codigo",
    "pdv código": "pdv_codigo",
    "cod. cliente": "cod_cliente",
    "cod cliente": "cod_cliente",
    "código cliente": "cod_cliente",
    "codigo cliente": "cod_cliente",
    "razon social": "razon_social",
    "razón social": "razon_social",
    "domicilio": "domicilio",
    "dirección": "domicilio",
    "direccion": "domicilio",
    "localidad": "localidad",
    "ciudad": "localidad",
    "tel. móvil": "tel_movil",
    "tel movil": "tel_movil",
    "tel. movil": "tel_movil",
    "telefono": "tel_movil",
    "teléfono": "tel_movil",
    "otro tel.": "otro_tel",
    "otro tel": "otro_tel",
    "cat.": "cat",
    "cat": "cat",
    "categoría": "cat",
    "categoria": "cat",
    "cartera": "cartera",
    "vendedor": "vendedor",
    "acuerdos comerciales": "acuerdos_comerciales",
    "zona": "zona",
    "obs. internas": "obs_internas",
    "obs internas": "obs_internas",
    "obs. logística": "obs_logistica",
    "obs logistica": "obs_logistica",
    "obs. logistica": "obs_logistica",
    "obs. facturas": "obs_facturas",
    "obs facturas": "obs_facturas",
    "canal distribución": "canal_distribucion",
    "canal distribucion": "canal_distribucion",
    "canal distribuc.": "canal_distribucion",
    "canal vta.": "canal_vta",
    "canal vta": "canal_vta",
    "canal venta": "canal_vta",
    "categoría iva": "categoria_iva",
    "categoria iva": "categoria_iva",
    "cuit": "cuit",
    "frec. de visita": "frecuencia_visita",
    "frec de visita": "frecuencia_visita",
    "frecuencia visita": "frecuencia_visita",
    "frecuencia de visita": "frecuencia_visita",
    "visitar esta semana": "visitar_esta_semana",
    "lun": "lun", "mar": "mar", "mié": "mie", "mie": "mie", "jue": "jue",
    "vie": "vie", "sáb": "sab", "sab": "sab", "dom": "dom",
    "hs. lun": "hs_lun", "hs lun": "hs_lun",
    "hs. mar": "hs_mar", "hs mar": "hs_mar",
    "hs. mié": "hs_mie", "hs mie": "hs_mie", "hs. mie": "hs_mie",
    "hs. jue": "hs_jue", "hs jue": "hs_jue",
    "hs. vie": "hs_vie", "hs vie": "hs_vie",
    "hs. sáb": "hs_sab", "hs sab": "hs_sab", "hs. sab": "hs_sab",
    "hs. dom": "hs_dom", "hs dom": "hs_dom",
    "prioridad preparado": "prioridad_preparado",
    "lat": "lat", "latitud": "lat",
    "lng": "lng", "lon": "lng", "longitud": "lng", "long": "lng",
}

BOOL_COLS = {"visitar_esta_semana", "lun", "mar", "mie", "jue", "vie", "sab", "dom"}
DATE_COLS = {"fecha_alta", "ultima_vta"}

CHUNK = 500  # rows per upsert batch


def _parse_bool(val) -> Optional[bool]:
    if pd.isna(val):
        return None
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    return s in ("si", "sí", "1", "true", "yes", "x")


def _cast_types_pdv(df: pd.DataFrame) -> pd.DataFrame:
    """Cast types para PDV (columnas ya renombradas al schema canónico)."""
    canonical = column_mapper.all_fields("pdv")
    df = df[[c for c in df.columns if c in canonical]]
    for c in BOOL_COLS & set(df.columns):
        df[c] = df[c].apply(_parse_bool)
    for c in DATE_COLS & set(df.columns):
        df[c] = pd.to_datetime(df[c], errors="coerce", dayfirst=True).dt.strftime("%Y-%m-%d")
    df = df.where(df.notnull(), None)
    for c in ("lat", "lng"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _upsert_chunk(sb: Client, rows: list[dict]) -> int:
    """Upsert a chunk to pdv table. Returns number of upserted rows."""
    res = sb.table("pdv").upsert(
        rows,
        on_conflict="tenant_id,cod_cliente",
    ).execute()
    return len(res.data) if res and res.data else 0


# ── Upload endpoint ────────────────────────────────────────────────
@router.post("/upload")
async def upload_pdv(
    file: UploadFile = File(...),
    mapping: str | None = Form(None, description="JSON con mapeo de columnas: {COL_EXCEL: campo_canonico}"),
    save_as_default: bool = Form(False, description="Guardar este mapeo como template default"),
    mapping_name: str = Form("Template principal", description="Nombre del template a guardar"),
    user: UserContext = Depends(require_roles("superadmin", "admin", "analista")),
    sb: Client = Depends(get_supabase),
):
    """Upload CSV o Excel de maestro PDV, aplica mapeo de columnas y hace upsert."""
    ext = (file.filename or "").rsplit(".", 1)[-1].lower()
    if ext not in ("csv", "xlsx", "xls"):
        raise HTTPException(400, "Solo se aceptan archivos .csv, .xlsx o .xls")

    contents = await file.read()
    if len(contents) > 50 * 1024 * 1024:
        raise HTTPException(400, "Archivo demasiado grande (máx 50 MB)")

    # Parse
    try:
        if ext == "csv":
            for enc in ("utf-8", "latin-1", "cp1252"):
                try:
                    df = pd.read_csv(io.BytesIO(contents), dtype=str, encoding=enc)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                raise HTTPException(400, "No se pudo decodificar el archivo CSV")
        else:
            df = pd.read_excel(io.BytesIO(contents), dtype=str)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(400, f"Error al leer archivo: {exc}")

    if df.empty:
        raise HTTPException(400, "El archivo está vacío")

    # Resolver mapping: parámetro JSON > template default en DB > auto-detección
    if mapping:
        try:
            mappings = json.loads(mapping)
        except Exception:
            raise HTTPException(400, "El parámetro 'mapping' no es un JSON válido")
        logger.debug("Usando mapping provisto en el request para PDV")
    else:
        tpl = column_mapper.get_default_template(sb, user.tenant_id, "pdv")
        if tpl:
            mappings = tpl["mappings"]
            logger.debug("Usando template guardado '%s' para PDV", tpl.get("nombre"))
        else:
            mappings = column_mapper.detect_columns(df.columns.tolist(), "pdv")
            logger.debug("Sin template guardado, usando auto-detección para PDV")

    df = column_mapper.apply_mapping(df, mappings)
    df = _cast_types_pdv(df)

    if "cod_cliente" not in df.columns:
        raise HTTPException(
            400,
            "No se encontró el campo 'cod_cliente' (requerido). "
            "Revisá el mapeo de columnas o cargá un archivo con las columnas correctas.",
        )

    # Drop rows without cod_cliente
    df = df.dropna(subset=["cod_cliente"])
    # Deduplicate: keep last occurrence of each cod_cliente (handles dupes within file)
    df = df.drop_duplicates(subset=["cod_cliente"], keep="last")

    # ── Geocoding reset ───────────────────────────────────────────────
    # If the CSV provides explicit lat/lng, honour them; otherwise reset geocoding
    # so updated addresses get re-geocoded on the next geocode run.
    has_lat = "lat" in df.columns
    has_lng = "lng" in df.columns
    if has_lat and has_lng:
        both_present = df["lat"].notna() & df["lng"].notna()
        df.loc[both_present, "geocoding_status"] = "ok"
        df.loc[~both_present, "geocoding_status"] = "pending"
        df.loc[~both_present, "geocoding_attempts"] = 0
    else:
        # No coords supplied → mark all rows for (re-)geocoding
        df["geocoding_status"] = "pending"
        df["geocoding_attempts"] = 0
        if has_lat:
            df["lat"] = None
        if has_lng:
            df["lng"] = None

    # Agregar tenant_id y timestamp a cada fila
    df["tenant_id"] = user.tenant_id
    df["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    total = len(df)
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"total": total, "processed": 0, "errors": 0, "status": "running"}

    # Guardar template si se solicitó (antes de lanzar el job async)
    if save_as_default and mappings:
        try:
            column_mapper.save_template(sb, user.tenant_id, "pdv", mapping_name, mappings, es_default=True)
            logger.info("Template PDV '%s' guardado para tenant %s", mapping_name, user.tenant_id)
        except Exception as exc:
            logger.warning("No se pudo guardar template PDV: %s", exc)

    async def _process():
        processed = 0
        errors = 0
        try:
            for start in range(0, total, CHUNK):
                chunk = df.iloc[start : start + CHUNK]
                rows = chunk.to_dict("records")
                try:
                    _upsert_chunk(sb, rows)
                except Exception as exc:
                    logger.error("Upsert error at chunk offset %d: %s", start, exc)
                    errors += len(rows)
                processed += len(rows)
                _jobs[job_id] = {"total": total, "processed": processed, "errors": errors, "status": "running"}
            _jobs[job_id]["status"] = "done"
        except Exception as exc:
            _jobs[job_id]["status"] = f"error: {exc}"

    asyncio.create_task(_process())

    return {"job_id": job_id, "total_rows": total, "message": f"Procesando {total} filas..."}


# ── SSE progress endpoint ─────────────────────────────────────────
@router.get("/upload/progress/{job_id}")
async def upload_progress(job_id: str, token: str = Query("")):
    """Stream upload progress via SSE (token via query param)."""
    if not token:
        raise HTTPException(401, "Token requerido")
    # Validate token
    try:
        sb = get_supabase()
        user_response = sb.auth.get_user(token)
        if not user_response.user:
            raise HTTPException(401, "Token inválido")
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(401, "Token inválido")
    if job_id not in _jobs:
        raise HTTPException(404, "Job no encontrado")

    async def event_gen():
        import json
        while True:
            job = _jobs.get(job_id)
            if not job:
                break
            yield {"data": json.dumps(job)}
            if job["status"] in ("done",) or job["status"].startswith("error"):
                break
            await asyncio.sleep(0.5)
        # Clean up after 60s
        await asyncio.sleep(60)
        _jobs.pop(job_id, None)

    return EventSourceResponse(event_gen())


# ── Geocoding trigger ──────────────────────────────────────────────
@router.post("/geocode")
async def trigger_geocode(
    user: UserContext = Depends(require_roles("superadmin", "admin")),
    sb: Client = Depends(get_supabase),
    limit: int = Query(100, le=9999, description="Max rows to geocode"),
    tenant_id: Optional[str] = Query(None, description="Tenant a geocodificar (solo superadmin)"),
):
    """Geocode PDV rows with status='pending'. Runs in background."""
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"total": 0, "processed": 0, "errors": 0, "status": "running"}
    effective_tenant = tenant_id if user.is_superadmin else user.tenant_id

    async def _run():
        try:
            await geocode_pending(sb, limit, _jobs, job_id, tenant_id=effective_tenant)
            _jobs[job_id]["status"] = "done"
        except Exception as exc:
            logger.error("Geocode job %s failed: %s", job_id, exc, exc_info=True)
            _jobs[job_id]["status"] = f"error: {exc}"

    asyncio.create_task(_run())
    return {"job_id": job_id, "message": f"Geocodificando hasta {limit} PDVs..."}


# ── List PDVs ──────────────────────────────────────────────────────
@router.get("/")
async def list_pdv(
    user: UserContext = Depends(get_current_user),
    sb: Client = Depends(get_supabase),
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    search: str = Query("", description="Buscar por razón social o cod_cliente"),
):
    """List PDVs with pagination and optional search."""
    q = sb.table("pdv").select("*", count="exact")
    if not user.is_superadmin:
        q = q.eq("tenant_id", user.tenant_id)
    if search:
        safe = (
            search.replace("\\", "\\\\").replace("%", "\\%")
            .replace("_", "\\_").replace(",", "").replace(")", "").replace("(", "")
        )
        q = q.or_(f"razon_social.ilike.%{safe}%,cod_cliente.ilike.%{safe}%")
    q = q.order("id", desc=True).range(offset, offset + limit - 1)
    res = q.execute()
    return {
        "data": res.data or [],
        "count": res.count or 0,
    }


# ── Stats ──────────────────────────────────────────────────────────
@router.get("/stats")
async def pdv_stats(
    user: UserContext = Depends(get_current_user),
    sb: Client = Depends(get_supabase),
):
    """Quick stats: total, geocoded, pending."""
    def _q():
        q = sb.table("pdv").select("id", count="exact")
        if not user.is_superadmin:
            q = q.eq("tenant_id", user.tenant_id)
        return q

    total_res   = _q().execute()
    geo_ok      = _q().eq("geocoding_status", "ok").execute()
    geo_pending = _q().eq("geocoding_status", "pending").execute()
    geo_failed  = _q().eq("geocoding_status", "failed").execute()
    return {
        "total": total_res.count or 0,
        "geocoded": geo_ok.count or 0,
        "pending": geo_pending.count or 0,
        "failed": geo_failed.count or 0,
    }
