"""Router Ventas: upload CSV/Excel de ventas."""

from __future__ import annotations

import asyncio
import hashlib
import io
import uuid
from datetime import datetime, timezone
from typing import Optional

import json

import pandas as pd
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from sse_starlette.sse import EventSourceResponse
from supabase import Client

from core.auth import UserContext, require_roles
from db.supabase import get_supabase
from services import column_mapper

router = APIRouter()
_jobs: dict[str, dict] = {}

_COL_MAP = {
    "cartera": "cartera",
    "vendedor": "vendedor",
    "pdv codigo": "pdv_codigo",
    "pdv código": "pdv_codigo",
    "pdv_codigo": "pdv_codigo",
    "razon social": "razon_social",
    "razón social": "razon_social",
    "razon_social": "razon_social",
    "fecha comprobante": "fecha_comprobante",
    "fecha": "fecha_comprobante",
    "fecha_comprobante": "fecha_comprobante",
    "comprobante": "comprobante",
    "marca": "marca",
    "rubro": "rubro",
    "sku": "sku",
    "articulo": "articulo",
    "artículo": "articulo",
    "neto": "neto",
    "kilos": "kilos",
    "bultos": "bultos",
    "unidades": "unidades",
    "bonificadas": "bonificadas",
    "totales": "totales",
    "dia": "dia",
    "día": "dia",
    "mes": "mes",
    "anio": "anio",
    "año": "anio",
    "peso": "peso",
    "vendedor2": "vendedor2",
    "vendedor 2": "vendedor2",
    "categoria": "categoria",
    "categoría": "categoria",
    "equipo": "equipo",
    "canal": "canal",
    "supervisor": "supervisor",
}

KNOWN_COLS = set(_COL_MAP.values())
NUM_COLS = {"neto", "kilos", "bultos", "unidades", "bonificadas", "totales", "peso"}
INT_COLS = {"dia", "mes", "anio"}
CHUNK = 500


def _cast_types_ventas(df: pd.DataFrame) -> pd.DataFrame:
    """Cast types para ventas (columnas ya renombradas al schema canónico)."""
    from core.logging_config import get_logger as _get_logger
    _logger = _get_logger("ventas")
    canonical = column_mapper.all_fields("ventas")
    df = df[[c for c in df.columns if c in canonical]]

    # Construir fecha desde dia/mes/anio si están presentes (más confiable que parsear string)
    if {"dia", "mes", "anio"}.issubset(df.columns):
        def _build_fecha(row):
            try:
                return f"{int(row['anio']):04d}-{int(row['mes']):02d}-{int(row['dia']):02d}"
            except (ValueError, TypeError):
                return None
        df["fecha_comprobante"] = df.apply(_build_fecha, axis=1)
        _logger.debug("fecha_comprobante construida desde dia/mes/anio")
    elif "fecha_comprobante" in df.columns:
        parsed = None
        for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
            try:
                parsed = pd.to_datetime(df["fecha_comprobante"], format=fmt, errors="coerce")
                if parsed.notna().sum() > 0:
                    break
            except Exception:
                continue
        if parsed is None:
            parsed = pd.to_datetime(df["fecha_comprobante"], errors="coerce", dayfirst=True)
        df["fecha_comprobante"] = parsed.dt.strftime("%Y-%m-%d")

    for c in NUM_COLS & set(df.columns):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in INT_COLS & set(df.columns):
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    df = df.where(df.notnull(), None)
    for c in INT_COLS & set(df.columns):
        df[c] = df[c].apply(lambda v: int(v) if pd.notna(v) else None)
    for c in NUM_COLS & set(df.columns):
        df[c] = df[c].apply(lambda v: float(v) if pd.notna(v) else None)
    return df


def _upsert_chunk(sb: Client, rows: list[dict]) -> int:
    res = sb.table("ventas").upsert(rows, on_conflict="tenant_id,comprobante,sku").execute()
    return len(res.data) if res and res.data else 0


def _touch_sales_update(sb: Client, user_id: str, tenant_id: str) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    # Usar clave con prefijo de tenant para que cada distribuidora tenga su propio timestamp
    key = f"tenant:{tenant_id}:ventas_ultima_actualizacion"
    sb.table("config").upsert(
        {
            "key": key,
            "value": {"timestamp": now_iso},
            "updated_at": now_iso,
            "updated_by": user_id,
        },
        on_conflict="key",
    ).execute()


def _refresh_resumen_mes_if_available(sb: Client) -> None:
    try:
        sb.rpc("refresh_resumen_mes").execute()
    except Exception as exc:
        print(f"[ventas] refresh_resumen_mes unavailable: {exc}")


def _sales_status(sb: Client, tenant_id: str | None) -> dict:
    q = sb.table("ventas").select("id", count="exact", head=True)
    if tenant_id:
        q = q.eq("tenant_id", tenant_id)
    total = q.execute().count or 0
    latest_date = None
    if total > 0:
        try:
            q2 = sb.table("ventas").select("fecha_comprobante").order("fecha_comprobante", desc=True).limit(1)
            if tenant_id:
                q2 = q2.eq("tenant_id", tenant_id)
            row = q2.single().execute()
            latest_date = row.data.get("fecha_comprobante") if row.data else None
        except Exception:
            pass
    config_row = None
    if tenant_id:
        key = f"tenant:{tenant_id}:ventas_ultima_actualizacion"
        res = sb.table("config").select("value, updated_at").eq("key", key).maybe_single().execute()
        config_row = res.data if res is not None else None
    value = (config_row or {}).get("value") or {}
    return {
        "total": total,
        "last_update": value.get("timestamp") or (config_row or {}).get("updated_at"),
        "latest_sale_date": latest_date,
    }


@router.post("/upload")
async def upload_ventas(
    file: UploadFile = File(...),
    mapping: str | None = Form(None, description="JSON con mapeo de columnas: {COL_EXCEL: campo_canonico}"),
    save_as_default: bool = Form(False, description="Guardar este mapeo como template default"),
    mapping_name: str = Form("Template principal", description="Nombre del template a guardar"),
    user: UserContext = Depends(require_roles("superadmin", "admin", "analista")),
    sb: Client = Depends(get_supabase),
):
    """Upload CSV o Excel de ventas, aplica mapeo de columnas y hace upsert."""
    from core.logging_config import get_logger as _get_logger
    _logger = _get_logger("ventas")

    ext = (file.filename or "").rsplit(".", 1)[-1].lower()
    if ext not in ("csv", "xlsx", "xls"):
        raise HTTPException(400, "Solo se aceptan archivos .csv, .xlsx o .xls")

    contents = await file.read()
    if len(contents) > 500 * 1024 * 1024:
        raise HTTPException(400, "Archivo demasiado grande (máx 500 MB)")

    try:
        if ext == "csv":
            for enc in ("utf-8", "latin-1", "cp1252"):
                try:
                    df = pd.read_csv(io.BytesIO(contents), dtype=str, encoding=enc)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                raise HTTPException(400, "No se pudo decodificar el CSV (probé utf-8, latin-1, cp1252)")
        else:
            df = pd.read_excel(io.BytesIO(contents), dtype=str)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(400, f"Error al leer archivo: {exc}")

    if df.empty:
        raise HTTPException(400, "El archivo está vacío")

    # Resolver mapping
    if mapping:
        try:
            mappings = json.loads(mapping)
        except Exception:
            raise HTTPException(400, "El parámetro 'mapping' no es un JSON válido")
        _logger.debug("Usando mapping provisto en el request para ventas")
    else:
        tpl = column_mapper.get_default_template(sb, user.tenant_id, "ventas")
        if tpl:
            mappings = tpl["mappings"]
            _logger.debug("Usando template guardado '%s' para ventas", tpl.get("nombre"))
        else:
            mappings = column_mapper.detect_columns(df.columns.tolist(), "ventas")
            _logger.debug("Sin template guardado, usando auto-detección para ventas")

    df = column_mapper.apply_mapping(df, mappings)
    df = _cast_types_ventas(df)

    # Validar campos requeridos para el upsert
    missing = column_mapper.required_fields("ventas") - set(df.columns)
    if missing:
        raise HTTPException(
            400,
            f"Faltan campos requeridos: {sorted(missing)}. "
            "Revisá el mapeo de columnas.",
        )

    # Generar comprobante sintético si no viene en el archivo (necesario para el upsert conflict key).
    # Hash determinístico: misma fila → mismo hash, así re-subidas no crean duplicados.
    if "comprobante" not in df.columns:
        key_cols = [c for c in ["pdv_codigo", "fecha_comprobante", "sku", "neto"] if c in df.columns]
        df["comprobante"] = (
            df[key_cols].astype(str).agg("||".join, axis=1)
            .apply(lambda s: "auto:" + hashlib.md5(s.encode()).hexdigest()[:12])
        )
    # sku es el otro eje del conflict key: usar cadena vacía si no viene en el archivo
    if "sku" not in df.columns:
        df["sku"] = ""

    df = df.drop_duplicates(subset=["comprobante", "sku"])
    df["tenant_id"] = user.tenant_id

    rows = df.to_dict(orient="records")
    total = len(rows)
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"total": total, "processed": 0, "errors": 0, "status": "running"}

    # Guardar template si se solicitó
    if save_as_default and mappings:
        try:
            column_mapper.save_template(sb, user.tenant_id, "ventas", mapping_name, mappings, es_default=True)
            _logger.info("Template ventas '%s' guardado para tenant %s", mapping_name, user.tenant_id)
        except Exception as exc:
            _logger.warning("No se pudo guardar template ventas: %s", exc)

    async def _process():
        processed = errors = 0
        for i in range(0, len(rows), CHUNK):
            chunk = rows[i : i + CHUNK]
            try:
                _upsert_chunk(sb, chunk)
            except Exception as exc:
                _logger.error("Upsert error chunk %d: %s", i, exc)
                errors += len(chunk)
            processed += len(chunk)
            _jobs[job_id].update({"processed": processed, "errors": errors})
            await asyncio.sleep(0)
        _refresh_resumen_mes_if_available(sb)
        if user.tenant_id:
            _touch_sales_update(sb, user.uid, user.tenant_id)
        _jobs[job_id]["status"] = "done"

    asyncio.create_task(_process())
    return {"job_id": job_id, "total_rows": total}


@router.get("/upload/progress/{job_id}")
async def ventas_progress(job_id: str, token: str = Query("")):
    """SSE stream for upload progress."""
    if not token:
        raise HTTPException(401, "Token requerido")
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

    async def event_stream():
        import json as _json

        while True:
            data = _jobs.get(job_id, {"status": "not_found"})
            yield {"data": _json.dumps(data)}
            if data.get("status") in ("done", "not_found") or str(data.get("status", "")).startswith("error"):
                break
            await asyncio.sleep(1)
        await asyncio.sleep(60)
        _jobs.pop(job_id, None)

    return EventSourceResponse(event_stream())


@router.get("/stats")
async def ventas_stats(
    user: UserContext = Depends(require_roles("superadmin", "admin", "analista")),
    sb: Client = Depends(get_supabase),
    tenant_id: Optional[str] = Query(None, description="Filtrar por tenant (solo superadmin)"),
):
    effective = tenant_id if user.is_superadmin else user.tenant_id
    return _sales_status(sb, effective)
