"""Router Ventas: upload CSV/Excel de ventas."""

from __future__ import annotations

import asyncio
import io
import uuid
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from sse_starlette.sse import EventSourceResponse
from supabase import Client

from core.auth import UserContext, require_roles
from db.supabase import get_supabase

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


def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    col_rename = {}
    for c in df.columns:
        k = c.strip().lower()
        if k in _COL_MAP:
            col_rename[c] = _COL_MAP[k]
        elif k in KNOWN_COLS:
            col_rename[c] = k
    print(f"[ventas] Original headers: {list(df.columns)}")
    print(f"[ventas] Mapped columns: {col_rename}")
    df = df.rename(columns=col_rename)
    df = df[[c for c in df.columns if c in KNOWN_COLS]]
    print(f"[ventas] Final columns: {list(df.columns)}")

    # Si existen las columnas enteras dia/mes/anio, construir fecha desde ellas
    # (más confiable que parsear el string con formato argentino dd/mm/yyyy)
    if {"dia", "mes", "anio"}.issubset(df.columns):
        def _build_fecha(row):
            try:
                return f"{int(row['anio']):04d}-{int(row['mes']):02d}-{int(row['dia']):02d}"
            except (ValueError, TypeError):
                return None
        df["fecha_comprobante"] = df.apply(_build_fecha, axis=1)
        print(f"[ventas] fecha_comprobante construida desde dia/mes/anio. Muestra: {df['fecha_comprobante'].dropna().head(3).tolist()}")
    elif "fecha_comprobante" in df.columns:
        # Fallback: intentar parsear el string con múltiples formatos
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
        print(f"[ventas] fecha_comprobante parseada desde string. Nulos: {df['fecha_comprobante'].isna().sum()}")
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
    res = sb.table("ventas").upsert(rows, on_conflict="comprobante,sku").execute()
    return len(res.data) if res and res.data else 0


def _touch_sales_update(sb: Client, user_id: str) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    sb.table("config").upsert(
        {
            "key": "ventas_ultima_actualizacion",
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


def _sales_status(sb: Client) -> dict:
    total = sb.table("ventas").select("id", count="exact", head=True).execute().count or 0
    latest_date = None
    if total > 0:
        try:
            row = sb.table("ventas").select("fecha_comprobante").order("fecha_comprobante", desc=True).limit(1).single().execute()
            latest_date = row.data.get("fecha_comprobante") if row.data else None
        except Exception:
            pass
    config_row = (
        sb.table("config")
        .select("value, updated_at")
        .eq("key", "ventas_ultima_actualizacion")
        .maybe_single()
        .execute()
        .data
    )
    value = (config_row or {}).get("value") or {}
    return {
        "total": total,
        "last_update": value.get("timestamp") or (config_row or {}).get("updated_at"),
        "latest_sale_date": latest_date,
    }


@router.post("/upload")
async def upload_ventas(
    file: UploadFile = File(...),
    user: UserContext = Depends(require_roles("admin")),
    sb: Client = Depends(get_supabase),
):
    """Upload CSV or Excel file, upsert into ventas table."""
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
                raise ValueError("No se pudo decodificar el CSV (probé utf-8, latin-1, cp1252)")
        else:
            df = pd.read_excel(io.BytesIO(contents), dtype=str)
    except Exception as exc:
        raise HTTPException(400, f"Error al leer archivo: {exc}")

    df = _clean_df(df)
    if "comprobante" not in df.columns or "sku" not in df.columns:
        raise HTTPException(400, "Columnas 'Comprobante' y 'SKU' son obligatorias (clave para upsert)")

    df = df.drop_duplicates(subset=["comprobante", "sku"])
    rows = df.to_dict(orient="records")
    total = len(rows)
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"total": total, "processed": 0, "errors": 0, "status": "running"}

    async def _process():
        processed = errors = 0
        for i in range(0, len(rows), CHUNK):
            chunk = rows[i : i + CHUNK]
            try:
                _upsert_chunk(sb, chunk)
            except Exception as exc:
                print(f"[ventas] upsert error chunk {i}: {exc}")
                errors += len(chunk)
            processed += len(chunk)
            _jobs[job_id].update({"processed": processed, "errors": errors})
            await asyncio.sleep(0)
        _refresh_resumen_mes_if_available(sb)
        _touch_sales_update(sb, user.uid)
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
    user: UserContext = Depends(require_roles("admin", "analista")),
    sb: Client = Depends(get_supabase),
):
    return _sales_status(sb)
