"""Router Mapa — GeoJSON endpoint for map markers."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query
from supabase import Client

from core.auth import UserContext, get_current_user
from db.supabase import get_supabase

router = APIRouter()


@router.get("/geojson")
async def pdv_geojson(
    user: UserContext = Depends(get_current_user),
    sb: Client = Depends(get_supabase),
    tenant_id: Optional[str] = Query(None, description="Filtrar por tenant (solo superadmin)"),
    cartera: str = Query("", description="Filtrar por cartera"),
    zona: str = Query("", description="Filtrar por zona"),
    canal: str = Query("", description="Filtrar por canal de venta"),
    localidad: str = Query("", description="Filtrar por localidad"),
):
    """Return geocoded PDVs as GeoJSON FeatureCollection."""
    def _build_q():
        q = sb.table("pdv").select(
            "id, cod_cliente, razon_social, domicilio, localidad, cartera, zona, "
            "canal_vta, vendedor, lat, lng, geocoding_status, tel_movil, categoria_iva"
        ).eq("geocoding_status", "ok")
        if user.is_superadmin:
            if tenant_id:
                q = q.eq("tenant_id", tenant_id)
        else:
            q = q.eq("tenant_id", user.tenant_id)
        if cartera:
            q = q.eq("cartera", cartera)
        if zona:
            q = q.eq("zona", zona)
        if canal:
            q = q.eq("canal_vta", canal)
        if localidad:
            q = q.eq("localidad", localidad)
        return q

    rows: list[dict] = []
    offset = 0
    while True:
        batch = _build_q().order("id").range(offset, offset + 999).execute()
        data = batch.data or []
        rows.extend(data)
        if len(data) < 1000:
            break
        offset += 1000

    features = []
    for r in rows:
        if r.get("lat") is None or r.get("lng") is None:
            continue
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [r["lng"], r["lat"]],
            },
            "properties": {
                "id": r["id"],
                "cod_cliente": r.get("cod_cliente"),
                "razon_social": r.get("razon_social"),
                "domicilio": r.get("domicilio"),
                "localidad": r.get("localidad"),
                "cartera": r.get("cartera"),
                "zona": r.get("zona"),
                "canal_vta": r.get("canal_vta"),
                "vendedor": r.get("vendedor"),
                "tel_movil": r.get("tel_movil"),
                "categoria_iva": r.get("categoria_iva"),
            },
        })

    return {
        "type": "FeatureCollection",
        "features": features,
    }


@router.get("/filtros")
async def filtros_mapa(
    user: UserContext = Depends(get_current_user),
    sb: Client = Depends(get_supabase),
    tenant_id: Optional[str] = Query(None, description="Filtrar por tenant (solo superadmin)"),
):
    """Return distinct values for filter dropdowns."""
    def _fetch_col(col: str) -> list[dict]:
        def _build_col_q():
            q = sb.table("pdv").select(col).eq("geocoding_status", "ok")
            if user.is_superadmin:
                if tenant_id:
                    q = q.eq("tenant_id", tenant_id)
            else:
                q = q.eq("tenant_id", user.tenant_id)
            return q
        rows: list[dict] = []
        offset = 0
        while True:
            batch = _build_col_q().range(offset, offset + 999).execute()
            data = batch.data or []
            rows.extend(data)
            if len(data) < 1000:
                break
            offset += 1000
        return rows

    carteras    = _fetch_col("cartera")
    zonas       = _fetch_col("zona")
    canales     = _fetch_col("canal_vta")
    localidades = _fetch_col("localidad")

    def unique_sorted(data, key):
        return sorted(set(r[key] for r in (data or []) if r.get(key)))

    return {
        "carteras":   unique_sorted(carteras,    "cartera"),
        "zonas":      unique_sorted(zonas,       "zona"),
        "canales":    unique_sorted(canales,     "canal_vta"),
        "localidades": unique_sorted(localidades, "localidad"),
    }
