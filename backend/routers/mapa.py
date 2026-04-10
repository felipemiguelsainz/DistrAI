"""Router Mapa — GeoJSON endpoint for map markers."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from supabase import Client

from core.auth import UserContext, get_current_user
from db.supabase import get_supabase

router = APIRouter()


@router.get("/geojson")
async def pdv_geojson(
    user: UserContext = Depends(get_current_user),
    sb: Client = Depends(get_supabase),
    cartera: str = Query("", description="Filtrar por cartera"),
    zona: str = Query("", description="Filtrar por zona"),
    canal: str = Query("", description="Filtrar por canal de venta"),
    localidad: str = Query("", description="Filtrar por localidad"),
):
    """Return geocoded PDVs as GeoJSON FeatureCollection."""
    q = sb.table("pdv").select(
        "id, cod_cliente, razon_social, domicilio, localidad, cartera, zona, "
        "canal_vta, vendedor, lat, lng, geocoding_status, tel_movil, categoria_iva"
    ).eq("geocoding_status", "ok")

    if cartera:
        q = q.eq("cartera", cartera)
    if zona:
        q = q.eq("zona", zona)
    if canal:
        q = q.eq("canal_vta", canal)
    if localidad:
        q = q.eq("localidad", localidad)

    res = q.order("id").execute()
    rows = res.data or []

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
):
    """Return distinct values for filter dropdowns."""
    carteras = sb.table("pdv").select("cartera").eq("geocoding_status", "ok").execute()
    zonas = sb.table("pdv").select("zona").eq("geocoding_status", "ok").execute()
    canales = sb.table("pdv").select("canal_vta").eq("geocoding_status", "ok").execute()
    localidades = sb.table("pdv").select("localidad").eq("geocoding_status", "ok").execute()

    def unique_sorted(data, key):
        return sorted(set(r[key] for r in (data or []) if r.get(key)))

    return {
        "carteras": unique_sorted(carteras.data, "cartera"),
        "zonas": unique_sorted(zonas.data, "zona"),
        "canales": unique_sorted(canales.data, "canal_vta"),
        "localidades": unique_sorted(localidades.data, "localidad"),
    }
