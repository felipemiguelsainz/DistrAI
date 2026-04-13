"""
Servicio de mapeo de columnas.

Responsabilidades:
  1. Definir los campos canónicos de cada tipo de dato (pdv, ventas, productos, equipo).
  2. Dado un DataFrame y un dict de mappings, renombrar columnas al schema canónico.
  3. Auto-detectar sugerencias de mapeo a partir de los headers del archivo.
  4. Consultar / guardar templates de mapeo en la base de datos.
"""

from __future__ import annotations

import io
from typing import Optional

import pandas as pd
from supabase import Client

from core.logging_config import get_logger

logger = get_logger("column_mapper")

# ── Schema canónico por tipo de dato ─────────────────────────────────────────
# required: el campo DEBE estar mapeado para que el upload sea válido.
# optional: puede estar o no; si no está, la columna se omite.
CANONICAL = {
    "pdv": {
        "required": {"cod_cliente", "razon_social"},
        "optional": {
            "pdv_codigo", "fecha_alta", "ultima_vta",
            "domicilio", "localidad", "provincia",
            "canal_distribucion", "canal_vta", "zona", "cartera", "vendedor",
            "cat", "categoria_iva", "cuit", "frecuencia_visita",
            "acuerdos_comerciales", "obs_internas", "obs_logistica", "obs_facturas",
            "tel_movil", "otro_tel",
            "visitar_esta_semana",
            "lun", "mar", "mie", "jue", "vie", "sab", "dom",
            "hs_lun", "hs_mar", "hs_mie", "hs_jue", "hs_vie", "hs_sab", "hs_dom",
            "prioridad_preparado", "lat", "lng",
        },
    },
    "ventas": {
        "required": {"fecha_comprobante", "pdv_codigo", "neto"},
        "optional": {
            "comprobante", "cartera", "vendedor", "vendedor2", "supervisor",
            "equipo", "razon_social", "marca", "rubro", "sku", "articulo",
            "categoria", "canal", "kilos", "bultos", "unidades",
            "bonificadas", "totales", "peso",
            # columnas auxiliares para reconstruir fecha desde partes
            "dia", "mes", "anio",
        },
    },
    "productos": {
        "required": {"codigo", "descripcion"},
        "optional": {"categoria", "marca", "precio_lista"},
    },
    "equipo": {
        "required": {"codigo", "nombre"},
        "optional": {"rol", "cartera", "supervisor_codigo"},
    },
}

# ── Alias conocidos: normalización de header → campo canónico ─────────────────
# Se usan para auto-detección de columnas cuando no hay template guardado.
_ALIAS: dict[str, dict[str, str]] = {
    "pdv": {
        "fecha alta": "fecha_alta", "fecha de alta": "fecha_alta",
        "última vta": "ultima_vta", "ultima vta": "ultima_vta",
        "última venta": "ultima_vta", "ultima venta": "ultima_vta",
        "pdv codigo": "pdv_codigo", "pdv código": "pdv_codigo",
        "cod. cliente": "cod_cliente", "cod cliente": "cod_cliente",
        "código cliente": "cod_cliente", "codigo cliente": "cod_cliente",
        "razon social": "razon_social", "razón social": "razon_social",
        "domicilio": "domicilio", "dirección": "domicilio", "direccion": "domicilio",
        "localidad": "localidad", "ciudad": "localidad",
        "tel. móvil": "tel_movil", "tel movil": "tel_movil", "tel. movil": "tel_movil",
        "telefono": "tel_movil", "teléfono": "tel_movil",
        "otro tel.": "otro_tel", "otro tel": "otro_tel",
        "cat.": "cat", "cat": "cat", "categoría": "cat", "categoria": "cat",
        "cartera": "cartera", "vendedor": "vendedor",
        "acuerdos comerciales": "acuerdos_comerciales", "zona": "zona",
        "obs. internas": "obs_internas", "obs internas": "obs_internas",
        "obs. logística": "obs_logistica", "obs logistica": "obs_logistica",
        "obs. logistica": "obs_logistica",
        "obs. facturas": "obs_facturas", "obs facturas": "obs_facturas",
        "canal distribución": "canal_distribucion",
        "canal distribucion": "canal_distribucion",
        "canal distribuc.": "canal_distribucion",
        "canal vta.": "canal_vta", "canal vta": "canal_vta", "canal venta": "canal_vta",
        "categoría iva": "categoria_iva", "categoria iva": "categoria_iva",
        "cuit": "cuit",
        "frec. de visita": "frecuencia_visita", "frec de visita": "frecuencia_visita",
        "frecuencia visita": "frecuencia_visita", "frecuencia de visita": "frecuencia_visita",
        "visitar esta semana": "visitar_esta_semana",
        "lun": "lun", "mar": "mar", "mié": "mie", "mie": "mie",
        "jue": "jue", "vie": "vie", "sáb": "sab", "sab": "sab", "dom": "dom",
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
    },
    "ventas": {
        "cartera": "cartera", "vendedor": "vendedor",
        "pdv codigo": "pdv_codigo", "pdv código": "pdv_codigo",
        "razon social": "razon_social", "razón social": "razon_social",
        "fecha comprobante": "fecha_comprobante", "fecha": "fecha_comprobante",
        "comprobante": "comprobante", "marca": "marca", "rubro": "rubro",
        "sku": "sku", "articulo": "articulo", "artículo": "articulo",
        "neto": "neto", "kilos": "kilos", "bultos": "bultos",
        "unidades": "unidades", "bonificadas": "bonificadas", "totales": "totales",
        "dia": "dia", "día": "dia", "mes": "mes", "anio": "anio", "año": "anio",
        "peso": "peso", "vendedor2": "vendedor2", "vendedor 2": "vendedor2",
        "categoria": "categoria", "categoría": "categoria",
        "equipo": "equipo", "canal": "canal", "supervisor": "supervisor",
    },
    "productos": {
        "codigo": "codigo", "código": "codigo",
        "descripcion": "descripcion", "descripción": "descripcion",
        "categoria": "categoria", "categoría": "categoria",
        "marca": "marca", "precio": "precio_lista", "precio lista": "precio_lista",
    },
    "equipo": {
        "codigo": "codigo", "código": "codigo",
        "nombre": "nombre", "rol": "rol",
        "cartera": "cartera",
        "supervisor": "supervisor_codigo",
        "supervisor codigo": "supervisor_codigo",
        "supervisor_codigo": "supervisor_codigo",
    },
}


def all_fields(data_type: str) -> set[str]:
    """Devuelve todos los campos canónicos (required + optional) para un data_type."""
    cf = CANONICAL.get(data_type, {})
    return cf.get("required", set()) | cf.get("optional", set())


def required_fields(data_type: str) -> set[str]:
    return CANONICAL.get(data_type, {}).get("required", set())


def detect_columns(headers: list[str], data_type: str) -> dict[str, str | None]:
    """
    Auto-detecta sugerencias de mapeo para una lista de headers de archivo.

    Retorna: {"COLUMNA_EXCEL": "campo_canonico"} o {"COLUMNA_EXCEL": None} si no hay match.
    El orden refleja el orden original del archivo.
    """
    alias = _ALIAS.get(data_type, {})
    canonical = all_fields(data_type)
    result: dict[str, str | None] = {}

    for col in headers:
        normalized = col.strip().lower()
        # 1. Coincidencia exacta por alias conocido
        if normalized in alias:
            result[col] = alias[normalized]
            continue
        # 2. Coincidencia directa con campo canónico (por si el Excel ya usa nombres de DB)
        if normalized in canonical or normalized.replace(" ", "_") in canonical:
            canon = normalized if normalized in canonical else normalized.replace(" ", "_")
            result[col] = canon
            continue
        # 3. Sin match
        result[col] = None

    return result


def apply_mapping(df: pd.DataFrame, mappings: dict[str, str | None]) -> pd.DataFrame:
    """
    Aplica un dict de mapeo al DataFrame: renombra columnas y descarta las ignoradas.

    mappings: {"COLUMNA_EXCEL": "campo_canonico"} o {"COLUMNA_EXCEL": None} para ignorar.
    Columnas del archivo que no estén en el dict se descartan también.
    """
    rename = {orig: canon for orig, canon in mappings.items() if canon is not None}
    # Solo conservar columnas que están en el mapping
    cols_presentes = [c for c in df.columns if c in mappings]
    df = df[cols_presentes].copy()
    df = df.rename(columns=rename)
    return df


def get_headers_from_file(contents: bytes, ext: str) -> list[str]:
    """Lee solo los headers de un archivo (sin cargar todo en memoria)."""
    if ext == "csv":
        for enc in ("utf-8", "latin-1", "cp1252"):
            try:
                df = pd.read_csv(io.BytesIO(contents), dtype=str, nrows=0, encoding=enc)
                return list(df.columns)
            except UnicodeDecodeError:
                continue
        raise ValueError("No se pudo decodificar el archivo CSV")
    else:
        df = pd.read_excel(io.BytesIO(contents), dtype=str, nrows=0)
        return list(df.columns)


# ── Operaciones con la base de datos ─────────────────────────────────────────

def get_default_template(sb: Client, tenant_id: str, data_type: str) -> Optional[dict]:
    """
    Retorna el mappings del template default para un tenant+data_type, o None si no existe.
    """
    res = (
        sb.table("column_mapping_templates")
        .select("id, nombre, mappings")
        .eq("tenant_id", tenant_id)
        .eq("data_type", data_type)
        .eq("es_default", True)
        .maybe_single()
        .execute()
    )
    if res and res.data:
        return res.data
    return None


def save_template(
    sb: Client,
    tenant_id: str,
    data_type: str,
    nombre: str,
    mappings: dict,
    es_default: bool = True,
) -> dict:
    """
    Guarda un template de mapeo. Si es_default=True, desactiva el default anterior.
    """
    if es_default:
        sb.table("column_mapping_templates").update({"es_default": False}).eq(
            "tenant_id", tenant_id
        ).eq("data_type", data_type).eq("es_default", True).execute()

    res = sb.table("column_mapping_templates").insert({
        "tenant_id": tenant_id,
        "data_type": data_type,
        "nombre": nombre,
        "mappings": mappings,
        "es_default": es_default,
    }).execute()

    return res.data[0] if res.data else {}
