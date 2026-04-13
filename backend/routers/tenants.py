"""
Router: gestión de tenants y templates de mapeo de columnas.

Endpoints:
  Tenants (solo superadmin):
    POST   /api/admin/tenants              — crear distribuidora
    GET    /api/admin/tenants              — listar distribuidoras
    PATCH  /api/admin/tenants/{id}         — actualizar distribuidora
    POST   /api/admin/tenants/{id}/usuarios — crear usuario en una distribuidora

  Mapping templates (superadmin + admin + analista del tenant):
    GET    /api/admin/mapping-templates                    — listar templates del tenant
    POST   /api/admin/mapping-templates                    — crear template
    PATCH  /api/admin/mapping-templates/{id}               — actualizar template
    DELETE /api/admin/mapping-templates/{id}               — eliminar template

  Detección de columnas (para la UI de mapeo, antes de hacer upload):
    POST   /api/admin/detect-columns?data_type=pdv         — subir archivo y recibir sugerencias
"""

from __future__ import annotations

import io

import pandas as pd
from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from pydantic import BaseModel
from supabase import Client

from core.auth import UserContext, get_current_user, require_roles
from core.logging_config import get_logger
from db.supabase import get_supabase
from services import column_mapper

logger = get_logger("tenants")
router = APIRouter()


# ── Tenants ───────────────────────────────────────────────────────────────────

class TenantCreate(BaseModel):
    nombre: str
    slug: str
    plan: str = "basic"


class UsuarioCreate(BaseModel):
    email: str
    password: str
    nombre: str
    rol: str  # admin | analista | supervisor | vendedor
    cartera: str | None = None


@router.post("/tenants", status_code=201)
async def create_tenant(
    body: TenantCreate,
    _user: UserContext = Depends(require_roles("superadmin")),
    sb: Client = Depends(get_supabase),
):
    """Crea una nueva distribuidora."""
    if not body.slug.replace("-", "").replace("_", "").isalnum():
        raise HTTPException(400, "El slug solo puede contener letras, números, guiones y guiones bajos")

    existing = sb.table("tenants").select("id").eq("slug", body.slug).maybe_single().execute()
    if existing is not None and existing.data:
        raise HTTPException(409, f"Ya existe una distribuidora con el slug '{body.slug}'")

    res = sb.table("tenants").insert({
        "nombre": body.nombre,
        "slug": body.slug,
        "plan": body.plan,
    }).execute()

    if not res.data:
        raise HTTPException(500, "No se pudo crear la distribuidora")

    return res.data[0]


@router.get("/tenants")
async def list_tenants(
    _user: UserContext = Depends(require_roles("superadmin")),
    sb: Client = Depends(get_supabase),
):
    """Lista todas las distribuidoras."""
    res = sb.table("tenants").select("*").order("nombre").execute()
    return res.data or []


@router.patch("/tenants/{tenant_id}")
async def update_tenant(
    tenant_id: str,
    body: dict,
    _user: UserContext = Depends(require_roles("superadmin")),
    sb: Client = Depends(get_supabase),
):
    """Actualiza nombre, plan o estado activo de una distribuidora."""
    allowed = {"nombre", "plan", "activo"}
    update_data = {k: v for k, v in body.items() if k in allowed}
    if not update_data:
        raise HTTPException(400, "Sin campos válidos para actualizar (nombre, plan, activo)")

    res = sb.table("tenants").update(update_data).eq("id", tenant_id).execute()
    if not res.data:
        raise HTTPException(404, "Distribuidora no encontrada")

    return res.data[0]


@router.post("/tenants/{tenant_id}/usuarios", status_code=201)
async def create_tenant_user(
    tenant_id: str,
    body: UsuarioCreate,
    _user: UserContext = Depends(require_roles("superadmin")),
    sb: Client = Depends(get_supabase),
):
    """Crea un usuario de Supabase Auth y su perfil para una distribuidora."""
    valid_roles = {"admin", "analista", "supervisor", "vendedor"}
    if body.rol not in valid_roles:
        raise HTTPException(400, f"Rol inválido. Opciones: {', '.join(valid_roles)}")

    # Verificar que el tenant existe
    tenant = sb.table("tenants").select("id").eq("id", tenant_id).eq("activo", True).maybe_single().execute()
    if tenant is None or not tenant.data:
        raise HTTPException(404, "Distribuidora no encontrada o inactiva")

    # Crear usuario en Supabase Auth
    try:
        auth_res = sb.auth.admin.create_user({
            "email": body.email,
            "password": body.password,
            "email_confirm": True,
        })
        if not auth_res.user:
            raise HTTPException(500, "No se pudo crear el usuario en Auth")
        uid = auth_res.user.id
    except Exception as exc:
        logger.error("create_user auth failed: %s", exc)
        raise HTTPException(400, f"Error al crear usuario: {exc}")

    # Crear perfil vinculado al tenant
    try:
        sb.table("perfiles").insert({
            "id": uid,
            "tenant_id": tenant_id,
            "rol": body.rol,
            "nombre": body.nombre,
            "cartera": body.cartera,
            "activo": True,
        }).execute()
    except Exception as exc:
        logger.error("create_profile failed for uid=%s: %s", uid, exc)
        # Intentar limpiar el usuario de Auth si el perfil falla
        try:
            sb.auth.admin.delete_user(uid)
        except Exception:
            pass
        raise HTTPException(500, "No se pudo crear el perfil del usuario")

    return {"uid": uid, "email": body.email, "rol": body.rol, "tenant_id": tenant_id}


# ── Column mapping templates ──────────────────────────────────────────────────

class TemplateSave(BaseModel):
    data_type: str
    nombre: str
    mappings: dict
    es_default: bool = True


def _resolve_tenant_id(user: UserContext, tenant_id_param: str | None = None) -> str:
    """
    Resuelve el tenant_id a usar. Superadmin puede pasar tenant_id como parámetro.
    Otros usuarios siempre usan su propio tenant_id.
    """
    if user.is_superadmin:
        if not tenant_id_param:
            raise HTTPException(400, "Superadmin debe especificar tenant_id")
        return tenant_id_param
    if not user.tenant_id:
        raise HTTPException(403, "Sin distribuidora asignada")
    return user.tenant_id


@router.get("/mapping-templates")
async def list_mapping_templates(
    data_type: str | None = Query(None),
    tenant_id: str | None = Query(None, description="Solo para superadmin"),
    user: UserContext = Depends(require_roles("superadmin", "admin", "analista")),
    sb: Client = Depends(get_supabase),
):
    """Lista los templates de mapeo del tenant."""
    tid = _resolve_tenant_id(user, tenant_id)
    q = sb.table("column_mapping_templates").select("*").eq("tenant_id", tid).order("data_type").order("nombre")
    if data_type:
        if data_type not in column_mapper.CANONICAL:
            raise HTTPException(400, f"data_type inválido: {data_type}")
        q = q.eq("data_type", data_type)
    res = q.execute()
    return res.data or []


@router.post("/mapping-templates", status_code=201)
async def create_mapping_template(
    body: TemplateSave,
    tenant_id: str | None = Query(None, description="Solo para superadmin"),
    user: UserContext = Depends(require_roles("superadmin", "admin", "analista")),
    sb: Client = Depends(get_supabase),
):
    """Crea un template de mapeo de columnas."""
    if body.data_type not in column_mapper.CANONICAL:
        raise HTTPException(400, f"data_type inválido: {body.data_type}")

    tid = _resolve_tenant_id(user, tenant_id)

    # Validar que los valores del mapping sean campos canónicos válidos o null
    canonical = column_mapper.all_fields(body.data_type)
    for excel_col, canon_field in body.mappings.items():
        if canon_field is not None and canon_field not in canonical:
            raise HTTPException(
                400,
                f"Campo '{canon_field}' no es un campo canónico válido para '{body.data_type}'. "
                f"Campos disponibles: {sorted(canonical)}",
            )

    result = column_mapper.save_template(sb, tid, body.data_type, body.nombre, body.mappings, body.es_default)
    return result


@router.patch("/mapping-templates/{template_id}")
async def update_mapping_template(
    template_id: str,
    body: TemplateSave,
    user: UserContext = Depends(require_roles("superadmin", "admin", "analista")),
    sb: Client = Depends(get_supabase),
):
    """Actualiza un template de mapeo existente."""
    # Verificar que existe y pertenece al tenant del usuario
    existing = (
        sb.table("column_mapping_templates")
        .select("tenant_id, data_type")
        .eq("id", template_id)
        .maybe_single()
        .execute()
    )
    if existing is None or not existing.data:
        raise HTTPException(404, "Template no encontrado")

    if not user.is_superadmin and existing.data["tenant_id"] != user.tenant_id:
        raise HTTPException(403, "Sin acceso a este template")

    tid = existing.data["tenant_id"]

    # Si pasa a ser default, quitar el anterior
    if body.es_default:
        sb.table("column_mapping_templates").update({"es_default": False}).eq(
            "tenant_id", tid
        ).eq("data_type", body.data_type).eq("es_default", True).neq("id", template_id).execute()

    res = sb.table("column_mapping_templates").update({
        "nombre": body.nombre,
        "mappings": body.mappings,
        "es_default": body.es_default,
    }).eq("id", template_id).execute()

    return res.data[0] if res.data else {}


@router.delete("/mapping-templates/{template_id}")
async def delete_mapping_template(
    template_id: str,
    user: UserContext = Depends(require_roles("superadmin", "admin", "analista")),
    sb: Client = Depends(get_supabase),
):
    """Elimina un template de mapeo."""
    existing = (
        sb.table("column_mapping_templates")
        .select("tenant_id")
        .eq("id", template_id)
        .maybe_single()
        .execute()
    )
    if existing is None or not existing.data:
        raise HTTPException(404, "Template no encontrado")

    if not user.is_superadmin and existing.data["tenant_id"] != user.tenant_id:
        raise HTTPException(403, "Sin acceso a este template")

    sb.table("column_mapping_templates").delete().eq("id", template_id).execute()
    return {"ok": True}


# ── Detección de columnas (para UI de mapeo pre-upload) ──────────────────────

@router.post("/detect-columns")
async def detect_columns(
    file: UploadFile = File(...),
    data_type: str = Query(..., description="pdv | ventas | productos | equipo"),
    tenant_id: str | None = Query(None, description="Solo para superadmin"),
    user: UserContext = Depends(require_roles("superadmin", "admin", "analista")),
    sb: Client = Depends(get_supabase),
):
    """
    Lee los headers de un archivo y devuelve:
    - columns: lista de columnas del archivo
    - suggestions: auto-detección de mapeo {col: campo_canonico_o_null}
    - default_template: template default guardado (si existe)
    - canonical_fields: campos canónicos disponibles para este data_type
    - required_fields: campos requeridos
    """
    if data_type not in column_mapper.CANONICAL:
        raise HTTPException(400, f"data_type inválido. Opciones: {list(column_mapper.CANONICAL)}")

    ext = (file.filename or "").rsplit(".", 1)[-1].lower()
    if ext not in ("csv", "xlsx", "xls"):
        raise HTTPException(400, "Solo se aceptan archivos .csv, .xlsx o .xls")

    contents = await file.read()
    if len(contents) > 50 * 1024 * 1024:
        raise HTTPException(400, "Archivo demasiado grande (máx 50 MB)")

    try:
        headers = column_mapper.get_headers_from_file(contents, "csv" if ext == "csv" else "excel")
    except Exception as exc:
        raise HTTPException(400, f"No se pudieron leer los headers del archivo: {exc}")

    suggestions = column_mapper.detect_columns(headers, data_type)

    # Buscar template default guardado para este tenant
    tid = _resolve_tenant_id(user, tenant_id)
    default_template = column_mapper.get_default_template(sb, tid, data_type)

    return {
        "columns": headers,
        "suggestions": suggestions,
        "default_template": default_template,
        "canonical_fields": sorted(column_mapper.all_fields(data_type)),
        "required_fields": sorted(column_mapper.required_fields(data_type)),
    }
