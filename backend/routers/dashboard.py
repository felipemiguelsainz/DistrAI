"""Router Dashboard: tabular monthly dashboard."""

from __future__ import annotations

import asyncio
from functools import partial
from typing import Optional

from fastapi import APIRouter, Depends, Query
from supabase import Client

from core.auth import UserContext, get_current_user
from db.supabase import get_supabase
from services.dashboard_calc import build_dashboard_dataset, get_dashboard_version, get_available_periods

router = APIRouter()


@router.get("")
async def dashboard_dataset(
    mes: Optional[int] = Query(None, ge=1, le=12),
    anio: Optional[int] = Query(None, ge=2020, le=2100),
    tenant_id: Optional[str] = Query(None, description="Filtrar por tenant (solo superadmin)"),
    user: UserContext = Depends(get_current_user),
    sb: Client = Depends(get_supabase),
):
    effective = tenant_id if user.is_superadmin else None
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None, partial(build_dashboard_dataset, sb, user, mes=mes, anio=anio, tenant_id_override=effective)
    )


@router.get("/version")
async def dashboard_version(
    tenant_id: Optional[str] = Query(None),
    user: UserContext = Depends(get_current_user),
    sb: Client = Depends(get_supabase),
):
    effective = tenant_id if user.is_superadmin else None
    return get_dashboard_version(sb, effective or user.tenant_id)


@router.get("/periods")
async def available_periods(
    tenant_id: Optional[str] = Query(None),
    user: UserContext = Depends(get_current_user),
    sb: Client = Depends(get_supabase),
):
    effective = tenant_id if user.is_superadmin else None
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None, partial(get_available_periods, sb, user, tenant_id_override=effective)
    )
