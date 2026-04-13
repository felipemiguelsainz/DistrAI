"""Auth dependency and role guards for FastAPI."""

from __future__ import annotations

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from supabase import Client

from core.logging_config import get_logger
from db.supabase import get_supabase

logger = get_logger("auth")
_bearer = HTTPBearer()


class UserContext:
    """Resolved user from JWT + perfiles table."""

    __slots__ = ("uid", "email", "rol", "cartera", "nombre", "tenant_id")

    def __init__(
        self,
        uid: str,
        email: str,
        rol: str,
        cartera: str | None,
        nombre: str | None,
        tenant_id: str | None,
    ):
        self.uid = uid
        self.email = email
        self.rol = rol
        self.cartera = cartera
        self.nombre = nombre
        self.tenant_id = tenant_id  # None solo para superadmin

    def __repr__(self) -> str:
        return f"<User {self.email} rol={self.rol} tenant={self.tenant_id}>"

    @property
    def is_superadmin(self) -> bool:
        return self.rol == "superadmin"


async def get_current_user(
    creds: HTTPAuthorizationCredentials = Depends(_bearer),
    sb: Client = Depends(get_supabase),
) -> UserContext:
    """Verify token via Supabase Auth server and resolve profile."""
    token = creds.credentials
    try:
        user_response = sb.auth.get_user(token)
        user = user_response.user
        if not user:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token inválido")
        uid = user.id
        email = user.email or ""
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("get_user failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token inválido o expirado")

    # Fetch profile (uses service key, bypasses RLS)
    try:
        res = sb.table("perfiles").select("rol, cartera, nombre, activo, tenant_id").eq("id", uid).maybe_single().execute()
    except Exception as exc:
        logger.error("perfiles query failed for uid=%s: %s", uid, exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="No se pudo verificar el usuario. Intente más tarde.",
        )

    # maybe_single() returns None when no row found
    profile = res.data if res is not None else None

    if not profile:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Sin perfil asignado. Contactá al admin.")
    if not profile.get("activo", False):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Usuario desactivado")

    rol = profile["rol"]
    tenant_id = profile.get("tenant_id")

    # Todos los roles excepto superadmin deben pertenecer a un tenant
    if rol != "superadmin" and not tenant_id:
        logger.error("Usuario uid=%s tiene rol=%s pero sin tenant_id asignado", uid, rol)
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Usuario sin distribuidora asignada. Contactá al admin.")

    return UserContext(
        uid=uid,
        email=email,
        rol=rol,
        cartera=profile.get("cartera"),
        nombre=profile.get("nombre"),
        tenant_id=tenant_id,
    )


def require_roles(*allowed_roles: str):
    """Dependency factory: restrict endpoint to specific roles."""

    async def _check(user: UserContext = Depends(get_current_user)) -> UserContext:
        if user.rol not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Requiere rol: {', '.join(allowed_roles)}",
            )
        return user

    return _check
