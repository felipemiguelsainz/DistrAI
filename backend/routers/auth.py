"""Auth router: /api/auth/*"""

from fastapi import APIRouter, Depends

from core.auth import UserContext, get_current_user

router = APIRouter()


@router.get("/me")
async def me(user: UserContext = Depends(get_current_user)) -> dict:
    """Return the authenticated user's profile."""
    return {
        "uid": user.uid,
        "email": user.email,
        "rol": user.rol,
        "cartera": user.cartera,
        "nombre": user.nombre,
    }
