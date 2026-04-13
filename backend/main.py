import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from core.config import get_settings
from core.logging_config import get_logger, setup_logging
from routers import auth, pdv, mapa, dashboard, ventas, tenants

setup_logging()
logger = get_logger("main")


@asynccontextmanager
async def lifespan(application: FastAPI):
    # Startup: validate config on boot so missing vars fail early
    settings = get_settings()
    logger.info("DistrAI backend starting — CORS origins: %s", settings.cors_origins)
    yield
    logger.info("DistrAI backend shutting down")


def create_app() -> FastAPI:
    settings = get_settings()

    application = FastAPI(
        title="Distribuidora API",
        version="0.1.0",
        lifespan=lifespan,
        docs_url="/api/docs",
        redoc_url="/api/redoc",
    )

    application.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @application.middleware("http")
    async def log_requests(request: Request, call_next):
        start = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - start) * 1000
            logger.error(
                "%s %s -> 500 (%.1f ms) %s",
                request.method,
                request.url.path,
                elapsed_ms,
                exc,
                exc_info=True,
            )
            return JSONResponse(
                status_code=500,
                content={"detail": f"Error interno: {exc}"},
            )
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info(
            "%s %s -> %d  (%.1f ms)",
            request.method,
            request.url.path,
            response.status_code,
            elapsed_ms,
        )
        return response

    @application.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        logger.error(
            "Unhandled exception on %s %s: %s",
            request.method,
            request.url.path,
            exc,
            exc_info=True,
        )
        return JSONResponse(
            status_code=500,
            content={"detail": f"Error interno: {exc}"},
        )

    application.include_router(auth.router,      prefix="/api/auth",      tags=["auth"])
    application.include_router(pdv.router,       prefix="/api/pdv",       tags=["pdv"])
    application.include_router(mapa.router,      prefix="/api/mapa",      tags=["mapa"])
    application.include_router(dashboard.router, prefix="/api/dashboard",  tags=["dashboard"])
    application.include_router(ventas.router,    prefix="/api/ventas",    tags=["ventas"])
    application.include_router(tenants.router,   prefix="/api/admin",     tags=["admin"])

    return application


app = create_app()


@app.get("/api/health", tags=["health"])
def health() -> dict[str, str]:
    return {"status": "ok"}
    