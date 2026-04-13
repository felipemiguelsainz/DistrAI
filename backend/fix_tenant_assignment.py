"""
Assigns all existing NULL-tenant data to the Candysur tenant.
Runs the PDV+ventas UPDATE in batches to avoid Supabase statement timeout.
"""
from __future__ import annotations

import os
import sys
from dotenv import load_dotenv

load_dotenv()

try:
    import psycopg
except ImportError:
    print("[ERROR] pip install psycopg[binary]")
    sys.exit(1)

from urllib.parse import quote

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if not DATABASE_URL:
    print("[ERROR] DATABASE_URL not set in .env")
    sys.exit(1)


def normalize(url: str) -> str:
    if "://" not in url or "@" not in url:
        return url
    scheme, rest = url.split("://", 1)
    auth_host, path = (rest.split("/", 1) if "/" in rest else (rest, ""))
    if "/" in rest:
        path = "/" + path
    if "@" not in auth_host:
        return url
    auth, host = auth_host.rsplit("@", 1)
    if ":" not in auth:
        return url
    user, pw = auth.split(":", 1)
    if pw.startswith("[") and pw.endswith("]"):
        pw = pw[1:-1]
    return f"{scheme}://{user}:{quote(pw, safe='')}@{host}{path}"


conn = psycopg.connect(normalize(DATABASE_URL), autocommit=True)
print("Conectado OK")

with conn.cursor() as cur:
    # Check if tenant already exists
    cur.execute("SELECT id FROM public.tenants WHERE slug = 'candysur'")
    row = cur.fetchone()
    if row:
        tenant_id = row[0]
        print(f"Tenant ya existe: {tenant_id}")
    else:
        cur.execute(
            "INSERT INTO public.tenants (nombre, slug, plan) VALUES (%s, %s, %s) RETURNING id",
            ("Candysur", "candysur", "basic"),
        )
        tenant_id = cur.fetchone()[0]
        print(f"Tenant creado: {tenant_id}")

    # Update PDV (6747 rows — safe in one shot)
    cur.execute("UPDATE public.pdv SET tenant_id = %s WHERE tenant_id IS NULL", (tenant_id,))
    print(f"PDVs actualizados: {cur.rowcount}")

    # Update supervisores
    cur.execute("UPDATE public.supervisores SET tenant_id = %s WHERE tenant_id IS NULL", (tenant_id,))
    print(f"Supervisores actualizados: {cur.rowcount}")

    # Update perfiles (non-superadmin only, keep superadmin as NULL)
    cur.execute("UPDATE public.perfiles SET tenant_id = %s WHERE tenant_id IS NULL AND rol != 'superadmin'", (tenant_id,))
    print(f"Perfiles actualizados: {cur.rowcount}")

    # Update config key
    cur.execute(
        "UPDATE public.config SET key = %s WHERE key = 'ventas_ultima_actualizacion'",
        (f"tenant:{tenant_id}:ventas_ultima_actualizacion",),
    )
    print(f"Config key actualizado: {cur.rowcount}")

    # Batch update ventas (1M+ rows — do in chunks of 50k)
    print("\nActualizando ventas en batches de 50.000 filas...")
    total = 0
    batch = 50_000
    while True:
        cur.execute(
            """
            UPDATE public.ventas
            SET tenant_id = %s
            WHERE id IN (
                SELECT id FROM public.ventas WHERE tenant_id IS NULL LIMIT %s
            )
            """,
            (tenant_id, batch),
        )
        updated = cur.rowcount
        total += updated
        print(f"  {total:,} filas actualizadas...", end="\r", flush=True)
        if updated == 0:
            break

    print(f"\nVentas actualizadas: {total:,}")
    print(f"\n✓ Listo. Tenant ID: {tenant_id}")
    print("  Guardalo si necesitás referenciarlo manualmente.")

conn.close()
