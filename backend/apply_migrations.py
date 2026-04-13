"""
Aplica migraciones SQL en orden sobre la base de datos de Supabase.

Uso:
  cd distribuidora-app/backend
  python apply_migrations.py

Requiere DATABASE_URL en .env. Para obtenerla:
  Supabase dashboard → Settings → Database → Connection string → URI
  Ejemplo: postgresql://postgres.xxxx:[PASSWORD]@aws-0-us-east-1.pooler.supabase.com:6543/postgres
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from urllib.parse import quote

from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

if not DATABASE_URL:
    print(
        "\n[ERROR] DATABASE_URL no está configurada en .env\n"
        "\nPara obtenerla:\n"
        "  1. Abrí dashboard.supabase.com\n"
        "  2. Tu proyecto → Settings → Database\n"
        "  3. Connection string → URI\n"
        "  4. Copiala y pegala en backend/.env como:\n"
        "     DATABASE_URL=postgresql://postgres.[ref]:[password]@...\n"
    )
    sys.exit(1)

try:
    import psycopg
except ImportError:
    print("[ERROR] psycopg no instalado. Corré: pip install psycopg[binary]")
    sys.exit(1)

MIGRATIONS_DIR = Path(__file__).parent / "db" / "migrations"

# Orden estricto de ejecución
MIGRATION_FILES = [
    "001_init_schema.sql",
    "002_dashboard_functions.sql",
    "003_resumen_mes_refresh.sql",
    "004_multi_tenant.sql",
    "004b_migrate_existing_tenant.sql",
    "005_column_mapping.sql",
    "006_rls_multi_tenant.sql",
    "007_dashboard_rpcs.sql",
    "008_ventas_covering_index.sql",
    "009_fix_resumen_mes_client_count.sql",
]

TRACKING_TABLE_SQL = """
create table if not exists public._migrations (
    filename   text primary key,
    applied_at timestamptz not null default now()
);
"""


def normalize_database_url(url: str) -> str:
    """Normalize DATABASE_URL by percent-encoding password when needed.

    Handles common cases where the password contains special characters
    (for example '@') and is provided unescaped in the URI.
    """
    if "://" not in url or "@" not in url:
        return url

    scheme, rest = url.split("://", 1)
    if "/" in rest:
        auth_host, path = rest.split("/", 1)
        path = "/" + path
    else:
        auth_host, path = rest, ""

    if "@" not in auth_host:
        return url

    auth, host = auth_host.rsplit("@", 1)
    if ":" not in auth:
        return url

    user, raw_password = auth.split(":", 1)
    if raw_password.startswith("[") and raw_password.endswith("]") and len(raw_password) >= 2:
        raw_password = raw_password[1:-1]

    encoded_password = quote(raw_password, safe="")
    return f"{scheme}://{user}:{encoded_password}@{host}{path}"


def get_applied(conn) -> set[str]:
    try:
        with conn.cursor() as cur:
            cur.execute("select filename from public._migrations")
            return {row[0] for row in cur.fetchall()}
    except Exception:
        return set()


def apply(conn, filename: str, sql: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql)
        cur.execute(
            "insert into public._migrations (filename) values (%s) on conflict do nothing",
            (filename,),
        )


def main() -> None:
    print(f"Conectando a la base de datos...")
    try:
        normalized_url = normalize_database_url(DATABASE_URL)
        conn = psycopg.connect(normalized_url, autocommit=False)
    except Exception as exc:
        print(f"[ERROR] No se pudo conectar: {exc}")
        print(
            "[TIP] Revisá DATABASE_URL en .env. "
            "Si la contraseña tiene caracteres especiales (ej: @, #, :, /), "
            "debe ir URL-encoded o entre variables separadas."
        )
        print(
            "[TIP] Si usás el host db.<project-ref>.supabase.co y tu red no tiene IPv6, "
            "copiá la URI del Session/Transaction Pooler desde Supabase > Settings > Database > Connection string."
        )
        sys.exit(1)

    print("Conexión OK\n")

    # Crear tabla de tracking si no existe
    with conn.cursor() as cur:
        cur.execute(TRACKING_TABLE_SQL)
    conn.commit()

    applied = get_applied(conn)

    pending = [f for f in MIGRATION_FILES if f not in applied]

    if not pending:
        print("✓ Todas las migraciones ya están aplicadas.")
        conn.close()
        return

    print(f"Migraciones pendientes: {len(pending)}")
    print("-" * 50)

    for filename in pending:
        path = MIGRATIONS_DIR / filename
        if not path.exists():
            print(f"  [SKIP] {filename} — archivo no encontrado")
            continue

        sql = path.read_text(encoding="utf-8")
        print(f"  Aplicando {filename}...", end=" ", flush=True)

        try:
            apply(conn, filename, sql)
            conn.commit()
        except Exception as exc:
            conn.rollback()
            print(f"\n  [ERROR] {filename} fallo:\n  {exc}\n")
            print("Abortando. Las migraciones anteriores fueron commiteadas.")
            conn.close()
            sys.exit(1)
        print("OK")

    print("-" * 50)
    print(f"OK {len(pending)} migracion(es) aplicada(s) correctamente.")
    conn.close()


if __name__ == "__main__":
    main()
