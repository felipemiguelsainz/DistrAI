"""Geocoding service: Nominatim (free) → OpenAI fallback."""

from __future__ import annotations

import asyncio
import re
from typing import Optional

import httpx

from core.config import get_settings

_NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
_HEADERS = {"User-Agent": "distribuidora-app/1.0 (contact@distribuidora.ar)"}

# Rate limit: max 1 req/sec for Nominatim
_nominatim_lock = asyncio.Lock()


async def _nominatim_geocode(address: str) -> Optional[tuple[float, float]]:
    """Try Nominatim. Returns (lat, lng) or None."""
    async with _nominatim_lock:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(
                    _NOMINATIM_URL,
                    params={"q": address, "format": "json", "limit": 1, "countrycodes": "ar"},
                    headers=_HEADERS,
                )
        except Exception as exc:
            print(f"[geocode] Nominatim request error: {exc}")
            return None
        await asyncio.sleep(1.1)  # respect rate limit

    if resp.status_code == 200 and resp.json():
        hit = resp.json()[0]
        return float(hit["lat"]), float(hit["lon"])
    return None


async def _openai_geocode(address: str) -> Optional[tuple[float, float]]:
    """Fallback: ask OpenAI for coords of a specific street address."""
    settings = get_settings()
    if not settings.openai_api_key:
        return None

    prompt = (
        "Sos un geocodificador experto en el Gran Buenos Aires, Argentina. "
        "Dada la siguiente dirección, devolvé ÚNICAMENTE las coordenadas "
        "lat,lng separadas por coma, sin texto adicional. "
        "Si la dirección incluye un número de calle tipo 'CALLE 704' o similar, "
        "es una nomenclatura de Florencio Varela / Berazategui donde las calles "
        "se numeran. Buscá la ubicación real de esa calle.\n\n"
        f"Dirección: {address}"
    )

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {settings.openai_api_key}"},
                json={
                    "model": "gpt-4o-mini",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 30,
                    "temperature": 0,
                },
            )
    except Exception as exc:
        print(f"[geocode] OpenAI request error: {exc}")
        return None

    if resp.status_code != 200:
        print(f"[geocode] OpenAI HTTP {resp.status_code}: {resp.text[:100]}")
        return None

    text = resp.json()["choices"][0]["message"]["content"].strip()
    parts = text.replace(" ", "").split(",")
    if len(parts) == 2:
        try:
            lat, lng = float(parts[0]), float(parts[1])
            # Sanity check: must be in Argentina roughly
            if -56 < lat < -22 and -74 < lng < -53:
                return lat, lng
            print(f"[geocode] OpenAI coords out of range: {lat},{lng}")
        except ValueError:
            print(f"[geocode] OpenAI unparseable: '{text}'")
    return None


def _clean_domicilio(domicilio: str) -> Optional[str]:
    """Extract a Nominatim-friendly address from Argentine GBA street format.

    Examples:
      'CALLE 222 ( SALLARES) 1193'  → 'SALLARES 1193'
      'CALLE 352(LAVALLE) 585'       → 'LAVALLE 585'
      'MAININI Nro.508 (PRIMERA JUNTA)' → 'MAININI 508'
    Returns None if no cleaning was possible or result equals original.
    """
    if not domicilio:
        return None
    d = domicilio.strip()

    # Pattern: CALLE NNN (ALIAS) HOUSE_NUM → use ALIAS HOUSE_NUM
    m = re.match(r"^CALLE\s+\d+\s*\(([^)]+)\)\s*(\d+)", d, re.IGNORECASE)
    if m:
        alias = m.group(1).strip()
        house_num = m.group(2).strip()
        return f"{alias} {house_num}"

    # General: remove parenthetical notes, normalize "Nro." → just number
    cleaned = re.sub(r"\s*\([^)]*\)", "", d).strip()
    cleaned = re.sub(r"\bNro\.\s*", "", cleaned, flags=re.IGNORECASE).strip()
    if cleaned and cleaned.upper() != d.upper():
        return cleaned

    return None


async def geocode_address(address: str) -> tuple[Optional[float], Optional[float], str]:
    """Geocode a single address. Returns (lat, lng, status)."""
    # Try Nominatim first
    result = await _nominatim_geocode(address)
    if result:
        return result[0], result[1], "ok"

    # Fallback to OpenAI
    result = await _openai_geocode(address)
    if result:
        return result[0], result[1], "ok"

    return None, None, "failed"


async def geocode_pending(sb, limit: int, jobs: dict, job_id: str) -> dict:
    """Geocode up to `limit` PDV rows with geocoding_status='pending'."""
    res = (
        sb.table("pdv")
        .select("id, domicilio, localidad, geocoding_attempts")
        .eq("geocoding_status", "pending")
        .lt("geocoding_attempts", 3)
        .order("id")
        .limit(limit)
        .execute()
    )

    rows = res.data or []
    total = len(rows)
    jobs[job_id]["total"] = total
    processed = 0
    errors = 0

    for row in rows:
        domicilio = (row.get("domicilio") or "").strip()
        localidad = (row.get("localidad") or "").strip()
        attempts = (row.get("geocoding_attempts") or 0) + 1

        # ── Level 0: no data at all ───────────────────────────────────────────
        if not domicilio and not localidad:
            try:
                sb.table("pdv").update({"geocoding_status": "failed", "geocoding_attempts": attempts}).eq("id", row["id"]).execute()
            except Exception:
                pass
            errors += 1
            processed += 1
            jobs[job_id].update({"processed": processed, "errors": errors})
            continue

        lat = lng = None
        status = "failed"
        full_addr = f"{domicilio}, {localidad}, Buenos Aires, Argentina" if domicilio and localidad else None
        cleaned = _clean_domicilio(domicilio) if domicilio else None
        clean_addr = f"{cleaned}, {localidad}, Buenos Aires, Argentina" if cleaned and localidad else None

        # ── Level 1: Nominatim full address ───────────────────────────────────
        if full_addr:
            print(f"[geocode] L1-Nom id={row['id']} '{full_addr}'")
            result = await _nominatim_geocode(full_addr)
            if result:
                lat, lng = result
                status = "ok"

        # ── Level 2: Nominatim cleaned domicilio + localidad ──────────────────
        if status != "ok" and clean_addr:
            print(f"[geocode] L2-Nom id={row['id']} '{clean_addr}'")
            result = await _nominatim_geocode(clean_addr)
            if result:
                lat, lng = result
                status = "ok"

        # ── Level 3: OpenAI full address ──────────────────────────────────────
        if status != "ok" and full_addr:
            print(f"[geocode] L3-AI id={row['id']} '{full_addr}'")
            result = await _openai_geocode(full_addr)
            if result:
                lat, lng = result
                status = "ok"

        # ── Level 4: OpenAI cleaned domicilio ─────────────────────────────────
        if status != "ok" and clean_addr and clean_addr != full_addr:
            print(f"[geocode] L4-AI id={row['id']} '{clean_addr}'")
            result = await _openai_geocode(clean_addr)
            if result:
                lat, lng = result
                status = "ok"

        # ── Level 5: OpenAI domicilio only (sin localidad) ────────────────────
        if status != "ok" and domicilio:
            fallback = f"{domicilio}, Gran Buenos Aires, Argentina"
            print(f"[geocode] L5-AI id={row['id']} '{fallback}'")
            result = await _openai_geocode(fallback)
            if result:
                lat, lng = result
                status = "ok"

        update: dict = {"geocoding_attempts": attempts}
        if lat is not None and lng is not None:
            update["lat"] = lat
            update["lng"] = lng
            update["geocoding_status"] = "ok"
        else:
            update["geocoding_status"] = "failed"
            errors += 1

        try:
            sb.table("pdv").update(update).eq("id", row["id"]).execute()
        except Exception as exc:
            print(f"[geocode] update error for id={row['id']}: {exc}")
            errors += 1

        processed += 1
        jobs[job_id].update({"processed": processed, "errors": errors})

    return {"total": total, "processed": processed, "errors": errors}
