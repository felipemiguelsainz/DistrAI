"""Supabase client singleton (service-role key — bypasses RLS)."""

from __future__ import annotations

from functools import lru_cache

from supabase import Client, create_client

from core.config import get_settings


@lru_cache
def _build_client() -> Client:
    s = get_settings()
    return create_client(s.supabase_url, s.supabase_service_key)


def get_supabase() -> Client:
    return _build_client()
