begin;

-- ── Templates de mapeo de columnas ───────────────────────────────────────────
-- Guarda cómo se mapean las columnas del Excel de cada distribuidora
-- al schema canónico de la plataforma.
-- mappings es un JSON: {"COLUMNA_EXCEL": "campo_canonico", "OTRA": null}
-- null = ignorar esa columna
create table if not exists public.column_mapping_templates (
  id         uuid primary key default gen_random_uuid(),
  tenant_id  uuid not null references public.tenants(id) on delete cascade,
  data_type  text not null check (data_type in ('pdv', 'ventas', 'productos', 'equipo')),
  nombre     text not null,
  mappings   jsonb not null default '{}',
  es_default boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- Solo puede haber un template default por tenant + data_type
create unique index if not exists idx_one_default_per_tenant_type
  on public.column_mapping_templates (tenant_id, data_type)
  where (es_default = true);

create index if not exists idx_mapping_templates_tenant_type
  on public.column_mapping_templates (tenant_id, data_type);

-- ── Maestro de productos ──────────────────────────────────────────────────────
create table if not exists public.productos (
  id           bigserial primary key,
  tenant_id    uuid not null references public.tenants(id) on delete cascade,
  codigo       text not null,
  descripcion  text not null,
  categoria    text,
  marca        text,
  precio_lista decimal(12, 2),
  activo       boolean not null default true,
  updated_at   timestamptz not null default now(),
  unique (tenant_id, codigo)
);

create index if not exists idx_productos_tenant_id on public.productos (tenant_id);
create index if not exists idx_productos_tenant_categoria on public.productos (tenant_id, categoria);

-- ── Maestro de equipo de trabajo ──────────────────────────────────────────────
create table if not exists public.equipo (
  id                uuid primary key default gen_random_uuid(),
  tenant_id         uuid not null references public.tenants(id) on delete cascade,
  codigo            text not null,
  nombre            text not null,
  -- rol: vendedor / supervisor / gerente
  rol               text check (rol in ('vendedor', 'supervisor', 'gerente')),
  cartera           text,
  -- referencia al supervisor (codigo del equipo del mismo tenant)
  supervisor_codigo text,
  activo            boolean not null default true,
  updated_at        timestamptz not null default now(),
  unique (tenant_id, codigo)
);

create index if not exists idx_equipo_tenant_id on public.equipo (tenant_id);
create index if not exists idx_equipo_tenant_cartera on public.equipo (tenant_id, cartera);
create index if not exists idx_equipo_tenant_rol on public.equipo (tenant_id, rol);

-- ── RLS en tablas nuevas ──────────────────────────────────────────────────────
alter table public.column_mapping_templates enable row level security;
alter table public.productos enable row level security;
alter table public.equipo enable row level security;

-- Grants (las políticas en migration 006 controlan el acceso real)
grant select on public.column_mapping_templates to authenticated;
grant select on public.productos to authenticated;
grant select on public.equipo to authenticated;
grant select on public.tenants to authenticated;

commit;
