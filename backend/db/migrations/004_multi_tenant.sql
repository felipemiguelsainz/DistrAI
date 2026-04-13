begin;

-- ── Tabla principal: tenants (una fila por distribuidora) ─────────────────────
create table if not exists public.tenants (
  id         uuid primary key default gen_random_uuid(),
  nombre     text not null,
  slug       text unique not null,
  plan       text not null default 'basic',
  activo     boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- ── Agregar tenant_id a tablas existentes ─────────────────────────────────────
alter table public.perfiles
  add column if not exists tenant_id uuid references public.tenants(id) on delete cascade;

alter table public.pdv
  add column if not exists tenant_id uuid references public.tenants(id) on delete cascade;

alter table public.ventas
  add column if not exists tenant_id uuid references public.tenants(id) on delete cascade;

alter table public.supervisores
  add column if not exists tenant_id uuid references public.tenants(id) on delete cascade;

-- ── Ampliar rol para incluir superadmin ───────────────────────────────────────
alter table public.perfiles
  drop constraint if exists perfiles_rol_check;

alter table public.perfiles
  add constraint perfiles_rol_check
  check (rol in ('superadmin', 'admin', 'analista', 'supervisor', 'vendedor'));

-- ── Actualizar unique constraints para incluir tenant_id ─────────────────────
-- PDV: código de cliente único por distribuidora
alter table public.pdv drop constraint if exists pdv_cod_cliente_key;
alter table public.pdv
  add constraint pdv_tenant_cod_cliente_key unique (tenant_id, cod_cliente);

-- Ventas: comprobante+sku único por distribuidora
alter table public.ventas drop constraint if exists ventas_comprobante_sku_key;
alter table public.ventas
  add constraint ventas_tenant_comprobante_sku_key unique (tenant_id, comprobante, sku);

-- Supervisores: cartera+supervisor único por distribuidora
alter table public.supervisores drop constraint if exists supervisores_cartera_supervisor_key;
alter table public.supervisores
  add constraint supervisores_tenant_cartera_supervisor_key unique (tenant_id, cartera, supervisor);

-- ── Índices para tenant_id ────────────────────────────────────────────────────
create index if not exists idx_perfiles_tenant_id on public.perfiles (tenant_id);
create index if not exists idx_pdv_tenant_id on public.pdv (tenant_id);
create index if not exists idx_ventas_tenant_id on public.ventas (tenant_id);
create index if not exists idx_supervisores_tenant_id on public.supervisores (tenant_id);

-- Índices compuestos para queries frecuentes del dashboard y mapa
create index if not exists idx_ventas_tenant_cartera_fecha
  on public.ventas (tenant_id, cartera, fecha_comprobante desc);

create index if not exists idx_pdv_tenant_cartera
  on public.pdv (tenant_id, cartera);

create index if not exists idx_pdv_tenant_geocoding
  on public.pdv (tenant_id, geocoding_status);

-- ── RLS en tenants ────────────────────────────────────────────────────────────
alter table public.tenants enable row level security;

commit;
