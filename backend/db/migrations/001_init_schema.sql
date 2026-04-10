begin;

create extension if not exists pgcrypto;

create table if not exists public.pdv (
  id bigserial primary key,
  fecha_alta date,
  ultima_vta date,
  pdv_codigo varchar,
  cod_cliente varchar unique,
  razon_social varchar,
  domicilio varchar,
  localidad varchar,
  tel_movil varchar,
  otro_tel varchar,
  cat varchar,
  cartera varchar,
  vendedor varchar,
  acuerdos_comerciales text,
  zona varchar,
  obs_internas text,
  obs_logistica text,
  obs_facturas text,
  canal_distribucion varchar,
  canal_vta varchar,
  categoria_iva varchar,
  cuit varchar,
  frecuencia_visita varchar,
  visitar_esta_semana boolean,
  lun boolean,
  mar boolean,
  mie boolean,
  jue boolean,
  vie boolean,
  sab boolean,
  dom boolean,
  hs_lun varchar,
  hs_mar varchar,
  hs_mie varchar,
  hs_jue varchar,
  hs_vie varchar,
  hs_sab varchar,
  hs_dom varchar,
  prioridad_preparado varchar,
  lat decimal(10, 8),
  lng decimal(11, 8),
  geocoding_status varchar default 'pending' check (geocoding_status in ('ok', 'pending', 'failed', 'manual')),
  geocoding_attempts int default 0,
  updated_at timestamp without time zone default now()
);

create table if not exists public.ventas (
  id bigserial primary key,
  cartera varchar,
  vendedor varchar,
  pdv_codigo varchar,
  razon_social varchar,
  fecha_comprobante date,
  comprobante varchar,
  marca varchar,
  rubro varchar,
  sku varchar,
  articulo varchar,
  neto decimal(12, 2),
  kilos decimal(10, 3),
  bultos decimal(10, 3),
  unidades decimal(10, 3),
  bonificadas decimal(10, 3),
  totales decimal(10, 3),
  dia int,
  mes int,
  anio int,
  peso decimal(10, 3),
  vendedor2 varchar,
  categoria varchar,
  equipo varchar,
  canal varchar,
  supervisor varchar,
  unique (comprobante, sku)
);

create table if not exists public.supervisores (
  id bigserial primary key,
  cartera varchar,
  supervisor varchar,
  unique (cartera, supervisor)
);

create table if not exists public.perfiles (
  id uuid primary key references auth.users (id) on delete cascade,
  rol varchar not null check (rol in ('admin', 'analista', 'supervisor', 'vendedor')),
  cartera varchar,
  nombre varchar,
  activo boolean not null default true,
  created_at timestamp without time zone not null default now(),
  updated_at timestamp without time zone not null default now()
);

create table if not exists public.feriados (
  id bigserial primary key,
  fecha date not null unique,
  descripcion varchar,
  tipo varchar,
  created_at timestamp without time zone not null default now()
);

create table if not exists public.config (
  key text primary key,
  value jsonb not null,
  updated_at timestamp without time zone not null default now(),
  updated_by uuid references auth.users (id) on delete set null
);

insert into public.config (key, value)
values ('ventas_ultima_actualizacion', jsonb_build_object('timestamp', null))
on conflict (key) do nothing;

create index if not exists idx_pdv_cartera on public.pdv (cartera);
create index if not exists idx_pdv_zona on public.pdv (zona);
create index if not exists idx_pdv_geocoding_status on public.pdv (geocoding_status);
create index if not exists idx_pdv_cod_cliente on public.pdv (cod_cliente);
create index if not exists idx_pdv_pdv_codigo on public.pdv (pdv_codigo);

create index if not exists idx_ventas_fecha on public.ventas (fecha_comprobante);
create index if not exists idx_ventas_cartera on public.ventas (cartera);
create index if not exists idx_ventas_pdv on public.ventas (pdv_codigo);
create index if not exists idx_ventas_categoria on public.ventas (categoria);
create index if not exists idx_ventas_supervisor on public.ventas (supervisor);
create index if not exists idx_ventas_mes_anio on public.ventas (mes, anio);
create index if not exists idx_ventas_canal on public.ventas (canal);
create index if not exists idx_ventas_zona_proxy on public.ventas (cartera, supervisor, fecha_comprobante);

create index if not exists idx_supervisores_supervisor on public.supervisores (supervisor);
create index if not exists idx_supervisores_cartera on public.supervisores (cartera);

create index if not exists idx_perfiles_rol on public.perfiles (rol);
create index if not exists idx_perfiles_cartera on public.perfiles (cartera);

create or replace view public.v_ventas_enriquecida as
select
  v.id,
  v.cartera,
  v.vendedor,
  v.pdv_codigo,
  v.razon_social,
  v.fecha_comprobante,
  v.comprobante,
  v.marca,
  v.rubro,
  v.sku,
  v.articulo,
  v.neto,
  v.kilos,
  v.bultos,
  v.unidades,
  v.bonificadas,
  v.totales,
  v.dia,
  v.mes,
  v.anio,
  v.peso,
  v.vendedor2,
  v.categoria,
  v.equipo,
  v.canal,
  v.supervisor,
  p.zona
from public.ventas v
left join public.pdv p on p.pdv_codigo = v.pdv_codigo;

create materialized view if not exists public.resumen_mes as
select
  coalesce(ve.categoria, 'sin_categoria') as categoria,
  coalesce(ve.cartera, 'sin_cartera') as cartera,
  coalesce(ve.supervisor, 'sin_supervisor') as supervisor,
  coalesce(ve.zona, 'sin_zona') as zona,
  coalesce(ve.canal, 'sin_canal') as canal,
  ve.mes,
  ve.anio,
  sum(coalesce(ve.neto, 0)) as total_neto,
  count(distinct ve.pdv_codigo) as pdv_activos,
  count(*) as transacciones
from public.v_ventas_enriquecida ve
group by
  coalesce(ve.categoria, 'sin_categoria'),
  coalesce(ve.cartera, 'sin_cartera'),
  coalesce(ve.supervisor, 'sin_supervisor'),
  coalesce(ve.zona, 'sin_zona'),
  coalesce(ve.canal, 'sin_canal'),
  ve.mes,
  ve.anio;

create unique index if not exists uq_resumen_mes_key
on public.resumen_mes (categoria, cartera, supervisor, zona, canal, mes, anio);

create index if not exists idx_resumen_mes_mes_anio on public.resumen_mes (mes, anio);
create index if not exists idx_resumen_mes_cartera on public.resumen_mes (cartera);
create index if not exists idx_resumen_mes_supervisor on public.resumen_mes (supervisor);
create index if not exists idx_resumen_mes_zona on public.resumen_mes (zona);
create index if not exists idx_resumen_mes_categoria on public.resumen_mes (categoria);
create index if not exists idx_resumen_mes_canal on public.resumen_mes (canal);

alter table public.pdv enable row level security;
alter table public.ventas enable row level security;
alter table public.supervisores enable row level security;
alter table public.perfiles enable row level security;
alter table public.feriados enable row level security;
alter table public.config enable row level security;

create or replace function public.current_user_role()
returns text
language sql
stable
security definer
set search_path = public
as $$
  select p.rol
  from public.perfiles p
  where p.id = auth.uid() and p.activo = true
  limit 1;
$$;

create or replace function public.current_user_cartera()
returns text
language sql
stable
security definer
set search_path = public
as $$
  select p.cartera
  from public.perfiles p
  where p.id = auth.uid() and p.activo = true
  limit 1;
$$;

create or replace function public.current_user_supervisor_name()
returns text
language sql
stable
security definer
set search_path = public
as $$
  select p.nombre
  from public.perfiles p
  where p.id = auth.uid() and p.activo = true and p.rol = 'supervisor'
  limit 1;
$$;

create or replace function public.can_view_cartera(target_cartera text)
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select case
    when auth.uid() is null then false
    when public.current_user_role() in ('admin', 'analista') then true
    when public.current_user_role() = 'vendedor' then target_cartera is not distinct from public.current_user_cartera()
    when public.current_user_role() = 'supervisor' then exists (
      select 1
      from public.supervisores s
      where s.supervisor is not distinct from public.current_user_supervisor_name()
        and s.cartera is not distinct from target_cartera
    )
    else false
  end;
$$;

drop policy if exists pdv_select_policy on public.pdv;
create policy pdv_select_policy
on public.pdv
for select
using (
  public.can_view_cartera(cartera)
);

drop policy if exists pdv_modify_policy on public.pdv;
create policy pdv_modify_policy
on public.pdv
for all
using (
  public.current_user_role() in ('admin', 'analista')
)
with check (
  public.current_user_role() in ('admin', 'analista')
);

drop policy if exists ventas_select_policy on public.ventas;
create policy ventas_select_policy
on public.ventas
for select
using (
  public.can_view_cartera(cartera)
);

drop policy if exists ventas_modify_policy on public.ventas;
create policy ventas_modify_policy
on public.ventas
for all
using (
  public.current_user_role() = 'admin'
)
with check (
  public.current_user_role() = 'admin'
);

drop policy if exists supervisores_select_policy on public.supervisores;
create policy supervisores_select_policy
on public.supervisores
for select
using (
  public.current_user_role() in ('admin', 'analista')
  or (
    public.current_user_role() = 'supervisor'
    and supervisor is not distinct from public.current_user_supervisor_name()
  )
  or (
    public.current_user_role() = 'vendedor'
    and cartera is not distinct from public.current_user_cartera()
  )
);

drop policy if exists supervisores_modify_policy on public.supervisores;
create policy supervisores_modify_policy
on public.supervisores
for all
using (
  public.current_user_role() = 'admin'
)
with check (
  public.current_user_role() = 'admin'
);

drop policy if exists perfiles_select_policy on public.perfiles;
create policy perfiles_select_policy
on public.perfiles
for select
using (
  id = auth.uid() or public.current_user_role() = 'admin'
);

drop policy if exists perfiles_modify_policy on public.perfiles;
create policy perfiles_modify_policy
on public.perfiles
for all
using (
  public.current_user_role() = 'admin'
)
with check (
  public.current_user_role() = 'admin'
);

drop policy if exists feriados_select_policy on public.feriados;
create policy feriados_select_policy
on public.feriados
for select
using (
  public.current_user_role() in ('admin', 'analista', 'supervisor', 'vendedor')
);

drop policy if exists feriados_modify_policy on public.feriados;
create policy feriados_modify_policy
on public.feriados
for all
using (
  public.current_user_role() = 'admin'
)
with check (
  public.current_user_role() = 'admin'
);

drop policy if exists config_select_policy on public.config;
create policy config_select_policy
on public.config
for select
using (
  public.current_user_role() in ('admin', 'analista', 'supervisor', 'vendedor')
);

drop policy if exists config_modify_policy on public.config;
create policy config_modify_policy
on public.config
for all
using (
  public.current_user_role() in ('admin', 'analista')
)
with check (
  public.current_user_role() in ('admin', 'analista')
);

grant usage on schema public to anon, authenticated;
grant select on public.resumen_mes to authenticated;
grant select on public.v_ventas_enriquecida to authenticated;

commit;
