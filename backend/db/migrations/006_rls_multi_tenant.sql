begin;

-- ── Funciones auxiliares actualizadas ─────────────────────────────────────────

-- Obtiene el tenant_id del usuario autenticado
create or replace function public.current_user_tenant_id()
returns uuid
language sql stable security definer
set search_path = public
as $$
  select p.tenant_id
  from public.perfiles p
  where p.id = auth.uid() and p.activo = true
  limit 1;
$$;

-- Actualizar can_view_cartera para reconocer superadmin y filtrar por tenant
create or replace function public.can_view_cartera(target_cartera text)
returns boolean
language sql stable security definer
set search_path = public
as $$
  select case
    when auth.uid() is null then false
    when public.current_user_role() = 'superadmin' then true
    when public.current_user_role() in ('admin', 'analista') then true
    when public.current_user_role() = 'vendedor'
      then target_cartera is not distinct from public.current_user_cartera()
    when public.current_user_role() = 'supervisor'
      then exists (
        select 1
        from public.supervisores s
        where s.supervisor is not distinct from public.current_user_supervisor_name()
          and s.cartera is not distinct from target_cartera
          and s.tenant_id = public.current_user_tenant_id()
      )
    else false
  end;
$$;

-- ── Actualizar RLS: todas las tablas con tenant_id ────────────────────────────
-- NOTA: el backend usa service_key (bypasa RLS); estas políticas protegen
-- acceso directo a la DB (dashboard Supabase, conexiones directas, etc.)

-- Tenants: superadmin gestiona todo; otros solo ven su propio tenant
drop policy if exists tenants_select_policy on public.tenants;
create policy tenants_select_policy on public.tenants
  for select using (
    public.current_user_role() = 'superadmin'
    or id = public.current_user_tenant_id()
  );

drop policy if exists tenants_modify_policy on public.tenants;
create policy tenants_modify_policy on public.tenants
  for all
  using  (public.current_user_role() = 'superadmin')
  with check (public.current_user_role() = 'superadmin');

-- PDV
drop policy if exists pdv_select_policy on public.pdv;
create policy pdv_select_policy on public.pdv
  for select using (
    public.current_user_role() = 'superadmin'
    or (
      tenant_id = public.current_user_tenant_id()
      and public.can_view_cartera(cartera)
    )
  );

drop policy if exists pdv_modify_policy on public.pdv;
create policy pdv_modify_policy on public.pdv
  for all
  using (
    public.current_user_role() = 'superadmin'
    or (
      tenant_id = public.current_user_tenant_id()
      and public.current_user_role() in ('admin', 'analista')
    )
  )
  with check (
    public.current_user_role() = 'superadmin'
    or (
      tenant_id = public.current_user_tenant_id()
      and public.current_user_role() in ('admin', 'analista')
    )
  );

-- Ventas
drop policy if exists ventas_select_policy on public.ventas;
create policy ventas_select_policy on public.ventas
  for select using (
    public.current_user_role() = 'superadmin'
    or (
      tenant_id = public.current_user_tenant_id()
      and public.can_view_cartera(cartera)
    )
  );

drop policy if exists ventas_modify_policy on public.ventas;
create policy ventas_modify_policy on public.ventas
  for all
  using (
    public.current_user_role() = 'superadmin'
    or (
      tenant_id = public.current_user_tenant_id()
      and public.current_user_role() = 'admin'
    )
  )
  with check (
    public.current_user_role() = 'superadmin'
    or (
      tenant_id = public.current_user_tenant_id()
      and public.current_user_role() = 'admin'
    )
  );

-- Supervisores
drop policy if exists supervisores_select_policy on public.supervisores;
create policy supervisores_select_policy on public.supervisores
  for select using (
    public.current_user_role() = 'superadmin'
    or (
      tenant_id = public.current_user_tenant_id()
      and (
        public.current_user_role() in ('admin', 'analista')
        or (public.current_user_role() = 'supervisor'
            and supervisor is not distinct from public.current_user_supervisor_name())
        or (public.current_user_role() = 'vendedor'
            and cartera is not distinct from public.current_user_cartera())
      )
    )
  );

drop policy if exists supervisores_modify_policy on public.supervisores;
create policy supervisores_modify_policy on public.supervisores
  for all
  using (
    public.current_user_role() = 'superadmin'
    or (
      tenant_id = public.current_user_tenant_id()
      and public.current_user_role() = 'admin'
    )
  )
  with check (
    public.current_user_role() = 'superadmin'
    or (
      tenant_id = public.current_user_tenant_id()
      and public.current_user_role() = 'admin'
    )
  );

-- Perfiles: cada usuario ve el suyo; admin ve los de su tenant; superadmin ve todos
drop policy if exists perfiles_select_policy on public.perfiles;
create policy perfiles_select_policy on public.perfiles
  for select using (
    id = auth.uid()
    or public.current_user_role() = 'superadmin'
    or (
      tenant_id = public.current_user_tenant_id()
      and public.current_user_role() = 'admin'
    )
  );

drop policy if exists perfiles_modify_policy on public.perfiles;
create policy perfiles_modify_policy on public.perfiles
  for all
  using (
    public.current_user_role() = 'superadmin'
    or (
      tenant_id = public.current_user_tenant_id()
      and public.current_user_role() = 'admin'
    )
  )
  with check (
    public.current_user_role() = 'superadmin'
    or (
      tenant_id = public.current_user_tenant_id()
      and public.current_user_role() = 'admin'
    )
  );

-- Column mapping templates
drop policy if exists mapping_select_policy on public.column_mapping_templates;
create policy mapping_select_policy on public.column_mapping_templates
  for select using (
    public.current_user_role() = 'superadmin'
    or (
      tenant_id = public.current_user_tenant_id()
      and public.current_user_role() in ('admin', 'analista')
    )
  );

drop policy if exists mapping_modify_policy on public.column_mapping_templates;
create policy mapping_modify_policy on public.column_mapping_templates
  for all
  using (
    public.current_user_role() = 'superadmin'
    or (
      tenant_id = public.current_user_tenant_id()
      and public.current_user_role() in ('admin', 'analista')
    )
  )
  with check (
    public.current_user_role() = 'superadmin'
    or (
      tenant_id = public.current_user_tenant_id()
      and public.current_user_role() in ('admin', 'analista')
    )
  );

-- Productos
drop policy if exists productos_select_policy on public.productos;
create policy productos_select_policy on public.productos
  for select using (
    public.current_user_role() = 'superadmin'
    or tenant_id = public.current_user_tenant_id()
  );

drop policy if exists productos_modify_policy on public.productos;
create policy productos_modify_policy on public.productos
  for all
  using (
    public.current_user_role() = 'superadmin'
    or (
      tenant_id = public.current_user_tenant_id()
      and public.current_user_role() in ('admin', 'analista')
    )
  )
  with check (
    public.current_user_role() = 'superadmin'
    or (
      tenant_id = public.current_user_tenant_id()
      and public.current_user_role() in ('admin', 'analista')
    )
  );

-- Equipo
drop policy if exists equipo_select_policy on public.equipo;
create policy equipo_select_policy on public.equipo
  for select using (
    public.current_user_role() = 'superadmin'
    or tenant_id = public.current_user_tenant_id()
  );

drop policy if exists equipo_modify_policy on public.equipo;
create policy equipo_modify_policy on public.equipo
  for all
  using (
    public.current_user_role() = 'superadmin'
    or (
      tenant_id = public.current_user_tenant_id()
      and public.current_user_role() in ('admin', 'analista')
    )
  )
  with check (
    public.current_user_role() = 'superadmin'
    or (
      tenant_id = public.current_user_tenant_id()
      and public.current_user_role() in ('admin', 'analista')
    )
  );

-- ── Recrear vista v_ventas_enriquecida con join por tenant_id ────────────────
drop materialized view if exists public.resumen_mes;
drop view if exists public.v_ventas_enriquecida;
create or replace view public.v_ventas_enriquecida as
select
  v.id,
  v.tenant_id,
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
left join public.pdv p
  on p.pdv_codigo = v.pdv_codigo
  and p.tenant_id = v.tenant_id;

-- ── Recrear resumen_mes con tenant_id + total_kilos ───────────────────────────
drop materialized view if exists public.resumen_mes;

create materialized view if not exists public.resumen_mes as
select
  ve.tenant_id,
  coalesce(ve.categoria, 'sin_categoria') as categoria,
  coalesce(ve.cartera,   'sin_cartera')   as cartera,
  coalesce(ve.supervisor,'sin_supervisor') as supervisor,
  coalesce(ve.zona,      'sin_zona')      as zona,
  coalesce(ve.canal,     'sin_canal')     as canal,
  ve.mes,
  ve.anio,
  sum(coalesce(ve.neto,  0)) as total_neto,
  sum(coalesce(ve.kilos, 0)) as total_kilos,
  count(distinct ve.pdv_codigo) as pdv_activos,
  count(*) as transacciones
from public.v_ventas_enriquecida ve
group by
  ve.tenant_id,
  coalesce(ve.categoria, 'sin_categoria'),
  coalesce(ve.cartera,   'sin_cartera'),
  coalesce(ve.supervisor,'sin_supervisor'),
  coalesce(ve.zona,      'sin_zona'),
  coalesce(ve.canal,     'sin_canal'),
  ve.mes,
  ve.anio;

create unique index if not exists uq_resumen_mes_key
  on public.resumen_mes (tenant_id, categoria, cartera, supervisor, zona, canal, mes, anio);

create index if not exists idx_resumen_mes_tenant_mes_anio  on public.resumen_mes (tenant_id, mes, anio);
create index if not exists idx_resumen_mes_tenant_cartera   on public.resumen_mes (tenant_id, cartera);
create index if not exists idx_resumen_mes_tenant_supervisor on public.resumen_mes (tenant_id, supervisor);
create index if not exists idx_resumen_mes_zona             on public.resumen_mes (zona);
create index if not exists idx_resumen_mes_categoria        on public.resumen_mes (categoria);
create index if not exists idx_resumen_mes_canal            on public.resumen_mes (canal);

grant select on public.resumen_mes to authenticated;
grant select on public.v_ventas_enriquecida to authenticated;

commit;
