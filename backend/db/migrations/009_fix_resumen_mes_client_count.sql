begin;

-- Fix resumen_mes: count distinct razon_social instead of pdv_codigo
-- (pdv_codigo is NULL in all ventas rows — razon_social is the client identifier)
drop materialized view if exists public.resumen_mes;

create materialized view public.resumen_mes as
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
  count(distinct ve.razon_social) as pdv_activos,
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

create unique index uq_resumen_mes_key
  on public.resumen_mes (tenant_id, categoria, cartera, supervisor, zona, canal, mes, anio);

create index idx_resumen_mes_tenant_mes_anio  on public.resumen_mes (tenant_id, mes, anio);
create index idx_resumen_mes_tenant_cartera   on public.resumen_mes (tenant_id, cartera);
create index idx_resumen_mes_tenant_supervisor on public.resumen_mes (tenant_id, supervisor);
create index idx_resumen_mes_zona             on public.resumen_mes (zona);
create index idx_resumen_mes_categoria        on public.resumen_mes (categoria);
create index idx_resumen_mes_canal            on public.resumen_mes (canal);

grant select on public.resumen_mes to authenticated;

commit;
