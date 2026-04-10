create or replace function public.refresh_resumen_mes()
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  refresh materialized view public.resumen_mes;
end;
$$;

drop materialized view if exists public.resumen_mes;

create materialized view public.resumen_mes as
select
  coalesce(ve.categoria, 'sin_categoria') as categoria,
  coalesce(ve.cartera, 'sin_cartera') as cartera,
  coalesce(ve.supervisor, 'sin_supervisor') as supervisor,
  coalesce(ve.zona, 'sin_zona') as zona,
  coalesce(ve.canal, 'sin_canal') as canal,
  ve.mes,
  ve.anio,
  sum(coalesce(ve.neto, 0)) as total_neto,
  sum(coalesce(ve.kilos, 0)) as total_kilos,
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