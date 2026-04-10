-- KPIs generales
create or replace function fn_dashboard_kpis()
returns json language sql stable as $$
  select json_build_object(
    'total_pdv', (select count(*) from pdv),
    'pdv_geocoded', (select count(*) from pdv where geocoding_status = 'ok'),
    'pdv_pending', (select count(*) from pdv where geocoding_status = 'pending'),
    'pdv_failed', (select count(*) from pdv where geocoding_status = 'failed'),
    'total_ventas_neto', coalesce((select sum(neto) from ventas), 0),
    'total_ventas_rows', (select count(*) from ventas),
    'clientes_activos', (select count(distinct pdv_codigo) from ventas),
    'fecha_desde', (select min(fecha_comprobante) from ventas),
    'fecha_hasta', (select max(fecha_comprobante) from ventas)
  );
$$;

-- Ventas por mes
create or replace function fn_ventas_por_mes()
returns table(anio int, mes int, total_neto numeric, total_kilos numeric) language sql stable as $$
  select anio, mes, sum(neto), sum(kilos)
  from ventas group by anio, mes order by anio, mes;
$$;

-- Ventas por cartera
create or replace function fn_ventas_por_cartera()
returns table(cartera varchar, total_neto numeric) language sql stable as $$
  select coalesce(cartera,'Sin cartera'), sum(neto)
  from ventas group by cartera order by sum(neto) desc;
$$;

-- Ventas por categoría (top 15)
create or replace function fn_ventas_por_categoria()
returns table(categoria varchar, total_neto numeric) language sql stable as $$
  select coalesce(categoria,'Sin categoría'), sum(neto)
  from ventas group by categoria order by sum(neto) desc limit 15;
$$;

-- Top vendedores
create or replace function fn_top_vendedores(lim int default 20)
returns table(vendedor varchar, total_neto numeric, comprobantes bigint) language sql stable as $$
  select vendedor, sum(neto), count(distinct comprobante)
  from ventas group by vendedor order by sum(neto) desc limit lim;
$$;

-- PDV por cartera
create or replace function fn_pdv_por_cartera()
returns table(cartera varchar, cantidad bigint) language sql stable as $$
  select coalesce(cartera,'Sin cartera'), count(*)
  from pdv group by cartera order by count(*) desc;
$$;
