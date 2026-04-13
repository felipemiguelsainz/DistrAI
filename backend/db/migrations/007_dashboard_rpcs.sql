begin;

-- Fast COUNT(DISTINCT pdv_codigo) for a tenant + date range
create or replace function public.count_distinct_pdvs(
    p_tenant_id uuid,
    p_start      date,
    p_end        date,
    p_carteras   text[]  default null,
    p_supervisor text    default null
) returns bigint
language sql stable security definer
set search_path = public
as $$
    select count(distinct pdv_codigo)
    from public.ventas
    where (p_tenant_id is null or tenant_id = p_tenant_id)
      and fecha_comprobante between p_start and p_end
      and (p_carteras   is null or cartera    = any(p_carteras))
      and (p_supervisor is null or supervisor = p_supervisor);
$$;

-- Fast SUM(neto, kilos) for a tenant + date range
create or replace function public.sum_neto_kilos(
    p_tenant_id  uuid,
    p_start      date,
    p_end        date,
    p_carteras   text[]  default null,
    p_supervisor text    default null
) returns table(total_neto numeric, total_kilos numeric)
language sql stable security definer
set search_path = public
as $$
    select
        coalesce(sum(neto),  0) as total_neto,
        coalesce(sum(kilos), 0) as total_kilos
    from public.ventas
    where (p_tenant_id is null or tenant_id = p_tenant_id)
      and fecha_comprobante between p_start and p_end
      and (p_carteras   is null or cartera    = any(p_carteras))
      and (p_supervisor is null or supervisor = p_supervisor);
$$;

grant execute on function public.count_distinct_pdvs to authenticated, service_role;
grant execute on function public.sum_neto_kilos      to authenticated, service_role;

commit;
