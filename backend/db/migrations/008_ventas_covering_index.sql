begin;

-- Covering index for fast COUNT(DISTINCT razon_social) on tenant + date range
-- Enables index-only scan; no heap access needed for cartera/cobertura metrics
create index if not exists idx_ventas_tenant_fecha_razon
  on public.ventas (tenant_id, fecha_comprobante)
  include (razon_social);

-- Update RPC functions to use razon_social as client identifier
create or replace function public.count_distinct_pdvs(
    p_tenant_id  uuid,
    p_start      date,
    p_end        date,
    p_carteras   text[]  default null,
    p_supervisor text    default null
) returns bigint
language sql stable security definer
set search_path = public
as $$
    select count(distinct razon_social)
    from public.ventas
    where (p_tenant_id is null or tenant_id = p_tenant_id)
      and fecha_comprobante between p_start and p_end
      and (p_carteras   is null or cartera    = any(p_carteras))
      and (p_supervisor is null or supervisor = p_supervisor);
$$;

commit;
