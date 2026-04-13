-- ── Migración de datos existentes al esquema multi-tenant ───────────────────
-- Correr DESPUÉS de 004_multi_tenant.sql y ANTES de 005 y 006.
--
-- Este script:
--   1. Crea el tenant para la primera distribuidora (la data que ya está cargada)
--   2. Asigna toda la data existente a ese tenant
--   3. Preserva el geocoding ya realizado (no hay que volver a geocodificar)
--
-- Personalizar las variables al principio antes de correr.

begin;

-- ── PERSONALIZAR ESTOS VALORES ────────────────────────────────────────────────
do $$
declare
  v_nombre text := 'Candysur';  -- ← Nombre de tu distribuidora
  v_slug   text := 'candysur';  -- ← Slug (sin espacios, solo letras/números/guiones)
  v_plan   text := 'basic';
  v_tenant_id uuid;
begin

  -- Crear el tenant
  insert into public.tenants (nombre, slug, plan)
  values (v_nombre, v_slug, v_plan)
  returning id into v_tenant_id;

  raise notice 'Tenant creado: % (id: %)', v_nombre, v_tenant_id;

  -- Asignar toda la data existente a este tenant
  update public.pdv        set tenant_id = v_tenant_id where tenant_id is null;
  update public.ventas     set tenant_id = v_tenant_id where tenant_id is null;
  update public.supervisores set tenant_id = v_tenant_id where tenant_id is null;
  update public.perfiles   set tenant_id = v_tenant_id where tenant_id is null;

  raise notice 'PDVs actualizados:        %', (select count(*) from public.pdv        where tenant_id = v_tenant_id);
  raise notice 'Ventas actualizadas:      %', (select count(*) from public.ventas     where tenant_id = v_tenant_id);
  raise notice 'Supervisores actualizados:%', (select count(*) from public.supervisores where tenant_id = v_tenant_id);
  raise notice 'Perfiles actualizados:    %', (select count(*) from public.perfiles   where tenant_id = v_tenant_id);

  -- Actualizar la clave de config para que use el prefijo de tenant
  update public.config
  set key = 'tenant:' || v_tenant_id::text || ':ventas_ultima_actualizacion'
  where key = 'ventas_ultima_actualizacion';

  raise notice 'Tenant ID para guardar: %', v_tenant_id;
  raise notice '✓ Migración completada. Guardá el tenant_id de arriba.';

end $$;

commit;
