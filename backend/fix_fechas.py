from dotenv import load_dotenv
load_dotenv(r'C:\Users\felip\DistrAi\distribuidora-app\backend\.env')
import os, time
from supabase import create_client
from datetime import date

sb = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_SERVICE_KEY'])
total_updated = 0
batch = 0
FETCH = 1000
UPSERT = 200

log = open(r'C:\Users\felip\DistrAi\distribuidora-app\backend\fecha_update.log', 'a')
log.write('=== START ===\n')
log.flush()

while True:
    rows = (
        sb.table('ventas')
        .select('id, anio, mes, dia')
        .is_('fecha_comprobante', 'null')
        .not_.is_('anio', 'null')
        .not_.is_('mes', 'null')
        .not_.is_('dia', 'null')
        .limit(FETCH)
        .execute()
    ).data

    if not rows:
        log.write(f'LISTO. Total: {total_updated}\n')
        log.flush()
        break

    updates = []
    for r in rows:
        try:
            d = date(int(r['anio']), int(r['mes']), int(r['dia']))
            updates.append({'id': r['id'], 'fecha_comprobante': d.isoformat()})
        except Exception:
            pass

    for i in range(0, len(updates), UPSERT):
        try:
            sb.table('ventas').upsert(updates[i:i+UPSERT]).execute()
            total_updated += len(updates[i:i+UPSERT])
        except Exception as e:
            log.write(f'Error chunk: {e}\n')
            log.flush()
            time.sleep(2)

    batch += 1
    if batch % 50 == 0:
        log.write(f'Batch {batch}: {total_updated} actualizadas\n')
        log.flush()
    elif batch % 10 == 0:
        print(f'Batch {batch}: {total_updated}...')

log.close()
print(f'LISTO. Total actualizado: {total_updated}')
