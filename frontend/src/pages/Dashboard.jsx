import { useCallback, useEffect, useRef, useState } from 'react';
import { useAuth } from '../hooks/useAuth.jsx';

const API = import.meta.env.VITE_API_URL;

const TABS = [
  { key: 'neto',     label: 'VOL. $' },
  { key: 'kilos',    label: 'VOL. KG' },
  { key: 'clientes', label: 'CLIENTES' },
];

function fmtMoney(value) {
  return value == null ? '—' : `$${Number(value).toLocaleString('es-AR', { minimumFractionDigits: 0, maximumFractionDigits: 0 })}`;
}

function fmtKilos(value) {
  return value == null ? '—' : `${Number(value).toLocaleString('es-AR', { minimumFractionDigits: 0, maximumFractionDigits: 3 })} KG`;
}

function fmtInt(value) {
  return value == null ? '—' : Number(value).toLocaleString('es-AR');
}

function fmtPct(value) {
  return value == null ? '—' : `${Number(value).toLocaleString('es-AR', { minimumFractionDigits: 1, maximumFractionDigits: 1 })}%`;
}

function deltaSign(value) {
  if (value == null) return '';
  if (value > 0) return '+';
  return '';
}

function deltaColor(value) {
  if (value == null) return 'text-nothing-dim';
  if (value > 0) return 'text-green-400';
  if (value < 0) return 'text-nothing-red';
  return 'text-nothing-dim';
}

const KPI_DEFS = [
  { key: 'acumulado_neto',      label: 'ACUM. $',       fmt: fmtMoney,  accent: false },
  { key: 'acumulado_kilos',     label: 'ACUM. KG',      fmt: fmtKilos,  accent: false },
  { key: 'tendencia_neto',      label: 'TENDENCIA $',   fmt: fmtMoney,  accent: false },
  { key: 'tendencia_kilos',     label: 'TENDENCIA KG',  fmt: fmtKilos,  accent: false },
  { key: 'variacion_neto_pct',  label: 'VAR. $ MES ANT', fmt: fmtPct,   accent: true },
  { key: 'variacion_kilos_pct', label: 'VAR. KG MES ANT', fmt: fmtPct,  accent: true },
  { key: 'cartera_activa',      label: 'CARTERA ACTIVA', fmt: fmtInt,   accent: false },
  { key: 'cobertura_pct',       label: 'COBERTURA',     fmt: fmtPct,    accent: false },
  { key: 'pdvs_dia',            label: 'PDVs DÍA',      fmt: fmtInt,    accent: false },
  { key: 'total_pdv_maestro',   label: 'PDVs MAESTRO',  fmt: fmtInt,    accent: false },
];

const MONTH_NAMES = ['', 'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'];

export default function Dashboard() {
  const { session } = useAuth();
  const [tab, setTab] = useState('neto');
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const versionRef = useRef(null);

  // Period selector state
  const [periods, setPeriods] = useState([]);
  const [selectedMes, setSelectedMes] = useState(null);
  const [selectedAnio, setSelectedAnio] = useState(null);

  const headers = useCallback(() => ({
    Authorization: `Bearer ${session?.access_token}`,
  }), [session]);

  // Fetch available periods lazily (only when user opens the selector)
  const [periodsLoaded, setPeriodsLoaded] = useState(false);
  const loadPeriods = useCallback(() => {
    if (periodsLoaded || !session?.access_token) return;
    setPeriodsLoaded(true);
    fetch(`${API}/api/dashboard/periods`, { headers: headers() })
      .then(r => r.ok ? r.json() : [])
      .then(setPeriods)
      .catch(() => {});
  }, [periodsLoaded, session?.access_token, headers]);

  const loadDashboard = useCallback(async ({ silent = false, mes, anio } = {}) => {
    if (!session?.access_token) return;
    if (!silent) setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      if (mes != null) params.set('mes', mes);
      if (anio != null) params.set('anio', anio);
      const qs = params.toString();
      const res = await fetch(`${API}/api/dashboard${qs ? `?${qs}` : ''}`, { headers: headers() });
      if (!res.ok) throw new Error(`ERROR ${res.status}`);
      const body = await res.json();
      versionRef.current = body.version || null;
      setData(body);
    } catch (err) {
      setError(err.message || 'ERROR CARGANDO DASHBOARD');
    } finally {
      if (!silent) setLoading(false);
    }
  }, [headers, session?.access_token]);

  // Load dashboard when period selection changes
  useEffect(() => {
    loadDashboard({ mes: selectedMes, anio: selectedAnio });
  }, [selectedMes, selectedAnio, loadDashboard]);

  useEffect(() => {
    if (!session?.access_token) return undefined;
    // Only auto-refresh when viewing latest (no specific period selected)
    if (selectedMes != null || selectedAnio != null) return undefined;
    const timer = window.setInterval(async () => {
      try {
        const res = await fetch(`${API}/api/dashboard/version`, { headers: headers() });
        if (!res.ok) return;
        const body = await res.json();
        const nextVersion = body.version || null;
        if (nextVersion && nextVersion !== versionRef.current) {
          loadDashboard({ silent: true });
        }
      } catch { /* ignore */ }
    }, 30000);
    return () => window.clearInterval(timer);
  }, [headers, loadDashboard, session?.access_token, selectedMes, selectedAnio]);

  if (loading) {
    return (
      <div className="p-6 space-y-6 font-mono animate-fade-in">
        <div className="flex items-center gap-3">
          <div className="w-2 h-2 bg-nothing-red animate-pulse" />
          <span className="text-nothing-dim text-xs uppercase tracking-widest dot-cursor">
            CARGANDO DASHBOARD
          </span>
        </div>
        <div className="grid grid-cols-2 sm:grid-cols-3 xl:grid-cols-5 gap-2">
          {Array.from({ length: 10 }).map((_, i) => (
            <div key={i} className="h-20 bg-nothing-card border border-nothing-border animate-pulse" />
          ))}
        </div>
        <div className="h-48 bg-nothing-card border border-nothing-border animate-pulse" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-6 font-mono">
        <div className="border border-nothing-red/50 bg-nothing-red/5 px-5 py-4">
          <p className="text-nothing-red text-xs uppercase tracking-widest font-bold mb-1">! ERROR</p>
          <p className="text-nothing-light text-sm">{error}</p>
          <button
            onClick={() => loadDashboard()}
            className="mt-3 text-[10px] uppercase tracking-widest text-nothing-red hover:underline"
          >
            REINTENTAR →
          </button>
        </div>
      </div>
    );
  }

  const header = data?.header || {};
  const businessDays = data?.business_days || { elapsed: 0, total: 0 };
  const rows = data?.tabs?.[tab] || [];
  const bdPct = businessDays.total > 0 ? (businessDays.elapsed / businessDays.total) * 100 : 0;

  return (
    <div className="p-6 space-y-5 font-mono animate-fade-in">
      {/* Page header */}
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <div className="flex items-center gap-2 mb-1">
            <div className="w-2 h-2 bg-nothing-red" />
            <span className="text-[10px] uppercase tracking-[0.25em] text-nothing-dim font-bold">DISTRAI / DASHBOARD</span>
          </div>
          <div className="flex flex-wrap items-center gap-4 text-xs text-nothing-dim">
            <span>
              CORTE:{' '}
              <span className="text-nothing-white font-bold">{data?.latest_date || '—'}</span>
            </span>
            <span className="flex items-center gap-2">
              DÍAS HÁB.:{' '}
              <span className="text-nothing-white font-bold">
                {businessDays.elapsed}/{businessDays.total}
              </span>
              <span className="inline-block w-16 h-1 bg-nothing-border align-middle relative overflow-hidden">
                <span
                  className="absolute left-0 top-0 h-full bg-nothing-red transition-all"
                  style={{ width: `${bdPct}%` }}
                />
              </span>
            </span>
          </div>
        </div>
        <div className="flex items-center gap-3">
          {/* Period selector */}
          <select
            onFocus={loadPeriods}
            value={selectedMes != null && selectedAnio != null ? `${selectedAnio}-${selectedMes}` : ''}
            onChange={(e) => {
              const val = e.target.value;
              if (!val) {
                setSelectedMes(null);
                setSelectedAnio(null);
              } else {
                const [a, m] = val.split('-').map(Number);
                setSelectedAnio(a);
                setSelectedMes(m);
              }
            }}
            className="bg-nothing-card border border-nothing-border text-nothing-light text-[11px] uppercase tracking-widest font-bold px-3 py-1.5 focus:outline-none focus:border-nothing-red transition-colors"
          >
            <option value="">ÚLTIMO PERÍODO</option>
            {periods.map((p) => (
              <option key={`${p.anio}-${p.mes}`} value={`${p.anio}-${p.mes}`}>
                {MONTH_NAMES[p.mes]} {p.anio}
              </option>
            ))}
          </select>
          {!data?.meta?.uses_summary_for_kilos && (
            <div className="border border-yellow-600/40 bg-yellow-600/5 px-3 py-2 text-[10px] text-yellow-500 uppercase tracking-widest font-bold">
              ! KG USA FALLBACK TEMPORAL
            </div>
          )}
        </div>
      </div>

      {/* KPI grid */}
      <section className="grid grid-cols-2 sm:grid-cols-3 xl:grid-cols-5 gap-2">
        {KPI_DEFS.map(({ key, label, fmt, accent }) => {
          const val = header[key];
          const isDelta = accent;
          const valClass = isDelta ? deltaColor(val) : 'text-nothing-white';

          return (
            <div key={key} className="bg-nothing-card border border-nothing-border p-3 group hover:border-nothing-muted transition-colors">
              <p className="n-label mb-2">{label}</p>
              <p className={`text-lg font-bold leading-none ${valClass}`}>
                {isDelta && <span className="text-[13px]">{deltaSign(val)}</span>}
                {fmt(val)}
              </p>
            </div>
          );
        })}
      </section>

      {/* Tabs + table */}
      <section className="border border-nothing-border bg-nothing-card">
        {/* Tab bar */}
        <div className="flex border-b border-nothing-border">
          {TABS.map((item) => (
            <button
              key={item.key}
              type="button"
              onClick={() => setTab(item.key)}
              className={[
                'px-4 py-2.5 text-[11px] font-bold uppercase tracking-widest transition-colors relative',
                tab === item.key
                  ? 'text-nothing-white bg-nothing-bg'
                  : 'text-nothing-dim hover:text-nothing-light',
              ].join(' ')}
            >
              {tab === item.key && (
                <span className="absolute bottom-0 left-0 right-0 h-0.5 bg-nothing-red" />
              )}
              {item.label}
            </button>
          ))}
        </div>

        <div className="overflow-x-auto">
          {tab === 'clientes' ? <ClientesTable rows={rows} /> : <VolumenTable rows={rows} metric={tab} />}
        </div>
      </section>
    </div>
  );
}

function VolumenTable({ rows, metric }) {
  return (
    <table className="min-w-full text-xs font-mono">
      <thead>
        <tr className="border-b border-nothing-border">
          <TH>CATEGORÍA</TH>
          <TH right>ACUMULADO</TH>
          <TH right>TENDENCIA</TH>
          <TH right>MEDIA REAL</TH>
          <TH right>MISMO DÍA -7</TH>
          <TH right>MISMO DÍA -14</TH>
        </tr>
      </thead>
      <tbody>
        {rows.length === 0 ? (
          <EmptyRow colSpan={6} />
        ) : (
          rows.map((row, i) => (
            <tr
              key={row.categoria}
              className={`border-t border-nothing-border hover:bg-nothing-border/30 transition-colors ${i % 2 === 0 ? '' : 'bg-white/[0.02]'}`}
            >
              <TD><span className="font-bold text-nothing-white">{row.categoria}</span></TD>
              <TD right>{metric === 'neto' ? fmtMoney(row.acumulado) : fmtKilos(row.acumulado)}</TD>
              <TD right>{metric === 'neto' ? fmtMoney(row.tendencia) : fmtKilos(row.tendencia)}</TD>
              <TD right>{metric === 'neto' ? fmtMoney(row.media_real) : fmtKilos(row.media_real)}</TD>
              <TD right>{metric === 'neto' ? fmtMoney(row.mismo_dia_7) : fmtKilos(row.mismo_dia_7)}</TD>
              <TD right>{metric === 'neto' ? fmtMoney(row.mismo_dia_14) : fmtKilos(row.mismo_dia_14)}</TD>
            </tr>
          ))
        )}
      </tbody>
    </table>
  );
}

function ClientesTable({ rows }) {
  return (
    <table className="min-w-full text-xs font-mono">
      <thead>
        <tr className="border-b border-nothing-border">
          <TH>CATEGORÍA</TH>
          <TH right>PDVs ACUM.</TH>
          <TH right>PDVs HOY</TH>
          <TH right>SIN CONCRETAR</TH>
          <TH right>% PDVs DÍA</TH>
        </tr>
      </thead>
      <tbody>
        {rows.length === 0 ? (
          <EmptyRow colSpan={5} />
        ) : (
          rows.map((row, i) => (
            <tr
              key={row.categoria}
              className={`border-t border-nothing-border hover:bg-nothing-border/30 transition-colors ${i % 2 === 0 ? '' : 'bg-white/[0.02]'}`}
            >
              <TD><span className="font-bold text-nothing-white">{row.categoria}</span></TD>
              <TD right>{fmtInt(row.pdvs_acumulados)}</TD>
              <TD right>{fmtInt(row.pdvs_hoy)}</TD>
              <TD right>{fmtInt(row.sin_concretar)}</TD>
              <TD right>{fmtPct(row.pct_pdvs_dia)}</TD>
            </tr>
          ))
        )}
      </tbody>
    </table>
  );
}

function TH({ children, right }) {
  return (
    <th className={`px-4 py-2.5 ${right ? 'text-right' : 'text-left'} text-[10px] font-bold uppercase tracking-[0.18em] text-nothing-dim`}>
      {children}
    </th>
  );
}

function TD({ children, right }) {
  return (
    <td className={`px-4 py-2.5 ${right ? 'text-right text-nothing-light' : 'text-left'}`}>
      {children}
    </td>
  );
}

function EmptyRow({ colSpan }) {
  return (
    <tr>
      <td colSpan={colSpan} className="px-4 py-10 text-center text-nothing-muted text-xs uppercase tracking-widest font-bold">
        // SIN DATOS
      </td>
    </tr>
  );
}
