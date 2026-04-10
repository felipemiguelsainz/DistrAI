import { useState, useEffect, useRef, useCallback } from 'react';
import { useAuth } from '../hooks/useAuth.jsx';

const API = import.meta.env.VITE_API_URL;

export default function CargaDatos() {
  const { session } = useAuth();
  const [file, setFile] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [progress, setProgress] = useState(null);
  const [message, setMessage] = useState(null);
  const [stats, setStats] = useState(null);
  const [geocoding, setGeocoding] = useState(false);
  const [geoProgress, setGeoProgress] = useState(null);
  const [geoLimit, setGeoLimit] = useState(100);
  const fileRef = useRef(null);

  // Ventas state
  const [ventasFile, setVentasFile] = useState(null);
  const [ventasUploading, setVentasUploading] = useState(false);
  const [ventasProgress, setVentasProgress] = useState(null);
  const [ventasStats, setVentasStats] = useState(null);
  const ventasFileRef = useRef(null);

  // Reconnect to in-flight jobs on mount (survives tab switches)
  useEffect(() => {
    const savedPdv = sessionStorage.getItem('pdv_job_id');
    if (savedPdv && session?.access_token) {
      setUploading(true);
      setProgress({ total: 0, processed: 0, status: 'running' });
      watchJob(
        savedPdv,
        (data) => setProgress(data),
        (data) => {
          setUploading(false);
          sessionStorage.removeItem('pdv_job_id');
          if (data?.status === 'done') {
            setMessage({ type: 'success', text: `✓ ${data.processed} PDVs cargados. ${data.errors || 0} errores.` });
          } else if (data) {
            setMessage({ type: 'error', text: data?.status || 'Error desconocido' });
          }
          fetch(`${API}/api/pdv/stats`, { headers: headers() }).then(r => r.ok ? r.json() : null).then(setStats).catch(() => {});
        },
      );
    }
    const savedVentas = sessionStorage.getItem('ventas_job_id');
    const savedVentasTotal = parseInt(sessionStorage.getItem('ventas_job_total') || '0', 10);
    if (savedVentas && session?.access_token) {
      setVentasUploading(true);
      setVentasProgress({ total: savedVentasTotal, processed: 0, status: 'running' });
      setMessage({ type: 'info', text: 'Reconectando al proceso de carga de ventas…' });
      watchJob(
        savedVentas,
        (data) => setVentasProgress(data),
        (data) => {
          setVentasUploading(false);
          sessionStorage.removeItem('ventas_job_id');
          sessionStorage.removeItem('ventas_job_total');
          if (data?.status === 'done') {
            setMessage({ type: 'success', text: `✓ ${data.processed} filas de ventas cargadas. ${data.errors || 0} errores.` });
          } else if (data) {
            setMessage({ type: 'error', text: data?.status || 'Error desconocido' });
          }
          fetch(`${API}/api/ventas/stats`, { headers: headers() }).then(r => r.ok ? r.json() : null).then(setVentasStats).catch(() => {});
        },
        '/api/ventas',
      );
    }
  }, [session?.access_token]); // eslint-disable-line react-hooks/exhaustive-deps

  const headers = useCallback(() => ({
    Authorization: `Bearer ${session?.access_token}`,
  }), [session]);

  // Fetch PDV stats on mount
  useEffect(() => {
    fetch(`${API}/api/pdv/stats`, { headers: headers() })
      .then((r) => r.ok ? r.json() : null)
      .then(setStats)
      .catch(() => {});
    fetch(`${API}/api/ventas/stats`, { headers: headers() })
      .then((r) => r.ok ? r.json() : null)
      .then(setVentasStats)
      .catch(() => {});
  }, [headers]);

  // SSE listener for a job
  function watchJob(jobId, onProgress, onDone, prefix = '/api/pdv') {
    const token = session?.access_token || '';
    const evtSource = new EventSource(`${API}${prefix}/upload/progress/${jobId}?token=${encodeURIComponent(token)}`);
    evtSource.onmessage = (e) => {
      const data = JSON.parse(e.data);
      onProgress(data);
      if (data.status === 'done' || data.status?.startsWith('error')) {
        evtSource.close();
        onDone(data);
      }
    };
    evtSource.onerror = () => {
      evtSource.close();
      onDone(null);
    };
  }

  async function handleUpload(e) {
    e.preventDefault();
    if (!file) return;
    setUploading(true);
    setProgress(null);
    setMessage(null);

    const form = new FormData();
    form.append('file', file);

    try {
      const res = await fetch(`${API}/api/pdv/upload`, {
        method: 'POST',
        headers: headers(),
        body: form,
      });

      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        setMessage({ type: 'error', text: err.detail || `Error ${res.status}` });
        setUploading(false);
        return;
      }

      const { job_id, total_rows } = await res.json();
      sessionStorage.setItem('pdv_job_id', job_id);
      setProgress({ total: total_rows, processed: 0, status: 'running' });

      watchJob(
        job_id,
        (data) => setProgress(data),
        (data) => {
          setUploading(false);
          sessionStorage.removeItem('pdv_job_id');
          if (data?.status === 'done') {
            setMessage({ type: 'success', text: `✓ ${data.processed} PDVs cargados. ${data.errors || 0} errores.` });
          } else {
            setMessage({ type: 'error', text: data?.status || 'Error desconocido' });
          }
          setFile(null);
          if (fileRef.current) fileRef.current.value = '';
          // Refresh stats
          fetch(`${API}/api/pdv/stats`, { headers: headers() })
            .then((r) => r.ok ? r.json() : null)
            .then(setStats)
            .catch(() => {});
        },
      );
    } catch (err) {
      setMessage({ type: 'error', text: err.message || 'Error de red' });
      setUploading(false);
    }
  }

  async function handleGeocode() {
    setGeocoding(true);
    setGeoProgress(null);
    try {
      const res = await fetch(`${API}/api/pdv/geocode?limit=${geoLimit}`, {
        method: 'POST',
        headers: headers(),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        setMessage({ type: 'error', text: err.detail || 'Error geocoding' });
        setGeocoding(false);
        return;
      }
      const { job_id } = await res.json();
      watchJob(
        job_id,
        (data) => setGeoProgress(data),
        (data) => {
          setGeocoding(false);
          if (data?.status === 'done') {
            setMessage({ type: 'success', text: `✓ Geocodificación: ${data.processed} procesados, ${data.errors} errores.` });
          }
          fetch(`${API}/api/pdv/stats`, { headers: headers() })
            .then((r) => r.ok ? r.json() : null)
            .then(setStats)
            .catch(() => {});
        },
      );
    } catch (err) {
      setMessage({ type: 'error', text: err.message });
      setGeocoding(false);
    }
  }

  const pct = progress ? Math.round((progress.processed / Math.max(progress.total, 1)) * 100) : 0;
  const geoPct = geoProgress ? Math.round((geoProgress.processed / Math.max(geoProgress.total, 1)) * 100) : 0;
  const ventasPct = ventasProgress ? Math.round((ventasProgress.processed / Math.max(ventasProgress.total, 1)) * 100) : 0;

  async function handleVentasUpload(e) {
    e.preventDefault();
    if (!ventasFile) return;
    setVentasUploading(true);
    setVentasProgress(null);
    setMessage({ type: 'info', text: `Subiendo ${ventasFile.name} (${(ventasFile.size / 1024).toFixed(0)} KB)…` });

    const form = new FormData();
    form.append('file', ventasFile);

    try {
      console.log('[ventas] uploading', ventasFile.name, ventasFile.size, 'bytes');
      const res = await fetch(`${API}/api/ventas/upload`, {
        method: 'POST',
        headers: headers(),
        body: form,
      });

      console.log('[ventas] response status', res.status);

      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        console.error('[ventas] upload error', err);
        setMessage({ type: 'error', text: err.detail || `Error ${res.status}` });
        setVentasUploading(false);
        return;
      }

      const body = await res.json();
      console.log('[ventas] upload ok', body);
      const { job_id, total_rows } = body;
      sessionStorage.setItem('ventas_job_id', job_id);
      sessionStorage.setItem('ventas_job_total', String(total_rows));
      setVentasProgress({ total: total_rows, processed: 0, status: 'running' });
      setMessage({ type: 'info', text: `Procesando ${total_rows} filas…` });

      watchJob(
        job_id,
        (data) => setVentasProgress(data),
        (data) => {
          setVentasUploading(false);
          sessionStorage.removeItem('ventas_job_id');
          sessionStorage.removeItem('ventas_job_total');
          if (data?.status === 'done') {
            setMessage({ type: 'success', text: `✓ ${data.processed} filas de ventas cargadas. ${data.errors || 0} errores.` });
          } else {
            setMessage({ type: 'error', text: data?.status || 'Error desconocido' });
          }
          setVentasFile(null);
          if (ventasFileRef.current) ventasFileRef.current.value = '';
          fetch(`${API}/api/ventas/stats`, { headers: headers() })
            .then((r) => r.ok ? r.json() : null)
            .then(setVentasStats)
            .catch(() => {});
        },
        '/api/ventas',
      );
    } catch (err) {
      console.error('[ventas] network error', err);
      setMessage({ type: 'error', text: err.message || 'Error de red' });
      setVentasUploading(false);
    }
  }

  return (
    <div className="p-6 max-w-3xl mx-auto space-y-6">
      <h1 className="text-2xl font-bold">Carga de Datos</h1>

      {/* Stats cards */}
      {stats && (
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
          <StatCard label="Total PDV" value={stats.total} />
          <StatCard label="Geocodificados" value={stats.geocoded} color="text-green-400" />
          <StatCard label="Pendientes" value={stats.pending} color="text-yellow-400" />
          <StatCard label="Fallidos" value={stats.failed} color="text-red-400" />
        </div>
      )}

      {/* Messages */}
      {message && (
        <div className={`rounded-lg px-4 py-3 text-sm ${
          message.type === 'error'
            ? 'bg-red-900/40 border border-red-700 text-red-300'
            : message.type === 'info'
            ? 'bg-blue-900/40 border border-blue-700 text-blue-300'
            : 'bg-green-900/40 border border-green-700 text-green-300'
        }`}>
          {message.text}
        </div>
      )}

      {/* Upload section */}
      <div className="bg-gray-900 border border-gray-800 rounded-xl p-6 space-y-4">
        <h2 className="text-lg font-semibold">Subir archivo de PDVs</h2>
        <p className="text-sm text-gray-400">
          Aceptamos <strong>.csv</strong>, <strong>.xlsx</strong> o <strong>.xls</strong>.
          La columna <code className="bg-gray-800 px-1 rounded">Cod. Cliente</code> es obligatoria (clave para upsert).
        </p>

        <form onSubmit={handleUpload} className="flex flex-col sm:flex-row gap-3 items-start">
          <input
            ref={fileRef}
            type="file"
            accept=".csv,.xlsx,.xls"
            onChange={(e) => setFile(e.target.files?.[0] || null)}
            className="block w-full text-sm text-gray-400 file:mr-4 file:py-2 file:px-4
              file:rounded-lg file:border-0 file:text-sm file:font-semibold
              file:bg-blue-600 file:text-white hover:file:bg-blue-500 file:cursor-pointer"
          />
          <button
            type="submit"
            disabled={!file || uploading}
            className="px-5 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium
              hover:bg-blue-500 disabled:opacity-40 disabled:cursor-not-allowed whitespace-nowrap"
          >
            {uploading ? 'Subiendo...' : 'Subir'}
          </button>
        </form>

        {/* Upload progress bar */}
        {progress && (
          <div className="space-y-1">
            <div className="flex justify-between text-xs text-gray-400">
              <span>{progress.processed} / {progress.total} filas</span>
              <span>{pct}%</span>
            </div>
            <div className="w-full bg-gray-800 rounded-full h-2">
              <div
                className="bg-blue-500 h-2 rounded-full transition-all duration-300"
                style={{ width: `${pct}%` }}
              />
            </div>
          </div>
        )}
      </div>

      {/* Geocoding section */}
      <div className="bg-gray-900 border border-gray-800 rounded-xl p-6 space-y-4">
        <h2 className="text-lg font-semibold">Geocodificación</h2>
        <p className="text-sm text-gray-400">
          Geocodifica PDVs pendientes usando Nominatim (gratuito) + OpenAI como fallback.
        </p>
        <div className="flex items-center gap-3">
          <label className="text-sm text-gray-400 whitespace-nowrap">Tanda:</label>
          <select
            value={geoLimit}
            onChange={(e) => setGeoLimit(Number(e.target.value))}
            disabled={geocoding}
            className="bg-gray-800 border border-gray-700 text-white text-sm rounded-lg px-3 py-1.5
              focus:outline-none focus:ring-1 focus:ring-emerald-500 disabled:opacity-40"
          >
            {[100, 250, 500, 1000, 2500, 9999].map((n) => (
              <option key={n} value={n}>{n === 9999 ? 'Todos' : n}</option>
            ))}
          </select>
        </div>
        <button
          onClick={handleGeocode}
          disabled={geocoding || !stats?.pending}
          className="px-5 py-2 bg-emerald-600 text-white rounded-lg text-sm font-medium
            hover:bg-emerald-500 disabled:opacity-40 disabled:cursor-not-allowed"
        >
          {geocoding ? 'Geocodificando...' : `Geocodificar (${stats?.pending || 0} pendientes)`}
        </button>

        {geoProgress && (
          <div className="space-y-1">
            <div className="flex justify-between text-xs text-gray-400">
              <span>{geoProgress.processed} / {geoProgress.total}</span>
              <span>{geoPct}%</span>
            </div>
            <div className="w-full bg-gray-800 rounded-full h-2">
              <div
                className="bg-emerald-500 h-2 rounded-full transition-all duration-300"
                style={{ width: `${geoPct}%` }}
              />
            </div>
          </div>
        )}
      </div>

      {/* Ventas upload section */}
      <div className="bg-gray-900 border border-gray-800 rounded-xl p-6 space-y-4">
        <h2 className="text-lg font-semibold">Subir archivo de Ventas</h2>
        <p className="text-sm text-gray-400">
          Aceptamos <strong>.csv</strong>, <strong>.xlsx</strong> o <strong>.xls</strong>.
          Las columnas <code className="bg-gray-800 px-1 rounded">Comprobante</code> y{' '}
          <code className="bg-gray-800 px-1 rounded">SKU</code> son obligatorias (clave para upsert).
        </p>

        <div className="rounded-xl border border-cyan-900 bg-cyan-950/30 p-4 text-sm text-cyan-100 space-y-1">
          <p className="font-medium">Carga diaria de ventas</p>
          <p className="text-cyan-200/80">
            Esta sección queda para la operación diaria. Cada archivo que subas actualiza la tabla de ventas,
            marca la última actualización y dispara el refresco automático del dashboard.
          </p>
        </div>

        {ventasStats && (
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
            <StatCard label="Filas de ventas" value={ventasStats.total} color="text-cyan-400" />
            <InfoCard label="Última actualización" value={ventasStats.last_update ? formatDateTime(ventasStats.last_update) : 'Sin carga'} />
            <InfoCard label="Última fecha vendida" value={ventasStats.latest_sale_date || 'Sin datos'} />
          </div>
        )}

        <form onSubmit={handleVentasUpload} className="flex flex-col sm:flex-row gap-3 items-start">
          <input
            ref={ventasFileRef}
            type="file"
            accept=".csv,.xlsx,.xls"
            onChange={(e) => setVentasFile(e.target.files?.[0] || null)}
            className="block w-full text-sm text-gray-400 file:mr-4 file:py-2 file:px-4
              file:rounded-lg file:border-0 file:text-sm file:font-semibold
              file:bg-cyan-600 file:text-white hover:file:bg-cyan-500 file:cursor-pointer"
          />
          <button
            type="submit"
            disabled={!ventasFile || ventasUploading}
            className="px-5 py-2 bg-cyan-600 text-white rounded-lg text-sm font-medium
              hover:bg-cyan-500 disabled:opacity-40 disabled:cursor-not-allowed whitespace-nowrap"
          >
            {ventasUploading ? 'Subiendo...' : 'Subir Ventas'}
          </button>
        </form>

        {ventasProgress && (
          <div className="space-y-1">
            <div className="flex justify-between text-xs text-gray-400">
              <span>{ventasProgress.processed} / {ventasProgress.total} filas</span>
              <span>{ventasPct}%</span>
            </div>
            <div className="w-full bg-gray-800 rounded-full h-2">
              <div
                className="bg-cyan-500 h-2 rounded-full transition-all duration-300"
                style={{ width: `${ventasPct}%` }}
              />
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function StatCard({ label, value, color = 'text-white' }) {
  return (
    <div className="bg-gray-900 border border-gray-800 rounded-xl p-4">
      <p className="text-xs text-gray-500 uppercase tracking-wide">{label}</p>
      <p className={`text-2xl font-bold mt-1 ${color}`}>{value.toLocaleString('es-AR')}</p>
    </div>
  );
}

function InfoCard({ label, value }) {
  return (
    <div className="bg-gray-900 border border-gray-800 rounded-xl p-4">
      <p className="text-xs text-gray-500 uppercase tracking-wide">{label}</p>
      <p className="mt-1 text-sm font-medium text-gray-200">{value}</p>
    </div>
  );
}

function formatDateTime(value) {
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) {
    return value;
  }
  return dt.toLocaleString('es-AR');
}
