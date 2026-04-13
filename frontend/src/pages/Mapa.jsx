import { useState, useEffect, useRef, useCallback } from 'react';
import { MapContainer, TileLayer, Marker, Popup, useMap } from 'react-leaflet';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';
import 'leaflet.markercluster/dist/MarkerCluster.css';
import 'leaflet.markercluster/dist/MarkerCluster.Default.css';
import 'leaflet.markercluster';
import { useAuth } from '../hooks/useAuth.jsx';

const API = import.meta.env.VITE_API_URL;

/* ── Color palette per cartera ─────────────────────────────── */
const COLORS = [
  '#3b82f6', '#ef4444', '#10b981', '#f59e0b', '#8b5cf6',
  '#ec4899', '#06b6d4', '#f97316', '#14b8a6', '#6366f1',
];

function getColor(cartera, carteras) {
  const idx = carteras.indexOf(cartera);
  return COLORS[idx % COLORS.length] || COLORS[0];
}

function makeIcon(color) {
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="25" height="41" viewBox="0 0 25 41">
    <path d="M12.5 0C5.6 0 0 5.6 0 12.5C0 21.9 12.5 41 12.5 41S25 21.9 25 12.5C25 5.6 19.4 0 12.5 0z" fill="${color}"/>
    <circle cx="12.5" cy="12.5" r="6" fill="white" opacity="0.9"/>
  </svg>`;
  return L.divIcon({
    html: svg,
    className: '',
    iconSize: [25, 41],
    iconAnchor: [12, 41],
    popupAnchor: [0, -35],
  });
}

/* ── MarkerCluster layer (imperative, leaflet.markercluster) ── */
function ClusterLayer({ features, carteras }) {
  const map = useMap();
  const clusterRef = useRef(null);

  useEffect(() => {
    if (clusterRef.current) {
      map.removeLayer(clusterRef.current);
    }

    const cluster = L.markerClusterGroup({
      chunkedLoading: true,
      maxClusterRadius: 50,
      spiderfyOnMaxZoom: true,
      showCoverageOnHover: false,
    });

    for (const f of features) {
      const [lng, lat] = f.geometry.coordinates;
      const p = f.properties;
      const color = getColor(p.cartera, carteras);
      const icon = makeIcon(color);

      const marker = L.marker([lat, lng], { icon });
      marker.bindPopup(`
        <div style="min-width:200px;font-family:system-ui;font-size:13px">
          <div style="font-weight:700;font-size:14px;margin-bottom:4px">${p.razon_social || 'Sin nombre'}</div>
          <div style="color:#666;margin-bottom:6px">${p.cod_cliente || ''}</div>
          <table style="width:100%;border-collapse:collapse">
            <tr><td style="color:#999;padding:2px 8px 2px 0">Domicilio</td><td>${p.domicilio || '-'}</td></tr>
            <tr><td style="color:#999;padding:2px 8px 2px 0">Localidad</td><td>${p.localidad || '-'}</td></tr>
            <tr><td style="color:#999;padding:2px 8px 2px 0">Cartera</td><td><span style="color:${color};font-weight:600">${p.cartera || '-'}</span></td></tr>
            <tr><td style="color:#999;padding:2px 8px 2px 0">Zona</td><td>${p.zona || '-'}</td></tr>
            <tr><td style="color:#999;padding:2px 8px 2px 0">Canal</td><td>${p.canal_vta || '-'}</td></tr>
            <tr><td style="color:#999;padding:2px 8px 2px 0">Vendedor</td><td>${p.vendedor || '-'}</td></tr>
            <tr><td style="color:#999;padding:2px 8px 2px 0">Teléfono</td><td>${p.tel_movil || '-'}</td></tr>
          </table>
        </div>
      `);
      cluster.addLayer(marker);
    }

    map.addLayer(cluster);
    clusterRef.current = cluster;

    // Fit bounds if we have markers
    if (features.length > 0) {
      const bounds = cluster.getBounds();
      if (bounds.isValid()) {
        map.fitBounds(bounds, { padding: [40, 40] });
      }
    }

    return () => {
      if (clusterRef.current) {
        map.removeLayer(clusterRef.current);
      }
    };
  }, [features, carteras, map]);

  return null;
}

/* ── Filter bar ────────────────────────────────────────────── */
function FilterBar({ filtros, filters, setFilters, total }) {
  const sel =
    'bg-gray-800 border border-gray-700 text-white text-xs rounded-lg px-2 py-1.5 ' +
    'focus:outline-none focus:ring-1 focus:ring-blue-500 min-w-[120px]';

  return (
    <div className="flex flex-wrap items-center gap-3 px-4 py-3 bg-gray-900/80 backdrop-blur border-b border-gray-800 z-[1000] relative">
      <span className="text-xs text-gray-400 font-medium">Filtros:</span>

      <select className={sel} value={filters.cartera} onChange={(e) => setFilters((f) => ({ ...f, cartera: e.target.value }))}>
        <option value="">Todas las carteras</option>
        {(filtros.carteras || []).map((c) => <option key={c} value={c}>{c}</option>)}
      </select>

      <select className={sel} value={filters.zona} onChange={(e) => setFilters((f) => ({ ...f, zona: e.target.value }))}>
        <option value="">Todas las zonas</option>
        {(filtros.zonas || []).map((z) => <option key={z} value={z}>{z}</option>)}
      </select>

      <select className={sel} value={filters.canal} onChange={(e) => setFilters((f) => ({ ...f, canal: e.target.value }))}>
        <option value="">Todos los canales</option>
        {(filtros.canales || []).map((c) => <option key={c} value={c}>{c}</option>)}
      </select>

      <select className={sel} value={filters.localidad} onChange={(e) => setFilters((f) => ({ ...f, localidad: e.target.value }))}>
        <option value="">Todas las localidades</option>
        {(filtros.localidades || []).map((l) => <option key={l} value={l}>{l}</option>)}
      </select>

      <span className="ml-auto text-xs text-gray-500">{total} PDVs en mapa</span>
    </div>
  );
}

/* ── Legend ─────────────────────────────────────────────────── */
function Legend({ carteras }) {
  if (!carteras.length) return null;
  return (
    <div className="absolute bottom-6 left-4 z-[1000] bg-gray-900/90 backdrop-blur border border-gray-700 rounded-lg px-3 py-2 space-y-1 max-h-48 overflow-y-auto">
      <div className="text-[10px] text-gray-400 font-semibold uppercase tracking-wider mb-1">Carteras</div>
      {carteras.map((c) => (
        <div key={c} className="flex items-center gap-2 text-xs text-gray-300">
          <span className="w-3 h-3 rounded-full flex-shrink-0" style={{ backgroundColor: getColor(c, carteras) }} />
          {c}
        </div>
      ))}
    </div>
  );
}

/* ── Main component ────────────────────────────────────────── */
export default function Mapa() {
  const { session, selectedTenant } = useAuth();
  const [geojson, setGeojson] = useState(null);
  const [filtros, setFiltros] = useState({ carteras: [], zonas: [], canales: [], localidades: [] });
  const [filters, setFilters] = useState({ cartera: '', zona: '', canal: '', localidad: '' });
  const [loading, setLoading] = useState(true);

  const headers = useCallback(() => ({
    Authorization: `Bearer ${session?.access_token}`,
  }), [session]);

  // Load filter options once (or when tenant changes)
  useEffect(() => {
    const params = new URLSearchParams();
    if (selectedTenant) params.set('tenant_id', selectedTenant);
    const qs = params.toString();
    fetch(`${API}/api/mapa/filtros${qs ? `?${qs}` : ''}`, { headers: headers() })
      .then((r) => r.ok ? r.json() : { carteras: [], zonas: [], canales: [], localidades: [] })
      .then((data) => {
        setFiltros(data);
        setFilters({ cartera: '', zona: '', canal: '', localidad: '' });
      })
      .catch(() => {});
  }, [headers, selectedTenant]);

  // Load GeoJSON whenever filters or tenant change
  useEffect(() => {
    setLoading(true);
    const params = new URLSearchParams();
    if (selectedTenant) params.set('tenant_id', selectedTenant);
    if (filters.cartera) params.set('cartera', filters.cartera);
    if (filters.zona) params.set('zona', filters.zona);
    if (filters.canal) params.set('canal', filters.canal);
    if (filters.localidad) params.set('localidad', filters.localidad);

    fetch(`${API}/api/mapa/geojson?${params}`, { headers: headers() })
      .then((r) => r.ok ? r.json() : { type: 'FeatureCollection', features: [] })
      .then((data) => {
        setGeojson(data);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, [filters, headers, selectedTenant]);

  const features = geojson?.features || [];

  // Center on Buenos Aires South (GBA)
  const center = [-34.75, -58.35];

  return (
    <div className="flex flex-col h-full relative">
      <FilterBar filtros={filtros} filters={filters} setFilters={setFilters} total={features.length} />

      {loading && (
        <div className="absolute inset-0 z-[1001] flex items-center justify-center bg-gray-950/60">
          <div className="text-sm text-gray-300 animate-pulse">Cargando mapa…</div>
        </div>
      )}

      <div className="flex-1 relative">
        <MapContainer
          center={center}
          zoom={11}
          className="h-full w-full"
          zoomControl={true}
          preferCanvas={true}
        >
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a>'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />
          <ClusterLayer features={features} carteras={filtros.carteras || []} />
        </MapContainer>

        <Legend carteras={filtros.carteras || []} />
      </div>
    </div>
  );
}
