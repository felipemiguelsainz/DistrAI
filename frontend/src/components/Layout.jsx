import { useState, useEffect } from 'react';
import { NavLink, Outlet } from 'react-router-dom';
import { useAuth } from '../hooks/useAuth.jsx';

const NAV = [
  { to: '/',         label: 'DASHBOARD',   tag: '01', roles: ['superadmin', 'admin', 'analista', 'supervisor', 'vendedor'] },
  { to: '/mapa',     label: 'MAPA',        tag: '02', roles: ['superadmin', 'admin', 'analista', 'supervisor', 'vendedor'] },
  { to: '/pdv',      label: 'GESTIÓN PDV', tag: '03', roles: ['superadmin', 'admin', 'analista'] },
  { to: '/carga',    label: 'CARGA DATOS', tag: '04', roles: ['superadmin', 'admin'] },
  { to: '/usuarios', label: 'USUARIOS',    tag: '05', roles: ['superadmin', 'admin'] },
];

const ROLE_COLOR = {
  superadmin: 'text-nothing-red',
  admin:      'text-nothing-red',
  analista:   'text-white',
  supervisor: 'text-nothing-light',
  vendedor:   'text-nothing-light',
};

function getInitials(name) {
  if (!name) return '??';
  return name.trim().split(/\s+/).slice(0, 2).map((w) => w[0].toUpperCase()).join('');
}

const API = import.meta.env.VITE_API_URL;

export default function Layout() {
  const { profile, signOut, session, selectedTenant, setSelectedTenant } = useAuth();
  const [collapsed, setCollapsed] = useState(false);
  const [tenants, setTenants] = useState([]);

  const isSuperadmin = profile?.rol === 'superadmin';

  useEffect(() => {
    if (!isSuperadmin || !session?.access_token) return;
    fetch(`${API}/api/admin/tenants`, {
      headers: { Authorization: `Bearer ${session.access_token}` },
    })
      .then((r) => r.ok ? r.json() : Promise.reject(r.status))
      .then((data) => setTenants(Array.isArray(data) ? data : []))
      .catch((err) => console.error('[Layout] tenant fetch failed:', err));
  }, [isSuperadmin, session?.access_token]);

  const visibleNav = NAV.filter((n) => n.roles.includes(profile?.rol));
  const displayName = profile?.nombre || profile?.email || '';
  const initials = getInitials(profile?.nombre || profile?.email);
  const roleClass = ROLE_COLOR[profile?.rol] || 'text-nothing-light';

  return (
    <div className="flex h-screen overflow-hidden bg-nothing-bg text-nothing-white font-mono">
      {/* Sidebar */}
      <aside
        className={`flex flex-col border-r border-nothing-border bg-nothing-bg transition-all duration-200 ${
          collapsed ? 'w-14' : 'w-52'
        }`}
      >
        {/* Logo */}
        <div className={`flex items-center h-12 border-b border-nothing-border px-3 gap-2 ${collapsed ? 'justify-center' : 'justify-between'}`}>
          {!collapsed && (
            <div className="flex items-center gap-2">
              <div className="w-2 h-2 bg-nothing-red shrink-0" />
              <span className="font-bold text-sm tracking-widest text-nothing-white">DISTRAI</span>
            </div>
          )}
          {collapsed && <div className="w-2 h-2 bg-nothing-red" />}
          {!collapsed && (
            <button
              onClick={() => setCollapsed(true)}
              className="text-nothing-muted hover:text-nothing-white transition-colors text-xs px-1"
              aria-label="Colapsar"
            >
              ◀
            </button>
          )}
        </div>

        {/* Navigation */}
        <nav className="flex-1 py-2 overflow-y-auto">
          {visibleNav.map(({ to, label, tag }) => (
            <NavLink
              key={to}
              to={to}
              end={to === '/'}
              title={collapsed ? label : undefined}
              className={({ isActive }) =>
                `flex items-center gap-3 px-3 py-2.5 text-xs transition-all duration-100 relative group ${
                  isActive
                    ? 'text-nothing-white bg-nothing-card'
                    : 'text-nothing-dim hover:text-nothing-light hover:bg-nothing-card/50'
                }`
              }
            >
              {({ isActive }) => (
                <>
                  {/* Active indicator */}
                  {isActive && (
                    <span className="absolute left-0 top-0 bottom-0 w-0.5 bg-nothing-red" />
                  )}
                  <span className={`font-bold shrink-0 ${isActive ? 'text-nothing-red' : 'text-nothing-muted group-hover:text-nothing-dim'}`}>
                    {tag}
                  </span>
                  {!collapsed && (
                    <span className={`uppercase tracking-widest font-bold text-[11px] ${isActive ? 'text-nothing-white' : ''}`}>
                      {label}
                    </span>
                  )}
                </>
              )}
            </NavLink>
          ))}
        </nav>

        {/* Superadmin tenant selector */}
        {isSuperadmin && !collapsed && (
          <div className="border-t border-nothing-border px-3 py-2">
            <p className="text-[9px] uppercase tracking-widest text-nothing-muted mb-1.5 font-bold">EMPRESA</p>
            <select
              value={selectedTenant || ''}
              onChange={(e) => setSelectedTenant(e.target.value || null)}
              className="w-full bg-nothing-bg border border-nothing-border text-nothing-light text-[10px] uppercase tracking-widest font-bold px-2 py-1.5 focus:outline-none focus:border-nothing-red transition-colors"
            >
              <option value="">TODAS</option>
              {tenants.map((t) => (
                <option key={t.id} value={t.id}>{t.nombre}</option>
              ))}
            </select>
          </div>
        )}

        {/* User footer */}
        <div className="border-t border-nothing-border p-3 space-y-2">
          {!collapsed ? (
            <div className="flex items-center gap-2.5">
              <div className="w-7 h-7 border border-nothing-border flex items-center justify-center text-[10px] font-bold text-nothing-dim shrink-0">
                {initials}
              </div>
              <div className="min-w-0 flex-1">
                <p className="text-xs text-nothing-white truncate leading-none mb-1 font-bold">
                  {displayName}
                </p>
                <span className={`text-[10px] uppercase tracking-widest font-bold ${roleClass}`}>
                  {profile?.rol}
                </span>
              </div>
            </div>
          ) : (
            <div className="flex justify-center">
              <div
                className="w-7 h-7 border border-nothing-border flex items-center justify-center text-[10px] font-bold text-nothing-dim"
                title={displayName}
              >
                {initials}
              </div>
            </div>
          )}

          <button
            onClick={signOut}
            className={`flex items-center gap-2 w-full text-[10px] uppercase tracking-widest font-bold text-nothing-muted hover:text-nothing-red transition-colors py-1 ${collapsed ? 'justify-center' : ''}`}
            title={collapsed ? 'Salir' : undefined}
          >
            <span>×</span>
            {!collapsed && <span>SALIR</span>}
          </button>
        </div>

        {/* Expand when collapsed */}
        {collapsed && (
          <button
            onClick={() => setCollapsed(false)}
            className="border-t border-nothing-border py-2 text-nothing-muted hover:text-nothing-white transition-colors text-xs text-center"
            aria-label="Expandir"
          >
            ▶
          </button>
        )}
      </aside>

      {/* Main */}
      <main className="flex-1 overflow-y-auto">
        <Outlet />
      </main>
    </div>
  );
}
