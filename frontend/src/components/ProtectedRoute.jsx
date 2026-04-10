import { Navigate, Outlet } from 'react-router-dom';
import { useAuth } from '../hooks/useAuth.jsx';

/**
 * Protects a route tree.
 * - Without `children`: renders <Outlet /> (use as layout route element).
 * - With `children`: renders children (use for wrapping a single page).
 * - `roles`: optional array of allowed roles. Redirects to / if denied.
 */
export default function ProtectedRoute({ children, roles }) {
  const { session, profile, profileError, profileLoading, loading, signOut } = useAuth();

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-950">
        <span className="text-gray-400 text-sm">Cargando...</span>
      </div>
    );
  }

  if (!session) return <Navigate to="/login" replace />;

  // Profile fetch in progress
  if (profileLoading || (!profile && !profileError)) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-950">
        <span className="text-gray-400 text-sm">Cargando perfil...</span>
      </div>
    );
  }

  // Profile fetch failed
  if (profileError) {
    return (
      <div className="min-h-screen flex flex-col items-center justify-center bg-gray-950 gap-4">
        <span className="text-red-400 text-sm">{profileError}</span>
        <button
          onClick={signOut}
          className="px-4 py-2 bg-gray-800 text-gray-300 rounded hover:bg-gray-700 text-sm"
        >
          Cerrar sesión
        </button>
      </div>
    );
  }

  if (roles && !roles.includes(profile.rol)) {
    return (
      <div className="flex items-center justify-center h-full p-12">
        <span className="text-red-400">No tenés permisos para ver esta sección.</span>
      </div>
    );
  }

  return children ?? <Outlet />;
}

