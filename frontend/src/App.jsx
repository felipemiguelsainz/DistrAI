import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { AuthProvider } from './hooks/useAuth.jsx';
import ProtectedRoute from './components/ProtectedRoute';
import Layout from './components/Layout';
import Login from './pages/Login';
import Dashboard from './pages/Dashboard';
import Mapa from './pages/Mapa';
import GestionPDV from './pages/GestionPDV';
import CargaDatos from './pages/CargaDatos';
import Usuarios from './pages/Usuarios';

export default function App() {
  return (
    <BrowserRouter>
      <AuthProvider>
        <Routes>
          {/* Public */}
          <Route path="/login" element={<Login />} />

          {/* Protected shell: ProtectedRoute renders <Outlet />, Layout is the nested layout */}
          <Route element={<ProtectedRoute />}>
            <Route element={<Layout />}>
              <Route index element={<Dashboard />} />
              <Route path="mapa" element={<Mapa />} />

              {/* admin + analista only */}
              <Route
                path="pdv"
                element={
                  <ProtectedRoute roles={['superadmin', 'admin', 'analista']}>
                    <GestionPDV />
                  </ProtectedRoute>
                }
              />

              {/* admin only */}
              <Route
                path="carga"
                element={
                  <ProtectedRoute roles={['superadmin', 'admin']}>
                    <CargaDatos />
                  </ProtectedRoute>
                }
              />
              <Route
                path="usuarios"
                element={
                  <ProtectedRoute roles={['superadmin', 'admin']}>
                    <Usuarios />
                  </ProtectedRoute>
                }
              />
            </Route>
          </Route>

          {/* Catch-all */}
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </AuthProvider>
    </BrowserRouter>
  );
}

