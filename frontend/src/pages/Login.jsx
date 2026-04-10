import { useState } from 'react';
import { Navigate } from 'react-router-dom';
import { useAuth } from '../hooks/useAuth.jsx';

export default function Login() {
  const { signIn, session, loading } = useAuth();
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [error, setError] = useState(null);
  const [submitting, setSubmitting] = useState(false);

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-nothing-bg">
        <span className="font-mono text-nothing-dim text-sm uppercase tracking-widest dot-cursor">
          INICIALIZANDO
        </span>
      </div>
    );
  }

  if (session) return <Navigate to="/" replace />;

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError(null);
    setSubmitting(true);
    const { error: err } = await signIn(email, password);
    if (err) setError(err.message);
    setSubmitting(false);
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-nothing-bg px-4">
      <div className="w-full max-w-sm animate-fade-in">

        {/* Header mark */}
        <div className="mb-8">
          <div className="flex items-center gap-3 mb-1">
            <div className="w-2 h-2 bg-nothing-red" />
            <span className="font-mono font-bold text-xs uppercase tracking-[0.3em] text-nothing-dim">
              DISTRAI / AUTH
            </span>
          </div>
          <h1 className="font-mono font-bold text-3xl text-nothing-white leading-none">
            INICIAR<br />SESIÓN
          </h1>
          <div className="mt-3 h-px bg-nothing-border w-full" />
        </div>

        {/* Error */}
        {error && (
          <div className="mb-5 border border-nothing-red/60 bg-nothing-red/5 px-4 py-3">
            <div className="flex items-start gap-2">
              <span className="text-nothing-red text-xs font-bold mt-0.5">!</span>
              <span className="font-mono text-xs text-nothing-red leading-relaxed">{error}</span>
            </div>
          </div>
        )}

        <form onSubmit={handleSubmit} className="space-y-5">
          {/* Email */}
          <div>
            <label className="n-label block mb-1.5">EMAIL</label>
            <input
              type="email"
              required
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              placeholder="usuario@empresa.com"
              className="n-input"
            />
          </div>

          {/* Password */}
          <div>
            <label className="n-label block mb-1.5">CONTRASEÑA</label>
            <div className="relative">
              <input
                type={showPassword ? 'text' : 'password'}
                required
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="••••••••••••"
                className="n-input pr-10"
              />
              <button
                type="button"
                onClick={() => setShowPassword(!showPassword)}
                className="absolute right-3 top-1/2 -translate-y-1/2 text-nothing-muted hover:text-nothing-white transition-colors"
                tabIndex={-1}
              >
                <span className="font-mono text-[10px] uppercase tracking-widest">
                  {showPassword ? 'OCL' : 'VER'}
                </span>
              </button>
            </div>
          </div>

          {/* Submit */}
          <div className="pt-2">
            <button
              type="submit"
              disabled={submitting}
              className="n-btn-red w-full flex items-center justify-center gap-2"
            >
              {submitting ? (
                <>
                  <span className="w-3 h-3 border border-black/30 border-t-black animate-spin" />
                  AUTENTICANDO
                </>
              ) : (
                'INGRESAR →'
              )}
            </button>
          </div>
        </form>

        {/* Footer */}
        <div className="mt-8 flex items-center justify-between">
          <div className="h-px flex-1 bg-nothing-border" />
          <span className="px-3 font-mono text-[10px] text-nothing-muted uppercase tracking-widest">
            v0.1.0
          </span>
          <div className="h-px flex-1 bg-nothing-border" />
        </div>
      </div>
    </div>
  );
}
