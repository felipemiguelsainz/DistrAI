# Distribuidora App

App web de análisis comercial para distribuidora argentina.

## Setup rápido

### 1. Supabase
1. Crear proyecto en [supabase.com](https://supabase.com).
2. Ir a SQL Editor y ejecutar `backend/db/migrations/001_init_schema.sql`.
3. Crear un usuario de prueba en Authentication > Users.
4. Insertar su perfil manualmente:
```sql
INSERT INTO perfiles (id, rol, nombre, activo)
VALUES ('<UUID-del-user>', 'admin', 'Admin Test', true);
```
5. Copiar el **JWT Secret** desde Settings > API > JWT Secret.

### 2. Backend (FastAPI)
```bash
cd backend
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt
copy .env.example .env        # Completar con tus claves
uvicorn main:app --reload --port 8000
```
Probar: `GET http://localhost:8000/api/health`  
Auth: `GET http://localhost:8000/api/auth/me` con header `Authorization: Bearer <token>`

### 3. Frontend (React + Vite)
```bash
cd frontend
npm install
copy .env.example .env        # Completar con tus claves
npm run dev
```
Abrir `http://localhost:5173` → debería redirigir a `/login`.

## Estructura de carpetas
```
distribuidora-app/
├── backend/
│   ├── main.py               # FastAPI entrypoint
│   ├── core/
│   │   ├── config.py         # Settings (env vars)
│   │   └── auth.py           # JWT validation + role guards
│   ├── routers/
│   │   ├── auth.py           # /api/auth/me
│   │   ├── pdv.py            # (Fase 3)
│   │   ├── ventas.py         # (Fase 3)
│   │   ├── mapa.py           # (Fase 3)
│   │   ├── dashboard.py      # (Fase 3)
│   │   └── usuarios.py       # (Fase 3)
│   ├── services/             # (Fase 3+)
│   ├── db/
│   │   ├── supabase.py       # Supabase client
│   │   └── migrations/       # SQL migrations
│   └── requirements.txt
├── frontend/
│   ├── src/
│   │   ├── main.jsx
│   │   ├── App.jsx           # Rutas protegidas
│   │   ├── pages/            # Mapa, Dashboard, PDV, Carga, Usuarios, Login
│   │   ├── components/
│   │   │   ├── Layout.jsx    # Sidebar + outlet
│   │   │   └── ProtectedRoute.jsx
│   │   ├── hooks/useAuth.js  # Supabase session + perfil
│   │   └── lib/supabaseClient.js
│   ├── index.html
│   └── package.json
└── README.md
```

## Fases
- **Fase 1** ✅ Scaffold + SQL migrations + dependencias
- **Fase 2** ✅ Auth + roles + login + rutas protegidas
- **Fase 3** 🔜 Carga maestro PDV + geocodificación
