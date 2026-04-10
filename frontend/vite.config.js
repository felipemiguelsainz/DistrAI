import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  // Keep Vite cache inside the frontend directory, never in workspace root
  cacheDir: 'node_modules/.vite',
  server: {
    port: 5173,
    strictPort: true,
  },
});
