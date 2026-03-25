import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'path';

export default defineConfig({
  root: path.resolve(__dirname, 'frontend/live-dashboard'),
  base: '/static/react/',
  plugins: [react()],
  build: {
    outDir: path.resolve(__dirname, 'fx_sr/web_live/react'),
    emptyOutDir: true,
  },
});
