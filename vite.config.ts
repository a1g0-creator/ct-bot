import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// В проде отдаём статикой за Nginx; для локалки — API_BASE_URL берём из public/web.config.json
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    host: true
  },
  preview: {
    port: 5173,
    host: true
  },
  build: {
    sourcemap: false,
    outDir: 'dist',
    emptyOutDir: true
  }
})
