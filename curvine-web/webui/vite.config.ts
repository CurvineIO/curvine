import { fileURLToPath, URL } from 'node:url'
import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'

// The Rust web server serves `dist/` at `/` and falls back to index.html for
// unknown paths, so the build output must stay in `dist/` with base `/`.
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')

  return {
    plugins: [react()],
    base: '/',
    resolve: {
      alias: {
        '@': fileURLToPath(new URL('./src', import.meta.url)),
      },
    },
    build: {
      outDir: 'dist',
      emptyOutDir: true,
    },
    server: {
      port: 8080,
      proxy: {
        '/api': {
          target: env.VITE_API_TARGET || 'http://127.0.0.1:9001',
          changeOrigin: true,
        },
      },
    },
  }
})
