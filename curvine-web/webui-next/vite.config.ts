import { fileURLToPath, URL } from 'node:url'
import { loadEnv } from 'vite'
import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  const apiBase = env.VITE_API_BASE || '/api'
  const apiTarget = env.VITE_API_BASE_URL || 'http://127.0.0.1:9000'

  return {
    plugins: [react(), tailwindcss()],
    resolve: {
      alias: {
        '@': fileURLToPath(new URL('./src', import.meta.url))
      }
    },
    server: {
      host: '0.0.0.0',
      port: 8080,
      proxy: {
        [apiBase]: {
          target: apiTarget,
          changeOrigin: true
        }
      }
    },
    build: {
      outDir: 'dist',
      emptyOutDir: true,
      rollupOptions: {
        output: {
          /**
           * Keep the rarely-changing libraries in their own files so a console
           * tweak does not invalidate ~150 kB of React in every visitor's cache,
           * and so the shell can fetch them in parallel with the entry chunk.
           * ECharts gets its own chunk whether or not it is named here — this
           * keeps it off the shared chunk when the observability page loads.
           */
          manualChunks(id) {
            if (!id.includes('node_modules')) return undefined
            if (id.includes('echarts') || id.includes('zrender')) return 'vendor-echarts'
            if (id.includes('lucide-react')) return 'vendor-icons'
            if (id.includes('radix-ui')) return 'vendor-ui'
            if (/node_modules\/(react|react-dom|react-router|react-router-dom|scheduler)\//.test(id)) {
              return 'vendor-react'
            }
            return undefined
          }
        }
      }
    },
    test: {
      environment: 'jsdom',
      globals: true,
      setupFiles: ['./src/test/setup.ts'],
      include: ['src/**/*.test.{ts,tsx}']
    }
  }
})
