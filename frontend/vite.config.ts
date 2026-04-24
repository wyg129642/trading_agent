import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

// Build-mode flags
//   `vite build`          → mode "production", output to dist/
//   `vite build --mode staging` → output to dist-staging/
// Dev-server target port is pulled from VITE_DEV_PROXY_TARGET (defaults to
// :8000) so you can `VITE_DEV_PROXY_TARGET=http://localhost:20301 npm run dev`
// to point the dev server at a running staging backend instead of prod.
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  const isStaging = mode === 'staging'
  const devProxyTarget = env.VITE_DEV_PROXY_TARGET || 'http://localhost:8000'
  const devProxyWs = devProxyTarget.replace(/^http/, 'ws')

  return {
    plugins: [react()],
    resolve: {
      alias: {
        '@': path.resolve(__dirname, './src'),
      },
    },
    // Per-mode output directory keeps prod and staging bundles side by
    // side under frontend/ so promotion is a file move, not a rebuild.
    build: {
      outDir: isStaging ? 'dist-staging' : 'dist',
      emptyOutDir: true,
    },
    server: {
      port: 5173,
      proxy: {
        '/api': { target: devProxyTarget, changeOrigin: true, ws: true },
        '/ws': { target: devProxyWs, ws: true },
      },
    },
  }
})
