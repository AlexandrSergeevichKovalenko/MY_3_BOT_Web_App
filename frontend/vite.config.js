
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { VitePWA } from 'vite-plugin-pwa'
import axios from 'axios'

// Функция для получения URL от локального API ngrok
async function getNgrokUrl() {
  try {
    const res = await axios.get('http://127.0.0.1:4040/api/tunnels')
    // Ищем туннель с https
    const httpsTunnel = res.data.tunnels.find(t => t.public_url.startsWith('https'))
    return httpsTunnel?.public_url || ''
  } catch (e) {
    console.warn('⚠️ Ngrok API недоступен (возможно, он еще запускается), использую localhost.')
    return ''
  }
}

function manualChunks(id) {
  const normalizedId = String(id || '').replace(/\\/g, '/')

  if (normalizedId.includes('/node_modules/')) {
    if (
      normalizedId.includes('/node_modules/react/')
      || normalizedId.includes('/node_modules/react-dom/')
      || normalizedId.includes('/node_modules/scheduler/')
    ) {
      return 'react-vendor'
    }
    if (
      normalizedId.includes('/node_modules/echarts/')
      || normalizedId.includes('/node_modules/zrender/')
    ) {
      return 'charts-vendor'
    }
    if (
      normalizedId.includes('/node_modules/@livekit/')
      || normalizedId.includes('/node_modules/livekit-client/')
    ) {
      return 'livekit-vendor'
    }
  }

  if (normalizedId.includes('/src/components/ReaderSection.jsx')) {
    return 'reader-feature'
  }
  if (normalizedId.includes('/src/components/BlocksTrainer.jsx')) {
    return 'blocks-feature'
  }
  if (
    normalizedId.includes('/src/components/HomeDashboardTiles.jsx')
    || normalizedId.includes('/src/components/HomeMoreTiles.jsx')
    || normalizedId.includes('/src/components/WeeklySummaryModal.jsx')
  ) {
    return 'home-feature'
  }

  return undefined
}

export default defineConfig(async () => {
  const ngrokUrl = await getNgrokUrl()
  console.log('🌍 NGROK URL FOUND:', ngrokUrl)

  // Вытаскиваем только хост (без https://) для allowedHosts
  let ngrokHost = null
  if (ngrokUrl) {
    try {
      ngrokHost = new URL(ngrokUrl).hostname
    } catch (e) {
      console.warn('Ошибка парсинга URL:', e)
    }
  }

  return {
    plugins: [
      react(),
      VitePWA({
        registerType: 'autoUpdate',
        injectRegister: false,
        manifest: false,
        workbox: {
          // Main bundle is currently slightly above 2 MiB, so keep precache build stable.
          maximumFileSizeToCacheInBytes: 3 * 1024 * 1024,
          navigateFallbackDenylist: [/^\/api\//],
          runtimeCaching: [
            {
              urlPattern: ({ url, request }) => {
                if (url.pathname.startsWith('/api/')) return false
                return ['script', 'style', 'image', 'font'].includes(request.destination)
              },
              handler: 'StaleWhileRevalidate',
              options: {
                cacheName: 'static-assets',
              },
            },
          ],
        },
      }),
    ],
    server: {
      host: true, // Слушать все интерфейсы
      port: 5173,
      strictPort: true,
      
      // Разрешаем наш динамический хост ngrok
      allowedHosts: ngrokHost ? [ngrokHost, 'localhost'] : ['localhost'],

      proxy: {
        '/api': {
          target: 'http://127.0.0.1:5001',
          changeOrigin: true
        }
      }
    },
    build: {
      chunkSizeWarningLimit: 800,
      rollupOptions: {
        output: {
          manualChunks,
        },
      },
    }
  }
})
