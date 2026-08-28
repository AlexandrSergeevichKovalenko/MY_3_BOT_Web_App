
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

  // Отметка сборки, видимая на экране. Заведена 28.08.2026: кнопка на экране очереди
  // не работала, и первый же вопрос — «а этот телефон вообще выполняет новый код?» —
  // ответа не имел. В этом проекте на таком уже сгорело три дня (мини-апп неделями
  // крутил старый бандл, пока чинили «поломку», которой в живом коде не было).
  // Теперь ответ виден глазами, без раскопок.
  const buildStamp = new Date().toISOString().slice(5, 16).replace('T', ' ')

  return {
    define: {
      __BUILD_STAMP__: JSON.stringify(buildStamp),
    },
    plugins: [
      react(),
      VitePWA({
        registerType: 'autoUpdate',
        injectRegister: false,
        manifest: false,
        workbox: {
          // Take over as soon as a new build is deployed instead of waiting for
          // every Telegram webview to close first. Without this the Mini-App keeps
          // serving the previously cached bundle for a long time after a deploy
          // (users see the OLD version). skipWaiting + clientsClaim activate the
          // new service worker immediately on the next open; cleanupOutdatedCaches
          // drops stale precaches so the fresh bundle wins.
          skipWaiting: true,
          clientsClaim: true,
          cleanupOutdatedCaches: true,
          // Main bundle is currently slightly above 2 MiB, so keep precache build stable.
          maximumFileSizeToCacheInBytes: 3 * 1024 * 1024,
          // /dict (+ short /d) must ALWAYS hit the server, never the cached index.html:
          // the server bakes the auth token into the <link rel="manifest"> (…?dqt=…) so
          // iOS "Add to Home Screen" captures a start_url that carries it. If the SW
          // serves its precached index.html instead, that link is the tokenless hero
          // manifest and the installed icon cold-launches unauthenticated (only the
          // no-auth quick translate works — audio / breakdown / save all 401).
          // Same reason for the MAIN app's token entry /webapp/t/<token> (and /webapp): the
          // server rewrites the manifest link to carry the app token (…?aqt=…), so the
          // installed home-screen icon cold-launches authenticated instead of logged out.
          navigateFallbackDenylist: [/^\/api\//, /^\/dict(\/|$|\?)/, /^\/d(\/|$|\?)/, /^\/webapp(\/|$|\?)/],
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
