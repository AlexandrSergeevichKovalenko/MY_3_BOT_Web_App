
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
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
    plugins: [react()],
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
    }
  }
})
