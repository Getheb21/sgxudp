# Railway Gateway Proxy

Proxy server yang mendukung WebSocket dengan protokol Trojan, VMess, dan Shadowsocks, serta UDP native untuk Railway.

## Fitur

- ✅ WebSocket proxy (Trojan, VMess, Shadowsocks)
- ✅ TCP tunneling
- ✅ UDP native (tanpa relay eksternal)
- ✅ Reverse proxy HTTP
- ✅ Health check endpoint
- ✅ API untuk daftar proxy
- ✅ Pemilihan proxy berdasarkan negara/wilayah
- ✅ Graceful shutdown
- ✅ CORS support

## Instalasi

### Local Development
```bash
git clone <repository-url>
cd railway-gateway
cp .env.example .env
npm install
npm run dev