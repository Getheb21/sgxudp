// ============================================
// RAILWAY GATEWAY + VLESS + XUDP SERVER-SIDE
// ============================================

const WebSocket = require('ws');
const net = require('net');
const dgram = require('dgram');
const fetch = require('node-fetch');
const http = require('http');
const https = require('https');
const url = require('url');
const { createHash } = require('node:crypto');
const { EventEmitter } = require('node:events');
const dns = require('node:dns').promises;

const horse = 'trojan';
const flash = 'vmess';
const vless = 'vless';

const KV_PRX_URL = "https://raw.githubusercontent.com/backup-heavenly-demons/gateway/refs/heads/main/kvProxyList.json";
const CORS_HEADER_OPTIONS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,HEAD,POST,OPTIONS",
  "Access-Control-Max-Age": "86400",
};

const REGION_MAP = {
  ASIA: ["ID","SG","MY","PH","TH","VN","JP","KR","CN","HK","TW"],
  EUROPE: ["FR","DE","NL","BE","AT","CH","IE","LU","IT","ES","PT","GR","SE","NO","DK","FI","PL","CZ","SK","HU","RO","BG"],
  AMERICA: ["US","CA","MX","BR","AR","CL","CO","PE"],
  AFRICA: ["ZA","NG","EG","MA","KE"],
  OCEANIA: ["AU","NZ"],
  GLOBAL: []
};

// ==================== XUDP BRIDGE ====================
// Menangani frame XUDP dari client Xray/v2rayNG (packetEncoding=xudp)
// Format frame: [2B id][1B status][1B option] ... [2B len][payload]
//   status: 0x01=New, 0x02=Keep, 0x03=End, 0x04=KeepAlive
//   option: 0x01=Data
// Untuk New (network UDP): [1B network=0x02][2B port][1B atyp][addr][8B gid]
// ====================
class XrayXudpBridge {
    constructor(gateway, ws, vlessResponse, log) {
        this.gateway = gateway;
        this.ws = ws;
        this.header = vlessResponse;
        this.log = log || (() => {});
        this.buffer = Buffer.alloc(0);
        this.sessions = new Map();
        this.closed = false;
        this.sentHeader = false;
    }

    feed(data) {
        if (this.closed) return;
        if (!data || !data.length) return;
        this.buffer = Buffer.concat([this.buffer, data]);
        this._drain();
    }

    _drain() {
        while (this.buffer.length >= 4) {
            let consumed;
            try {
                consumed = this._parseFrame();
            } catch (e) {
                this.log(`XUDP frame error: ${e.message}`);
                this.close();
                return;
            }
            if (consumed <= 0) break;
            this.buffer = this.buffer.slice(consumed);
        }
    }

    _parseFrame() {
        const buf = this.buffer;
        const sessionID = buf.readUInt16BE(0);
        const status = buf[2];
        const option = buf[3];
        let cursor = 4;

        let target = null;
        let network = 0;

        if (status === 0x01) { // New
            if (buf.length < cursor + 1) return 0;
            network = buf[cursor++];
            if (buf.length < cursor + 3) return 0;
            const port = buf.readUInt16BE(cursor);
            cursor += 2;
            const atyp = buf[cursor++];
            let host;
            if (atyp === 0x01) {
                if (buf.length < cursor + 4) return 0;
                host = `${buf[cursor]}.${buf[cursor+1]}.${buf[cursor+2]}.${buf[cursor+3]}`;
                cursor += 4;
            } else if (atyp === 0x02) {
                if (buf.length < cursor + 1) return 0;
                const len = buf[cursor++];
                if (buf.length < cursor + len) return 0;
                host = buf.slice(cursor, cursor + len).toString('utf8');
                cursor += len;
            } else if (atyp === 0x03) {
                if (buf.length < cursor + 16) return 0;
                const parts = [];
                for (let i = 0; i < 8; i++) parts.push(buf.readUInt16BE(cursor + i*2).toString(16));
                host = parts.join(':');
                cursor += 16;
            } else {
                throw new Error(`invalid XUDP atyp 0x${atyp.toString(16)}`);
            }
            // Xray XUDP (VLESS + packetEncoding=xudp) pakai 8-byte globalID untuk UDP
            if (network === 0x02) {
                if (buf.length < cursor + 8) return 0;
                cursor += 8; // skip globalID
            }
            target = { host, port, atyp };
        } else if (status === 0x02) {
            // Keep - data lanjutan ke session yang sudah ada
        } else if (status === 0x03) {
            // End - tutup session
        } else if (status === 0x04) {
            return cursor; // KeepAlive, tidak ada payload
        } else {
            throw new Error(`unknown XUDP status 0x${status.toString(16)}`);
        }

        if (buf.length < cursor + 2) return 0;
        const payloadLen = buf.readUInt16BE(cursor);
        cursor += 2;
        if (buf.length < cursor + payloadLen) return 0;
        const payload = buf.slice(cursor, cursor + payloadLen);
        cursor += payloadLen;

        this._handleFrame(sessionID, status, target, payload);
        return cursor;
    }

    _handleFrame(sessionID, status, target, payload) {
        if (status === 0x01) {
            this._openSession(sessionID, target, payload);
        } else if (status === 0x02) {
            const s = this.sessions.get(sessionID);
            if (s && payload.length) s.send(payload);
        } else if (status === 0x03) {
            const s = this.sessions.get(sessionID);
            if (s) s.close();
            this.sessions.delete(sessionID);
        }
    }

    async _openSession(sessionID, target, initialPayload) {
        if (!target) return;
        const self = this;

        let resolved;
        try {
            resolved = await dns.lookup(target.host, { family: 4 });
        } catch (e) {
            try { resolved = await dns.lookup(target.host, { family: 6 }); }
            catch (e2) {
                self.log(`DNS fail ${target.host}: ${e2.message}`);
                return;
            }
        }

        const socket = dgram.createSocket(resolved.family === 6 ? 'udp6' : 'udp4');
        const address = resolved.address;

        const session = {
            socket,
            closed: false,
            send(data) {
                if (this.closed) return;
                socket.send(data, target.port, address, (e) => {
                    if (e) self.log(`send err: ${e.message}`);
                    else if (self.gateway) {
                        self.gateway.udpStats.outPackets++;
                        self.gateway.udpStats.outBytes += data.length;
                    }
                });
            },
            close() {
                if (this.closed) return;
                this.closed = true;
                try { socket.close(); } catch(_) {}
            }
        };

        socket.on('message', (msg, rinfo) => {
            if (self.closed) return;
            if (self.gateway) {
                self.gateway.udpStats.inPackets++;
                self.gateway.udpStats.inBytes += msg.length;
            }
            const frame = self._buildResponseFrame(sessionID, rinfo, msg);
            try {
                self._sendHeaderOnce();
                self.ws.send(frame);
            } catch(_) {}
        });

        socket.on('error', (e) => {
            self.log(`socket err: ${e.message}`);
            session.close();
            self.sessions.delete(sessionID);
        });

        this.sessions.set(sessionID, session);
        if (initialPayload && initialPayload.length > 0) {
            session.send(initialPayload);
        }
    }

    _buildResponseFrame(sessionID, rinfo, data) {
        const family = net.isIP(rinfo.address);
        let addrBuf;
        if (family === 4) {
            const parts = rinfo.address.split('.').map(Number);
            addrBuf = Buffer.from([0x01, ...parts]);
        } else {
            // IPv6 - tulis 16 byte
            const bytes = Buffer.alloc(16);
            const groups = rinfo.address.split(':');
            for (let i = 0; i < 8; i++) {
                const g = groups[i] || '0';
                bytes.writeUInt16BE(parseInt(g, 16) || 0, i * 2);
            }
            addrBuf = Buffer.concat([Buffer.from([0x03]), bytes]);
        }

        const portBuf = Buffer.alloc(2);
        portBuf.writeUInt16BE(rinfo.port, 0);

        const head = Buffer.alloc(4);
        head.writeUInt16BE(sessionID, 0);
        head[2] = 0x02; // Keep
        head[3] = 0x01; // Data
        const network = Buffer.from([0x02]); // UDP

        const lenBuf = Buffer.alloc(2);
        lenBuf.writeUInt16BE(data.length, 0);

        return Buffer.concat([head, network, portBuf, addrBuf, lenBuf, data]);
    }

    _sendHeaderOnce() {
        if (this.sentHeader) return;
        this.sentHeader = true;
        if (this.header && this.header.length) {
            try { this.ws.send(Buffer.from(this.header)); } catch(_) {}
        }
    }

    close() {
        if (this.closed) return;
        this.closed = true;
        for (const s of this.sessions.values()) s.close();
        this.sessions.clear();
    }
}

// ==================== GATEWAY SERVER ====================
class GatewayServer {
  constructor() {
    this.prxIP = "";
    this.wss = null;
    this.httpServer = null;
    this.activeUDPConnections = new Map();
    this.CORS_HEADER_OPTIONS = CORS_HEADER_OPTIONS;
    this.totalRX = 0;
    this.totalTX = 0;
    this._relayHandler = null;
    this.udpStats = {
      outPackets: 0, outBytes: 0, inPackets: 0, inBytes: 0,
      lastError: null, lastTarget: null, lastActivity: null,
      wsConnections: 0, activeUdpSockets: 0,
    };
  }

  handleHealthCheck(req, res) {
    const formatBytes = (b) => {
      if (b < 1024) return b + ' B';
      if (b < 1048576) return (b/1024).toFixed(1) + ' KB';
      if (b < 1073741824) return (b/1048576).toFixed(1) + ' MB';
      return (b/1073741824).toFixed(2) + ' GB';
    };
    const data = {
      status: 'healthy', timestamp: new Date().toISOString(),
      service: 'railway-gateway', uptime: process.uptime(),
      memory: process.memoryUsage(), version: '1.0.0',
      features: { websocket: true, tcp: true, udp: true, xudp: true },
      traffic: {
        rx_bytes: this.totalRX, tx_bytes: this.totalTX,
        rx_formatted: formatBytes(this.totalRX), tx_formatted: formatBytes(this.totalTX)
      }
    };
    res.writeHead(200, { 'Content-Type': 'application/json', ...this.CORS_HEADER_OPTIONS });
    res.end(JSON.stringify(data, null, 2));
  }

  handleCorsPreflight(req, res) {
    res.writeHead(200, this.CORS_HEADER_OPTIONS);
    res.end();
  }

  async handleApiRequest(req, res, parsedUrl) {
    try {
      if (parsedUrl.pathname === '/api/proxies') {
        const proxies = await this.getPrxList(process.env.PRX_BANK_URL);
        const format = parsedUrl.query.format || 'json';
        if (format === 'text') {
          res.writeHead(200, { 'Content-Type': 'text/plain', ...this.CORS_HEADER_OPTIONS });
          res.end(proxies.map(p => `${p.country} - ${p.prxIP}:${p.prxPort}`).join('\n'));
          return;
        }
        res.writeHead(200, { 'Content-Type': 'application/json', ...this.CORS_HEADER_OPTIONS });
        res.end(JSON.stringify(proxies, null, 2));
        return;
      }
      if (parsedUrl.pathname === '/api/relay-stats') {
        const customStats = this._relayHandler ? this._relayHandler.stats : null;
        const payload = {
          gateway: { ...this.udpStats, activeUdpConnections: this.activeUDPConnections.size, totalRX: this.totalRX, totalTX: this.totalTX },
          customRelay: customStats,
        };
        res.writeHead(200, { 'Content-Type': 'application/json', ...this.CORS_HEADER_OPTIONS });
        res.end(JSON.stringify(payload, null, 2));
        return;
      }
    } catch (error) {
      console.error('API error:', error);
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Internal server error' }));
    }
  }

  async handleHttpRequest(req, res) {
    const parsedUrl = url.parse(req.url, true);

    if (req.method === 'OPTIONS') { this.handleCorsPreflight(req, res); return; }
    if (parsedUrl.pathname === '/health') { this.handleHealthCheck(req, res); return; }
    if (parsedUrl.pathname.startsWith('/api/')) { await this.handleApiRequest(req, res, parsedUrl); return; }

    if (parsedUrl.pathname === '/') {
      const currentHost = req.headers.host || 'localhost:3000';
      const protocolWs = req.headers['x-forwarded-proto'] === 'https' ? 'wss' : 'ws';
      const protocolHttp = req.headers['x-forwarded-proto'] === 'https' ? 'https' : 'http';
      const uptime = Math.floor(process.uptime());
      const ramUsed = Math.round(process.memoryUsage().heapUsed / 1024 / 1024);
      const nodeVersion = process.version;
      const formatBytes = (b) => {
        if (b < 1024) return b + ' B';
        if (b < 1048576) return (b/1024).toFixed(1) + ' KB';
        if (b < 1073741824) return (b/1048576).toFixed(1) + ' MB';
        return (b/1073741824).toFixed(2) + ' GB';
      };

      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>RAILWAY GATEWAY // DASHBOARD</title>
  <script src="https://cdn.tailwindcss.com"><\/script>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
  <style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;700&display=swap');
    body { font-family: 'JetBrains Mono', monospace; background-color: #0a0b10; }
    .cyber-glow { box-shadow: 0 0 15px rgba(59, 130, 246, 0.2); }
    .cyber-glow-green { box-shadow: 0 0 15px rgba(16, 185, 129, 0.4); }
    .neon-border { border: 1px solid rgba(59, 130, 246, 0.3); }
    .neon-border:hover { border-color: rgba(59, 130, 246, 0.8); }
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: #0f111a; }
    ::-webkit-scrollbar-thumb { background: #1e293b; border-radius: 3px; }
    ::-webkit-scrollbar-thumb:hover { background: #3b82f6; }
  </style>
</head>
<body class="text-slate-300 min-h-screen flex flex-col justify-between">

  <header class="border-b border-slate-900 bg-[#0d0e16]/80 backdrop-blur-md sticky top-0 z-50 px-6 py-4">
    <div class="max-w-7xl mx-auto flex flex-col sm:flex-row items-center justify-between gap-4">
      <div class="flex items-center gap-3">
        <div class="h-10 w-10 rounded-lg bg-blue-600/10 border border-blue-500/30 flex items-center justify-center text-blue-400 cyber-glow animate-pulse">
          <i class="fa-solid fa-terminal text-lg"></i>
        </div>
        <div>
          <h1 class="text-xl font-bold tracking-wider text-white">RAILWAY_GATEWAY<span class="text-blue-500">.sys</span></h1>
          <p class="text-xs text-slate-500">CORE NODE ACTIVE & SECURED</p>
        </div>
      </div>
      <div class="flex items-center gap-2 bg-[#121420] neon-border px-4 py-2 rounded-lg">
        <span class="h-2.5 w-2.5 rounded-full bg-emerald-500 cyber-glow-green animate-ping"></span>
        <span class="text-xs font-semibold text-emerald-400 tracking-wider">SYSTEM ONLINE</span>
      </div>
    </div>
  </header>

  <main class="max-w-7xl w-full mx-auto p-6 space-y-8 flex-grow">
    <div class="grid grid-cols-1 md:grid-cols-5 gap-4">
      <div class="bg-[#0d0e16] neon-border p-5 rounded-xl flex items-center justify-between">
        <div><p class="text-xs text-slate-500 mb-1">SYSTEM UPTIME</p><p id="uptime-val" class="text-lg font-bold text-white">${uptime}s</p></div>
        <i class="fa-solid fa-clock text-slate-700 text-2xl"></i>
      </div>
      <div class="bg-[#0d0e16] neon-border p-5 rounded-xl flex items-center justify-between">
        <div><p class="text-xs text-slate-500 mb-1">RAM</p><p class="text-lg font-bold text-white">${ramUsed} MB</p></div>
        <i class="fa-solid fa-microchip text-slate-700 text-2xl"></i>
      </div>
      <div class="bg-[#0d0e16] neon-border p-5 rounded-xl flex items-center justify-between">
        <div><p class="text-xs text-slate-500 mb-1">TRAFFIC RX</p><p id="rx-val" class="text-lg font-bold text-cyan-400">${formatBytes(this.totalRX)}</p></div>
        <i class="fa-solid fa-download text-cyan-900/50 text-2xl"></i>
      </div>
      <div class="bg-[#0d0e16] neon-border p-5 rounded-xl flex items-center justify-between">
        <div><p class="text-xs text-slate-500 mb-1">TRAFFIC TX</p><p id="tx-val" class="text-lg font-bold text-pink-400">${formatBytes(this.totalTX)}</p></div>
        <i class="fa-solid fa-upload text-pink-900/50 text-2xl"></i>
      </div>
      <div class="bg-[#0d0e16] neon-border p-5 rounded-xl flex items-center justify-between">
        <div><p class="text-xs text-slate-500 mb-1">NODE</p><p class="text-lg font-bold text-blue-400">${nodeVersion}</p></div>
        <i class="fa-brands fa-node-js text-blue-900/50 text-2xl"></i>
      </div>
    </div>

    <div class="bg-[#0d0e16] border border-slate-900 rounded-xl p-6 space-y-5">
      <div class="flex items-center gap-2 border-b border-slate-900 pb-3">
        <i class="fa-solid fa-key text-yellow-400"></i>
        <h2 class="text-md font-bold text-white">VLESS / TROJAN GENERATOR</h2>
        <span id="current-domain-badge" class="text-[10px] bg-blue-500/10 text-blue-400 px-2 py-0.5 rounded border border-blue-500/20 ml-auto">🌐 detecting...</span>
      </div>
      <div class="grid grid-cols-1 md:grid-cols-2 gap-5">
        <div class="space-y-4">
          <div>
            <label class="text-xs text-slate-400 mb-1.5 block">UUID</label>
            <div class="flex gap-2">
              <input id="uuidInput" type="text" value="853b8456-0c0b-4bfa-b3b4-b2619248a9bc" class="w-full bg-[#10121d] border border-slate-800 rounded-lg px-3 py-2 text-sm text-white font-mono">
              <button id="randomUuidBtn" class="bg-blue-600/20 border border-blue-500/30 text-blue-400 hover:bg-blue-600 hover:text-white px-3 py-2 rounded-lg text-xs">RND</button>
            </div>
          </div>
          <div>
            <label class="text-xs text-slate-400 mb-1.5 block">Host</label>
            <input id="hostInput" type="text" value="${currentHost}" class="w-full bg-[#10121d] border border-slate-800 rounded-lg px-3 py-2 text-sm text-white font-mono">
          </div>
          <div>
            <label class="text-xs text-slate-400 mb-1.5 block">Port</label>
            <input id="portInput" type="text" value="443" class="w-full bg-[#10121d] border border-slate-800 rounded-lg px-3 py-2 text-sm text-white font-mono">
          </div>
          <div>
            <label class="text-xs text-slate-400 mb-1.5 block">Network Mode</label>
            <div class="flex gap-2">
              <label class="flex-1 flex items-center gap-2 bg-[#10121d] border border-slate-800 rounded-lg px-3 py-2 cursor-pointer">
                <input type="radio" name="netMode" value="tcp" checked>
                <span class="text-xs text-slate-300">TCP</span>
              </label>
              <label class="flex-1 flex items-center gap-2 bg-[#10121d] border border-slate-800 rounded-lg px-3 py-2 cursor-pointer">
                <input type="radio" name="netMode" value="xudp">
                <span class="text-xs text-slate-300">XUDP</span>
              </label>
            </div>
          </div>
          <div>
            <label class="text-xs text-slate-400 mb-1.5 block">Path</label>
            <div class="flex gap-2">
              <select id="pathSelect" class="bg-[#10121d] border border-slate-800 rounded-lg px-3 py-2 text-sm text-white font-mono">
                <option value="/ALL">/ALL</option>
                <option value="/xudp">/xudp</option>
                <option value="/ID">/ID</option>
                <option value="/SG">/SG</option>
                <option value="/JP">/JP</option>
                <option value="/US">/US</option>
                <option value="/ASIA">/ASIA</option>
                <option value="/EUROPE">/EUROPE</option>
                <option value="/AMERICA">/AMERICA</option>
              </select>
              <input id="pathInput" type="text" value="/ALL" class="w-full bg-[#10121d] border border-slate-800 rounded-lg px-3 py-2 text-sm text-white font-mono">
            </div>
          </div>
          <div>
            <label class="text-xs text-slate-400 mb-1.5 block">SNI</label>
            <input id="sniInput" type="text" value="business.whatsapp.com" class="w-full bg-[#10121d] border border-slate-800 rounded-lg px-3 py-2 text-sm text-white font-mono">
          </div>
          <div>
            <label class="text-xs text-slate-400 mb-1.5 block">Remark</label>
            <input id="remarkInput" type="text" value="KOPI KAPAL ⚡" class="w-full bg-[#10121d] border border-slate-800 rounded-lg px-3 py-2 text-sm text-white font-mono">
          </div>
          <button id="generateBtn" class="w-full bg-gradient-to-r from-yellow-500 to-orange-600 text-black font-bold py-2.5 rounded-lg text-sm">GENERATE</button>
        </div>
        <div class="space-y-3">
          <div class="bg-[#07080e] rounded-lg p-4 border border-slate-950">
            <div class="flex justify-between mb-2">
              <span class="text-[10px] bg-purple-500/10 text-purple-400 px-2 py-0.5 rounded font-bold">VLESS</span>
              <button onclick="copyText(document.getElementById('vlessOutput').textContent)" class="text-xs text-slate-400">COPY</button>
            </div>
            <p id="vlessOutput" class="text-xs text-purple-300 font-mono break-all">Loading...</p>
          </div>
          <div class="bg-[#07080e] rounded-lg p-4 border border-slate-950">
            <div class="flex justify-between mb-2">
              <span class="text-[10px] bg-orange-500/10 text-orange-400 px-2 py-0.5 rounded font-bold">TROJAN</span>
              <button onclick="copyText(document.getElementById('trojanOutput').textContent)" class="text-xs text-slate-400">COPY</button>
            </div>
            <p id="trojanOutput" class="text-xs text-orange-300 font-mono break-all">Loading...</p>
          </div>
        </div>
      </div>
    </div>

    <div class="bg-[#0d0e16] border border-slate-900 rounded-xl p-6 space-y-4">
      <div class="flex items-center gap-2 border-b border-slate-900 pb-3">
        <i class="fa-solid fa-network-wired text-blue-400"></i>
        <h2 class="text-md font-bold text-white">WEBSOCKET ENDPOINTS</h2>
      </div>
      <div class="space-y-2">
        <div class="bg-[#10121d] border border-slate-900/60 p-4 rounded-lg flex justify-between items-center">
          <div>
            <span class="text-xs bg-emerald-500/10 text-emerald-400 px-2 py-0.5 rounded font-bold">XUDP NATIVE (VLESS+XUDP)</span>
            <p class="text-sm text-slate-200 mt-2"><span class="ws-domain">${protocolWs}</span>://<span class="ws-host">${currentHost}</span>/xudp</p>
          </div>
          <button onclick="copyDynamic('xudp')" class="text-xs bg-[#171a29] border border-slate-800 text-slate-400 px-3 py-1.5 rounded">COPY</button>
        </div>
        <div class="bg-[#10121d] border border-slate-900/60 p-4 rounded-lg flex justify-between items-center">
          <div>
            <span class="text-xs bg-blue-500/10 text-blue-400 px-2 py-0.5 rounded font-bold">RAW UDP/TCP</span>
            <p class="text-sm text-slate-200 mt-2"><span class="ws-domain">${protocolWs}</span>://<span class="ws-host">${currentHost}</span>/ALL</p>
          </div>
          <button onclick="copyDynamic('ALL')" class="text-xs bg-[#171a29] border border-slate-800 text-slate-400 px-3 py-1.5 rounded">COPY</button>
        </div>
      </div>
    </div>
  </main>

  <footer class="border-t border-slate-950 bg-[#07080d] px-6 py-4 text-center text-xs text-slate-600">
    &copy; 2025 RAILWAY GATEWAY
  </footer>

  <div id="toast" class="fixed bottom-6 right-6 bg-blue-600 text-white px-4 py-2 rounded opacity-0 pointer-events-none text-xs"></div>

  <script>
    const currentDomain = window.location.hostname;
    const isSecure = window.location.protocol === 'https:';
    const wsProtocol = isSecure ? 'wss' : 'ws';
    const httpProtocol = isSecure ? 'https' : 'http';

    function updateAllDomains() {
      const hostInput = document.getElementById('hostInput');
      if (hostInput) hostInput.value = currentDomain;
      document.getElementById('current-domain-badge').innerHTML = '🌐 ' + currentDomain;
      document.querySelectorAll('.ws-domain').forEach(el => el.textContent = wsProtocol);
      document.querySelectorAll('.ws-host').forEach(el => el.textContent = currentDomain);
    }
    updateAllDomains();

    function copyDynamic(path) { copyText(wsProtocol + '://' + currentDomain + '/' + path); }
    function copyText(text) {
      navigator.clipboard.writeText(text).then(() => {
        const toast = document.getElementById('toast');
        toast.textContent = 'Copied!';
        toast.classList.remove('opacity-0','pointer-events-none');
        setTimeout(() => toast.classList.add('opacity-0','pointer-events-none'), 1500);
      });
    }

    let uptimeStart = ${uptime};
    setInterval(() => {
      uptimeStart++;
      document.getElementById('uptime-val').innerText = uptimeStart + 's';
    }, 1000);

    async function updateTraffic() {
      try {
        const res = await fetch('/health');
        const data = await res.json();
        if(data.traffic) {
          document.getElementById('rx-val').innerText = data.traffic.rx_formatted || '0 B';
          document.getElementById('tx-val').innerText = data.traffic.tx_formatted || '0 B';
        }
      } catch(e) {}
    }
    setInterval(updateTraffic, 5000);

    function generateUUID() {
      const uuid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
        const r = Math.random() * 16 | 0;
        const v = c === 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
      });
      document.getElementById('uuidInput').value = uuid;
      generateAccounts();
    }

    function generateTrojanPass() {
      const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
      let pass = '';
      for (let i = 0; i < 36; i++) {
        if (i === 8 || i === 13 || i === 18 || i === 23) pass += '-';
        else pass += chars.charAt(Math.floor(Math.random() * chars.length));
      }
      return pass;
    }

    function generateAccounts() {
      try {
        const uuid = document.getElementById('uuidInput').value.trim() || '853b8456-0c0b-4bfa-b3b4-b2619248a9bc';
        const host = document.getElementById('hostInput').value.trim() || currentDomain;
        const port = document.getElementById('portInput').value.trim() || '443';
        let path = document.getElementById('pathInput').value.trim() || '/ALL';
        const netModeEl = document.querySelector('input[name="netMode"]:checked');
        const netMode = netModeEl ? netModeEl.value : 'tcp';
        const isXudp = netMode === 'xudp';
        if (isXudp && (path === '/ALL' || path === '')) path = '/xudp';
        const sni = document.getElementById('sniInput').value.trim() || 'business.whatsapp.com';
        const remark = document.getElementById('remarkInput').value.trim() || 'KOPI KAPAL';
        const encodedPath = encodeURIComponent(path);
        const encodedRemark = encodeURIComponent(remark);

        let vlessQuery = 'encryption=none&security=tls&sni=' + sni +
                         '&fp=randomized&type=ws&host=' + host +
                         '&path=' + encodedPath;
        if (isXudp) vlessQuery += '&packetEncoding=xudp';
        const vlessUrl = 'vless://' + uuid + '@' + host + ':' + port + '?' + vlessQuery + '#' + encodedRemark;

        const trojanPass = generateTrojanPass();
        const trojanUrl = 'trojan://' + trojanPass + '@' + host + ':' + port +
                          '?security=tls&sni=' + sni +
                          '&type=ws&host=' + host +
                          '&path=' + encodedPath + '#' + encodedRemark;

        document.getElementById('vlessOutput').textContent = vlessUrl;
        document.getElementById('trojanOutput').textContent = trojanUrl;
      } catch(err) { console.error(err); }
    }

    setTimeout(generateAccounts, 300);
    setTimeout(() => {
      ['uuidInput','hostInput','portInput','pathInput','sniInput','remarkInput'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.addEventListener('input', generateAccounts);
      });
      document.querySelectorAll('input[name="netMode"]').forEach(el => el.addEventListener('change', generateAccounts));
      document.getElementById('pathSelect').addEventListener('change', function() {
        document.getElementById('pathInput').value = this.value;
        generateAccounts();
      });
      document.getElementById('generateBtn').addEventListener('click', function(e) { e.preventDefault(); generateAccounts(); });
      document.getElementById('randomUuidBtn').addEventListener('click', function(e) { e.preventDefault(); generateUUID(); });
    }, 600);
  </script>
</body>
</html>`);
      return;
    }

    const targetReversePrx = process.env.REVERSE_PRX_TARGET;
    if (targetReversePrx) await this.reverseWeb(req, res, targetReversePrx);
    else { res.writeHead(404); res.end('Not Found'); }
  }

  async getKVPrxList(kvPrxUrl = KV_PRX_URL) {
    if (!kvPrxUrl) throw new Error("No URL Provided!");
    try {
      const kvPrx = await fetch(kvPrxUrl);
      if (kvPrx.status == 200) return await kvPrx.json();
      return {};
    } catch { return {}; }
  }

  async getPrxList(prxBankUrl) {
    if (!prxBankUrl) return [];
    try {
      const response = await fetch(prxBankUrl);
      if (response.status === 200) {
        const data = await response.json();
        return data.map(proxy => {
          const ip = proxy.prxIP || proxy.ip || proxy.server;
          const port = proxy.prxPort || proxy.port;
          const country = proxy.country || proxy.cc || 'XX';
          if (!ip || !port) return null;
          return { prxIP: ip, prxPort: port, country: country.toUpperCase() };
        }).filter(Boolean);
      }
      return [];
    } catch { return []; }
  }

  async reverseWeb(request, response, target, targetPath) {
    try {
      const targetUrl = new URL(request.url);
      const targetChunk = target.split(":");
      targetUrl.hostname = targetChunk[0];
      targetUrl.port = targetChunk[1]?.toString() || "443";
      targetUrl.pathname = targetPath || targetUrl.pathname;
      const options = {
        hostname: targetUrl.hostname, port: targetUrl.port,
        path: targetUrl.pathname + targetUrl.search,
        method: request.method, headers: { ...request.headers }
      };
      options.headers['host'] = targetUrl.hostname;
      options.headers['x-forwarded-host'] = request.headers.host;
      const proxyReq = (targetUrl.protocol === 'https:' ? https : http).request(options, (proxyRes) => {
        response.writeHead(proxyRes.statusCode, {
          ...Object.fromEntries(Object.entries(this.CORS_HEADER_OPTIONS)),
          ...Object.fromEntries(Object.entries(proxyRes.headers)),
          'x-proxied-by': 'Railway Gateway'
        });
        proxyRes.pipe(response);
      });
      proxyReq.on('error', () => { response.writeHead(500); response.end('Proxy error'); });
      if (request.method !== 'GET' && request.method !== 'HEAD') {
        let body = [];
        request.on('data', c => body.push(c)).on('end', () => { proxyReq.write(Buffer.concat(body)); proxyReq.end(); });
      } else proxyReq.end();
    } catch { response.writeHead(500); response.end('Internal server error'); }
  }

  // ==================== PROXY SELECTION ====================
  async handleWebSocketConnection(ws, request) {
    try {
      const parsedUrl = url.parse(request.url, true);
      const originalPath = parsedUrl.pathname;
      const isXudpPath = originalPath === '/xudp' || originalPath === '/XUDP';
      const path = isXudpPath ? '/ALL' : originalPath;
      console.log(`WebSocket request path: ${originalPath}${isXudpPath ? ' (XUDP mode)' : ''}`);

      const proxyListMatch = path.match(/^\/PROXYLIST\/([A-Z]{2}(,[A-Z]{2})*)$/i);
      if (proxyListMatch) {
        const countryCodes = proxyListMatch[1].toUpperCase().split(",");
        const proxies = await this.getPrxList(process.env.PRX_BANK_URL);
        if (proxies.length === 0) {
          const kvPrx = await this.getKVPrxList();
          const available = countryCodes.filter(c => kvPrx[c] && kvPrx[c].length > 0);
          if (!available.length) { ws.close(1000, `No proxies`); return; }
          const key = available[Math.floor(Math.random() * available.length)];
          this.prxIP = kvPrx[key][Math.floor(Math.random() * kvPrx[key].length)];
        } else {
          const filtered = proxies.filter(p => countryCodes.includes(p.country));
          if (!filtered.length) { ws.close(1000, `No proxies`); return; }
          const sel = filtered[Math.floor(Math.random() * filtered.length)];
          this.prxIP = `${sel.prxIP}:${sel.prxPort}`;
        }
        await this._dispatch(ws, isXudpPath);
        return;
      }

      const allMatch = path.match(/^\/ALL(\d+)?$/i);
      if (allMatch) {
        const proxies = await this.getPrxList(process.env.PRX_BANK_URL);
        if (proxies.length === 0) {
          const kvPrx = await this.getKVPrxList();
          const all = Object.values(kvPrx).flat();
          if (!all.length) { ws.close(1000, 'No proxies for /ALL'); return; }
          this.prxIP = all[Math.floor(Math.random() * all.length)];
        } else {
          const sel = proxies[Math.floor(Math.random() * proxies.length)];
          this.prxIP = `${sel.prxIP}:${sel.prxPort}`;
        }
        await this._dispatch(ws, isXudpPath);
        return;
      }

      const putarMatch = path.match(/^\/PUTAR(\d+)?$/i);
      if (putarMatch) {
        const proxies = await this.getPrxList(process.env.PRX_BANK_URL);
        if (proxies.length === 0) {
          const kvPrx = await this.getKVPrxList();
          const countries = Object.keys(kvPrx).filter(c => kvPrx[c]?.length > 0);
          if (!countries.length) { ws.close(1000, 'No proxies'); return; }
          const key = countries[Math.floor(Math.random() * countries.length)];
          this.prxIP = kvPrx[key][Math.floor(Math.random() * kvPrx[key].length)];
        } else {
          const sel = proxies[Math.floor(Math.random() * proxies.length)];
          this.prxIP = `${sel.prxIP}:${sel.prxPort}`;
        }
        await this._dispatch(ws, isXudpPath);
        return;
      }

      const regionMatch = path.match(/^\/([A-Z]+)(\d+)?$/i);
      if (regionMatch && REGION_MAP[regionMatch[1].toUpperCase()]) {
        const regionKey = regionMatch[1].toUpperCase();
        const countries = regionKey === 'GLOBAL' ? [] : REGION_MAP[regionKey];
        const proxies = await this.getPrxList(process.env.PRX_BANK_URL);
        if (proxies.length === 0) {
          const kvPrx = await this.getKVPrxList();
          let available = [];
          if (regionKey === 'GLOBAL') available = Object.values(kvPrx).flat();
          else for (const c of countries) if (kvPrx[c]) available.push(...kvPrx[c]);
          if (!available.length) { ws.close(1000, `No proxies`); return; }
          this.prxIP = available[Math.floor(Math.random() * available.length)];
        } else {
          const filtered = regionKey === 'GLOBAL' ? proxies : proxies.filter(p => countries.includes(p.country));
          if (!filtered.length) { ws.close(1000, `No proxies`); return; }
          const sel = filtered[Math.floor(Math.random() * filtered.length)];
          this.prxIP = `${sel.prxIP}:${sel.prxPort}`;
        }
        await this._dispatch(ws, isXudpPath);
        return;
      }

      const countryMatch = path.match(/^\/([A-Z]{2})(\d+)?$/);
      if (countryMatch) {
        const countryCode = countryMatch[1].toUpperCase();
        const proxies = await this.getPrxList(process.env.PRX_BANK_URL);
        if (proxies.length === 0) {
          const kvPrx = await this.getKVPrxList();
          if (!kvPrx[countryCode] || !kvPrx[countryCode].length) { ws.close(1000, `No proxies`); return; }
          this.prxIP = kvPrx[countryCode][Math.floor(Math.random() * kvPrx[countryCode].length)];
        } else {
          const filtered = proxies.filter(p => p.country === countryCode);
          if (!filtered.length) { ws.close(1000, `No proxies`); return; }
          const sel = filtered[Math.floor(Math.random() * filtered.length)];
          this.prxIP = `${sel.prxIP}:${sel.prxPort}`;
        }
        await this._dispatch(ws, isXudpPath);
        return;
      }

      const ipPortMatch = path.match(/^\/(.+[:=-]\d+)$/);
      if (ipPortMatch) {
        this.prxIP = ipPortMatch[1].replace(/[=:-]/, ':');
        await this._dispatch(ws, isXudpPath);
        return;
      }

      ws.close(1000, "Invalid WebSocket path");
    } catch (err) {
      console.error('WebSocket error:', err);
      ws.close(1011, 'Internal server error');
    }
  }

  async _dispatch(ws, isXudpPath) {
    if (isXudpPath) {
      await this.websocketHandlerXudp(ws);
    } else {
      await this.websocketHandler(ws);
    }
  }

  // ==================== XUDP HANDLER (VLESS + XUDP) ====================
  async websocketHandlerXudp(ws) {
    let bridge = null;
    const log = (m) => console.log(`[XUDP] ${m}`);
    this.udpStats.wsConnections++;

    ws.on('message', async (message) => {
      try {
        const chunk = Buffer.from(message);
        this.totalRX += chunk.length;

        if (bridge) { bridge.feed(chunk); return; }

        // Baca VLESS header
        if (chunk.length < 18) throw new Error('short vless header');
        if (chunk[0] !== 0x00) throw new Error('vless version must be 0');

        const addonLen = chunk[17];
        let cursor = 18 + addonLen;
        if (chunk.length < cursor + 4) throw new Error('short vless after addons');

        const cmd = chunk[cursor++];
        const port = chunk.readUInt16BE(cursor);
        cursor += 2;
        const atyp = chunk[cursor++];

        let alen = 0, addr = '';
        if (atyp === 0x01) { alen = 4; addr = Array.from(chunk.slice(cursor, cursor+4)).join('.'); }
        else if (atyp === 0x02) { alen = chunk[cursor]; cursor++; addr = chunk.slice(cursor, cursor+alen).toString(); }
        else if (atyp === 0x03) {
          alen = 16;
          const p = [];
          for (let i = 0; i < 8; i++) p.push(chunk.readUInt16BE(cursor + i*2).toString(16));
          addr = p.join(':');
        } else throw new Error('invalid vless atyp');

        cursor += alen;

        if (cmd !== 0x02) throw new Error('XUDP path only handles UDP command');
        if (!addr || !port) throw new Error('empty target');

        log(`Client connected: ${ws._socket?.remoteAddress || '?'} → ${addr}:${port}`);

        // VLESS response: version=0x00, addon length=0x00
        const response = Buffer.from([0x00, 0x00]);

        bridge = new XrayXudpBridge(this, ws, response, log);

        const remaining = chunk.slice(cursor);
        if (remaining.length > 0) bridge.feed(remaining);
      } catch (err) {
        console.error('[XUDP] error:', err.message);
        ws.close(1011, err.message);
      }
    });

    ws.on('close', () => {
      if (bridge) bridge.close();
      this.udpStats.wsConnections = Math.max(0, this.udpStats.wsConnections - 1);
      log('WebSocket closed');
    });

    ws.on('error', (err) => {
      console.error('[XUDP] ws error:', err);
      if (bridge) bridge.close();
    });
  }

  // ==================== STANDARD WS HANDLER (VLESS/Trojan/VMess/SS + raw UDP) ====================
  async websocketHandler(ws) {
    let addressLog = "", portLog = "";
    const log = (info) => console.log(`[${addressLog}:${portLog}] ${info}`);
    let remoteSocketWrapper = { value: null };
    this.udpStats.wsConnections++;

    ws.on('message', async (message) => {
      try {
        const chunk = Buffer.from(message);
        this.totalRX += chunk.length;
        if (remoteSocketWrapper.value) { remoteSocketWrapper.value.write(chunk); return; }

        const protocol = await this.protocolSniffer(chunk);
        let protocolHeader;

        if (protocol === horse) protocolHeader = this.readHorseHeader(chunk);
        else if (protocol === flash) protocolHeader = this.readFlashHeader(chunk);
        else if (protocol === vless) protocolHeader = this.readVlessHeader(chunk);
        else if (protocol === "ss") protocolHeader = this.readSsHeader(chunk);
        else throw new Error("Unknown Protocol!");

        addressLog = protocolHeader.addressRemote;
        portLog = `${protocolHeader.portRemote} -> ${protocolHeader.isUDP ? "UDP" : "TCP"}`;
        if (protocolHeader.hasError) throw new Error(protocolHeader.message);

        if (protocolHeader.isUDP) {
          return await this.handleUDPOutbound(protocolHeader.addressRemote, protocolHeader.portRemote, chunk.slice(protocolHeader.rawDataIndex), ws, protocolHeader.version, log);
        }

        this.handleTCPOutBound(remoteSocketWrapper, protocolHeader.addressRemote, protocolHeader.portRemote, protocolHeader.rawClientData, ws, protocolHeader.version, log);
      } catch (err) {
        console.error('WS message error:', err);
        ws.close(1011, err.message);
      }
    });

    ws.on('close', () => {
      if (remoteSocketWrapper.value) remoteSocketWrapper.value.end();
      this.cleanupUDPConnections(ws);
      this.udpStats.wsConnections = Math.max(0, this.udpStats.wsConnections - 1);
      log('WebSocket closed');
    });

    ws.on('error', (err) => {
      console.error('WebSocket error:', err);
      this.cleanupUDPConnections(ws);
    });
  }

  // ==================== PROTOCOL SNIFFERS ====================
  async protocolSniffer(buffer) {
    // Trojan
    if (buffer.length >= 62) {
      const d = buffer.slice(56, 60);
      if (d[0] === 0x0d && d[1] === 0x0a && [0x01,0x03,0x7f].includes(d[2]) && [0x01,0x03,0x04].includes(d[3])) return horse;
    }
    // VLESS: version 0x00
    if (buffer.length >= 18 && buffer[0] === 0x00 && buffer[17] <= 64) return vless;
    // VMess: version 0x01
    if (buffer.length >= 17 && buffer[0] === 0x01) {
      const h = buffer.slice(1, 17).toString('hex');
      if (h.match(/^[0-9a-f]{8}[0-9a-f]{4}4[0-9a-f]{3}[89ab][0-9a-f]{3}[0-9a-f]{12}$/i)) return flash;
    }
    return "ss";
  }

  readVlessHeader(buf) {
    try {
      if (buf.length < 18) return { hasError: true, message: 'short vless' };
      if (buf[0] !== 0x00) return { hasError: true, message: 'bad vless version' };
      const addonLen = buf[17];
      let cursor = 18 + addonLen;
      if (buf.length < cursor + 4) return { hasError: true, message: 'short vless body' };
      const cmd = buf[cursor++];
      const port = buf.readUInt16BE(cursor);
      cursor += 2;
      const atyp = buf[cursor++];
      let alen = 0, addr = '';
      if (atyp === 0x01) { alen = 4; addr = Array.from(buf.slice(cursor, cursor+4)).join('.'); }
      else if (atyp === 0x02) { alen = buf[cursor]; cursor++; addr = buf.slice(cursor, cursor+alen).toString(); }
      else if (atyp === 0x03) {
        alen = 16;
        const p = [];
        for (let i = 0; i < 8; i++) p.push(buf.readUInt16BE(cursor + i*2).toString(16));
        addr = p.join(':');
      } else return { hasError: true, message: `bad vless atyp ${atyp}` };
      cursor += alen;
      const isUDP = cmd === 0x02;
      return {
        hasError: false,
        addressRemote: addr,
        portRemote: port,
        rawDataIndex: cursor,
        rawClientData: buf.slice(cursor),
        version: Buffer.from([0x00, 0x00]),
        isUDP
      };
    } catch (e) {
      return { hasError: true, message: e.message };
    }
  }

  readSsHeader(buf) {
    const at = buf[0]; let al = 0, avi = 1, av = "";
    if (at === 1) { al = 4; av = Array.from(buf.slice(avi, avi+al)).join("."); }
    else if (at === 3) { al = buf[avi]; avi += 1; av = buf.slice(avi, avi+al).toString(); }
    else if (at === 4) { al = 16; const ip = []; for (let i = 0; i < 8; i++) ip.push(buf.readUInt16BE(avi+i*2).toString(16)); av = ip.join(":"); }
    else return { hasError: true, message: `Invalid addr type: ${at}` };
    if (!av) return { hasError: true, message: "Address empty" };
    const pi = avi + al;
    const pr = buf.readUInt16BE(pi);
    return { hasError: false, addressRemote: av, portRemote: pr, rawDataIndex: pi+2, rawClientData: buf.slice(pi+2), version: null, isUDP: pr == 53 };
  }

  readFlashHeader(buf) {
    const v = buf[0]; let udp = false;
    const ol = buf[17]; const cmd = buf[18+ol];
    if (cmd === 2) udp = true; else if (cmd !== 1) return { hasError: true, message: `Cmd ${cmd} unsupported` };
    const pi = 18+ol+1; const pr = buf.readUInt16BE(pi);
    let ai = pi+2; const at = buf[ai]; let al = 0, avi = ai+1, av = "";
    if (at === 1) { al = 4; av = Array.from(buf.slice(avi, avi+al)).join("."); }
    else if (at === 2) { al = buf[avi]; avi += 1; av = buf.slice(avi, avi+al).toString(); }
    else if (at === 3) { al = 16; const ip = []; for (let i = 0; i < 8; i++) ip.push(buf.readUInt16BE(avi+i*2).toString(16)); av = ip.join(":"); }
    else return { hasError: true, message: `Invalid addr type: ${at}` };
    if (!av) return { hasError: true, message: "Address empty" };
    return { hasError: false, addressRemote: av, portRemote: pr, rawDataIndex: avi+al, rawClientData: buf.slice(avi+al), version: Buffer.from([v,0]), isUDP: udp };
  }

  readHorseHeader(buf) {
    const db = buf.slice(58);
    if (db.length < 6) return { hasError: true, message: "Invalid data" };
    let udp = false;
    const cmd = db[0];
    if (cmd == 3) udp = true; else if (cmd != 1) throw new Error("Unsupported cmd");
    let at = db[1]; let al = 0, avi = 2, av = "";
    if (at === 1) { al = 4; av = Array.from(db.slice(avi, avi+al)).join("."); }
    else if (at === 3) { al = db[avi]; avi += 1; av = db.slice(avi, avi+al).toString(); }
    else if (at === 4) { al = 16; const ip = []; for (let i = 0; i < 8; i++) ip.push(db.readUInt16BE(avi+i*2).toString(16)); av = ip.join(":"); }
    else return { hasError: true, message: `Invalid addr type: ${at}` };
    if (!av) return { hasError: true, message: "Address empty" };
    const pi = avi + al;
    const pr = db.readUInt16BE(pi);
    return { hasError: false, addressRemote: av, portRemote: pr, rawDataIndex: pi+4, rawClientData: db.slice(pi+4), version: null, isUDP: udp };
  }

  // ==================== TCP + RAW UDP ====================
  async handleTCPOutBound(remoteSocket, addressRemote, portRemote, rawClientData, webSocket, responseHeader, log) {
    const connectAndWrite = (address, port) => new Promise((resolve, reject) => {
      const s = net.createConnection({ host: address, port }, () => {
        log(`connected to ${address}:${port}`);
        if (responseHeader) s.write(Buffer.from(responseHeader));
        s.write(rawClientData);
        resolve(s);
      });
      s.on('error', reject);
    });
    const retry = async () => {
      try {
        const parts = (this.prxIP || '').split(/[:=-]/);
        const s = await connectAndWrite(parts[0] || addressRemote, parts[1] || portRemote);
        remoteSocket.value = s;
        s.on('close', () => webSocket.close());
        s.on('error', () => webSocket.close());
        this.remoteSocketToWS(s, webSocket, null, null, log);
      } catch { webSocket.close(); }
    };
    try {
      const s = await connectAndWrite(addressRemote, portRemote);
      remoteSocket.value = s;
      s.on('close', () => webSocket.close());
      s.on('error', () => webSocket.close());
      this.remoteSocketToWS(s, webSocket, null, retry, log);
    } catch { await retry(); }
  }

  async handleUDPOutbound(targetAddress, targetPort, dataChunk, webSocket, responseHeader, log) {
    try {
      this.udpStats.lastTarget = `${targetAddress}:${targetPort}`;
      this.udpStats.lastActivity = new Date().toISOString();

      let header = responseHeader;
      let resolvedAddress = targetAddress;
      let family = 4;

      if (net.isIPv4(targetAddress)) { resolvedAddress = targetAddress; family = 4; }
      else if (net.isIPv6(targetAddress)) { resolvedAddress = targetAddress; family = 6; }
      else {
        try {
          const rec = await dns.lookup(targetAddress, { family: 4 });
          resolvedAddress = rec.address; family = 4;
        } catch (e) {
          try {
            const rec6 = await dns.lookup(targetAddress, { family: 6 });
            resolvedAddress = rec6.address; family = 6;
          } catch (e2) {
            this.udpStats.lastError = `DNS FAIL ${targetAddress}`;
            return;
          }
        }
      }

      const key = `${targetAddress}:${targetPort}:${Date.now()}`;
      const sock = dgram.createSocket(family === 6 ? 'udp6' : 'udp4');
      this.activeUDPConnections.set(key, { socket: sock, webSocket });
      this.udpStats.activeUdpSockets = this.activeUDPConnections.size;

      const cleanup = () => {
        try { sock.close(); } catch(_) {}
        this.activeUDPConnections.delete(key);
        this.udpStats.activeUdpSockets = this.activeUDPConnections.size;
      };

      sock.on('error', (e) => { this.udpStats.lastError = `SOCK ${e.message}`; cleanup(); });
      sock.send(dataChunk, targetPort, resolvedAddress, (e) => {
        if (e) { this.udpStats.lastError = `SEND ${e.message}`; cleanup(); }
        else { this.udpStats.outPackets++; this.udpStats.outBytes += dataChunk.length; }
      });
      sock.on('message', (msg, rinfo) => {
        this.udpStats.inPackets++; this.udpStats.inBytes += msg.length;
        this.totalRX += msg.length;
        if (webSocket.readyState === WebSocket.OPEN) {
          if (header) {
            const buf = Buffer.from(header);
            webSocket.send(Buffer.concat([buf, msg]));
            header = null;
          } else webSocket.send(msg);
        }
      });
      sock.on('close', () => { this.activeUDPConnections.delete(key); this.udpStats.activeUdpSockets = this.activeUDPConnections.size; });

      let t = setTimeout(cleanup, 30000);
      sock.on('message', () => { clearTimeout(t); t = setTimeout(cleanup, 30000); });
    } catch (e) { this.udpStats.lastError = e.message; }
  }

  cleanupUDPConnections(webSocket) {
    for (const [key, conn] of this.activeUDPConnections) {
      if (conn.webSocket === webSocket) { try { conn.socket.close(); } catch(_) {} this.activeUDPConnections.delete(key); }
    }
  }

  remoteSocketToWS(remoteSocket, webSocket, responseHeader, retry, log) {
    let header = responseHeader, hasData = false;
    remoteSocket.on('data', (chunk) => {
      hasData = true;
      this.totalRX += chunk.length;
      if (webSocket.readyState !== WebSocket.OPEN) { remoteSocket.destroy(); return; }
      if (header) {
        const buf = Buffer.from(header);
        webSocket.send(Buffer.concat([buf, chunk]));
        header = null;
      } else webSocket.send(chunk);
    });
    remoteSocket.on('close', () => { if (!hasData && retry) retry(); });
    remoteSocket.on('error', (e) => console.error(`Socket error:`, e));
  }

  // ==================== START ====================
  start(port = process.env.PORT || 3000) {
    const server = http.createServer((req, res) => {
      this.handleHttpRequest(req, res).catch(error => {
        console.error('HTTP handler error:', error);
        res.writeHead(500); res.end('Internal Server Error');
      });
    });

    this.wss = new WebSocket.Server({ noServer: true, perMessageDeflate: false });
    this.wss.on('connection', (ws, req) => { this.handleWebSocketConnection(ws, req); });

    const relayHandler = createRelayUpgradeHandler();
    this._relayHandler = relayHandler;

    server.on('upgrade', (req, socket, head) => {
      let pathname = '/';
      try { pathname = url.parse(req.url).pathname || '/'; } catch (_) {}

      if (pathname === RELAY_WS_PATH) {
        relayHandler.handleUpgrade(req, socket, head);
        return;
      }

      this.wss.handleUpgrade(req, socket, head, (ws) => {
        this.wss.emit('connection', ws, req);
      });
    });

    const gracefulShutdown = () => {
      console.log('Shutting down...');
      if (this.wss) { this.wss.clients.forEach(c => c.close()); this.wss.close(); }
      for (const [, conn] of this.activeUDPConnections) { try { conn.socket.close(); } catch(_) {} }
      this.activeUDPConnections.clear();
      if (this._relayHandler) this._relayHandler.close();
      if (this.httpServer) this.httpServer.close(() => process.exit(0));
      setTimeout(() => process.exit(1), 10000);
    };
    process.on('SIGTERM', gracefulShutdown);
    process.on('SIGINT', gracefulShutdown);

    server.listen(port, '0.0.0.0', () => {
      console.log(`✅ Gateway running on port ${port}`);
      console.log(`🛰️  /xudp = VLESS+XUDP native`);
      console.log(`🛰️  /ALL, /ID, dll = raw UDP/TCP`);
    });

    this.httpServer = server;
    server.on('error', (error) => {
      console.error('Server error:', error);
      if (error.code === 'EADDRINUSE') process.exit(1);
    });
  }
}

// ==================== VLRLY004 RELAY ====================
const RELAY_WS_PATH = process.env.RELAY_WS_PATH || '/xudp-native';
const RELAY_CFG = Object.freeze({
    WS_PATH: RELAY_WS_PATH,
    MAX_WS_MESSAGE_BYTES: 4 * 1024 * 1024,
    HANDSHAKE_TIMEOUT_MS: 10000,
    IDLE_TIMEOUT_MS: 300000,
    XUDP_GRACE_MS: 60000,
    MAX_CONNECTIONS: 4096,
    REJECT_UDP_443: false,
});
const STATS = { startTime: Date.now(), activeClients: 0, totalHandshakes: 0, udpPacketsOut: 0, udpBytesOut: 0, udpPacketsIn: 0, udpBytesIn: 0, recentLogs: [] };

function addLog(msg) {
    const time = new Date().toLocaleTimeString('id-ID');
    STATS.recentLogs.unshift(`[${time}] ${msg}`);
    if (STATS.recentLogs.length > 60) STATS.recentLogs.pop();
    console.log(`[RELAY] ${msg}`);
}

const RELAY_MAGIC = Buffer.from('VLRLY004', 'ascii');
const RELAY_MODE_FIXED_UDP = 0x01;
const RELAY_MODE_MUX = 0x02;
const RELAY_MODE_PACKET_UDP = 0x03;
const ATYP_IPV4 = 0x01;
const ATYP_DOMAIN = 0x02;
const ATYP_IPV6 = 0x03;
const MUX_STATUS_NEW = 0x01;
const MUX_STATUS_KEEP = 0x02;
const MUX_STATUS_END = 0x03;
const MUX_STATUS_KEEPALIVE = 0x04;
const MUX_OPTION_DATA = 0x01;
const MUX_OPTION_ERROR = 0x02;
const MUX_NETWORK_UDP = 0x02;
const MAX_MUX_META_LEN = 512;
const MAX_PACKET_LEN = 65535;
const utf8Fatal = new TextDecoder('utf-8', { fatal: true });

function rejectUdpTarget(target) { return Boolean(RELAY_CFG.REJECT_UDP_443 && Number(target?.port) === 443); }

class AsyncByteReader {
    constructor(socket) {
        this.socket = socket; this.buffers = []; this.available = 0;
        this.waiters = []; this.ended = false; this.error = null;
        socket.on('data', (chunk) => {
            if (!chunk || chunk.length === 0) return;
            this.buffers.push(Buffer.from(chunk));
            this.available += chunk.length;
            this._flush();
        });
        socket.on('end', () => { this.ended = true; this._flush(); });
        socket.on('close', () => { this.ended = true; this._flush(); });
        socket.on('error', (err) => { this.error = err; this._flush(); });
    }
    readExactly(length) {
        if (!Number.isInteger(length) || length < 0) return Promise.reject(new Error('invalid read length'));
        if (length === 0) return Promise.resolve(Buffer.alloc(0));
        if (this.available >= length) return Promise.resolve(this._take(length));
        if (this.error) return Promise.reject(this.error);
        if (this.ended) return Promise.reject(new Error('unexpected EOF'));
        return new Promise((resolve, reject) => this.waiters.push({ length, resolve, reject }));
    }
    _flush() {
        while (this.waiters.length > 0) {
            const w = this.waiters[0];
            if (this.available >= w.length) { this.waiters.shift(); w.resolve(this._take(w.length)); continue; }
            if (this.error || this.ended) { this.waiters.shift(); w.reject(this.error || new Error('unexpected EOF')); continue; }
            break;
        }
    }
    _take(length) {
        const out = Buffer.allocUnsafe(length);
        let offset = 0;
        while (offset < length) {
            const first = this.buffers[0];
            const need = length - offset;
            if (first.length <= need) { first.copy(out, offset); offset += first.length; this.buffers.shift(); }
            else { first.copy(out, offset, 0, need); this.buffers[0] = first.subarray(need); offset += need; }
        }
        this.available -= length;
        return out;
    }
}

async function readLengthPayload(reader) {
    const lenBuf = await reader.readExactly(2);
    const length = lenBuf.readUInt16BE(0);
    return length === 0 ? Buffer.alloc(0) : reader.readExactly(length);
}

async function readEndpoint(reader) {
    const head = await reader.readExactly(3);
    const port = head.readUInt16BE(0);
    const atyp = head[2];
    if (port === 0) throw new Error('zero port');
    return readEndpointBody(reader, atyp, port);
}

async function readEndpointBody(reader, atyp, port) {
    if (atyp === ATYP_IPV4) { const b = await reader.readExactly(4); return { host: `${b[0]}.${b[1]}.${b[2]}.${b[3]}`, port, atyp }; }
    if (atyp === ATYP_DOMAIN) {
        const len = (await reader.readExactly(1))[0];
        if (len === 0) throw new Error('empty domain');
        const b = await reader.readExactly(len);
        let host; try { host = utf8Fatal.decode(b); } catch { throw new Error('invalid UTF-8'); }
        if (!host) throw new Error('empty domain');
        return { host, port, atyp };
    }
    if (atyp === ATYP_IPV6) { const b = await reader.readExactly(16); return { host: formatIPv6(b), port, atyp }; }
    throw new Error(`unknown address type ${atyp}`);
}

function parseEndpointBytes(buffer, offset) {
    if (offset < 0 || buffer.length - offset < 3) throw new Error('unexpected EOF');
    const port = buffer.readUInt16BE(offset);
    if (port === 0) throw new Error('zero port');
    const atyp = buffer[offset + 2];
    let cursor = offset + 3;
    if (atyp === ATYP_IPV4) { const b = buffer.subarray(cursor, cursor + 4); cursor += 4; return { endpoint: { host: `${b[0]}.${b[1]}.${b[2]}.${b[3]}`, port, atyp }, next: cursor }; }
    if (atyp === ATYP_DOMAIN) {
        const len = buffer[cursor++];
        if (len === 0 || buffer.length - cursor < len) throw new Error('bad domain');
        let host; try { host = utf8Fatal.decode(buffer.subarray(cursor, cursor + len)); } catch { throw new Error('invalid UTF-8'); }
        cursor += len;
        return { endpoint: { host, port, atyp }, next: cursor };
    }
    if (atyp === ATYP_IPV6) { const host = formatIPv6(buffer.subarray(cursor, cursor + 16)); cursor += 16; return { endpoint: { host, port, atyp }, next: cursor }; }
    throw new Error(`unknown atyp ${atyp}`);
}

function formatIPv6(bytes) { const p = []; for (let i = 0; i < 16; i += 2) p.push(bytes.readUInt16BE(i).toString(16)); return p.join(':'); }

function writeSocket(socket, data) {
    if (socket.destroyed || !socket.writable) return Promise.reject(new Error('closed'));
    return new Promise((resolve, reject) => socket.write(data, (err) => err ? reject(err) : resolve()));
}

async function writeControlError(socket, message) {
    let body = Buffer.from(String(message || 'error'), 'utf8');
    if (body.length > MAX_PACKET_LEN) body = body.subarray(0, MAX_PACKET_LEN);
    const out = Buffer.allocUnsafe(3 + body.length);
    out[0] = 1; out.writeUInt16BE(body.length, 1); body.copy(out, 3);
    try { await writeSocket(socket, out); } catch { }
}

async function readControl(reader) {
    const magic = await reader.readExactly(RELAY_MAGIC.length);
    if (!magic.equals(RELAY_MAGIC)) throw new Error('bad magic');
    const mode = (await reader.readExactly(1))[0];
    if (![RELAY_MODE_FIXED_UDP, RELAY_MODE_MUX, RELAY_MODE_PACKET_UDP].includes(mode)) throw new Error('bad mode');
    const target = mode === RELAY_MODE_FIXED_UDP ? await readEndpoint(reader) : null;
    return { mode, target };
}

async function resolveTarget(target) {
    if (target.atyp === ATYP_IPV4) return { address: target.host, family: 4 };
    if (target.atyp === ATYP_IPV6) return { address: target.host, family: 6 };
    const records = await dns.lookup(target.host, { all: true, verbatim: true });
    if (!records.length) throw new Error(`DNS no address`);
    return records.find((r) => r.family === 4) || records.find((r) => r.family === 6);
}

function bindDgram(socket, port, address) {
    return new Promise((resolve, reject) => {
        const onError = (err) => { cleanup(); reject(err); };
        const onListening = () => { cleanup(); resolve(); };
        const cleanup = () => { socket.off('error', onError); socket.off('listening', onListening); };
        socket.once('error', onError);
        socket.once('listening', onListening);
        socket.bind(port, address);
    });
}

class UDPAssociation {
    constructor() { this.udp4 = null; this.udp6 = null; this.port = 0; this.sink = null; this.closed = false; }
    static async create() {
        const assoc = new UDPAssociation();
        assoc.udp4 = dgram.createSocket({ type: 'udp4', reuseAddr: true });
        await bindDgram(assoc.udp4, 0, '0.0.0.0');
        assoc.port = assoc.udp4.address().port;
        assoc.udp4.on('message', (msg, rinfo) => assoc._onMessage(msg, rinfo));
        assoc.udp4.on('error', () => { });
        assoc.udp6 = dgram.createSocket({ type: 'udp6', reuseAddr: true, ipv6Only: true });
        try {
            await bindDgram(assoc.udp6, assoc.port, '::');
            assoc.udp6.on('message', (msg, rinfo) => assoc._onMessage(msg, rinfo));
            assoc.udp6.on('error', () => { });
        } catch { try { assoc.udp6.close(); } catch { } assoc.udp6 = null; }
        return assoc;
    }
    attach(sink) { const old = this.sink; this.sink = sink; return old; }
    detach(mux, id) { if (this.sink && this.sink.mux === mux && this.sink.id === id) { this.sink = null; return true; } return false; }
    async send(target, payload) {
        if (this.closed) throw new Error('closed');
        if (payload.length > MAX_PACKET_LEN) throw new Error('too large');
        const resolved = await resolveTarget(target);
        const socket = resolved.family === 6 ? this.udp6 : this.udp4;
        if (!socket) throw new Error('unavailable');
        await new Promise((resolve, reject) => socket.send(payload, target.port, resolved.address, (err) => err ? reject(err) : resolve()));
        STATS.udpPacketsOut++; STATS.udpBytesOut += payload.length;
    }
    _onMessage(msg, rinfo) {
        STATS.udpPacketsIn++; STATS.udpBytesIn += msg.length;
        const sink = this.sink;
        if (!sink || this.closed) return;
        Promise.resolve(sink.mux.sendUDPData(sink.id, rinfo, Buffer.from(msg))).catch(() => { });
    }
    close() {
        if (this.closed) return;
        this.closed = true; this.sink = null;
        if (this.udp4) { try { this.udp4.close(); } catch { } }
        if (this.udp6) { try { this.udp6.close(); } catch { } }
        this.udp4 = null; this.udp6 = null;
    }
}

class XUDPManager {
    constructor(graceMs) { this.graceMs = graceMs; this.entries = new Map(); }
    async attach(globalID, mux, sessionID) {
        const key = Buffer.from(globalID).toString('hex');
        let entry = this.entries.get(key);
        if (!entry) { entry = { assoc: await UDPAssociation.create(), timer: null }; this.entries.set(key, entry); }
        if (entry.timer) { clearTimeout(entry.timer); entry.timer = null; }
        const oldSink = entry.assoc.attach({ mux, id: sessionID });
        return { assoc: entry.assoc, oldSink };
    }
    detach(globalID, mux, sessionID) {
        const key = Buffer.from(globalID).toString('hex');
        const entry = this.entries.get(key);
        if (!entry || !entry.assoc.detach(mux, sessionID)) return;
        if (entry.timer) clearTimeout(entry.timer);
        entry.timer = setTimeout(() => {
            const current = this.entries.get(key);
            if (current !== entry) return;
            this.entries.delete(key);
            entry.assoc.close();
        }, this.graceMs);
        entry.timer.unref?.();
    }
    close() {
        for (const e of this.entries.values()) { if (e.timer) clearTimeout(e.timer); e.assoc.close(); }
        this.entries.clear();
    }
}

async function serveDirectUDP(socket, reader, target) {
    if (rejectUdpTarget(target)) { await writeControlError(socket, 'UDP/443 rejected'); return; }
    const assoc = await UDPAssociation.create();
    let closed = false;
    assoc.attach({
        mux: {
            sendUDPData: async (_id, _rinfo, data) => {
                if (closed || socket.destroyed) return;
                if (data.length > MAX_PACKET_LEN) return;
                const frame = Buffer.allocUnsafe(2 + data.length);
                frame.writeUInt16BE(data.length, 0);
                data.copy(frame, 2);
                await writeSocket(socket, frame);
            },
        }, id: 0,
    });
    try {
        await writeSocket(socket, Buffer.from([0]));
        for (;;) {
            const payload = await readLengthPayload(reader);
            if (payload.length === 0) continue;
            if (rejectUdpTarget(target)) continue;
            await assoc.send(target, payload);
        }
    } finally { closed = true; assoc.close(); }
}

async function servePacketUDP(socket, reader) {
    const assoc = await UDPAssociation.create();
    let closed = false;
    const writeChain = { value: Promise.resolve() };
    assoc.attach({
        mux: {
            sendUDPData: (_id, rinfo, data) => {
                if (closed || socket.destroyed || data.length > MAX_PACKET_LEN) return Promise.resolve();
                const endpoint = encodeUDPSource(rinfo);
                const len = Buffer.allocUnsafe(2);
                len.writeUInt16BE(data.length, 0);
                const frame = Buffer.concat([endpoint, len, data]);
                const op = writeChain.value.then(() => writeSocket(socket, frame));
                writeChain.value = op.catch(() => { });
                return op;
            },
        }, id: 0,
    });
    try {
        await writeSocket(socket, Buffer.from([0]));
        for (;;) {
            const target = await readEndpoint(reader);
            const payload = await readLengthPayload(reader);
            if (payload.length === 0) continue;
            if (rejectUdpTarget(target)) continue;
            await assoc.send(target, payload);
        }
    } finally { closed = true; assoc.close(); }
}

function encodeUDPSource(rinfo) {
    const port = Number(rinfo.port);
    if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error('bad port');
    const family = net.isIP(rinfo.address);
    const head = Buffer.alloc(3);
    head.writeUInt16BE(port, 0);
    if (family === 4) { head[2] = ATYP_IPV4; return Buffer.concat([head, Buffer.from(rinfo.address.split('.').map(Number))]); }
    if (family === 6) { head[2] = ATYP_IPV6; return Buffer.concat([head, ipv6ToBytes(rinfo.address)]); }
    throw new Error('bad ip');
}

function ipv6ToBytes(address) {
    let input = address;
    const zone = input.indexOf('%');
    if (zone >= 0) input = input.slice(0, zone);
    const lastColon = input.lastIndexOf(':');
    if (input.includes('.') && lastColon >= 0) {
        const ipv4 = input.slice(lastColon + 1).split('.').map(Number);
        const tail = [((ipv4[0] << 8) | ipv4[1]).toString(16), ((ipv4[2] << 8) | ipv4[3]).toString(16)];
        input = input.slice(0, lastColon) + ':' + tail.join(':');
    }
    const halves = input.split('::');
    const left = halves[0] ? halves[0].split(':').filter(Boolean) : [];
    const right = halves.length === 2 && halves[1] ? halves[1].split(':').filter(Boolean) : [];
    const missing = 8 - left.length - right.length;
    const words = [...left, ...Array(Math.max(0, missing)).fill('0'), ...right];
    const out = Buffer.alloc(16);
    words.forEach((word, i) => out.writeUInt16BE(parseInt(word, 16), i * 2));
    return out;
}

async function readMuxFrame(reader) {
    const metaLen = (await reader.readExactly(2)).readUInt16BE(0);
    if (metaLen < 4 || metaLen > MAX_MUX_META_LEN) throw new Error(`invalid mux metadata length ${metaLen}`);
    const meta = await reader.readExactly(metaLen);
    const frame = { id: meta.readUInt16BE(0), status: meta[2], option: meta[3], network: 0, target: null, globalID: null, data: Buffer.alloc(0) };
    let cursor = 4;
    if (frame.status === MUX_STATUS_NEW) {
        if (cursor >= meta.length) throw new Error('missing network');
        frame.network = meta[cursor++];
        const parsed = parseEndpointBytes(meta, cursor);
        frame.target = parsed.endpoint;
        cursor = parsed.next;
        if (frame.network === MUX_NETWORK_UDP && meta.length - cursor >= 8) {
            const gid = meta.subarray(cursor, cursor + 8);
            if (!gid.equals(Buffer.alloc(8))) frame.globalID = Buffer.from(gid);
            cursor += 8;
        }
        if (cursor !== meta.length) throw new Error(`unexpected ${meta.length - cursor} bytes`);
    } else if (frame.status === MUX_STATUS_KEEP && meta.length > cursor && meta[cursor] === MUX_NETWORK_UDP) {
        frame.network = meta[cursor++];
        frame.target = parseEndpointBytes(meta, cursor).endpoint;
    }
    if ((frame.option & MUX_OPTION_DATA) !== 0) frame.data = await readLengthPayload(reader);
    return frame;
}

class MuxSession {
    constructor(mux, id, network, target) { this.mux = mux; this.id = id; this.network = network; this.target = target; this.udp = null; this.global = false; this.gid = null; this.closed = false; }
    async sendUDP(target, payload) { if (!this.udp) throw new Error('unavailable'); await this.udp.send(target, payload); }
    closeWithoutRemoving() {
        if (this.closed) return;
        this.closed = true;
        if (this.udp) {
            if (this.global) this.mux.xm.detach(this.gid, this.mux, this.id);
            else { this.udp.detach(this.mux, this.id); this.udp.close(); }
            this.udp = null;
        }
    }
    async close(sendEnd) {
        if (this.mux.sessions.get(this.id) === this) this.mux.sessions.delete(this.id);
        this.closeWithoutRemoving();
        if (sendEnd) await this.mux.sendEnd(this.id, true).catch(() => { });
    }
}

class MuxConnection {
    constructor(socket, reader, cfg, xm) { this.socket = socket; this.reader = reader; this.cfg = cfg; this.xm = xm; this.sessions = new Map(); this.closed = false; this.writeChain = Promise.resolve(); }
    async serve() {
        try {
            await writeSocket(this.socket, Buffer.from([0]));
            for (;;) { const frame = await readMuxFrame(this.reader); await this.handleFrame(frame); }
        } finally { this.closeAll(); }
    }
    async handleFrame(frame) {
        if (frame.status === MUX_STATUS_KEEPALIVE) return;
        if (frame.status === MUX_STATUS_NEW) return this.handleNew(frame);
        if (frame.status === MUX_STATUS_KEEP) return this.handleKeep(frame);
        if (frame.status === MUX_STATUS_END) {
            const s = this.sessions.get(frame.id);
            if (s && frame.data.length) await s.sendUDP(s.target, frame.data).catch(() => { });
            this.removeSession(frame.id);
            return;
        }
        throw new Error(`unknown mux status 0x${frame.status.toString(16)}`);
    }
    async handleNew(frame) {
        if (frame.network !== MUX_NETWORK_UDP || !frame.target?.host || !frame.target?.port || rejectUdpTarget(frame.target)) {
            await this.sendEnd(frame.id, true).catch(() => { }); return;
        }
        this.removeSession(frame.id);
        const session = new MuxSession(this, frame.id, frame.network, frame.target);
        if (frame.globalID) {
            try {
                const { assoc, oldSink } = await this.xm.attach(frame.globalID, this, frame.id);
                session.udp = assoc; session.global = true; session.gid = Buffer.from(frame.globalID);
                this.sessions.set(session.id, session);
                if (oldSink && (oldSink.mux !== this || oldSink.id !== frame.id)) {
                    oldSink.mux.removeSession(oldSink.id);
                    await oldSink.mux.sendEnd(oldSink.id, false).catch(() => { });
                }
            } catch { await this.sendEnd(frame.id, true).catch(() => { }); return; }
        } else {
            try {
                const assoc = await UDPAssociation.create();
                assoc.attach({ mux: this, id: frame.id });
                session.udp = assoc;
                this.sessions.set(session.id, session);
            } catch { await this.sendEnd(frame.id, true).catch(() => { }); return; }
        }
        if (frame.data.length) await session.sendUDP(frame.target, frame.data).catch(() => session.close(true));
    }
    async handleKeep(frame) {
        const session = this.sessions.get(frame.id);
        if (!session) { await this.sendEnd(frame.id, false).catch(() => { }); return; }
        if (!frame.data.length) return;
        let target = session.target;
        if (frame.network === MUX_NETWORK_UDP && frame.target?.host && frame.target?.port) { target = frame.target; session.target = target; }
        if (rejectUdpTarget(target)) { await session.close(true); return; }
        await session.sendUDP(target, frame.data).catch(() => session.close(true));
    }
    removeSession(id) { const s = this.sessions.get(id); if (!s) return; this.sessions.delete(id); s.closeWithoutRemoving(); }
    closeAll() { if (this.closed) return; this.closed = true; for (const s of this.sessions.values()) s.closeWithoutRemoving(); this.sessions.clear(); }
    _queueWrite(data) { const op = this.writeChain.then(() => writeSocket(this.socket, data)); this.writeChain = op.catch(() => { }); return op; }
    sendUDPData(id, source, data) {
        const addr = encodeUDPSource(source);
        const meta = Buffer.allocUnsafe(5 + addr.length);
        meta.writeUInt16BE(id, 0); meta[2] = MUX_STATUS_KEEP; meta[3] = MUX_OPTION_DATA; meta[4] = MUX_NETWORK_UDP;
        addr.copy(meta, 5);
        return this.writeMuxPacket(meta, data);
    }
    sendEnd(id, hasError) {
        const meta = Buffer.alloc(4);
        meta.writeUInt16BE(id, 0); meta[2] = MUX_STATUS_END; meta[3] = hasError ? MUX_OPTION_ERROR : 0;
        return this.writeMuxMeta(meta);
    }
    writeMuxPacket(meta, data) {
        if (data.length > MAX_PACKET_LEN) return Promise.reject(new Error('too large'));
        const out = Buffer.allocUnsafe(2 + meta.length + 2 + data.length);
        out.writeUInt16BE(meta.length, 0); meta.copy(out, 2);
        const off = 2 + meta.length;
        out.writeUInt16BE(data.length, off); data.copy(out, off + 2);
        return this._queueWrite(out);
    }
    writeMuxMeta(meta) { const out = Buffer.allocUnsafe(2 + meta.length); out.writeUInt16BE(meta.length, 0); meta.copy(out, 2); return this._queueWrite(out); }
}

const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
function websocketAccept(key) { return createHash('sha1').update(String(key) + WS_GUID, 'ascii').digest('base64'); }
function websocketFrame(opcode, payload = Buffer.alloc(0)) {
    const body = Buffer.isBuffer(payload) ? payload : Buffer.from(payload);
    let header;
    if (body.length < 126) { header = Buffer.allocUnsafe(2); header[1] = body.length; }
    else if (body.length <= 0xffff) { header = Buffer.allocUnsafe(4); header[1] = 126; header.writeUInt16BE(body.length, 2); }
    else { header = Buffer.allocUnsafe(10); header[1] = 127; header.writeBigUInt64BE(BigInt(body.length), 2); }
    header[0] = 0x80 | (opcode & 0x0f);
    return body.length ? Buffer.concat([header, body]) : header;
}
function websocketClosePayload(code, reason = '') {
    let text = Buffer.from(String(reason), 'utf8');
    if (text.length > 123) text = text.subarray(0, 123);
    const out = Buffer.allocUnsafe(2 + text.length);
    out.writeUInt16BE(code, 0); text.copy(out, 2);
    return out;
}

class WebSocketRelaySocket extends EventEmitter {
    constructor(raw, maxMessageBytes) {
        super();
        this.raw = raw; this.remoteAddress = raw.remoteAddress; this.remotePort = raw.remotePort;
        this.destroyed = false; this.writable = true; this.buffer = Buffer.alloc(0);
        this.fragmentOpcode = 0; this.fragmentParts = []; this.fragmentBytes = 0;
        this.maxMessageBytes = maxMessageBytes; this.timeoutMs = 0; this.timeoutCallback = null;
        this.timeoutTimer = null; this.sentClose = false; this.gotClose = false; this.ended = false;
        raw.on('data', (chunk) => {
            if (this.destroyed || !chunk?.length) return;
            this._touch();
            this.buffer = this.buffer.length ? Buffer.concat([this.buffer, chunk]) : Buffer.from(chunk);
            this._parse();
        });
        raw.on('end', () => this._emitEnd());
        raw.on('close', () => {
            if (this.destroyed) return;
            this.destroyed = true; this.writable = false; this._clearTimeout();
            this._emitEnd(); this.emit('close');
        });
        raw.on('error', (err) => { if (this.listenerCount('error')) this.emit('error', err); });
    }
    feedHead(head) { if (!head?.length || this.destroyed) return; this.buffer = this.buffer.length ? Buffer.concat([this.buffer, head]) : Buffer.from(head); this._parse(); }
    setNoDelay(v = true) { this.raw.setNoDelay(v); return this; }
    setTimeout(ms, cb) { this.timeoutMs = Number(ms) || 0; this.timeoutCallback = typeof cb === 'function' ? cb : null; this._touch(); return this; }
    _clearTimeout() { if (this.timeoutTimer) clearTimeout(this.timeoutTimer); this.timeoutTimer = null; }
    _touch() {
        this._clearTimeout();
        if (this.timeoutMs > 0 && !this.destroyed) {
            this.timeoutTimer = setTimeout(() => {
                this.timeoutTimer = null;
                if (this.timeoutCallback && !this.destroyed) this.timeoutCallback();
            }, this.timeoutMs);
            this.timeoutTimer.unref?.();
        }
    }
    write(data, cb) {
        if (this.destroyed || !this.writable) {
            const err = new Error('closed'); err.code = 'EPIPE';
            if (cb) queueMicrotask(() => cb(err));
            return false;
        }
        const frame = websocketFrame(0x2, Buffer.from(data));
        this._touch();
        return this.raw.write(frame, cb);
    }
    _writeControl(opcode, payload = Buffer.alloc(0)) { if (this.destroyed || !this.writable) return; this._touch(); this.raw.write(websocketFrame(opcode, payload)); }
    _protocolError(reason) {
        if (!this.sentClose) { this.sentClose = true; this._writeControl(0x8, websocketClosePayload(1002, reason)); }
        const err = new Error(`WS protocol: ${reason}`); err.code = 'EPROTO';
        if (this.listenerCount('error')) this.emit('error', err);
        this.destroy(err);
    }
    _messageTooLarge() { if (!this.sentClose) { this.sentClose = true; this._writeControl(0x8, websocketClosePayload(1009, 'too large')); } this.destroy(new Error('too large')); }
    _parse() {
        try {
            while (!this.destroyed) {
                if (this.buffer.length < 2) return;
                const b0 = this.buffer[0], b1 = this.buffer[1];
                const fin = Boolean(b0 & 0x80), rsv = b0 & 0x70, opcode = b0 & 0x0f;
                const masked = Boolean(b1 & 0x80);
                let length = b1 & 0x7f, offset = 2;
                if (rsv) return this._protocolError('RSV');
                if (!masked) return this._protocolError('unmasked');
                if (length === 126) { if (this.buffer.length < 4) return; length = this.buffer.readUInt16BE(2); offset = 4; }
                else if (length === 127) { if (this.buffer.length < 10) return; const n = this.buffer.readBigUInt64BE(2); if (n > BigInt(Number.MAX_SAFE_INTEGER)) return this._messageTooLarge(); length = Number(n); offset = 10; }
                const control = opcode >= 0x8;
                if (control && (!fin || length > 125)) return this._protocolError('ctrl');
                if (length > this.maxMessageBytes) return this._messageTooLarge();
                if (this.buffer.length < offset + 4 + length) return;
                const mask = this.buffer.subarray(offset, offset + 4); offset += 4;
                const payload = Buffer.from(this.buffer.subarray(offset, offset + length));
                this.buffer = this.buffer.subarray(offset + length);
                for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
                if (opcode === 0x8) {
                    this.gotClose = true;
                    if (!this.sentClose) { this.sentClose = true; this._writeControl(0x8, payload); }
                    this._emitEnd(); this.writable = false; this.raw.end(); return;
                }
                if (opcode === 0x9) { this._writeControl(0xA, payload); continue; }
                if (opcode === 0xA) continue;
                if (opcode === 0x1) { if (!this.sentClose) { this.sentClose = true; this._writeControl(0x8, websocketClosePayload(1003, 'binary only')); } this.raw.end(); return; }
                if (opcode === 0x2) {
                    if (this.fragmentOpcode) return this._protocolError('new data');
                    if (fin) this._emitBinary(payload);
                    else { this.fragmentOpcode = opcode; this.fragmentParts = [payload]; this.fragmentBytes = payload.length; }
                    continue;
                }
                if (opcode === 0x0) {
                    if (!this.fragmentOpcode) return this._protocolError('bad cont');
                    this.fragmentBytes += payload.length;
                    if (this.fragmentBytes > this.maxMessageBytes) return this._messageTooLarge();
                    this.fragmentParts.push(payload);
                    if (fin) {
                        const joined = Buffer.concat(this.fragmentParts, this.fragmentBytes);
                        this.fragmentOpcode = 0; this.fragmentParts = []; this.fragmentBytes = 0;
                        this._emitBinary(joined);
                    }
                    continue;
                }
                return this._protocolError(`opcode ${opcode}`);
            }
        } catch (err) { if (this.listenerCount('error')) this.emit('error', err); this.destroy(err); }
    }
    _emitBinary(p) { if (p.length) this.emit('data', p); }
    _emitEnd() { if (this.ended) return; this.ended = true; this.emit('end'); }
    destroy(error) {
        if (this.destroyed) return;
        this.destroyed = true; this.writable = false; this._clearTimeout();
        if (error && this.listenerCount('error')) this.emit('error', error);
        this.raw.destroy(); this._emitEnd(); this.emit('close');
    }
}

function rejectUpgrade(raw, status, message) {
    if (raw.destroyed) return;
    const body = Buffer.from(String(message || 'rejected'), 'utf8');
    raw.end(`HTTP/1.1 ${status}\r\nConnection: close\r\nContent-Type: text/plain\r\nContent-Length: ${body.length}\r\n\r\n${body}`);
}

function acceptWebSocketUpgrade(req, raw, head, cfg) {
    const upgrade = String(req.headers.upgrade || '').toLowerCase();
    const connection = String(req.headers.connection || '').toLowerCase();
    const key = String(req.headers['sec-websocket-key'] || '');
    const version = String(req.headers['sec-websocket-version'] || '');
    if (upgrade !== 'websocket' || !connection.split(',').some((v) => v.trim() === 'upgrade') || version !== '13') {
        rejectUpgrade(raw, '400 Bad Request', 'invalid upgrade'); return null;
    }
    let keyBytes;
    try { keyBytes = Buffer.from(key, 'base64'); } catch { keyBytes = Buffer.alloc(0); }
    if (keyBytes.length !== 16) { rejectUpgrade(raw, '400 Bad Request', 'bad key'); return null; }
    if (cfg.wsPath) {
        let pathname = '/';
        try { pathname = new URL(req.url || '/', 'http://x.invalid').pathname; } catch { }
        if (pathname !== cfg.wsPath) { rejectUpgrade(raw, '404 Not Found', 'not found'); return null; }
    }
    const accept = websocketAccept(key);
    raw.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n' + `Sec-WebSocket-Accept: ${accept}\r\n\r\n`);
    return { socket: new WebSocketRelaySocket(raw, cfg.maxWsMessageBytes), head };
}

async function handleConnection(socket, cfg, xm) {
    const reader = new AsyncByteReader(socket);
    let established = false;
    socket.setNoDelay(true);
    socket.setTimeout(cfg.handshakeTimeout, () => socket.destroy(new Error('handshake timeout')));
    try {
        const control = await readControl(reader);
        established = true;
        addLog(`[OK] Client: ${socket.remoteAddress || 'Worker'} mode 0x${control.mode.toString(16)}`);
        socket.setTimeout(cfg.idleTimeout > 0 ? cfg.idleTimeout : 0, () => socket.destroy(new Error('idle timeout')));
        if (control.mode === RELAY_MODE_FIXED_UDP) await serveDirectUDP(socket, reader, control.target);
        else if (control.mode === RELAY_MODE_PACKET_UDP) await servePacketUDP(socket, reader);
        else { const mux = new MuxConnection(socket, reader, cfg, xm); await mux.serve(); }
    } catch (err) {
        if (!established && !socket.destroyed) { addLog(`[WARN] malformed handshake`); await writeControlError(socket, 'malformed'); }
        else if (!isNormalClose(err)) { addLog(`[ERR] ${err.message || err}`); }
    } finally { socket.destroy(); }
}

function isNormalClose(err) {
    if (!err) return true;
    const code = err.code || '';
    if (['EOF','ECONNRESET','EPIPE','ERR_STREAM_PREMATURE_CLOSE'].includes(code)) return true;
    const msg = String(err.message || err).toLowerCase();
    return msg.includes('unexpected eof') || msg.includes('closed') || msg.includes('idle timeout');
}

function createRelayUpgradeHandler() {
    const xm = new XUDPManager(RELAY_CFG.XUDP_GRACE_MS);
    let active = 0;
    return {
        handleUpgrade(req, raw, head) {
            STATS.totalHandshakes++;
            if (active >= RELAY_CFG.MAX_CONNECTIONS) { rejectUpgrade(raw, '503', 'busy'); return; }
            const accepted = acceptWebSocketUpgrade(req, raw, head, { wsPath: RELAY_CFG.WS_PATH, maxWsMessageBytes: RELAY_CFG.MAX_WS_MESSAGE_BYTES });
            if (!accepted) return;
            const { socket, head: initial } = accepted;
            active++; STATS.activeClients = active;
            let counted = true;
            socket.once('close', () => {
                if (counted) { counted = false; active--; STATS.activeClients = active; addLog(`disconnect, active=${active}`); }
            });
            handleConnection(socket, { handshakeTimeout: RELAY_CFG.HANDSHAKE_TIMEOUT_MS, idleTimeout: RELAY_CFG.IDLE_TIMEOUT_MS }, xm)
                .catch((err) => { console.error('[RELAY]', err); socket.destroy(); });
            socket.feedHead(initial);
        },
        close() { xm.close(); },
        get stats() { return STATS; },
    };
}

// ==================== START ====================
if (require.main === module) {
  const server = new GatewayServer();
  try { require('dotenv').config(); } catch (e) {}
  const port = process.env.PORT || 3000;
  server.start(port);
}

module.exports = GatewayServer;
