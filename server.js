// ============================================
// RAILWAY GATEWAY + VLESS/XUDP NATIVE + RAW UDP
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
class XrayXudpBridge {
    constructor(gateway, ws, log) {
        this.gateway = gateway;
        this.ws = ws;
        this.log = log || (() => {});
        this.buffer = Buffer.alloc(0);
        this.sessions = new Map();
        this.closed = false;
        this.headerSent = false;
    }

    feed(data) {
        if (this.closed || !data?.length) return;
        this.buffer = Buffer.concat([this.buffer, data]);
        this._drain();
    }

    _drain() {
        while (this.buffer.length >= 4) {
            let consumed;
            try { consumed = this._parseFrame(); }
            catch (e) { this.log(`XUDP frame error: ${e.message}`); this.close(); return; }
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
            network = buf[cursor++];
            const port = buf.readUInt16BE(cursor); cursor += 2;
            const atyp = buf[cursor++];
            let host;
            if (atyp === 0x01) { host = `${buf[cursor]}.${buf[cursor+1]}.${buf[cursor+2]}.${buf[cursor+3]}`; cursor += 4; }
            else if (atyp === 0x02) { const len = buf[cursor++]; host = buf.slice(cursor, cursor + len).toString('utf8'); cursor += len; }
            else if (atyp === 0x03) {
                const p = []; for (let i = 0; i < 8; i++) p.push(buf.readUInt16BE(cursor + i*2).toString(16));
                host = p.join(':'); cursor += 16;
            } else throw new Error(`invalid atyp 0x${atyp.toString(16)}`);
            target = { host, port, atyp };
        } else if (status === 0x02) { /* Keep */ }
        else if (status === 0x03) { /* End */ }
        else if (status === 0x04) { return cursor; } // KeepAlive

        if (buf.length < cursor + 2) return 0;
        const payloadLen = buf.readUInt16BE(cursor); cursor += 2;
        if (buf.length < cursor + payloadLen) return 0;
        const payload = buf.slice(cursor, cursor + payloadLen);
        cursor += payloadLen;

        this._handleFrame(sessionID, status, target, payload);
        return cursor;
    }

    _handleFrame(sessionID, status, target, payload) {
        if (status === 0x01) this._openSession(sessionID, target, payload);
        else if (status === 0x02) { const s = this.sessions.get(sessionID); if (s && payload.length) s.send(payload); }
        else if (status === 0x03) { const s = this.sessions.get(sessionID); if (s) s.close(); this.sessions.delete(sessionID); }
    }

    async _openSession(sessionID, target, initialPayload) {
        if (!target) return;
        const self = this;

        let resolved;
        try { resolved = await dns.lookup(target.host, { family: 4 }); }
        catch (e) {
            try { resolved = await dns.lookup(target.host, { family: 6 }); }
            catch (e2) { self.log(`DNS fail ${target.host}: ${e2.message}`); return; }
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
            close() { if (this.closed) return; this.closed = true; try { socket.close(); } catch(_) {} }
        };

        socket.on('message', (msg) => {
            if (self.closed) return;
            if (self.gateway) {
                self.gateway.udpStats.inPackets++;
                self.gateway.udpStats.inBytes += msg.length;
            }
            const frame = self._buildResponseFrame(sessionID, msg);
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
        if (initialPayload?.length > 0) session.send(initialPayload);
    }

    // Format response server→client: sessionID + status + option + len + data
    _buildResponseFrame(sessionID, data) {
        const head = Buffer.alloc(4);
        head.writeUInt16BE(sessionID, 0);
        head[2] = 0x02; // Keep
        head[3] = 0x01; // Data
        const lenBuf = Buffer.alloc(2);
        lenBuf.writeUInt16BE(data.length, 0);
        return Buffer.concat([head, lenBuf, data]);
    }

    _sendHeaderOnce() {
        if (this.headerSent) return;
        this.headerSent = true;
        // VLESS response: version 0x00, addon_len 0x00
        try { this.ws.send(Buffer.from([0x00, 0x00])); } catch(_) {}
    }

    close() {
        if (this.closed) return;
        this.closed = true;
        for (const s of this.sessions.values()) s.close();
        this.sessions.clear();
    }
}

// ==================== GATEWAY ====================
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
<title>RAILWAY GATEWAY</title>
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
<body class="text-slate-300 min-h-screen flex flex-col justify-between selection:bg-blue-600 selection:text-white">

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
      <div><p class="text-xs text-slate-500 mb-1">UPTIME</p><p id="uptime-val" class="text-lg font-bold text-white">${uptime}s</p></div>
      <i class="fa-solid fa-clock text-slate-700 text-2xl"></i>
    </div>
    <div class="bg-[#0d0e16] neon-border p-5 rounded-xl flex items-center justify-between">
      <div><p class="text-xs text-slate-500 mb-1">RAM</p><p class="text-lg font-bold text-white">${ramUsed} MB</p></div>
      <i class="fa-solid fa-microchip text-slate-700 text-2xl"></i>
    </div>
    <div class="bg-[#0d0e16] neon-border p-5 rounded-xl flex items-center justify-between">
      <div><p class="text-xs text-slate-500 mb-1">RX</p><p id="rx-val" class="text-lg font-bold text-cyan-400">${formatBytes(this.totalRX)}</p></div>
      <i class="fa-solid fa-download text-cyan-900/50 text-2xl"></i>
    </div>
    <div class="bg-[#0d0e16] neon-border p-5 rounded-xl flex items-center justify-between">
      <div><p class="text-xs text-slate-500 mb-1">TX</p><p id="tx-val" class="text-lg font-bold text-pink-400">${formatBytes(this.totalTX)}</p></div>
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
      <h2 class="text-md font-bold tracking-wide text-white">VLESS / TROJAN GENERATOR</h2>
      <span id="current-domain-badge" class="text-[10px] bg-blue-500/10 text-blue-400 px-2 py-0.5 rounded border border-blue-500/20 ml-auto">🌐 detecting...</span>
    </div>

    <div class="grid grid-cols-1 md:grid-cols-2 gap-5">
      <div class="space-y-4">
        <div>
          <label class="text-xs text-slate-400 mb-1.5 block">UUID</label>
          <div class="flex gap-2">
            <input id="uuidInput" type="text" value="853b8456-0c0b-4bfa-b3b4-b2619248a9bc"
                   class="w-full bg-[#10121d] border border-slate-800 rounded-lg px-3 py-2 text-sm text-white font-mono">
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
              <span class="text-xs text-slate-300">TCP/raw UDP</span>
            </label>
            <label class="flex-1 flex items-center gap-2 bg-[#10121d] border border-slate-800 rounded-lg px-3 py-2 cursor-pointer">
              <input type="radio" name="netMode" value="xudp">
              <span class="text-xs text-slate-300">XUDP native</span>
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
      <h2 class="text-md font-bold tracking-wide text-white">WEBSOCKET ENDPOINTS</h2>
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

  <div class="flex justify-center mt-4">
    <button id="toggleApiBtn" onclick="toggleApi()" class="bg-[#1e293b] border border-slate-700 text-slate-400 hover:text-white px-5 py-2.5 rounded-lg text-sm">Toggle API</button>
  </div>

  <div class="bg-[#0d0e16] border border-slate-900 rounded-xl p-6 space-y-4" id="api-section" style="display:none">
    <div class="text-sm font-bold text-white mb-3">API ENDPOINTS</div>
    <div class="space-y-2 text-xs text-slate-400">
      <div><span class="text-emerald-400">GET</span> <span class="http-domain">${protocolHttp}</span>://<span class="http-host">${currentHost}</span>/health</div>
      <div><span class="text-emerald-400">GET</span> <span class="http-domain">${protocolHttp}</span>://<span class="http-host">${currentHost}</span>/api/proxies</div>
      <div><span class="text-emerald-400">GET</span> <span class="http-domain">${protocolHttp}</span>://<span class="http-host">${currentHost}</span>/api/relay-stats</div>
    </div>
  </div>

</main>

<footer class="border-t border-slate-950 bg-[#07080d] px-6 py-4 text-center text-xs text-slate-600">
  &copy; 2025 RAILWAY GATEWAY
</footer>

<div id="toast" class="fixed bottom-6 right-6 bg-blue-600 text-white px-4 py-2 rounded opacity-0 pointer-events-none text-xs">Copied!</div>

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
  document.querySelectorAll('.http-domain').forEach(el => el.textContent = httpProtocol);
  document.querySelectorAll('.http-host').forEach(el => el.textContent = currentDomain);
}
updateAllDomains();

function copyDynamic(path) { copyText(wsProtocol + '://' + currentDomain + '/' + path); }
function copyText(text) {
  navigator.clipboard.writeText(text).then(() => {
    const toast = document.getElementById('toast');
    toast.classList.remove('opacity-0','pointer-events-none');
    setTimeout(() => toast.classList.add('opacity-0','pointer-events-none'), 1500);
  });
}

let apiVisible = false;
function toggleApi() {
  const s = document.getElementById('api-section');
  apiVisible = !apiVisible;
  s.style.display = apiVisible ? 'block' : 'none';
}

let uptimeStart = ${uptime};
setInterval(() => { uptimeStart++; document.getElementById('uptime-val').innerText = uptimeStart + 's'; }, 1000);

async function updateTraffic() {
  try {
    const r = await fetch('/health');
    const d = await r.json();
    if (d.traffic) {
      document.getElementById('rx-val').innerText = d.traffic.rx_formatted || '0 B';
      document.getElementById('tx-val').innerText = d.traffic.tx_formatted || '0 B';
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
    if (isXudp) path = '/xudp';
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
  const pathSelect = document.getElementById('pathSelect');
  if (pathSelect) pathSelect.addEventListener('change', function() {
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
    else { res.writeHead(404, { 'Content-Type': 'text/plain' }); res.end('Not Found'); }
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
        return data.map(p => {
          const ip = p.prxIP || p.ip || p.server;
          const port = p.prxPort || p.port;
          const country = p.country || p.cc || 'XX';
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
      const tc = target.split(":");
      targetUrl.hostname = tc[0];
      targetUrl.port = tc[1]?.toString() || "443";
      targetUrl.pathname = targetPath || targetUrl.pathname;
      const options = {
        hostname: targetUrl.hostname, port: targetUrl.port,
        path: targetUrl.pathname + targetUrl.search,
        method: request.method, headers: { ...request.headers }
      };
      options.headers['host'] = targetUrl.hostname;
      const proxyReq = (targetUrl.protocol === 'https:' ? https : http).request(options, (proxyRes) => {
        response.writeHead(proxyRes.statusCode, {
          ...Object.fromEntries(Object.entries(this.CORS_HEADER_OPTIONS)),
          ...Object.fromEntries(Object.entries(proxyRes.headers)),
        });
        proxyRes.pipe(response);
      });
      proxyReq.on('error', () => { response.writeHead(500); response.end('Proxy error'); });
      if (request.method !== 'GET' && request.method !== 'HEAD') {
        const body = [];
        request.on('data', c => body.push(c)).on('end', () => { proxyReq.write(Buffer.concat(body)); proxyReq.end(); });
      } else proxyReq.end();
    } catch { response.writeHead(500); response.end('Internal server error'); }
  }

  // ==================== ROUTER ====================
  async handleWebSocketConnection(ws, request) {
    try {
      const parsedUrl = url.parse(request.url, true);
      const originalPath = parsedUrl.pathname;
      // /xudp → XUDP native (VLESS+XUDP). Path lain = gateway biasa.
      const isXudpPath = originalPath === '/xudp' || originalPath === '/XUDP';
      console.log(`WebSocket path: ${originalPath}${isXudpPath ? ' (XUDP native)' : ''}`);

      if (isXudpPath) {
        this.websocketHandlerXudp(ws);
        return;
      }

      const path = originalPath;
      const proxyListMatch = path.match(/^\/PROXYLIST\/([A-Z]{2}(,[A-Z]{2})*)$/i);
      if (proxyListMatch) {
        const ccs = proxyListMatch[1].toUpperCase().split(",");
        const proxies = await this.getPrxList(process.env.PRX_BANK_URL);
        if (!proxies.length) {
          const kv = await this.getKVPrxList();
          const avail = ccs.filter(c => kv[c]?.length);
          if (!avail.length) { ws.close(1000, `No proxies`); return; }
          const k = avail[Math.floor(Math.random() * avail.length)];
          this.prxIP = kv[k][Math.floor(Math.random() * kv[k].length)];
        } else {
          const f = proxies.filter(p => ccs.includes(p.country));
          if (!f.length) { ws.close(1000, `No proxies`); return; }
          const s = f[Math.floor(Math.random() * f.length)];
          this.prxIP = `${s.prxIP}:${s.prxPort}`;
        }
        await this.websocketHandler(ws);
        return;
      }

      const allMatch = path.match(/^\/ALL(\d+)?$/i);
      if (allMatch) {
        const proxies = await this.getPrxList(process.env.PRX_BANK_URL);
        if (!proxies.length) {
          const kv = await this.getKVPrxList();
          const all = Object.values(kv).flat();
          if (!all.length) { ws.close(1000, `No proxies`); return; }
          this.prxIP = all[Math.floor(Math.random() * all.length)];
        } else {
          const s = proxies[Math.floor(Math.random() * proxies.length)];
          this.prxIP = `${s.prxIP}:${s.prxPort}`;
        }
        await this.websocketHandler(ws);
        return;
      }

      const putarMatch = path.match(/^\/PUTAR(\d+)?$/i);
      if (putarMatch) {
        const proxies = await this.getPrxList(process.env.PRX_BANK_URL);
        if (!proxies.length) {
          const kv = await this.getKVPrxList();
          const cs = Object.keys(kv).filter(c => kv[c]?.length);
          if (!cs.length) { ws.close(1000, `No proxies`); return; }
          const k = cs[Math.floor(Math.random() * cs.length)];
          this.prxIP = kv[k][Math.floor(Math.random() * kv[k].length)];
        } else {
          const s = proxies[Math.floor(Math.random() * proxies.length)];
          this.prxIP = `${s.prxIP}:${s.prxPort}`;
        }
        await this.websocketHandler(ws);
        return;
      }

      const regionMatch = path.match(/^\/([A-Z]+)(\d+)?$/i);
      if (regionMatch && REGION_MAP[regionMatch[1].toUpperCase()]) {
        const rk = regionMatch[1].toUpperCase();
        const cs = rk === 'GLOBAL' ? [] : REGION_MAP[rk];
        const proxies = await this.getPrxList(process.env.PRX_BANK_URL);
        if (!proxies.length) {
          const kv = await this.getKVPrxList();
          let avail = [];
          if (rk === 'GLOBAL') avail = Object.values(kv).flat();
          else for (const c of cs) if (kv[c]) avail.push(...kv[c]);
          if (!avail.length) { ws.close(1000, `No proxies`); return; }
          this.prxIP = avail[Math.floor(Math.random() * avail.length)];
        } else {
          const f = rk === 'GLOBAL' ? proxies : proxies.filter(p => cs.includes(p.country));
          if (!f.length) { ws.close(1000, `No proxies`); return; }
          const s = f[Math.floor(Math.random() * f.length)];
          this.prxIP = `${s.prxIP}:${s.prxPort}`;
        }
        await this.websocketHandler(ws);
        return;
      }

      const countryMatch = path.match(/^\/([A-Z]{2})(\d+)?$/);
      if (countryMatch) {
        const cc = countryMatch[1].toUpperCase();
        const proxies = await this.getPrxList(process.env.PRX_BANK_URL);
        if (!proxies.length) {
          const kv = await this.getKVPrxList();
          if (!kv[cc]?.length) { ws.close(1000, `No proxies`); return; }
          this.prxIP = kv[cc][Math.floor(Math.random() * kv[cc].length)];
        } else {
          const f = proxies.filter(p => p.country === cc);
          if (!f.length) { ws.close(1000, `No proxies`); return; }
          const s = f[Math.floor(Math.random() * f.length)];
          this.prxIP = `${s.prxIP}:${s.prxPort}`;
        }
        await this.websocketHandler(ws);
        return;
      }

      const ipPortMatch = path.match(/^\/(.+[:=-]\d+)$/);
      if (ipPortMatch) {
        this.prxIP = ipPortMatch[1].replace(/[=:-]/, ":");
        await this.websocketHandler(ws);
        return;
      }

      ws.close(1000, "Invalid WebSocket path");
    } catch (err) {
      console.error('WS error:', err);
      ws.close(1011, 'Internal error');
    }
  }

  // ==================== XUDP NATIVE HANDLER ====================
  async websocketHandlerXudp(ws) {
    let bridge = null;
    const log = (m) => console.log(`[XUDP] ${m}`);
    this.udpStats.wsConnections++;

    ws.on('message', async (message) => {
      try {
        const chunk = Buffer.from(message);
        this.totalRX += chunk.length;

        if (bridge) { bridge.feed(chunk); return; }

        // Parse VLESS header
        if (chunk.length < 18) throw new Error('short vless header');
        if (chunk[0] !== 0x00) throw new Error('bad vless version');
        const addonLen = chunk[17];
        let cursor = 18 + addonLen;
        if (chunk.length < cursor + 4) throw new Error('short body');
        const cmd = chunk[cursor++];
        const port = chunk.readUInt16BE(cursor); cursor += 2;
        const atyp = chunk[cursor++];
        let alen = 0, addr = '';
        if (atyp === 0x01) { alen = 4; addr = Array.from(chunk.slice(cursor, cursor+4)).join('.'); }
        else if (atyp === 0x02) { alen = chunk[cursor]; cursor++; addr = chunk.slice(cursor, cursor+alen).toString(); }
        else if (atyp === 0x03) {
          alen = 16;
          const p = []; for (let i = 0; i < 8; i++) p.push(chunk.readUInt16BE(cursor + i*2).toString(16));
          addr = p.join(':');
        } else throw new Error('bad atyp');
        cursor += alen;

        if (cmd !== 0x02) throw new Error('XUDP path only UDP');
        if (!addr || !port) throw new Error('empty target');

        log(`Client: ${ws._socket?.remoteAddress || '?'} → ${addr}:${port}`);

        bridge = new XrayXudpBridge(this, ws, log);
        // VLESS response di-kirim saat frame pertama UDP balik, bukan di awal.
        // Ini sesuai perilaku Xray server yang hanya kirim header sekali.
        // Simpan flag, akan dikirim bersamaan dengan data pertama.

        const remaining = chunk.slice(cursor);
        if (remaining.length > 0) bridge.feed(remaining);
        else {
          // Kirim VLESS response lebih awal supaya client tahu handshake OK
          bridge._sendHeaderOnce();
        }
      } catch (err) {
        console.error('[XUDP] error:', err.message);
        ws.close(1011, err.message);
      }
    });

    ws.on('close', () => {
      if (bridge) bridge.close();
      this.udpStats.wsConnections = Math.max(0, this.udpStats.wsConnections - 1);
      log('WS closed');
    });

    ws.on('error', (err) => {
      console.error('[XUDP] ws error:', err);
      if (bridge) bridge.close();
    });
  }

  // ==================== STANDARD WS HANDLER ====================
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
        let h;
        if (protocol === horse) h = this.readHorseHeader(chunk);
        else if (protocol === flash) h = this.readFlashHeader(chunk);
        else if (protocol === vless) h = this.readVlessHeader(chunk);
        else if (protocol === "ss") h = this.readSsHeader(chunk);
        else throw new Error("Unknown Protocol!");

        addressLog = h.addressRemote;
        portLog = `${h.portRemote} -> ${h.isUDP ? "UDP" : "TCP"}`;
        if (h.hasError) throw new Error(h.message);

        if (h.isUDP) {
          return await this.handleUDPOutbound(h.addressRemote, h.portRemote, chunk.slice(h.rawDataIndex), ws, h.version, log);
        }

        this.handleTCPOutBound(remoteSocketWrapper, h.addressRemote, h.portRemote, h.rawClientData, ws, h.version, log);
      } catch (err) {
        console.error('WS err:', err.message);
        ws.close(1011, err.message);
      }
    });

    ws.on('close', () => {
      if (remoteSocketWrapper.value) remoteSocketWrapper.value.end();
      this.cleanupUDPConnections(ws);
      this.udpStats.wsConnections = Math.max(0, this.udpStats.wsConnections - 1);
    });

    ws.on('error', (err) => {
      console.error('WS error:', err);
      this.cleanupUDPConnections(ws);
    });
  }

  // ==================== SNIFFERS ====================
  async protocolSniffer(buf) {
    if (buf.length >= 62) {
      const d = buf.slice(56, 60);
      if (d[0] === 0x0d && d[1] === 0x0a && [0x01,0x03,0x7f].includes(d[2]) && [0x01,0x03,0x04].includes(d[3])) return horse;
    }
    if (buf.length >= 18 && buf[0] === 0x00 && buf[17] <= 64) return vless;
    if (buf.length >= 17 && buf[0] === 0x01) {
      const h = buf.slice(1, 17).toString('hex');
      if (h.match(/^[0-9a-f]{8}[0-9a-f]{4}4[0-9a-f]{3}[89ab][0-9a-f]{3}[0-9a-f]{12}$/i)) return flash;
    }
    return "ss";
  }

  readVlessHeader(buf) {
    try {
      if (buf.length < 18) return { hasError: true, message: 'short vless' };
      if (buf[0] !== 0x00) return { hasError: true, message: 'bad version' };
      const al = buf[17];
      let cursor = 18 + al;
      if (buf.length < cursor + 4) return { hasError: true, message: 'short body' };
      const cmd = buf[cursor++];
      const port = buf.readUInt16BE(cursor); cursor += 2;
      const atyp = buf[cursor++];
      let alen = 0, addr = '';
      if (atyp === 0x01) { alen = 4; addr = Array.from(buf.slice(cursor, cursor+4)).join('.'); }
      else if (atyp === 0x02) { alen = buf[cursor]; cursor++; addr = buf.slice(cursor, cursor+alen).toString(); }
      else if (atyp === 0x03) {
        alen = 16;
        const p = []; for (let i = 0; i < 8; i++) p.push(buf.readUInt16BE(cursor + i*2).toString(16));
        addr = p.join(':');
      } else return { hasError: true, message: `bad atyp ${atyp}` };
      cursor += alen;
      return {
        hasError: false,
        addressRemote: addr,
        portRemote: port,
        rawDataIndex: cursor,
        rawClientData: buf.slice(cursor),
        version: Buffer.from([0x00, 0x00]),
        isUDP: cmd === 0x02
      };
    } catch (e) { return { hasError: true, message: e.message }; }
  }

  readSsHeader(buf) {
    const at = buf[0]; let al = 0, avi = 1, av = "";
    if (at === 1) { al = 4; av = Array.from(buf.slice(avi, avi+al)).join("."); }
    else if (at === 3) { al = buf[avi]; avi++; av = buf.slice(avi, avi+al).toString(); }
    else if (at === 4) { al = 16; const ip = []; for (let i = 0; i < 8; i++) ip.push(buf.readUInt16BE(avi+i*2).toString(16)); av = ip.join(":"); }
    else return { hasError: true, message: `bad atyp ${at}` };
    if (!av) return { hasError: true, message: "empty addr" };
    const pi = avi + al;
    const pr = buf.readUInt16BE(pi);
    return { hasError: false, addressRemote: av, portRemote: pr, rawDataIndex: pi+2, rawClientData: buf.slice(pi+2), version: null, isUDP: pr == 53 };
  }

  readFlashHeader(buf) {
    const v = buf[0]; let udp = false;
    const ol = buf[17]; const cmd = buf[18+ol];
    if (cmd === 2) udp = true; else if (cmd !== 1) return { hasError: true, message: `bad cmd` };
    const pi = 18+ol+1; const pr = buf.readUInt16BE(pi);
    let ai = pi+2; const at = buf[ai]; let al = 0, avi = ai+1, av = "";
    if (at === 1) { al = 4; av = Array.from(buf.slice(avi, avi+al)).join("."); }
    else if (at === 2) { al = buf[avi]; avi++; av = buf.slice(avi, avi+al).toString(); }
    else if (at === 3) { al = 16; const ip = []; for (let i = 0; i < 8; i++) ip.push(buf.readUInt16BE(avi+i*2).toString(16)); av = ip.join(":"); }
    else return { hasError: true, message: `bad atyp ${at}` };
    if (!av) return { hasError: true, message: "empty addr" };
    return { hasError: false, addressRemote: av, portRemote: pr, rawDataIndex: avi+al, rawClientData: buf.slice(avi+al), version: Buffer.from([v,0]), isUDP: udp };
  }

  readHorseHeader(buf) {
    const db = buf.slice(58);
    if (db.length < 6) return { hasError: true, message: "short" };
    let udp = false;
    const cmd = db[0];
    if (cmd == 3) udp = true; else if (cmd != 1) throw new Error("bad cmd");
    let at = db[1]; let al = 0, avi = 2, av = "";
    if (at === 1) { al = 4; av = Array.from(db.slice(avi, avi+al)).join("."); }
    else if (at === 3) { al = db[avi]; avi++; av = db.slice(avi, avi+al).toString(); }
    else if (at === 4) { al = 16; const ip = []; for (let i = 0; i < 8; i++) ip.push(db.readUInt16BE(avi+i*2).toString(16)); av = ip.join(":"); }
    else return { hasError: true, message: `bad atyp ${at}` };
    if (!av) return { hasError: true, message: "empty addr" };
    const pi = avi + al;
    const pr = db.readUInt16BE(pi);
    return { hasError: false, addressRemote: av, portRemote: pr, rawDataIndex: pi+4, rawClientData: db.slice(pi+4), version: null, isUDP: udp };
  }

  // ==================== TCP + RAW UDP ====================
  async handleTCPOutBound(remoteSocket, addressRemote, portRemote, rawClientData, webSocket, responseHeader, log) {
    const connectAndWrite = (address, port) => new Promise((resolve, reject) => {
      const s = net.createConnection({ host: address, port }, () => {
        if (responseHeader) s.write(Buffer.from(responseHeader));
        s.write(rawClientData);
        resolve(s);
      });
      s.on('error', reject);
    });
    const retry = async () => {
      try {
        const p = (this.prxIP || '').split(/[:=-]/);
        const s = await connectAndWrite(p[0] || addressRemote, p[1] || portRemote);
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
            webSocket.send(Buffer.concat([Buffer.from(header), msg]));
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
        webSocket.send(Buffer.concat([Buffer.from(header), chunk]));
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
        console.error('HTTP error:', error);
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
      if (pathname === RELAY_WS_PATH) { relayHandler.handleUpgrade(req, socket, head); return; }
      this.wss.handleUpgrade(req, socket, head, (ws) => { this.wss.emit('connection', ws, req); });
    });

    const gracefulShutdown = () => {
      if (this.wss) { this.wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.close(); }); this.wss.close(); }
      for (const [, conn] of this.activeUDPConnections) { try { conn.socket.close(); } catch(_) {} }
      this.activeUDPConnections.clear();
      if (this._relayHandler) this._relayHandler.close();
      if (this.httpServer) this.httpServer.close(() => process.exit(0));
      setTimeout(() => process.exit(1), 10000);
    };
    process.on('SIGTERM', gracefulShutdown);
    process.on('SIGINT', gracefulShutdown);

    server.listen(port, '0.0.0.0', () => {
      console.log(`✅ Gateway on :${port}`);
      console.log(`🛰️  /xudp = XUDP native (VLESS+XUDP)`);
      console.log(`🛰️  /ALL, /ID, dll = raw UDP/TCP`);
    });

    this.httpServer = server;
    server.on('error', (error) => {
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

function rejectUdpTarget(t) { return Boolean(RELAY_CFG.REJECT_UDP_443 && Number(t?.port) === 443); }

class AsyncByteReader {
    constructor(s) {
        this.s = s; this.buf = []; this.av = 0; this.waiters = []; this.ended = false; this.err = null;
        s.on('data', c => { if (!c?.length) return; this.buf.push(Buffer.from(c)); this.av += c.length; this._f(); });
        s.on('end', () => { this.ended = true; this._f(); });
        s.on('close', () => { this.ended = true; this._f(); });
        s.on('error', e => { this.err = e; this._f(); });
    }
    readExactly(n) {
        if (!Number.isInteger(n) || n < 0) return Promise.reject(new Error('bad len'));
        if (n === 0) return Promise.resolve(Buffer.alloc(0));
        if (this.av >= n) return Promise.resolve(this._t(n));
        if (this.err) return Promise.reject(this.err);
        if (this.ended) return Promise.reject(new Error('unexpected EOF'));
        return new Promise((res, rej) => this.waiters.push({ n, res, rej }));
    }
    _f() {
        while (this.waiters.length) {
            const w = this.waiters[0];
            if (this.av >= w.n) { this.waiters.shift(); w.res(this._t(w.n)); continue; }
            if (this.err || this.ended) { this.waiters.shift(); w.rej(this.err || new Error('EOF')); continue; }
            break;
        }
    }
    _t(n) {
        const out = Buffer.allocUnsafe(n); let o = 0;
        while (o < n) {
            const f = this.buf[0]; const need = n - o;
            if (f.length <= need) { f.copy(out, o); o += f.length; this.buf.shift(); }
            else { f.copy(out, o, 0, need); this.buf[0] = f.subarray(need); o += need; }
        }
        this.av -= n; return out;
    }
}

async function readLengthPayload(r) { const l = (await r.readExactly(2)).readUInt16BE(0); return l === 0 ? Buffer.alloc(0) : r.readExactly(l); }

async function readEndpoint(r) {
    const h = await r.readExactly(3);
    const p = h.readUInt16BE(0); const a = h[2];
    if (p === 0) throw new Error('zero port');
    return readEndpointBody(r, a, p);
}

async function readEndpointBody(r, a, p) {
    if (a === ATYP_IPV4) { const b = await r.readExactly(4); return { host: `${b[0]}.${b[1]}.${b[2]}.${b[3]}`, port: p, atyp: a }; }
    if (a === ATYP_DOMAIN) {
        const l = (await r.readExactly(1))[0];
        if (!l) throw new Error('empty');
        const b = await r.readExactly(l);
        return { host: utf8Fatal.decode(b), port: p, atyp: a };
    }
    if (a === ATYP_IPV6) { const b = await r.readExactly(16); return { host: formatIPv6(b), port: p, atyp: a }; }
    throw new Error(`atyp ${a}`);
}

function parseEndpointBytes(b, o) {
    if (b.length - o < 3) throw new Error('EOF');
    const p = b.readUInt16BE(o); const a = b[o+2]; let c = o + 3;
    if (a === ATYP_IPV4) { const bb = b.subarray(c, c+4); c += 4; return { endpoint: { host: `${bb[0]}.${bb[1]}.${bb[2]}.${bb[3]}`, port: p, atyp: a }, next: c }; }
    if (a === ATYP_DOMAIN) {
        const l = b[c++]; if (!l || b.length - c < l) throw new Error('domain');
        const h = utf8Fatal.decode(b.subarray(c, c+l)); c += l;
        return { endpoint: { host: h, port: p, atyp: a }, next: c };
    }
    if (a === ATYP_IPV6) { const h = formatIPv6(b.subarray(c, c+16)); c += 16; return { endpoint: { host: h, port: p, atyp: a }, next: c }; }
    throw new Error(`atyp ${a}`);
}

function formatIPv6(b) { const p = []; for (let i = 0; i < 16; i += 2) p.push(b.readUInt16BE(i).toString(16)); return p.join(':'); }

function writeSocket(s, d) {
    if (s.destroyed || !s.writable) return Promise.reject(new Error('closed'));
    return new Promise((res, rej) => s.write(d, e => e ? rej(e) : res()));
}

async function writeControlError(s, m) {
    let b = Buffer.from(String(m || 'err'), 'utf8');
    if (b.length > MAX_PACKET_LEN) b = b.subarray(0, MAX_PACKET_LEN);
    const o = Buffer.allocUnsafe(3 + b.length);
    o[0] = 1; o.writeUInt16BE(b.length, 1); b.copy(o, 3);
    try { await writeSocket(s, o); } catch { }
}

async function readControl(r) {
    const m = await r.readExactly(RELAY_MAGIC.length);
    if (!m.equals(RELAY_MAGIC)) throw new Error('bad magic');
    const mode = (await r.readExactly(1))[0];
    if (![RELAY_MODE_FIXED_UDP, RELAY_MODE_MUX, RELAY_MODE_PACKET_UDP].includes(mode)) throw new Error('bad mode');
    const t = mode === RELAY_MODE_FIXED_UDP ? await readEndpoint(r) : null;
    return { mode, target: t };
}

async function resolveTarget(t) {
    if (t.atyp === ATYP_IPV4) return { address: t.host, family: 4 };
    if (t.atyp === ATYP_IPV6) return { address: t.host, family: 6 };
    const recs = await dns.lookup(t.host, { all: true, verbatim: true });
    if (!recs.length) throw new Error('DNS');
    return recs.find(r => r.family === 4) || recs.find(r => r.family === 6);
}

function bindDgram(s, p, a) {
    return new Promise((res, rej) => {
        const onE = (e) => { cl(); rej(e); };
        const onL = () => { cl(); res(); };
        const cl = () => { s.off('error', onE); s.off('listening', onL); };
        s.once('error', onE); s.once('listening', onL);
        s.bind(p, a);
    });
}

class UDPAssociation {
    constructor() { this.udp4 = null; this.udp6 = null; this.sink = null; this.closed = false; }
    static async create() {
        const a = new UDPAssociation();
        a.udp4 = dgram.createSocket({ type: 'udp4', reuseAddr: true });
        await bindDgram(a.udp4, 0, '0.0.0.0');
        const port = a.udp4.address().port;
        a.udp4.on('message', (m, r) => a._onMessage(m, r));
        a.udp4.on('error', () => { });
        a.udp6 = dgram.createSocket({ type: 'udp6', reuseAddr: true, ipv6Only: true });
        try {
            await bindDgram(a.udp6, port, '::');
            a.udp6.on('message', (m, r) => a._onMessage(m, r));
            a.udp6.on('error', () => { });
        } catch { try { a.udp6.close(); } catch { } a.udp6 = null; }
        return a;
    }
    attach(s) { const o = this.sink; this.sink = s; return o; }
    detach(m, i) { if (this.sink && this.sink.mux === m && this.sink.id === i) { this.sink = null; return true; } return false; }
    async send(t, p) {
        if (this.closed) throw new Error('closed');
        if (p.length > MAX_PACKET_LEN) throw new Error('large');
        const r = await resolveTarget(t);
        const s = r.family === 6 ? this.udp6 : this.udp4;
        if (!s) throw new Error('unavail');
        await new Promise((res, rej) => s.send(p, t.port, r.address, e => e ? rej(e) : res()));
        STATS.udpPacketsOut++; STATS.udpBytesOut += p.length;
    }
    _onMessage(m, r) {
        STATS.udpPacketsIn++; STATS.udpBytesIn += m.length;
        const s = this.sink;
        if (!s || this.closed) return;
        Promise.resolve(s.mux.sendUDPData(s.id, r, Buffer.from(m))).catch(() => { });
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
    constructor(g) { this.g = g; this.e = new Map(); }
    async attach(id, m, s) {
        const k = Buffer.from(id).toString('hex');
        let e = this.e.get(k);
        if (!e) { e = { assoc: await UDPAssociation.create(), timer: null }; this.e.set(k, e); }
        if (e.timer) { clearTimeout(e.timer); e.timer = null; }
        const o = e.assoc.attach({ mux: m, id: s });
        return { assoc: e.assoc, oldSink: o };
    }
    detach(id, m, s) {
        const k = Buffer.from(id).toString('hex');
        const e = this.e.get(k);
        if (!e || !e.assoc.detach(m, s)) return;
        if (e.timer) clearTimeout(e.timer);
        e.timer = setTimeout(() => {
            const c = this.e.get(k);
            if (c !== e) return;
            this.e.delete(k); e.assoc.close();
        }, this.g);
        e.timer.unref?.();
    }
    close() {
        for (const e of this.e.values()) { if (e.timer) clearTimeout(e.timer); e.assoc.close(); }
        this.e.clear();
    }
}

function encodeUDPSource(r) {
    const p = Number(r.port);
    if (!Number.isInteger(p) || p < 1 || p > 65535) throw new Error('port');
    const f = net.isIP(r.address);
    const h = Buffer.alloc(3); h.writeUInt16BE(p, 0);
    if (f === 4) { h[2] = ATYP_IPV4; return Buffer.concat([h, Buffer.from(r.address.split('.').map(Number))]); }
    if (f === 6) { h[2] = ATYP_IPV6; return Buffer.concat([h, ipv6ToBytes(r.address)]); }
    throw new Error('ip');
}

function ipv6ToBytes(a) {
    let i = a; const z = i.indexOf('%'); if (z >= 0) i = i.slice(0, z);
    const lc = i.lastIndexOf(':');
    if (i.includes('.') && lc >= 0) {
        const v4 = i.slice(lc+1).split('.').map(Number);
        const t = [((v4[0]<<8)|v4[1]).toString(16), ((v4[2]<<8)|v4[3]).toString(16)];
        i = i.slice(0, lc) + ':' + t.join(':');
    }
    const h = i.split('::');
    const L = h[0] ? h[0].split(':').filter(Boolean) : [];
    const R = h.length === 2 && h[1] ? h[1].split(':').filter(Boolean) : [];
    const miss = 8 - L.length - R.length;
    const w = [...L, ...Array(Math.max(0, miss)).fill('0'), ...R];
    const o = Buffer.alloc(16);
    w.forEach((x, k) => o.writeUInt16BE(parseInt(x, 16), k * 2));
    return o;
}

async function readMuxFrame(r) {
    const ml = (await r.readExactly(2)).readUInt16BE(0);
    if (ml < 4 || ml > MAX_MUX_META_LEN) throw new Error(`bad meta ${ml}`);
    const meta = await r.readExactly(ml);
    const f = { id: meta.readUInt16BE(0), status: meta[2], option: meta[3], network: 0, target: null, globalID: null, data: Buffer.alloc(0) };
    let c = 4;
    if (f.status === MUX_STATUS_NEW) {
        f.network = meta[c++];
        const p = parseEndpointBytes(meta, c);
        f.target = p.endpoint; c = p.next;
        if (f.network === MUX_NETWORK_UDP && meta.length - c >= 8) {
            const g = meta.subarray(c, c+8);
            if (!g.equals(Buffer.alloc(8))) f.globalID = Buffer.from(g);
            c += 8;
        }
        if (c !== meta.length) throw new Error(`unexpected ${meta.length-c}`);
    } else if (f.status === MUX_STATUS_KEEP && meta.length > c && meta[c] === MUX_NETWORK_UDP) {
        f.network = meta[c++]; f.target = parseEndpointBytes(meta, c).endpoint;
    }
    if ((f.option & MUX_OPTION_DATA) !== 0) f.data = await readLengthPayload(r);
    return f;
}

class MuxSession {
    constructor(m, i, n, t) { this.mux = m; this.id = i; this.network = n; this.target = t; this.udp = null; this.global = false; this.gid = null; this.closed = false; }
    async sendUDP(t, p) { if (!this.udp) throw new Error('unavail'); await this.udp.send(t, p); }
    closeWithoutRemoving() {
        if (this.closed) return;
        this.closed = true;
        if (this.udp) {
            if (this.global) this.mux.xm.detach(this.gid, this.mux, this.id);
            else { this.udp.detach(this.mux, this.id); this.udp.close(); }
            this.udp = null;
        }
    }
    async close(se) {
        if (this.mux.sessions.get(this.id) === this) this.mux.sessions.delete(this.id);
        this.closeWithoutRemoving();
        if (se) await this.mux.sendEnd(this.id, true).catch(() => { });
    }
}

class MuxConnection {
    constructor(s, r, c, x) { this.socket = s; this.reader = r; this.cfg = c; this.xm = x; this.sessions = new Map(); this.closed = false; this.writeChain = Promise.resolve(); }
    async serve() {
        try {
            await writeSocket(this.socket, Buffer.from([0]));
            for (;;) { const f = await readMuxFrame(this.reader); await this.handleFrame(f); }
        } finally { this.closeAll(); }
    }
    async handleFrame(f) {
        if (f.status === MUX_STATUS_KEEPALIVE) return;
        if (f.status === MUX_STATUS_NEW) return this.handleNew(f);
        if (f.status === MUX_STATUS_KEEP) return this.handleKeep(f);
        if (f.status === MUX_STATUS_END) {
            const s = this.sessions.get(f.id);
            if (s && f.data.length) await s.sendUDP(s.target, f.data).catch(() => { });
            this.removeSession(f.id);
            return;
        }
        throw new Error(`bad status 0x${f.status.toString(16)}`);
    }
    async handleNew(f) {
        if (f.network !== MUX_NETWORK_UDP || !f.target?.host || !f.target?.port || rejectUdpTarget(f.target)) {
            await this.sendEnd(f.id, true).catch(() => { }); return;
        }
        this.removeSession(f.id);
        const s = new MuxSession(this, f.id, f.network, f.target);
        if (f.globalID) {
            try {
                const { assoc, oldSink } = await this.xm.attach(f.globalID, this, f.id);
                s.udp = assoc; s.global = true; s.gid = Buffer.from(f.globalID);
                this.sessions.set(s.id, s);
                if (oldSink && (oldSink.mux !== this || oldSink.id !== f.id)) {
                    oldSink.mux.removeSession(oldSink.id);
                    await oldSink.mux.sendEnd(oldSink.id, false).catch(() => { });
                }
            } catch { await this.sendEnd(f.id, true).catch(() => { }); return; }
        } else {
            try {
                const a = await UDPAssociation.create();
                a.attach({ mux: this, id: f.id });
                s.udp = a;
                this.sessions.set(s.id, s);
            } catch { await this.sendEnd(f.id, true).catch(() => { }); return; }
        }
        if (f.data.length) await s.sendUDP(f.target, f.data).catch(() => s.close(true));
    }
    async handleKeep(f) {
        const s = this.sessions.get(f.id);
        if (!s) { await this.sendEnd(f.id, false).catch(() => { }); return; }
        if (!f.data.length) return;
        let t = s.target;
        if (f.network === MUX_NETWORK_UDP && f.target?.host && f.target?.port) { t = f.target; s.target = t; }
        if (rejectUdpTarget(t)) { await s.close(true); return; }
        await s.sendUDP(t, f.data).catch(() => s.close(true));
    }
    removeSession(id) { const s = this.sessions.get(id); if (!s) return; this.sessions.delete(id); s.closeWithoutRemoving(); }
    closeAll() { if (this.closed) return; this.closed = true; for (const s of this.sessions.values()) s.closeWithoutRemoving(); this.sessions.clear(); }
    _q(d) { const o = this.writeChain.then(() => writeSocket(this.socket, d)); this.writeChain = o.catch(() => { }); return o; }
    sendUDPData(id, src, data) {
        const a = encodeUDPSource(src);
        const m = Buffer.allocUnsafe(5 + a.length);
        m.writeUInt16BE(id, 0); m[2] = MUX_STATUS_KEEP; m[3] = MUX_OPTION_DATA; m[4] = MUX_NETWORK_UDP;
        a.copy(m, 5);
        return this.writeMuxPacket(m, data);
    }
    sendEnd(id, e) {
        const m = Buffer.alloc(4);
        m.writeUInt16BE(id, 0); m[2] = MUX_STATUS_END; m[3] = e ? MUX_OPTION_ERROR : 0;
        return this.writeMuxMeta(m);
    }
    writeMuxPacket(m, d) {
        if (d.length > MAX_PACKET_LEN) return Promise.reject(new Error('large'));
        const o = Buffer.allocUnsafe(2 + m.length + 2 + d.length);
        o.writeUInt16BE(m.length, 0); m.copy(o, 2);
        const off = 2 + m.length;
        o.writeUInt16BE(d.length, off); d.copy(o, off + 2);
        return this._q(o);
    }
    writeMuxMeta(m) { const o = Buffer.allocUnsafe(2 + m.length); o.writeUInt16BE(m.length, 0); m.copy(o, 2); return this._q(o); }
}

const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
function websocketAccept(k) { return createHash('sha1').update(String(k) + WS_GUID, 'ascii').digest('base64'); }
function websocketFrame(op, p = Buffer.alloc(0)) {
    const b = Buffer.isBuffer(p) ? p : Buffer.from(p);
    let h;
    if (b.length < 126) { h = Buffer.allocUnsafe(2); h[1] = b.length; }
    else if (b.length <= 0xffff) { h = Buffer.allocUnsafe(4); h[1] = 126; h.writeUInt16BE(b.length, 2); }
    else { h = Buffer.allocUnsafe(10); h[1] = 127; h.writeBigUInt64BE(BigInt(b.length), 2); }
    h[0] = 0x80 | (op & 0x0f);
    return b.length ? Buffer.concat([h, b]) : h;
}
function websocketClosePayload(c, r = '') {
    let t = Buffer.from(String(r), 'utf8');
    if (t.length > 123) t = t.subarray(0, 123);
    const o = Buffer.allocUnsafe(2 + t.length);
    o.writeUInt16BE(c, 0); t.copy(o, 2);
    return o;
}

class WebSocketRelaySocket extends EventEmitter {
    constructor(r, m) {
        super();
        this.raw = r; this.remoteAddress = r.remoteAddress; this.remotePort = r.remotePort;
        this.destroyed = false; this.writable = true; this.buffer = Buffer.alloc(0);
        this.fop = 0; this.fp = []; this.fb = 0;
        this.max = m; this.tms = 0; this.tcb = null; this.tt = null;
        this.sentClose = false; this.ended = false;
        r.on('data', (c) => {
            if (this.destroyed || !c?.length) return;
            this._touch();
            this.buffer = this.buffer.length ? Buffer.concat([this.buffer, c]) : Buffer.from(c);
            this._parse();
        });
        r.on('end', () => this._e());
        r.on('close', () => {
            if (this.destroyed) return;
            this.destroyed = true; this.writable = false; this._ct();
            this._e(); this.emit('close');
        });
        r.on('error', (e) => { if (this.listenerCount('error')) this.emit('error', e); });
    }
    feedHead(h) { if (!h?.length || this.destroyed) return; this.buffer = this.buffer.length ? Buffer.concat([this.buffer, h]) : Buffer.from(h); this._parse(); }
    setNoDelay(v = true) { this.raw.setNoDelay(v); return this; }
    setTimeout(ms, cb) { this.tms = Number(ms) || 0; this.tcb = typeof cb === 'function' ? cb : null; this._touch(); return this; }
    _ct() { if (this.tt) clearTimeout(this.tt); this.tt = null; }
    _touch() {
        this._ct();
        if (this.tms > 0 && !this.destroyed) {
            this.tt = setTimeout(() => {
                this.tt = null;
                if (this.tcb && !this.destroyed) this.tcb();
            }, this.tms);
            this.tt.unref?.();
        }
    }
    write(d, cb) {
        if (this.destroyed || !this.writable) {
            const e = new Error('closed'); e.code = 'EPIPE';
            if (cb) queueMicrotask(() => cb(e));
            return false;
        }
        this._touch();
        return this.raw.write(websocketFrame(0x2, Buffer.from(d)), cb);
    }
    _wc(op, p = Buffer.alloc(0)) { if (this.destroyed || !this.writable) return; this._touch(); this.raw.write(websocketFrame(op, p)); }
    _pe(r) {
        if (!this.sentClose) { this.sentClose = true; this._wc(0x8, websocketClosePayload(1002, r)); }
        const e = new Error(`WS: ${r}`); e.code = 'EPROTO';
        if (this.listenerCount('error')) this.emit('error', e);
        this.destroy(e);
    }
    _mtl() { if (!this.sentClose) { this.sentClose = true; this._wc(0x8, websocketClosePayload(1009, 'large')); } this.destroy(new Error('large')); }
    _parse() {
        try {
            while (!this.destroyed) {
                if (this.buffer.length < 2) return;
                const b0 = this.buffer[0], b1 = this.buffer[1];
                const fin = !!(b0 & 0x80), rsv = b0 & 0x70, op = b0 & 0x0f;
                const masked = !!(b1 & 0x80);
                let len = b1 & 0x7f, off = 2;
                if (rsv) return this._pe('RSV');
                if (!masked) return this._pe('unmasked');
                if (len === 126) { if (this.buffer.length < 4) return; len = this.buffer.readUInt16BE(2); off = 4; }
                else if (len === 127) { if (this.buffer.length < 10) return; const n = this.buffer.readBigUInt64BE(2); if (n > BigInt(Number.MAX_SAFE_INTEGER)) return this._mtl(); len = Number(n); off = 10; }
                const ctrl = op >= 0x8;
                if (ctrl && (!fin || len > 125)) return this._pe('ctrl');
                if (len > this.max) return this._mtl();
                if (this.buffer.length < off + 4 + len) return;
                const mask = this.buffer.subarray(off, off + 4); off += 4;
                const payload = Buffer.from(this.buffer.subarray(off, off + len));
                this.buffer = this.buffer.subarray(off + len);
                for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
                if (op === 0x8) {
                    if (!this.sentClose) { this.sentClose = true; this._wc(0x8, payload); }
                    this._e(); this.writable = false; this.raw.end(); return;
                }
                if (op === 0x9) { this._wc(0xA, payload); continue; }
                if (op === 0xA) continue;
                if (op === 0x1) { if (!this.sentClose) { this.sentClose = true; this._wc(0x8, websocketClosePayload(1003, 'bin only')); } this.raw.end(); return; }
                if (op === 0x2) {
                    if (this.fop) return this._pe('new data');
                    if (fin) this._eb(payload);
                    else { this.fop = op; this.fp = [payload]; this.fb = payload.length; }
                    continue;
                }
                if (op === 0x0) {
                    if (!this.fop) return this._pe('bad cont');
                    this.fb += payload.length;
                    if (this.fb > this.max) return this._mtl();
                    this.fp.push(payload);
                    if (fin) {
                        const j = Buffer.concat(this.fp, this.fb);
                        this.fop = 0; this.fp = []; this.fb = 0;
                        this._eb(j);
                    }
                    continue;
                }
                return this._pe(`op ${op}`);
            }
        } catch (e) { if (this.listenerCount('error')) this.emit('error', e); this.destroy(e); }
    }
    _eb(p) { if (p.length) this.emit('data', p); }
    _e() { if (this.ended) return; this.ended = true; this.emit('end'); }
    destroy(err) {
        if (this.destroyed) return;
        this.destroyed = true; this.writable = false; this._ct();
        if (err && this.listenerCount('error')) this.emit('error', err);
        this.raw.destroy(); this._e(); this.emit('close');
    }
}

function rejectUpgrade(r, s, m) {
    if (r.destroyed) return;
    const b = Buffer.from(String(m || 'rejected'), 'utf8');
    r.end(`HTTP/1.1 ${s}\r\nConnection: close\r\nContent-Type: text/plain\r\nContent-Length: ${b.length}\r\n\r\n${b}`);
}

function acceptWebSocketUpgrade(req, raw, head, cfg) {
    const u = String(req.headers.upgrade || '').toLowerCase();
    const c = String(req.headers.connection || '').toLowerCase();
    const k = String(req.headers['sec-websocket-key'] || '');
    const v = String(req.headers['sec-websocket-version'] || '');
    if (u !== 'websocket' || !c.split(',').some(x => x.trim() === 'upgrade') || v !== '13') { rejectUpgrade(raw, '400', 'bad upgrade'); return null; }
    let kb;
    try { kb = Buffer.from(k, 'base64'); } catch { kb = Buffer.alloc(0); }
    if (kb.length !== 16) { rejectUpgrade(raw, '400', 'bad key'); return null; }
    if (cfg.wsPath) {
        let pn = '/';
        try { pn = new URL(req.url || '/', 'http://x.invalid').pathname; } catch { }
        if (pn !== cfg.wsPath) { rejectUpgrade(raw, '404', 'not found'); return null; }
    }
    const a = websocketAccept(k);
    raw.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n' + `Sec-WebSocket-Accept: ${a}\r\n\r\n`);
    return { socket: new WebSocketRelaySocket(raw, cfg.maxWsMessageBytes), head };
}

async function handleConnection(socket, cfg, xm) {
    const r = new AsyncByteReader(socket);
    let est = false;
    socket.setNoDelay(true);
    socket.setTimeout(cfg.handshakeTimeout, () => socket.destroy(new Error('handshake timeout')));
    try {
        const ctrl = await readControl(r);
        est = true;
        addLog(`[OK] ${socket.remoteAddress || 'Worker'} mode 0x${ctrl.mode.toString(16)}`);
        socket.setTimeout(cfg.idleTimeout > 0 ? cfg.idleTimeout : 0, () => socket.destroy(new Error('idle')));
        if (ctrl.mode === RELAY_MODE_FIXED_UDP) {
            if (rejectUdpTarget(ctrl.target)) { await writeControlError(socket, '443 rejected'); return; }
            const a = await UDPAssociation.create();
            let closed = false;
            a.attach({ mux: { sendUDPData: async (_i, _r, d) => {
                if (closed || socket.destroyed) return;
                if (d.length > MAX_PACKET_LEN) return;
                const f = Buffer.allocUnsafe(2 + d.length);
                f.writeUInt16BE(d.length, 0); d.copy(f, 2);
                await writeSocket(socket, f);
            } }, id: 0 });
            try {
                await writeSocket(socket, Buffer.from([0]));
                for (;;) {
                    const p = await readLengthPayload(r);
                    if (!p.length || rejectUdpTarget(ctrl.target)) continue;
                    await a.send(ctrl.target, p);
                }
            } finally { closed = true; a.close(); }
        } else if (ctrl.mode === RELAY_MODE_PACKET_UDP) {
            const a = await UDPAssociation.create();
            let closed = false;
            const wc = { v: Promise.resolve() };
            a.attach({ mux: { sendUDPData: (_i, ri, d) => {
                if (closed || socket.destroyed || d.length > MAX_PACKET_LEN) return Promise.resolve();
                const e = encodeUDPSource(ri);
                const l = Buffer.allocUnsafe(2); l.writeUInt16BE(d.length, 0);
                const f = Buffer.concat([e, l, d]);
                const o = wc.v.then(() => writeSocket(socket, f));
                wc.v = o.catch(() => { });
                return o;
            } }, id: 0 });
            try {
                await writeSocket(socket, Buffer.from([0]));
                for (;;) {
                    const t = await readEndpoint(r);
                    const p = await readLengthPayload(r);
                    if (!p.length || rejectUdpTarget(t)) continue;
                    await a.send(t, p);
                }
            } finally { closed = true; a.close(); }
        } else {
            const m = new MuxConnection(socket, r, cfg, xm);
            await m.serve();
        }
    } catch (err) {
        if (!est && !socket.destroyed) { addLog(`malformed handshake`); await writeControlError(socket, 'malformed'); }
        else if (!isNormalClose(err)) { addLog(`${err.message || err}`); }
    } finally { socket.destroy(); }
}

function isNormalClose(e) {
    if (!e) return true;
    const c = e.code || '';
    if (['EOF','ECONNRESET','EPIPE','ERR_STREAM_PREMATURE_CLOSE'].includes(c)) return true;
    const m = String(e.message || e).toLowerCase();
    return m.includes('unexpected eof') || m.includes('closed') || m.includes('idle');
}

function createRelayUpgradeHandler() {
    const xm = new XUDPManager(RELAY_CFG.XUDP_GRACE_MS);
    let active = 0;
    return {
        handleUpgrade(req, raw, head) {
            STATS.totalHandshakes++;
            if (active >= RELAY_CFG.MAX_CONNECTIONS) { rejectUpgrade(raw, '503', 'busy'); return; }
            const acc = acceptWebSocketUpgrade(req, raw, head, { wsPath: RELAY_CFG.WS_PATH, maxWsMessageBytes: RELAY_CFG.MAX_WS_MESSAGE_BYTES });
            if (!acc) return;
            const { socket, head: initial } = acc;
            active++; STATS.activeClients = active;
            let counted = true;
            socket.once('close', () => {
                if (counted) { counted = false; active--; STATS.activeClients = active; addLog(`disconnect active=${active}`); }
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
