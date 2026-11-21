'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT) || 8080;
const INDEX_PATH = path.join(__dirname, 'index.html');

// HTTP server (serves index.html if hosted together + handles health checks)
const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size }));
    return;
  }

  fs.readFile(INDEX_PATH, (err, data) => {
    if (err) {
      res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('Namasha Battle WebSocket Relay Server is running.');
      return;
    }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(data);
  });
});

// WebSocket Relay Server attached to the same HTTP port
const wss = new WebSocketServer({ server });
const rooms = new Map(); // code -> { host: ws, guest: ws | null, createdAt: number }

const ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
function generateCode(len = 5) {
  let code = '';
  do {
    code = '';
    for (let i = 0; i < len; i++) {
      code += ALPHABET[(Math.random() * ALPHABET.length) | 0];
    }
  } while (rooms.has(code));
  return code;
}

function send(ws, obj) {
  if (ws && ws.readyState === 1) {
    ws.send(JSON.stringify(obj));
  }
}

function cleanupSocket(ws, notifyPeer = true) {
  const code = ws.roomCode;
  if (!code) return;
  ws.roomCode = null;

  const room = rooms.get(code);
  if (!room) return;

  const peer = room.host === ws ? room.guest : room.host;
  rooms.delete(code);

  if (peer) {
    peer.roomCode = null;
    if (notifyPeer) send(peer, { type: 'peer_left' });
  }
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.roomCode = null;

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (raw) => {
    if (raw.length > 16384) return; // Guard against oversized payloads
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    switch (msg.type) {
      case 'create': {
        cleanupSocket(ws, true);
        const code = generateCode(5);
        rooms.set(code, { host: ws, guest: null, createdAt: Date.now() });
        ws.roomCode = code;
        send(ws, { type: 'created', code, role: 0 });
        break;
      }

      case 'join': {
        const code = String(msg.code || '').trim().toUpperCase();
        const room = rooms.get(code);
        if (!room) {
          send(ws, { type: 'error', reason: 'not_found' });
          return;
        }
        if (room.guest || room.host === ws) {
          send(ws, { type: 'error', reason: 'full' });
          return;
        }
        cleanupSocket(ws, true);
        room.guest = ws;
        ws.roomCode = code;
        send(ws, { type: 'joined', code, role: 1 });
        send(room.host, { type: 'peer_joined', code, role: 0 });
        break;
      }

      case 'relay': {
        const room = rooms.get(ws.roomCode);
        if (!room) return;
        const peer = room.host === ws ? room.guest : room.host;
        if (peer) send(peer, { type: 'relay', payload: msg.payload });
        break;
      }

      case 'leave': {
        cleanupSocket(ws, true);
        break;
      }
    }
  });

  ws.on('close', () => cleanupSocket(ws, true));
  ws.on('error', () => cleanupSocket(ws, true));
});

// Keep-alive ping every 25s (prevents proxy/mobile idle drops)
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      cleanupSocket(ws, true);
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 25000);

wss.on('close', () => clearInterval(heartbeat));

server.listen(PORT, () => {
  console.log(`Namasha Battle server listening on port ${PORT}`);
});
