// سرور رله‌ی مالتی‌پلیر برای بازی تیراندازی سه‌بعدی
// هر بازیکن موقعیت و وضعیتش رو می‌فرسته، سرور برای بقیه پخشش می‌کنه.
// همچنین پیام‌های ping/pong رو برای محاسبه‌ی پینگ هندل می‌کنه.

const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

const players = new Map(); // id -> { ws, name, x, y, z, yaw, hp, score }

function randomId() {
  return Math.random().toString(36).slice(2, 10) + Date.now().toString(36).slice(-4);
}

function safeSend(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(data)); } catch (e) {}
  }
}

function broadcast(data, exceptId) {
  for (const [id, p] of players) {
    if (id === exceptId) continue;
    safeSend(p.ws, data);
  }
}

wss.on('connection', (ws) => {
  const id = randomId();
  let joined = false;

  // پینگ سطح TCP برای تشخیص اتصال‌های مرده
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }
    if (!msg || typeof msg.type !== 'string') return;

    if (msg.type === 'ping') {
      safeSend(ws, { type: 'pong', t: msg.t });
      return;
    }

    if (msg.type === 'join') {
      const name = (msg.name || 'بازیکن').toString().slice(0, 14);
      players.set(id, { ws, name, x: 0, y: 0, z: 0, yaw: 0, hp: 5, score: 0 });
      joined = true;

      safeSend(ws, { type: 'welcome', id });

      const existing = [];
      for (const [pid, p] of players) {
        if (pid === id) continue;
        existing.push({ id: pid, name: p.name, x: p.x, y: p.y, z: p.z, yaw: p.yaw, hp: p.hp, score: p.score });
      }
      safeSend(ws, { type: 'roster', players: existing });

      broadcast({ type: 'join', id, name }, id);
      return;
    }

    if (!joined) return;
    const p = players.get(id);
    if (!p) return;

    if (msg.type === 'move') {
      p.x = Number(msg.x) || 0;
      p.y = Number(msg.y) || 0;
      p.z = Number(msg.z) || 0;
      p.yaw = Number(msg.yaw) || 0;
      p.hp = Number(msg.hp);
      p.score = Number(msg.score) || 0;
      broadcast({ type: 'move', id, x: p.x, y: p.y, z: p.z, yaw: p.yaw, hp: p.hp, score: p.score }, id);
      return;
    }

    if (msg.type === 'hit') {
      const target = players.get(msg.targetId);
      if (target) {
        safeSend(target.ws, { type: 'hit', from: id, fromName: p.name, dmg: msg.dmg || 1 });
      }
      return;
    }
  });

  ws.on('close', () => {
    if (joined) {
      players.delete(id);
      broadcast({ type: 'leave', id }, id);
    }
  });
});

// حذف اتصال‌های مرده هر 30 ثانیه
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

console.log('سرور رله‌ی مالتی‌پلیر روی پورت ' + PORT + ' در حال اجراست');
