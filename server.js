// سرور رله‌ی مالتی‌پلیر برای بازی تیراندازی سه‌بعدی
// هر بازیکن موقعیت و وضعیتش رو می‌فرسته، سرور برای بقیه پخشش می‌کنه.
// همچنین پیام‌های ping/pong رو برای محاسبه‌ی پینگ هندل می‌کنه.

const WebSocket = require('ws');
const { URL } = require('url');

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

const players = new Map(); // id -> { ws, name, x, y, z, yaw, hp, score, room }

function randomId() {
  return Math.random().toString(36).slice(2, 10) + Date.now().toString(36).slice(-4);
}

function safeSend(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(data)); } catch (e) {}
  }
}

// فقط برای بازیکن‌های همون سرور (room) پخش می‌کنه
function broadcast(data, exceptId, room) {
  for (const [id, p] of players) {
    if (id === exceptId) continue;
    if (p.room !== room) continue;
    safeSend(p.ws, data);
  }
}

function parseRoom(req) {
  try {
    const url = new URL(req.url, 'ws://x');
    const r = url.searchParams.get('room');
    const n = parseInt(r, 10);
    return (n === 1 || n === 2) ? n : 1;
  } catch (e) {
    return 1;
  }
}

wss.on('connection', (ws, req) => {
  const id = randomId();
  let joined = false;
  const connRoom = parseRoom(req);

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
      // اگه توی پیام join هم room اومده باشه، همون رو معتبر بشمار؛ وگرنه room همون‌طور که موقع اتصال از URL خونده شده استفاده می‌شه
      const msgRoom = parseInt(msg.room, 10);
      const room = (msgRoom === 1 || msgRoom === 2) ? msgRoom : connRoom;

      players.set(id, { ws, name, x: 0, y: 0, z: 0, yaw: 0, hp: 5, score: 0, room });
      joined = true;

      safeSend(ws, { type: 'welcome', id, room });

      const existing = [];
      for (const [pid, p] of players) {
        if (pid === id) continue;
        if (p.room !== room) continue;
        existing.push({ id: pid, name: p.name, x: p.x, y: p.y, z: p.z, yaw: p.yaw, hp: p.hp, score: p.score });
      }
      safeSend(ws, { type: 'roster', players: existing });

      broadcast({ type: 'join', id, name }, id, room);
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
      broadcast({ type: 'move', id, x: p.x, y: p.y, z: p.z, yaw: p.yaw, hp: p.hp, score: p.score }, id, p.room);
      return;
    }

    if (msg.type === 'hit') {
      const target = players.get(msg.targetId);
      if (target && target.room === p.room) {
        safeSend(target.ws, { type: 'hit', from: id, fromName: p.name, dmg: msg.dmg || 1 });
      }
      return;
    }

    // هر نوع پیام دیگه (مثلاً team_wave, team_boss, team_enemy_hit, team_enemy_state,
    // team_boss_state, team_end, lobby_team, lobby_sync_request, lobby_timer, lobby_start)
    // رو دست‌نخورده برای بقیه‌ی بازیکن‌های همون روم پخش کن. بدون این، این پیام‌ها بی‌صدا
    // نادیده گرفته می‌شدن و حالت هم‌تیمی/لابی اصلاً کار نمی‌کرد.
    broadcast(Object.assign({}, msg, { from: id }), id, p.room);
  });

  ws.on('close', () => {
    if (joined) {
      const p = players.get(id);
      const room = p ? p.room : connRoom;
      players.delete(id);
      broadcast({ type: 'leave', id }, id, room);
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
