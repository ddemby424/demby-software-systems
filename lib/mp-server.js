'use strict';
/*
 * DMB Tech 1 multiplayer relay (server).
 * ----------------------------------------------------------------
 * A tiny room-based WebSocket server. It does NOT simulate the game.
 * It just assigns each player an id, drops them in a room, and relays
 * their position / shots / hits to everyone else in that room.
 *
 * It also keeps a LOBBY: a live list of public rooms (name, level,
 * host, player count, names). Clients can subscribe with {type:"lobby"}
 * without joining a room; the server pushes {type:"rooms"} whenever
 * the list changes, throttled to about once a second.
 *
 * Think of it like a VB Winsock server that keeps a list of connected
 * sockets, grouped by a room code (the "level" you're playing).
 *
 * Two ways to run it:
 *   1. Standalone:  node server.js     (see ../server.js)
 *   2. Mounted on an existing Express HTTP server:
 *
 *        const http = require('http');
 *        const { attach } = require('./lib/mp-server');
 *        const server = http.createServer(app);
 *        attach(server, { path: '/ws' });
 *        server.listen(process.env.PORT || 3000);
 *
 * Open-source pieces used here:
 *   - `ws` library (MIT): https://github.com/websockets/ws
 *   - Heartbeat / dead-client pattern adapted from the ws README,
 *     "How to detect and close broken connections"
 */
const { WebSocketServer, WebSocket } = require('ws');

const DEFAULTS = {
  path: '/ws',             // URL path, same port as HTTP
  pingInterval: 5000,      // protocol ping every 5s (find dead sockets)
  pingTimeout: 10000,      // no pong after this → drop the client
  appPingInterval: 20000,  // JSON {type:"ping"} so proxies see real data
  joinTimeout: 10000,      // connect, then must send join OR lobby, or we hang up
  lobbyInterval: 1000,     // how often at most we push a new room list
  maxPayload: 2048,        // 2 KB is plenty for these tiny incoming JSON messages
  maxRate: 60,             // pose (~15 Hz) + auto-fire shots + pings; 20 was too tight
  maxPlayers: 8,           // default cap per room
  maxName: 24,
  maxRoom: 32
};

/**
 * Hook a WebSocket server onto an existing http.Server (Express or
 * a plain Node server). Returns the WebSocketServer so the caller can
 * close it later. Options override DEFAULTS (tests pass short timeouts).
 */
function attach(httpServer, options) {
  const opt = Object.assign({}, DEFAULTS, options || {});
  const wss = new WebSocketServer({
    server: httpServer,
    path: opt.path,
    maxPayload: opt.maxPayload
  });

  // rooms: roomCode -> { id, name, levelId, levelName, hostId, created,
  //                      private, maxPlayers, players: Map(id -> client) }
  const rooms = new Map();
  const lobbyWatchers = new Set();   // clients who asked for the room list
  let nextId = 1;
  let nextRoomSeq = 1;
  let lobbyDirty = false;
  let lastLobbyPush = 0;
  let lobbyTimer = null;

  function send(ws, obj) {
    if (ws.readyState === WebSocket.OPEN) {
      try { ws.send(JSON.stringify(obj)); } catch (e) { /* already closing */ }
    }
  }

  function broadcast(roomName, obj, exceptId) {
    const rec = rooms.get(roomName);
    if (!rec) return;
    const raw = JSON.stringify(obj);
    for (const [id, c] of rec.players) {
      if (id === exceptId) continue;
      if (c.ws.readyState === WebSocket.OPEN) {
        try { c.ws.send(raw); } catch (e) { /* ignore */ }
      }
    }
  }

  // The public snapshot we share about a player (never the socket itself).
  function publicPeer(c) {
    return {
      id: c.id, name: c.name, modelId: c.modelId,
      x: c.x, y: c.y, z: c.z, yaw: c.yaw, anim: c.anim
    };
  }

  function newRoomId() {
    let id;
    do { id = 'r' + (nextRoomSeq++).toString(36); } while (rooms.has(id));
    return id;
  }

  // What the lobby sees. Private rooms are left out on purpose — you can
  // still join one if you know the code, you just won't find it in the list.
  function publicRoomList() {
    const list = [];
    for (const rec of rooms.values()) {
      if (rec.private) continue;
      const names = [];
      let hostName = '';
      for (const c of rec.players.values()) {
        names.push(c.name);
        if (c.id === rec.hostId) hostName = c.name;
      }
      if (!hostName && names.length) hostName = names[0];
      list.push({
        id: rec.id,
        name: rec.name,
        level: rec.levelId,
        levelName: rec.levelName,
        host: hostName,
        players: rec.players.size,
        max: rec.maxPlayers,
        names: names,
        created: rec.created
      });
    }
    list.sort(function (a, b) { return a.created - b.created; });
    return list;
  }

  // Mark the lobby stale. We send at most once per lobbyInterval (default
  // 1s) so a pile of joins doesn't flood every watcher. A forced push
  // (someone just subscribed or hit Refresh) goes out immediately.
  function markLobbyDirty() {
    lobbyDirty = true;
    flushLobby(false);
  }

  function flushLobby(force) {
    const now = Date.now();
    const interval = opt.lobbyInterval;
    if (!lobbyDirty && !force) return;
    if (!force && now - lastLobbyPush < interval) {
      if (!lobbyTimer) {
        lobbyTimer = setTimeout(function () {
          lobbyTimer = null;
          flushLobby(false);
        }, interval - (now - lastLobbyPush));
      }
      return;
    }
    lobbyDirty = false;
    lastLobbyPush = now;
    if (lobbyTimer) { clearTimeout(lobbyTimer); lobbyTimer = null; }
    const roomsList = publicRoomList();
    for (const c of lobbyWatchers) {
      send(c.ws, { type: 'rooms', id: c.id, rooms: roomsList });
    }
  }

  function leave(client, reason) {
    if (!client || !client.room) return;
    const rec = rooms.get(client.room);
    const roomName = client.room;
    if (rec) {
      rec.players.delete(client.id);
      broadcast(roomName, { type: 'leave', id: client.id, reason: reason || 'disconnect' }, client.id);
      if (rec.players.size === 0) {
        // Empty rooms vanish so the lobby stays tidy.
        rooms.delete(roomName);
      } else if (rec.hostId === client.id) {
        // Host left: the next person in the room takes over.
        const next = rec.players.values().next().value;
        rec.hostId = next.id;
      }
      markLobbyDirty();
    }
    client.room = null;
  }

  function strike(client, code, message) {
    client.strikes += 1;
    send(client.ws, { type: 'error', code: code, message: message });
    if (client.strikes >= 5) {
      try { client.ws.close(1008, message); } catch (e) { /* ignore */ }
    }
  }

  function applyProfile(client, msg) {
    const playerName = msg.playerName || msg.name;
    if (typeof playerName === 'string' && playerName.length) {
      client.name = sanitizeName(playerName, opt.maxName);
    }
    if (typeof msg.modelId === 'string') client.modelId = sanitizeToken(msg.modelId, client.modelId);
    if (Number.isFinite(Number(msg.x))) client.x = clamp(Number(msg.x), -512, 512);
    if (Number.isFinite(Number(msg.y))) client.y = clamp(Number(msg.y), -512, 512);
    if (Number.isFinite(Number(msg.z))) client.z = clamp(Number(msg.z), -64, 64);
    if (Number.isFinite(Number(msg.yaw))) client.yaw = Number(msg.yaw);
    if (typeof msg.anim === 'string') client.anim = sanitizeToken(msg.anim, client.anim);
  }

  // Shared by join and create: put the player in the room, welcome them,
  // tell the others, and refresh the lobby list.
  function enterRoom(client, rec, msg) {
    if (rec.players.size >= rec.maxPlayers && !rec.players.has(client.id)) {
      send(client.ws, { type: 'error', code: 'room_full', message: 'Room is full' });
      return;
    }
    if (client.room && client.room !== rec.id) leave(client, 'switch');
    applyProfile(client, msg);
    const already = rec.players.has(client.id);
    client.room = rec.id;
    rec.players.set(client.id, client);
    if (!rec.hostId) rec.hostId = client.id;

    const peers = [];
    for (const [id, c] of rec.players) if (id !== client.id) peers.push(publicPeer(c));
    send(client.ws, {
      type: 'welcome', id: client.id, room: rec.id,
      roomName: rec.name, level: rec.levelId,
      peers: peers, serverTime: Date.now()
    });
    if (!already) broadcast(rec.id, Object.assign({ type: 'join' }, publicPeer(client)), client.id);
    markLobbyDirty();
  }

  wss.on('connection', function (ws) {
    const client = {
      ws: ws,
      id: 'p' + (nextId++),   // server-assigned; clients never pick their own
      room: null,
      name: 'Player',
      modelId: 'default',
      x: 0, y: 0, z: 0, yaw: 0, anim: 'idle',
      msgTimes: [],
      strikes: 0,
      watchingLobby: false
    };
    ws.isAlive = true;
    ws.client = client;

    // Protocol-level pong (automatic; this is the ws library's ping/pong).
    ws.on('pong', function () { ws.isAlive = true; });

    // If they connect and never join a room AND never subscribe to the
    // lobby, hang up so we don't leak sockets. A lobby watcher is allowed
    // to sit in the menu as long as they like.
    const joinTimer = setTimeout(function () {
      if (!client.room && !client.watchingLobby) {
        try { ws.close(1008, 'no join'); } catch (e) { /* ignore */ }
      }
    }, opt.joinTimeout);
    client._joinTimer = joinTimer;

    ws.on('message', function (buf, isBinary) {
      if (isBinary) { strike(client, 'bad_json', 'Binary messages are not allowed'); return; }
      const now = Date.now();
      // Sliding 1-second window rate limit.
      client.msgTimes = client.msgTimes.filter(function (t) { return now - t < 1000; });
      if (client.msgTimes.length >= opt.maxRate) {
        send(ws, { type: 'error', code: 'rate', message: 'Too many messages' });
        return;
      }
      client.msgTimes.push(now);

      let msg;
      try { msg = JSON.parse(String(buf)); }
      catch (e) { strike(client, 'bad_json', 'Message was not valid JSON'); return; }
      if (!msg || typeof msg !== 'object' || typeof msg.type !== 'string') {
        strike(client, 'bad_json', 'Message needs a type field');
        return;
      }
      handle(client, msg);
    });

    // Winsock_Close: take them out of their room and the lobby watcher list.
    ws.on('close', function () {
      clearTimeout(client._joinTimer);
      lobbyWatchers.delete(client);
      leave(client, 'disconnect');
    });
    ws.on('error', function () { try { ws.terminate(); } catch (e) { /* ignore */ } });
  });

  function handle(client, msg) {
    switch (msg.type) {
      case 'lobby': return onLobby(client);
      case 'create': return onCreate(client, msg);
      case 'join':  return onJoin(client, msg);
      case 'leave':
      case 'part':  return onPart(client);
      case 'state': return onState(client, msg);
      case 'shot':  return onShot(client, msg);
      case 'hit':   return onHit(client, msg);
      case 'pong':  return; // app-level pong; nothing to do
      case 'ping':  return send(client.ws, { type: 'pong', t: msg.t, serverTime: Date.now() });
      default:      return; // ignore unknown types (forward compatible)
    }
  }

  // Subscribe to the lobby. Immediate list, then live updates.
  // This is the "I'm just browsing" message — no room is joined.
  function onLobby(client) {
    client.watchingLobby = true;
    clearTimeout(client._joinTimer);
    lobbyWatchers.add(client);
    send(client.ws, { type: 'hello', id: client.id });
    send(client.ws, { type: 'rooms', id: client.id, rooms: publicRoomList() });
  }

  function onCreate(client, msg) {
    const display = sanitizeDisplayName(msg.name, 'Room', 32);
    const levelId = sanitizeToken(msg.level, 'e1m1');
    const levelName = sanitizeDisplayName(msg.levelName, levelId, 32);
    let max = Number(msg.max);
    if (!Number.isFinite(max)) max = opt.maxPlayers;
    max = clamp(Math.floor(max), 2, 16);
    const rec = {
      id: newRoomId(),
      name: display,
      levelId: levelId,
      levelName: levelName,
      hostId: client.id,
      created: Date.now(),
      private: !!msg.private,
      maxPlayers: max,
      players: new Map()
    };
    rooms.set(rec.id, rec);
    // The creator is the host and is put in the room right away.
    enterRoom(client, rec, msg);
  }

  function onJoin(client, msg) {
    const roomId = sanitizeRoom(msg.room, opt.maxRoom);
    if (!roomId) {
      send(client.ws, { type: 'error', code: 'bad_room', message: 'Invalid room code' });
      return;
    }
    let rec = rooms.get(roomId);
    if (!rec) {
      // Joining a code that doesn't exist yet starts a public room
      // (this is how the older demo and tests work).
      rec = {
        id: roomId,
        name: sanitizeDisplayName(msg.roomName, roomId, 32),
        levelId: sanitizeToken(msg.level, roomId),
        levelName: sanitizeDisplayName(msg.levelName, msg.level || roomId, 32),
        hostId: client.id,
        created: Date.now(),
        private: false,
        maxPlayers: opt.maxPlayers,
        players: new Map()
      };
      rooms.set(roomId, rec);
    }
    clearTimeout(client._joinTimer);
    enterRoom(client, rec, msg);
  }

  // Leave the room but stay connected (back to the lobby menu).
  function onPart(client) {
    leave(client, 'part');
    send(client.ws, { type: 'parted' });
  }

  function onState(client, msg) {
    if (!client.room) return;
    client.x = clampNum(msg.x, -512, 512, client.x);
    client.y = clampNum(msg.y, -512, 512, client.y);
    client.z = clampNum(msg.z, -64, 64, client.z);
    client.yaw = Number.isFinite(Number(msg.yaw)) ? Number(msg.yaw) : client.yaw;
    if (typeof msg.anim === 'string') client.anim = sanitizeToken(msg.anim, client.anim);
    if (typeof msg.modelId === 'string') client.modelId = sanitizeToken(msg.modelId, client.modelId);
    broadcast(client.room, {
      type: 'state', id: client.id,
      x: client.x, y: client.y, z: client.z, yaw: client.yaw,
      anim: client.anim, modelId: client.modelId
    }, client.id);
  }

  function onShot(client, msg) {
    if (!client.room) return;
    let weapon;
    if (typeof msg.weapon === 'number' && Number.isFinite(msg.weapon)) weapon = msg.weapon;
    else if (typeof msg.weapon === 'string') weapon = msg.weapon.slice(0, 24);
    broadcast(client.room, {
      type: 'shot', id: client.id,
      x: clampNum(msg.x, -512, 512, client.x),
      y: clampNum(msg.y, -512, 512, client.y),
      z: clampNum(msg.z, -64, 64, client.z),
      yaw: Number.isFinite(Number(msg.yaw)) ? Number(msg.yaw) : client.yaw,
      weapon: weapon
    }, client.id);
  }

  function onHit(client, msg) {
    if (!client.room) return;
    const targetId = typeof msg.targetId === 'string' ? msg.targetId.slice(0, 16) : '';
    if (!targetId) return;
    broadcast(client.room, {
      type: 'hit', id: client.id, targetId: targetId, dmg: clampNum(msg.dmg, 0, 200, 0)
    }, client.id);
  }

  // Heartbeat: protocol ping (detect dead sockets) + occasional JSON ping
  // (keeps proxies like Cloudflare from closing an "idle" connection).
  const timer = setInterval(function () {
    const now = Date.now();
    wss.clients.forEach(function (ws) {
      if (ws.isAlive === false) {
        leave(ws.client, 'timeout');
        lobbyWatchers.delete(ws.client);
        try { ws.terminate(); } catch (e) { /* ignore */ }
        return;
      }
      ws.isAlive = false;
      try { ws.ping(); } catch (e) { /* ignore */ }
      if (!ws._lastAppPing || now - ws._lastAppPing >= opt.appPingInterval) {
        ws._lastAppPing = now;
        send(ws, { type: 'ping', t: now });
      }
    });
  }, opt.pingInterval);

  function stopTimers() {
    clearInterval(timer);
    if (lobbyTimer) { clearTimeout(lobbyTimer); lobbyTimer = null; }
  }

  wss.on('close', stopTimers);
  // wss.close() does NOT terminate existing sockets when we attached to an
  // external HTTP server — it just waits for them to leave, forever. Tests
  // and a later SIGTERM shutdown need a real hang-up.
  wss.flushClose = function (cb) {
    stopTimers();
    wss.clients.forEach(function (socket) {
      try { socket.close(1012, 'server restart'); } catch (e) {
        try { socket.terminate(); } catch (e2) { /* ignore */ }
      }
    });
    wss.close(cb);
  };
  wss._rooms = rooms;
  wss._lobbyWatchers = lobbyWatchers;
  wss._opt = opt;
  wss._flushLobby = flushLobby;
  return wss;
}

/* ---------- small sanitizers (never trust the client) ---------- */
function clamp(v, min, max) { return v < min ? min : (v > max ? max : v); }
function clampNum(v, min, max, fallback) {
  v = Number(v);
  return Number.isFinite(v) ? clamp(v, min, max) : fallback;
}
function sanitizeName(s, max) {
  s = String(s == null ? '' : s).replace(/[\x00-\x1f\x7f]/g, '').trim();
  if (!s) s = 'Player';
  return s.slice(0, max || 24);
}
function sanitizeDisplayName(s, fallback, max) {
  // Room and level titles may have spaces ("Night Shift"), unlike room codes.
  s = String(s == null ? '' : s).replace(/[\x00-\x1f\x7f]/g, '').replace(/[<>]/g, '').trim();
  if (!s) s = fallback || 'Room';
  return s.slice(0, max || 32);
}
function sanitizeRoom(s, max) {
  s = String(s == null ? '' : s).toLowerCase().replace(/[^a-z0-9_-]/g, '');
  if (!s || s.length > (max || 32)) return null;
  return s;
}
function sanitizeToken(s, fallback) {
  s = String(s == null ? '' : s).replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 32);
  return s || fallback || 'default';
}

module.exports = { attach, DEFAULTS, clampNum, sanitizeName, sanitizeRoom, sanitizeDisplayName };
