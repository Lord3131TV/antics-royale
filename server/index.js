/**
 * Antics Royale 3D — Realtime Multiplayer Server
 * Socket.IO room/state relay matching the game's SDK-like API.
 */
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const cors = require("cors");
const crypto = require("crypto");

const PORT = process.env.PORT || 3001;
const MAX_PLAYERS = 8;
const CODE_LEN = 5;
const CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; // no I/O/0/1
const IDLE_ROOM_MS = 45 * 60 * 1000;
/** Empty rooms kept this long so create→leave→redirect→join (and friends joining the code) still works */
const EMPTY_ROOM_MS = 3 * 60 * 1000;
const HOST_TICK_MS = 33;

const path = require("path");
const app = express();
app.use(cors({ origin: true }));
app.use(express.json());

// Optional: serve frontend from ../public when SERVE_STATIC=1 (local all-in-one)
if (process.env.SERVE_STATIC === "1") {
  const pub = path.join(__dirname, "..", "public");
  app.use(express.static(pub));
  console.log("Serving static from", pub);
}

app.get("/api", (_req, res) => {
  res.json({
    ok: true,
    service: "antics-royale-mp",
    version: "1.0.0",
    rooms: rooms.size,
  });
});

app.get("/health", (_req, res) => res.json({ ok: true }));

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: true, methods: ["GET", "POST"] },
  transports: ["websocket", "polling"],
  pingInterval: 10000,
  pingTimeout: 20000,
});

/** @type {Map<string, Room>} */
const rooms = new Map();
/** socketId -> { roomCode, playerId } */
const socketIndex = new Map();

function genCode() {
  let code;
  do {
    let s = "";
    const bytes = crypto.randomBytes(CODE_LEN);
    for (let i = 0; i < CODE_LEN; i++) s += CODE_ALPHABET[bytes[i] % CODE_ALPHABET.length];
    code = s;
  } while (rooms.has(code));
  return code;
}

function genId() {
  return crypto.randomBytes(8).toString("hex");
}

function now() {
  return Date.now();
}

class Player {
  constructor(id, name, socketId) {
    this.id = id;
    this.name = name || "Oyuncu";
    this.socketId = socketId;
    this.state = {};
    this.joinedAt = now();
  }
  public() {
    return {
      id: this.id,
      name: this.name,
      state: this.state,
    };
  }
}

class Room {
  constructor(code, creatorSocketId, creatorName) {
    this.code = code;
    this.createdAt = now();
    this.lastActivity = now();
    /** Timestamp when room became empty (null while occupied) */
    this.emptyAt = null;
    /** @type {Map<string, Player>} */
    this.players = new Map();
    this.state = {
      phase: "lobby",
      round: 0,
      roster: [],
      elim: [],
      scores: {},
      light: "green",
      lightAt: now(),
      creatorId: null,
    };
    this.hostId = null;
    this._tickTimer = null;
  }

  touch() {
    this.lastActivity = now();
    if (this.players.size > 0) this.emptyAt = null;
  }

  addPlayer(player) {
    this.players.set(player.id, player);
    this.emptyAt = null;
    if (!this.hostId || !this.players.has(this.hostId)) {
      this.hostId = player.id;
    }
    // Creator may have left during create→redirect; reclaim if missing
    if (!this.state.creatorId || !this.players.has(this.state.creatorId)) {
      this.state.creatorId = player.id;
    }
    this.touch();
    this.ensureHostTick();
  }

  removePlayer(playerId) {
    const wasHost = this.hostId === playerId;
    this.players.delete(playerId);
    this.touch();

    if (this.players.size === 0) {
      this.hostId = null;
      this.emptyAt = now();
      this.stopHostTick();
      return { empty: true, wasHost };
    }

    if (wasHost || !this.players.has(this.hostId)) {
      // Promote oldest remaining player
      let next = null;
      for (const p of this.players.values()) {
        if (!next || p.joinedAt < next.joinedAt) next = p;
      }
      this.hostId = next ? next.id : null;
    }

    // Keep creatorId stable if still present; otherwise assign to new host
    if (this.state.creatorId && !this.players.has(this.state.creatorId)) {
      this.state.creatorId = this.hostId;
    }

    this.ensureHostTick();
    return { empty: false, wasHost, newHostId: this.hostId };
  }

  listPlayers() {
    return [...this.players.values()].map((p) => p.public());
  }

  hostPlayer() {
    return this.hostId ? this.players.get(this.hostId) || null : null;
  }

  ensureHostTick() {
    if (this._tickTimer) return;
    let last = performance.now();
    this._tickTimer = setInterval(() => {
      if (this.players.size === 0) {
        this.stopHostTick();
        return;
      }
      const t = performance.now();
      const dt = t - last;
      last = t;
      const host = this.hostPlayer();
      if (!host) return;
      const sock = io.sockets.sockets.get(host.socketId);
      if (sock) sock.emit("host_tick", { dt, serverNow: now() });
    }, HOST_TICK_MS);
  }

  stopHostTick() {
    if (this._tickTimer) {
      clearInterval(this._tickTimer);
      this._tickTimer = null;
    }
  }

  snapshot(forPlayerId) {
    const host = this.hostPlayer();
    return {
      code: this.code,
      state: this.state,
      players: this.listPlayers(),
      hostId: this.hostId,
      isHost: forPlayerId === this.hostId,
      serverNow: now(),
    };
  }
}

function normalizeCode(raw) {
  return String(raw || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "")
    .slice(0, 8);
}

function getRoom(code) {
  const c = normalizeCode(code);
  if (!c) return null;
  return rooms.get(c) || null;
}

function emitRoom(room, event, payload, exceptSocketId) {
  for (const p of room.players.values()) {
    if (exceptSocketId && p.socketId === exceptSocketId) continue;
    const sock = io.sockets.sockets.get(p.socketId);
    if (sock) sock.emit(event, payload);
  }
}

function broadcastPlayers(room) {
  emitRoom(room, "players", {
    players: room.listPlayers(),
    hostId: room.hostId,
    creatorId: room.state.creatorId || null,
  });
}

function cleanupIdleRooms() {
  const t = now();
  for (const [code, room] of rooms) {
    const emptyTooLong =
      room.players.size === 0 &&
      room.emptyAt != null &&
      t - room.emptyAt > EMPTY_ROOM_MS;
    const idleTooLong = t - room.lastActivity > IDLE_ROOM_MS;
    if (emptyTooLong || idleTooLong) {
      room.stopHostTick();
      rooms.delete(code);
    }
  }
}
setInterval(cleanupIdleRooms, 15_000);

io.on("connection", (socket) => {
  socket.emit("hello", { serverNow: now(), version: "1.0.0" });

  socket.on("create_room", (payload, ack) => {
    try {
      const name = String((payload && payload.name) || "Oyuncu").slice(0, 16);
      const code = genCode();
      const room = new Room(code, socket.id, name);
      const playerId = genId();
      const player = new Player(playerId, name, socket.id);
      room.addPlayer(player);
      rooms.set(code, room);
      socketIndex.set(socket.id, { roomCode: code, playerId });
      socket.join(code);

      const snap = room.snapshot(playerId);
      if (typeof ack === "function") {
        ack({
          ok: true,
          playerId,
          name: player.name,
          ...snap,
        });
      }
    } catch (err) {
      console.error("create_room", err);
      if (typeof ack === "function") ack({ ok: false, error: "create_failed" });
    }
  });

  socket.on("join_room", (payload, ack) => {
    try {
      const code = normalizeCode((payload && payload.code) || "");
      const name = String((payload && payload.name) || "Oyuncu").slice(0, 16);
      const room = getRoom(code);

      if (!code || !room) {
        if (typeof ack === "function") ack({ ok: false, error: "room_not_found" });
        return;
      }
      if (room.players.size >= MAX_PLAYERS) {
        if (typeof ack === "function") ack({ ok: false, error: "room_full" });
        return;
      }

      // Ban check from room state.adm if present
      const adm = room.state && room.state.adm;
      // Device bans are enforced client-side with did; server can block by id list later

      const playerId = genId();
      const player = new Player(playerId, name, socket.id);
      room.addPlayer(player);
      // Always index/join with the room's canonical code
      const roomCode = room.code;
      socketIndex.set(socket.id, { roomCode, playerId });
      socket.join(roomCode);

      // Notify others
      emitRoom(
        room,
        "player_join",
        { player: player.public(), hostId: room.hostId },
        socket.id
      );
      broadcastPlayers(room);

      if (typeof ack === "function") {
        ack({
          ok: true,
          playerId,
          name: player.name,
          ...room.snapshot(playerId),
        });
      }
    } catch (err) {
      console.error("join_room", err);
      if (typeof ack === "function") ack({ ok: false, error: "join_failed" });
    }
  });

  socket.on("set_name", (payload) => {
    const idx = socketIndex.get(socket.id);
    if (!idx) return;
    const room = getRoom(idx.roomCode);
    if (!room) return;
    const player = room.players.get(idx.playerId);
    if (!player) return;
    const name = String((payload && payload.name) || "").trim().slice(0, 16);
    if (!name) return;
    player.name = name;
    room.touch();
    emitRoom(room, "player_update", { id: player.id, name: player.name });
    broadcastPlayers(room);
  });

  socket.on("player_state", (partial) => {
    const idx = socketIndex.get(socket.id);
    if (!idx) return;
    const room = getRoom(idx.roomCode);
    if (!room) return;
    const player = room.players.get(idx.playerId);
    if (!player || !partial || typeof partial !== "object") return;

    for (const k of Object.keys(partial)) {
      if (partial[k] === null) delete player.state[k];
      else player.state[k] = partial[k];
    }
    room.touch();

    // Relay to others (high frequency — keep payload small)
    emitRoom(
      room,
      "player_state",
      { id: player.id, state: partial, full: false },
      socket.id
    );
  });

  socket.on("room_state", (partial) => {
    const idx = socketIndex.get(socket.id);
    if (!idx) return;
    const room = getRoom(idx.roomCode);
    if (!room) return;

    // Only host (or creator for lobby start) may set authoritative room state
    const isHost = idx.playerId === room.hostId;
    const isCreator = room.state.creatorId === idx.playerId;
    if (!isHost && !isCreator) return;
    if (!partial || typeof partial !== "object") return;

    for (const k of Object.keys(partial)) {
      if (partial[k] === null) delete room.state[k];
      else room.state[k] = partial[k];
    }
    room.touch();

    emitRoom(room, "room_state", { state: partial, full: false, serverNow: now() }, null);
  });

  socket.on("ping_check", (clientTs, ack) => {
    if (typeof ack === "function") ack({ serverNow: now(), clientTs });
  });

  socket.on("leave_room", () => {
    handleDisconnect(socket);
  });

  socket.on("disconnect", () => {
    handleDisconnect(socket);
  });
});

function handleDisconnect(socket) {
  const idx = socketIndex.get(socket.id);
  if (!idx) return;
  socketIndex.delete(socket.id);

  const room = getRoom(idx.roomCode);
  if (!room) return;

  const leaving = room.players.get(idx.playerId);
  const result = room.removePlayer(idx.playerId);

  // Do NOT delete empty rooms immediately — createNewRoom leaves then
  // redirects to ?room=CODE; friends also join by code. Grace period
  // is handled by cleanupIdleRooms (EMPTY_ROOM_MS).
  if (result.empty) {
    return;
  }

  emitRoom(room, "player_leave", {
    id: idx.playerId,
    name: leaving ? leaving.name : "Oyuncu",
    hostId: room.hostId,
    creatorId: room.state.creatorId,
  });
  broadcastPlayers(room);

  // If host changed, notify everyone
  if (result.wasHost) {
    emitRoom(room, "host_change", {
      hostId: room.hostId,
      creatorId: room.state.creatorId,
    });
    // Also patch room state so clients see new creator if reassigned
    if (room.state.creatorId) {
      emitRoom(room, "room_state", {
        state: { creatorId: room.state.creatorId },
        full: false,
        serverNow: now(),
      });
    }
  }
}

server.listen(PORT, () => {
  console.log(`Antics Royale MP server listening on :${PORT}`);
});
