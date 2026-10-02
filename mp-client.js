/**
 * Antics Royale — Multiplayer client adapter
 * Implements the same room API the game expects from /sdk/v1.js
 * so the existing game logic stays intact.
 *
 * Usage:
 *   import { joinRoom, setServerUrl } from "./mp-client.js";
 *   const room = await joinRoom({ room: "ABC12" }); // or {} to create
 */
import { io } from "https://cdn.socket.io/4.8.1/socket.io.esm.min.js";

const DEFAULT_SERVER_URL = "https://antics-royale.onrender.com";

function normalizeServerUrl(url) {
  let u = String(url || "").trim().replace(/\/$/, "");
  if (!u) return DEFAULT_SERVER_URL;
  // Eski localhost kayıtlarını Render'a taşı
  try {
    const host = new URL(u.includes("://") ? u : "http://" + u).hostname;
    if (host === "localhost" || host === "127.0.0.1") return DEFAULT_SERVER_URL;
  } catch (_) {
    if (/localhost|127\.0\.0\.1/i.test(u)) return DEFAULT_SERVER_URL;
  }
  return u;
}

function readStoredServerUrl() {
  try {
    if (typeof localStorage === "undefined") return null;
    const stored = localStorage.getItem("antics_mp_server");
    if (!stored) return null;
    const normalized = normalizeServerUrl(stored);
    // Yanlışlıkla kaydedilmiş localhost'u kalıcı düzelt
    if (normalized !== stored) {
      try {
        localStorage.setItem("antics_mp_server", normalized);
      } catch (_) {}
    }
    return normalized;
  } catch (_) {
    return null;
  }
}

let SERVER_URL = normalizeServerUrl(
  (typeof window !== "undefined" && window.ANTICS_MP_SERVER) ||
    readStoredServerUrl() ||
    DEFAULT_SERVER_URL
);

export function setServerUrl(url) {
  SERVER_URL = normalizeServerUrl(url);
  try {
    localStorage.setItem("antics_mp_server", SERVER_URL);
  } catch (_) {}
}

export function getServerUrl() {
  return SERVER_URL;
}

function mergeState(target, partial) {
  if (!partial || typeof partial !== "object") return target;
  for (const k of Object.keys(partial)) {
    if (partial[k] === null) delete target[k];
    else target[k] = partial[k];
  }
  return target;
}

function makePlayer(data, socket, isMe) {
  const p = {
    id: data.id,
    name: data.name || "Oyuncu",
    state: data.state ? { ...data.state } : {},
    setState(partial) {
      mergeState(this.state, partial);
      if (isMe && socket && socket.connected) {
        socket.emit("player_state", partial);
      }
    },
  };
  return p;
}

/**
 * Join or create a room. Returns a room object compatible with the game.
 * @param {{ room?: string, name?: string }} opts
 */
export function joinRoom(opts = {}) {
  return new Promise((resolve, reject) => {
    const nameHint =
      opts.name ||
      (typeof sessionStorage !== "undefined" && sessionStorage.getItem("antics_name")) ||
      "";
    const wantCode = opts.room
      ? String(opts.room)
          .trim()
          .toUpperCase()
          .replace(/[^A-Z0-9]/g, "")
          .slice(0, 8) || null
      : null;

    const socket = io(SERVER_URL, {
      transports: ["websocket", "polling"],
      reconnection: true,
      reconnectionAttempts: 12,
      reconnectionDelay: 800,
      timeout: 12000,
    });

    let settled = false;
    const fail = (err) => {
      if (settled) return;
      settled = true;
      try {
        socket.disconnect();
      } catch (_) {}
      reject(err instanceof Error ? err : new Error(String(err)));
    };

    const timeout = setTimeout(() => fail(new Error("Bağlantı zaman aşımı")), 15000);

    socket.on("connect_error", (e) => {
      // keep trying via reconnection; only fail on initial timeout
      console.warn("[mp] connect_error", e && e.message);
    });

    socket.once("connect", () => {
      const payload = { name: nameHint || "Oyuncu" };
      const event = wantCode ? "join_room" : "create_room";
      if (wantCode) payload.code = wantCode;

      socket.emit(event, payload, (res) => {
        clearTimeout(timeout);
        if (!res || !res.ok) {
          const errKey = (res && res.error) || "join_failed";
          const friendly =
            errKey === "room_not_found"
              ? "Oda bulunamadı (kod hatalı veya süresi dolmuş)"
              : errKey === "room_full"
                ? "Oda dolu"
                : errKey;
          fail(new Error(friendly));
          return;
        }
        try {
          const room = buildRoom(socket, res);
          settled = true;
          resolve(room);
        } catch (e) {
          fail(e);
        }
      });
    });
  });
}

function buildRoom(socket, res) {
  const playersMap = new Map();
  let hostId = res.hostId || null;
  let roomState = res.state ? { ...res.state } : { phase: "lobby" };
  let myId = res.playerId;
  let code = res.code;
  let ping = 0;
  let _serverOffset = (res.serverNow || Date.now()) - Date.now();

  const stateListeners = new Set();
  const playerStateListeners = new Set();
  const joinListeners = new Set();
  const leaveListeners = new Set();
  const hostChangeListeners = new Set();
  const hostTickListeners = new Set();

  function upsertPlayer(data) {
    let p = playersMap.get(data.id);
    if (!p) {
      p = makePlayer(data, socket, data.id === myId);
      playersMap.set(data.id, p);
    } else {
      if (data.name) p.name = data.name;
      if (data.state) mergeState(p.state, data.state);
    }
    return p;
  }

  // seed players
  (res.players || []).forEach((pl) => upsertPlayer(pl));
  if (!playersMap.has(myId)) {
    upsertPlayer({ id: myId, name: res.name || "Sen", state: {} });
  }

  const me = playersMap.get(myId);

  // RTT / ping
  let pingTimer = setInterval(() => {
    if (!socket.connected) return;
    const t0 = performance.now();
    const clientTs = Date.now();
    socket.emit("ping_check", clientTs, (ack) => {
      if (!ack) return;
      ping = Math.round(performance.now() - t0);
      if (typeof ack.serverNow === "number") {
        _serverOffset = ack.serverNow - Date.now();
      }
    });
  }, 2000);

  socket.on("players", (msg) => {
    if (!msg) return;
    const ids = new Set((msg.players || []).map((p) => p.id));
    for (const [id] of playersMap) {
      if (!ids.has(id) && id !== myId) playersMap.delete(id);
    }
    (msg.players || []).forEach((pl) => {
      const existed = playersMap.has(pl.id);
      const p = upsertPlayer(pl);
      // full state replace on roster sync if provided
      if (pl.state) p.state = { ...pl.state };
    });
    if (msg.hostId) hostId = msg.hostId;
  });

  socket.on("player_join", (msg) => {
    if (!msg || !msg.player) return;
    const p = upsertPlayer(msg.player);
    if (msg.hostId) hostId = msg.hostId;
    joinListeners.forEach((fn) => {
      try {
        fn(p);
      } catch (_) {}
    });
  });

  socket.on("player_leave", (msg) => {
    if (!msg || !msg.id) return;
    const p = playersMap.get(msg.id);
    playersMap.delete(msg.id);
    if (msg.hostId) hostId = msg.hostId;
    leaveListeners.forEach((fn) => {
      try {
        fn(p || { id: msg.id, name: msg.name || "Oyuncu", state: {} });
      } catch (_) {}
    });
  });

  socket.on("player_update", (msg) => {
    if (!msg || !msg.id) return;
    const p = playersMap.get(msg.id);
    if (p && msg.name) p.name = msg.name;
  });

  socket.on("player_state", (msg) => {
    if (!msg || !msg.id) return;
    const p = playersMap.get(msg.id);
    if (!p) return;
    if (msg.full && msg.state) p.state = { ...msg.state };
    else mergeState(p.state, msg.state);
    playerStateListeners.forEach((fn) => {
      try {
        fn(p);
      } catch (_) {}
    });
  });

  socket.on("room_state", (msg) => {
    if (!msg || !msg.state) return;
    if (msg.full) roomState = { ...msg.state };
    else mergeState(roomState, msg.state);
    if (typeof msg.serverNow === "number") {
      _serverOffset = msg.serverNow - Date.now();
    }
    stateListeners.forEach((fn) => {
      try {
        fn(roomState);
      } catch (_) {}
    });
  });

  socket.on("host_change", (msg) => {
    if (!msg) return;
    if (msg.hostId) hostId = msg.hostId;
    hostChangeListeners.forEach((fn) => {
      try {
        fn(playersMap.get(hostId) || null);
      } catch (_) {}
    });
  });

  socket.on("host_tick", (msg) => {
    const dt = (msg && msg.dt) || 33;
    hostTickListeners.forEach((fn) => {
      try {
        fn(dt);
      } catch (_) {}
    });
  });

  socket.on("disconnect", () => {
    // keep objects; reconnection will re-sync via server if we add resume later
  });

  const room = {
    get code() {
      return code;
    },
    get link() {
      try {
        const u = new URL(location.href);
        u.searchParams.delete("menu");
        u.searchParams.set("room", code);
        return u.toString();
      } catch (_) {
        return location.href;
      }
    },
    game: null,
    get ping() {
      return ping;
    },
    me,
    get players() {
      return [...playersMap.values()];
    },
    get host() {
      return hostId ? playersMap.get(hostId) || null : null;
    },
    get isHost() {
      return myId === hostId;
    },
    get state() {
      return roomState;
    },
    setState(partial) {
      mergeState(roomState, partial);
      if (socket.connected) socket.emit("room_state", partial);
      // local listeners (host applies immediately)
      stateListeners.forEach((fn) => {
        try {
          fn(roomState);
        } catch (_) {}
      });
    },
    player(id) {
      return playersMap.get(id) || null;
    },
    now() {
      return Date.now() + _serverOffset;
    },
    onState(fn) {
      if (typeof fn === "function") stateListeners.add(fn);
    },
    onPlayerState(fn) {
      if (typeof fn === "function") playerStateListeners.add(fn);
    },
    onJoin(fn) {
      if (typeof fn === "function") joinListeners.add(fn);
    },
    onLeave(fn) {
      if (typeof fn === "function") leaveListeners.add(fn);
    },
    onHostChange(fn) {
      if (typeof fn === "function") hostChangeListeners.add(fn);
    },
    on() {},
    send() {},
    onHostTick(fn) {
      if (typeof fn === "function") hostTickListeners.add(fn);
    },
    submitScore: async () => ({}),
    leave() {
      try {
        clearInterval(pingTimer);
      } catch (_) {}
      try {
        socket.emit("leave_room");
      } catch (_) {}
      try {
        socket.disconnect();
      } catch (_) {}
    },
    // internal
    _socket: socket,
  };

  // Expose name setter used by lobby
  room.setName = (name) => {
    const n = String(name || "").trim().slice(0, 16);
    if (!n) return;
    me.name = n;
    try {
      sessionStorage.setItem("antics_name", n);
    } catch (_) {}
    socket.emit("set_name", { name: n });
  };

  return room;
}

/** Default export shape similar to sdk module */
export default { joinRoom, setServerUrl, getServerUrl };
