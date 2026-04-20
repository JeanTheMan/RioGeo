const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const http = require('http');
const { URL } = require('url');
const WebSocket = require('ws');
const { Proxy } = require('http-mitm-proxy');
const { getStreetViewPanorama } = require('./streetview');

const ROOT_DIR = path.resolve(__dirname, '..');
const PROXY_PORT = Number(process.env.PROXY_PORT || process.env.PORT || 8080);
const PROXY_HOST = process.env.PROXY_HOST || '127.0.0.1';
const SSL_CA_DIR = path.resolve(process.env.SSL_CA_DIR || path.join(ROOT_DIR, '.mitm-proxy'));
const LOG_DIR = path.resolve(process.env.LOG_DIR || path.join(ROOT_DIR, 'logs'));
const BODY_PREVIEW_LIMIT = Number(process.env.BODY_PREVIEW_LIMIT || 8192);
const WS_PREVIEW_LIMIT = Number(process.env.WS_PREVIEW_LIMIT || 2048);
const MAX_HEADER_VALUE = Number(process.env.MAX_HEADER_VALUE || 512);
const LOCAL_MATCHMAKING_ENABLED = process.env.LOCAL_MATCHMAKING !== '0';
const KML_POINT_LIMIT = Number(process.env.KML_POINT_LIMIT || 25000);
const RANDOM_ROUNDS_PER_GAME = Number(process.env.RANDOM_ROUNDS_PER_GAME || 5);
const LOCAL_ENGINE_ROUNDS = Number(process.env.LOCAL_ENGINE_ROUNDS || 8);
const LOCAL_ENGINE_PORT = Number(process.env.LOCAL_ENGINE_PORT || 19080);
const ADMIN_PORT = Number(process.env.ADMIN_PORT || 19081);
const LOCAL_KML_DIR = path.resolve(path.join(__dirname, 'kml'));
const LOCAL_STREETVIEW_RADIUS_METERS = Number(process.env.LOCAL_STREETVIEW_RADIUS_METERS || 60);
const LOCAL_PANO_LOOKUP_MAX_ATTEMPTS = Number(process.env.LOCAL_PANO_LOOKUP_MAX_ATTEMPTS || 8);
const LOCAL_STREETVIEW_TIMEOUT_MS = Number(process.env.LOCAL_STREETVIEW_TIMEOUT_MS || 3000);
const LOCAL_STRICT_ROAD_ONLY = process.env.LOCAL_STRICT_ROAD_ONLY === '1';
const LOCAL_KML_LINE_PICK_ATTEMPTS = Number(process.env.LOCAL_KML_LINE_PICK_ATTEMPTS || 300);
const LOCAL_ROUND_TIME_SECONDS = Number(process.env.LOCAL_ROUND_TIME_SECONDS || 15);
const LOCAL_ROUND_GRACE_SECONDS = Number(process.env.LOCAL_ROUND_GRACE_SECONDS || 1);
const LOCAL_ROUND_TRANSITION_DELAY_MS = Number(process.env.LOCAL_ROUND_TRANSITION_DELAY_MS || 8000);
const LOCAL_ROUND_WIN_MULTIPLIER_INCREMENT_TENTHS = Number(process.env.LOCAL_ROUND_WIN_MULTIPLIER_INCREMENT_TENTHS || 5);
const LOCAL_ROUND_COUNTDOWN_DELAY_SECONDS = Number(process.env.LOCAL_ROUND_COUNTDOWN_DELAY_SECONDS || 4);
const LOCAL_ROUND_WIN_MULTIPLIER_INCREMENT = LOCAL_ROUND_WIN_MULTIPLIER_INCREMENT_TENTHS / 10;
const GAME_SERVER_HOST = 'game-server.geoguessr.com';
const GAME_SERVER_WS_HOST = 'game-server.geoguessr.com';
const GS2_HOST = 'gs2.geoguessr.com';
const SENSITIVE_HEADERS = new Set(['authorization', 'proxy-authorization', 'cookie', 'set-cookie']);

const matchmakingQueue = [];
const matchmakingPlayers = new Map();
const matchmakingAutoMode = { enabled: true };
const playerSessions = new Map();
const playersByIp = new Map();
const usernamesByIp = new Map();
const localIdentityByUsername = new Map();
const protocolPlayerIdByIdentity = new Map();
const localSocketsByIdentity = new Map();
const localGames = new Map();
const localGameByPlayer = new Map();
const pendingQueueRemovals = new Map();
const fallbackPanoramas = [
  {
    panoId: '56596C5655725233714D57746F433532467354476D41',
    lat: -22.902956469263117,
    lng: -43.1754766383668,
    countryCode: 'br',
    heading: 143.20716365861256,
    pitch: -1.7426840273336095,
    zoom: 0
  },
  {
    panoId: '78704562503678566E4C4D72456D42724D4365435A67',
    lat: 53.27833613834867,
    lng: 50.2328208951214,
    countryCode: 'ru',
    heading: 25.564130576359844,
    pitch: 0.57488476042478,
    zoom: 0
  },
  {
    panoId: '636C5A5776635F4F48727859744C5172485A46685441',
    lat: 10.972197886365388,
    lng: -74.41045493996988,
    countryCode: 'co',
    heading: 95.66774,
    pitch: 0,
    zoom: 0
  },
  {
    panoId: '31624C316D5338714A32377A6F6C7A483531484E4A67',
    lat: 43.88612201909718,
    lng: 135.24127354587355,
    countryCode: 'ru',
    heading: 166.3784113993583,
    pitch: -0.3292869163361587,
    zoom: 0
  },
  {
    panoId: '54325F6842367771426A4A33716A6832366365454B67',
    lat: -23.13081709781139,
    lng: 26.216545973879093,
    countryCode: 'bw',
    heading: 305.2859357596394,
    pitch: -1.3075630609160385,
    zoom: 0
  }
];
const kmlPoints = loadKmlPoints();

function normalizeIp(value) {
  if (!value) {
    return '';
  }
  return String(value).replace('::ffff:', '');
}

function originIpFromReq(req) {
  if (!req || !req.socket) {
    return '';
  }
  return normalizeIp(req.socket.remoteAddress);
}

function forwardedUsernameFromReq(req) {
  if (!req || !req.headers) {
    return '';
  }
  return toSafeString(req.headers['x-riogeo-username']).trim();
}

function buildLocalIdentity(protocolPlayerId, username) {
  const protocolId = toSafeString(protocolPlayerId).trim();
  const user = toSafeString(username).trim();
  if (!protocolId) {
    return '';
  }
  if (!user) {
    return protocolId;
  }
  return `${protocolId}::${user}`;
}

function registerIdentityFromRequest(req, identity) {
  const ip = originIpFromReq(req);
  const value = toSafeString(identity).trim();
  if (!ip || !value) {
    return null;
  }
  usernamesByIp.set(ip, value);
  return { ip, identity: value };
}

function shouldHandleLocalHttp(host, requestUrl) {
  const h = String(host || '').toLowerCase();
  const u = String(requestUrl || '');
  if (!LOCAL_MATCHMAKING_ENABLED) {
    return false;
  }
  if (h.includes(GAME_SERVER_HOST) && /\/api\/(duels|lobby)\//.test(u)) {
    return true;
  }
  if (h.includes(GS2_HOST) && /\/(pin|guess)(\?|$)/.test(u)) {
    return true;
  }
  return false;
}

function shouldHandleLocalWs(host) {
  const h = String(host || '').toLowerCase();
  return LOCAL_MATCHMAKING_ENABLED && (h.includes(GAME_SERVER_WS_HOST) || h.includes(GS2_HOST));
}

function chooseFallbackPanorama() {
  return fallbackPanoramas[Math.floor(Math.random() * fallbackPanoramas.length)];
}

function resolveMapName(mapSlug) {
  if (mapSlug === '6983611e411dbe3f3b2a8c5b') {
    return 'A Figsy World';
  }
  return 'RioGeo Local';
}

function haversineMeters(lat1, lon1, lat2, lon2) {
  const toRad = (deg) => (deg * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) * Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return 6371000 * c;
}

function scoreFromDistance(distanceMeters) {
  const value = Math.round(5000 * Math.exp(-distanceMeters / 2000000));
  return Math.max(0, Math.min(5000, value));
}

function loadKmlPoints() {
  const files = fs.readdirSync(ROOT_DIR).filter((name) => name.toLowerCase().endsWith('.kml'));
  const points = [];

  for (const fileName of files) {
    try {
      const fullPath = path.join(ROOT_DIR, fileName);
      const text = fs.readFileSync(fullPath, 'utf8');
      const matches = text.match(/-?\d+(?:\.\d+)?,-?\d+(?:\.\d+)?(?:,-?\d+(?:\.\d+)?)?/g);
      if (!matches) {
        continue;
      }

      for (const item of matches) {
        const parts = item.split(',');
        if (parts.length < 2) {
          continue;
        }

        const lon = Number(parts[0]);
        const lat = Number(parts[1]);
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
          continue;
        }
        if (lat < -90 || lat > 90 || lon < -180 || lon > 180) {
          continue;
        }

        points.push({ lat, lon, source: fileName });
        if (points.length >= KML_POINT_LIMIT) {
          return points;
        }
      }
    } catch (error) {
      // Ignore malformed KML files and keep loading others.
    }
  }

  return points;
}

function randomId(bytes = 12) {
  return crypto.randomBytes(bytes).toString('hex');
}

function randomPoint() {
  if (!kmlPoints.length) {
    return {
      lat: -22.902956469263117,
      lon: -43.1754766383668,
      source: 'fallback'
    };
  }

  const index = Math.floor(Math.random() * kmlPoints.length);
  return kmlPoints[index];
}

function parseCoordinateText(rawText) {
  if (!rawText) {
    return null;
  }

  const match = String(rawText).match(/(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)(?:\s*,\s*-?\d+(?:\.\d+)?)?/);
  if (!match) {
    return null;
  }

  const lon = Number(match[1]);
  const lat = Number(match[2]);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    return null;
  }
  if (lat < -90 || lat > 90 || lon < -180 || lon > 180) {
    return null;
  }

  return { lat, lng: lon };
}

function extractCoordinatesFromText(rawText) {
  const text = toSafeString(rawText);
  if (!text) {
    return [];
  }

  const matches = text.match(/-?\d+(?:\.\d+)?\s*,\s*-?\d+(?:\.\d+)?(?:\s*,\s*-?\d+(?:\.\d+)?)?/g) || [];
  const coordinates = [];
  for (const candidate of matches) {
    const parsed = parseCoordinateText(candidate);
    if (parsed) {
      coordinates.push(parsed);
    }
  }
  return coordinates;
}

function isHexEncodedString(value) {
  const text = toSafeString(value).trim();
  if (!text || text.length % 2 !== 0) {
    return false;
  }
  return /^[0-9a-fA-F]+$/.test(text);
}

function normalizePanoIdForClient(panoId) {
  const text = toSafeString(panoId).trim();
  if (!text) {
    return '';
  }
  if (isHexEncodedString(text)) {
    return text;
  }
  return Buffer.from(text, 'utf8').toString('hex');
}

function pickRandomCoordinateFromKmlFile(filePath) {
  let text;
  try {
    text = fs.readFileSync(filePath, 'utf8');
  } catch (error) {
    return null;
  }

  const lines = text.split(/\r?\n/).filter((line) => line.trim().length > 0);
  if (lines.length > 0) {
    const attempts = Math.min(lines.length, LOCAL_KML_LINE_PICK_ATTEMPTS);
    for (let i = 0; i < attempts; i += 1) {
      const lineIndex = Math.floor(Math.random() * lines.length);
      const lineCoordinates = extractCoordinatesFromText(lines[lineIndex]);
      if (lineCoordinates.length) {
        return lineCoordinates[Math.floor(Math.random() * lineCoordinates.length)];
      }
    }
  }

  const allCoordinates = extractCoordinatesFromText(text);
  if (!allCoordinates.length) {
    return null;
  }

  return allCoordinates[Math.floor(Math.random() * allCoordinates.length)];
}

function pickRandomKmlCoordinate() {
  let files;
  try {
    files = fs.readdirSync(LOCAL_KML_DIR).filter((name) => name.toLowerCase().endsWith('.kml'));
  } catch (error) {
    return null;
  }

  if (!files.length) {
    return null;
  }

  const fileName = files[Math.floor(Math.random() * files.length)];
  const filePath = path.join(LOCAL_KML_DIR, fileName);
  return pickRandomCoordinateFromKmlFile(filePath);
}

async function pickRoundPanoramaFromLocalKml() {
  for (let attempt = 0; attempt < LOCAL_PANO_LOOKUP_MAX_ATTEMPTS; attempt += 1) {
    const coordinate = pickRandomKmlCoordinate();
    if (!coordinate) {
      break;
    }

    try {
      const panorama = await getStreetViewPanorama(coordinate.lat, coordinate.lng, {
        radius: LOCAL_STREETVIEW_RADIUS_METERS,
        source: 'outdoor',
        roadOnly: true,
        strictRoadOnly: LOCAL_STRICT_ROAD_ONLY,
        timeoutMs: LOCAL_STREETVIEW_TIMEOUT_MS
      });
      if (panorama && panorama.found && panorama.panoId) {
        return {
          panoId: normalizePanoIdForClient(panorama.panoId),
          lat: panorama.lat,
          lng: panorama.lng,
          countryCode: panorama.countryCode || '',
          heading: panorama.heading,
          pitch: panorama.pitch,
          zoom: panorama.zoom
        };
      }
    } catch (error) {
      // Try another random point.
    }
  }

  return chooseFallbackPanorama();
}

async function hydrateRoundPanorama(game, roundNumber) {
  const round = game && Array.isArray(game.rounds) ? game.rounds[roundNumber - 1] : null;
  if (!round) {
    return null;
  }

  const panorama = await pickRoundPanoramaFromLocalKml();
  if (!panorama || !panorama.panoId) {
    return null;
  }

  round.panorama = {
    panoId: panorama.panoId,
    lat: Number.isFinite(Number(panorama.lat)) ? Number(panorama.lat) : round.panorama.lat,
    lng: Number.isFinite(Number(panorama.lng)) ? Number(panorama.lng) : round.panorama.lng,
    countryCode: toSafeString(panorama.countryCode).trim(),
    heading: Number.isFinite(Number(panorama.heading)) ? Number(panorama.heading) : 0,
    pitch: Number.isFinite(Number(panorama.pitch)) ? Number(panorama.pitch) : 0,
    zoom: Number.isFinite(Number(panorama.zoom)) ? Number(panorama.zoom) : 0
  };

  return round.panorama;
}

function wsHost(ctx) {
  const req = ctx.clientToProxyWebSocket && ctx.clientToProxyWebSocket.upgradeReq;
  return req && req.headers
    ? String(req.headers['x-riogeo-original-host'] || req.headers.host || '').toLowerCase()
    : '';
}

function tryParseJson(bufferOrString) {
  try {
    const raw = Buffer.isBuffer(bufferOrString) ? bufferOrString.toString('utf8') : toSafeString(bufferOrString);
    return JSON.parse(raw);
  } catch (error) {
    return null;
  }
}

function sendWsJsonToClient(ctx, payload) {
  if (!ctx || !ctx.clientToProxyWebSocket || ctx.clientToProxyWebSocket.readyState !== 1) {
    return;
  }
  ctx.clientToProxyWebSocket.send(JSON.stringify(payload));
}

function queueMatchmakingPlayer(playerId, details) {
  const existingIndex = matchmakingQueue.indexOf(playerId);
  if (existingIndex === -1) {
    matchmakingQueue.push(playerId);
  }

  matchmakingPlayers.set(playerId, Object.assign({
    playerId,
    username: playerId,
    ip: '',
    avatarPath: '',
    fullBodyPath: '',
    borderUrl: '',
    isSteam: false,
    queuedAt: nowIso(),
    lastSeenAt: nowIso()
  }, details || {}));
}

function removeMatchmakingPlayer(playerId) {
  const queueIndex = matchmakingQueue.indexOf(playerId);
  if (queueIndex !== -1) {
    matchmakingQueue.splice(queueIndex, 1);
  }
  matchmakingPlayers.delete(playerId);
}

function cancelPendingQueueRemoval(playerId) {
  const timer = pendingQueueRemovals.get(playerId);
  if (timer) {
    clearTimeout(timer);
    pendingQueueRemovals.delete(playerId);
  }
}

function scheduleQueueRemoval(playerId, delayMs = 15000) {
  if (!playerId) {
    return;
  }
  cancelPendingQueueRemoval(playerId);
  const timer = setTimeout(() => {
    const activeSocket = localSocketsByIdentity.get(playerId);
    if (activeSocket && activeSocket.readyState === WebSocket.OPEN) {
      pendingQueueRemovals.delete(playerId);
      return;
    }

    const gameId = localGameByPlayer.get(playerId);
    const game = gameId ? localGames.get(gameId) : null;
    if (game && !game.finished) {
      pendingQueueRemovals.delete(playerId);
      return;
    }

    removeMatchmakingPlayer(playerId);
    pendingQueueRemovals.delete(playerId);
  }, delayMs);

  pendingQueueRemovals.set(playerId, timer);
}

function listWaitingPlayers() {
  return matchmakingQueue
    .map((playerId) => matchmakingPlayers.get(playerId))
    .filter(Boolean)
    .map((entry, index) => Object.assign({ position: index + 1 }, entry));
}

function takeNextQueuedPlayers(count = 2) {
  const picked = [];
  while (picked.length < count && matchmakingQueue.length > 0) {
    const playerId = matchmakingQueue.shift();
    const details = matchmakingPlayers.get(playerId) || { playerId };
    matchmakingPlayers.delete(playerId);
    picked.push(Object.assign({}, details, { playerId }));
  }
  return picked;
}

function makeLocalDuelForPlayers(playerAId, playerBId, playerAInfo, playerBInfo) {
  const game = buildLocalDuelState(playerAId, playerBId, playerAInfo, playerBInfo);
  const matchedPayload = { gameId: game.gameId, gameServerNodeId: game.gameServerNodeId };
  const socketA = localSocketsByIdentity.get(playerAId);
  const socketB = localSocketsByIdentity.get(playerBId);
  const protocolA = protocolPlayerIdByIdentity.get(playerAId) || playerAId;
  const protocolB = protocolPlayerIdByIdentity.get(playerBId) || playerBId;
  if (socketA && socketA.readyState === WebSocket.OPEN) {
    socketA.send(JSON.stringify(makeMatchmakingEvent('MatchmakingMatched', protocolA, matchedPayload)));
  }
  if (socketB && socketB.readyState === WebSocket.OPEN) {
    socketB.send(JSON.stringify(makeMatchmakingEvent('MatchmakingMatched', protocolB, matchedPayload)));
  }
  return game;
}

function tryAutoMatchmake() {
  if (!matchmakingAutoMode.enabled) {
    return [];
  }

  const createdGames = [];
  while (matchmakingQueue.length >= 2) {
    const pair = takeNextQueuedPlayers(2);
    if (pair.length < 2) {
      break;
    }
    createdGames.push(makeLocalDuelForPlayers(pair[0].playerId, pair[1].playerId, pair[0], pair[1]));
  }

  return createdGames;
}

function manualPairPlayers(playerAId, playerBId) {
  const playerAInfo = matchmakingPlayers.get(playerAId) || { playerId: playerAId };
  const playerBInfo = matchmakingPlayers.get(playerBId) || { playerId: playerBId };
  removeMatchmakingPlayer(playerAId);
  removeMatchmakingPlayer(playerBId);
  return makeLocalDuelForPlayers(playerAId, playerBId, playerAInfo, playerBInfo);
}

function makeMatchmakingEvent(code, playerId, payload) {
  return {
    code,
    gameId: `matchmaking:${playerId}`,
    playerId: null,
    payload: payload || null,
    timestamp: nowIso(),
    lobby: null,
    countryGuess: null,
    coordinateGuess: null,
    battleRoyaleGameState: null,
    battleRoyalePlayer: null,
    duel: null,
    bullseye: null,
    liveChallenge: null
  };
}

function matchmakingAdminHtml() {
  return `<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>RioGeo Matchmaking</title>
  <style>
    body { font-family: system-ui, sans-serif; margin: 24px; background: #0b1020; color: #e6eefc; }
    h1, h2 { margin: 0 0 12px; }
    .row { display: flex; gap: 24px; align-items: flex-start; flex-wrap: wrap; }
    .panel { background: #131a2f; border: 1px solid #233050; border-radius: 12px; padding: 16px; min-width: 320px; flex: 1; }
    button { background: #4c7dff; color: white; border: 0; border-radius: 8px; padding: 8px 12px; cursor: pointer; }
    button.secondary { background: #2c3550; }
    input, select { width: 100%; box-sizing: border-box; margin: 6px 0 12px; padding: 8px; border-radius: 8px; border: 1px solid #394766; background: #0b1020; color: #e6eefc; }
    ul { list-style: none; padding: 0; margin: 0; }
    li { display: flex; justify-content: space-between; gap: 12px; padding: 10px 0; border-bottom: 1px solid #233050; }
    code { color: #96b7ff; }
    .muted { color: #91a0c7; }
    .actions { display: flex; gap: 8px; flex-wrap: wrap; }
  </style>
</head>
<body>
  <h1>Matchmaking Control</h1>
  <p class="muted">View the waiting list, manually pair two players, or toggle auto mode.</p>
  <div class="row">
    <div class="panel">
      <h2>Status</h2>
      <div>Auto mode: <strong id="autoState">loading</strong></div>
      <div class="actions" style="margin-top:12px;">
        <button onclick="toggleAuto(true)">Auto On</button>
        <button class="secondary" onclick="toggleAuto(false)">Auto Off</button>
        <button class="secondary" onclick="refreshAll()">Refresh</button>
      </div>
    </div>
    <div class="panel">
      <h2>Waiting Players</h2>
      <ul id="waitingList"></ul>
    </div>
    <div class="panel">
      <h2>Manual Pair</h2>
      <label>Player A</label>
      <select id="playerA"></select>
      <label>Player B</label>
      <select id="playerB"></select>
      <button onclick="pairPlayers()">Start Game</button>
      <p class="muted">Select two waiting players and start a duel immediately.</p>
    </div>
  </div>
  <script>
    async function fetchState() {
      const response = await fetch('/matchmaking');
      return response.json();
    }

    function render(state) {
      document.getElementById('autoState').textContent = state.autoMode ? 'on' : 'off';
      const list = document.getElementById('waitingList');
      const playerA = document.getElementById('playerA');
      const playerB = document.getElementById('playerB');
      list.innerHTML = '';
      playerA.innerHTML = '';
      playerB.innerHTML = '';

      for (const player of state.waitingPlayers) {
        const item = document.createElement('li');
        item.innerHTML = '<span><strong>' + (player.username || player.playerId) + '</strong><div class="muted"><code>' + player.playerId + '</code> · ' + (player.ip || '-') + '</div></span><span>#' + player.position + '</span>';
        list.appendChild(item);

        const optionA = document.createElement('option');
        optionA.value = player.playerId;
        optionA.textContent = player.username || player.playerId;
        playerA.appendChild(optionA);

        const optionB = document.createElement('option');
        optionB.value = player.playerId;
        optionB.textContent = player.username || player.playerId;
        playerB.appendChild(optionB);
      }
    }

    async function refreshAll() {
      render(await fetchState());
    }

    async function toggleAuto(enabled) {
      await fetch('/matchmaking/auto', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ enabled })
      });
      await refreshAll();
    }

    async function pairPlayers() {
      const playerA = document.getElementById('playerA').value;
      const playerB = document.getElementById('playerB').value;
      if (!playerA || !playerB || playerA === playerB) {
        alert('Pick two different players.');
        return;
      }
      await fetch('/matchmaking/pair', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ playerA, playerB })
      });
      await refreshAll();
    }

    refreshAll();
    setInterval(refreshAll, 3000);
  </script>
</body>
</html>`;
}

function buildLocalDuelState(playerAId, playerBId, playerAInfo, playerBInfo) {
  const mapName = resolveMapName('6983611e411dbe3f3b2a8c5b');
  const normalizeProfile = (info, fallbackName) => ({
    username: toSafeString(info && (info.username || info.playerId)).trim() || fallbackName,
    countryCode: toSafeString(info && info.countryCode).trim() || 'gb',
    rating: Number(info && info.rating) || 0,
    avatarPath: toSafeString(info && info.avatarPath).trim(),
    fullBodyPath: toSafeString(info && info.fullBodyPath).trim(),
    borderUrl: toSafeString(info && info.borderUrl).trim(),
    isSteam: !!(info && info.isSteam)
  });
  const rounds = [];
  for (let i = 1; i <= LOCAL_ENGINE_ROUNDS; i += 1) {
    const pano = chooseFallbackPanorama();
    rounds.push({
      roundNumber: i,
      panorama: {
        panoId: pano.panoId,
        lat: pano.lat,
        lng: pano.lng,
        countryCode: pano.countryCode,
        heading: pano.heading,
        pitch: pano.pitch,
        zoom: pano.zoom
      },
      hasProcessedRoundTimeout: false,
      isHealingRound: false,
      multiplier: 1,
      damageMultiplier: 1,
      startTime: null,
      endTime: null,
      timerStartTime: null,
      guesses: {}
    });
  }

  const game = {
    gameId: randomId(12),
    gameServerNodeId: 'local-node',
    createdAt: Date.now(),
    createdAtIso: nowIso(),
    accessToken: crypto.randomBytes(32).toString('base64'),
    mapSlug: '6983611e411dbe3f3b2a8c5b',
    mapBounds: {
      min: { lat: -54.88672138251638, lng: -177.39450713648608 },
      max: { lat: 78.23591049351379, lng: 178.5434054107903 }
    },
    started: false,
    finished: false,
    winnerTeamId: null,
    roundResolveTimer: null,
    roundTransitionTimer: null,
    startTransitionTimer: null,
    gs2Clients: new Set(),
    players: [playerAId, playerBId],
    playerProfiles: {
      [playerAId]: normalizeProfile(playerAInfo, 'Player 1'),
      [playerBId]: normalizeProfile(playerBInfo, 'Player 2')
    },
    teams: [
      {
        id: randomId(16),
        name: 'blue',
        health: 6000,
        players: [{ playerId: playerAId, guesses: [], rating: playerAInfo.rating || 0, countryCode: playerAInfo.countryCode || 'gb', progressChange: null, pin: null, helpRequested: false, isSteam: !!playerAInfo.isSteam }],
        roundResults: [],
        isMultiplierActive: true,
        currentMultiplier: 1
      },
      {
        id: randomId(16),
        name: 'red',
        health: 6000,
        players: [{ playerId: playerBId, guesses: [], rating: playerBInfo.rating || 0, countryCode: playerBInfo.countryCode || 'gb', progressChange: null, pin: null, helpRequested: false, isSteam: !!playerBInfo.isSteam }],
        roundResults: [],
        isMultiplierActive: true,
        currentMultiplier: 1
      }
    ],
    rounds,
    currentRoundNumber: 1,
    status: 'Created',
    version: 0,
    mapName
  };

  localGames.set(game.gameId, game);
  localGameByPlayer.set(playerAId, game.gameId);
  localGameByPlayer.set(playerBId, game.gameId);
  return game;
}

function publicPlayerId(localPlayerId) {
  return protocolPlayerIdByIdentity.get(localPlayerId) || localPlayerId;
}

function buildLobbyPlayer(game, localPlayerId) {
  const profile = (game.playerProfiles && game.playerProfiles[localPlayerId]) || {};
  const protocolId = outwardPlayerId(game, localPlayerId);
  const safeNick = toSafeString(profile.username).trim() || protocolId;
  const rating = Number.isFinite(profile.rating) ? profile.rating : 0;
  return {
    playerId: protocolId,
    nick: safeNick,
    countryCode: profile.countryCode || 'gb',
    isVerified: false,
    flair: 0,
    avatarPath: profile.avatarPath || 'pin/eb4427425009f170f0944f94d6f6d4f6.png',
    level: 1,
    titleTierId: 10,
    division: 'Bronze',
    performanceStreak: 'None',
    rank: { rank: 0, division: { id: 0, divisionId: 0, tierId: 0 } },
    team: '',
    competitive: {
      rating,
      division: { type: 10, startRating: 0, endRating: 450 }
    },
    avatar: { fullBodyPath: profile.fullBodyPath || '' },
    fullBodyPath: profile.fullBodyPath || '',
    isGuest: false,
    club: null,
    borderUrl: profile.borderUrl || 'avatarasseticon/78c04b52c44c7521637746c87f208c7a.webp',
    isSteam: !!profile.isSteam
  };
}

function syntheticPublicPlayerId(localPlayerId) {
  return crypto.createHash('md5').update(String(localPlayerId)).digest('hex').slice(0, 24);
}

function protocolIdCountsForGame(game) {
  const counts = new Map();
  for (const localId of game.players || []) {
    const protocolId = publicPlayerId(localId);
    counts.set(protocolId, (counts.get(protocolId) || 0) + 1);
  }
  return counts;
}

function outwardPlayerId(game, localPlayerId, viewerLocalPlayerId) {
  const protocolId = publicPlayerId(localPlayerId);
  const counts = protocolIdCountsForGame(game);
  const hasCollision = (counts.get(protocolId) || 0) > 1;
  if (!hasCollision) {
    return protocolId;
  }

  if (viewerLocalPlayerId && viewerLocalPlayerId === localPlayerId) {
    return protocolId;
  }

  return syntheticPublicPlayerId(localPlayerId);
}

function buildPublicRound(game, round, viewerLocalPlayerId) {
  const guesses = {};
  for (const [localPlayerId, guess] of Object.entries((round && round.guesses) || {})) {
    const outwardId = outwardPlayerId(game, localPlayerId, viewerLocalPlayerId);
    guesses[outwardId] = guess ? { ...guess } : guess;
  }

  return {
    ...round,
    panorama: round && round.panorama ? { ...round.panorama } : round.panorama,
    guesses
  };
}

function buildLobbyJoinState(game, viewerLocalPlayerId) {
  const localPlayerIds = Array.isArray(game.players) ? game.players : [];
  const orderedLocalPlayerIds = viewerLocalPlayerId
    ? [...localPlayerIds.filter((localId) => localId !== viewerLocalPlayerId), ...localPlayerIds.filter((localId) => localId === viewerLocalPlayerId)]
    : localPlayerIds.slice();
  const playerIds = orderedLocalPlayerIds.map((localId) => outwardPlayerId(game, localId, viewerLocalPlayerId));
  const players = orderedLocalPlayerIds.map((localId) => {
    const lobbyPlayer = buildLobbyPlayer(game, localId);
    lobbyPlayer.playerId = outwardPlayerId(game, localId, viewerLocalPlayerId);
    return lobbyPlayer;
  });
  return {
    id: game.gameId,
    gameId: game.gameId,
    gameServerNodeId: game.gameServerNodeId,
    participants: playerIds,
    gameLobbyId: game.gameId,
    title: '',
    type: 'None',
    gameType: 'Duels',
    status: 'Closed',
    numPlayersJoined: players.length,
    totalSpots: 2,
    numOpenSpots: Math.max(0, 2 - players.length),
    minPlayersRequired: 2,
    playerIds,
    players,
    visibility: 'Public',
    closingTime: null,
    timestamp: nowIso(),
    owner: '',
    host: null,
    isAutoStarted: false,
    canBeStartedManually: false,
    partyId: null,
    isRated: true,
    competitionId: '',
    gameOptions: {
      initialHealth: 6000,
      individualInitialHealth: false,
      initialHealthTeamOne: 6000,
      initialHealthTeamTwo: 6000,
      roundTime: LOCAL_ROUND_TIME_SECONDS,
      maxRoundTime: 0,
      maxNumberOfRounds: 0,
      forbidMoving: false,
      forbidZooming: false,
      forbidRotating: false,
      mapSlug: game.mapSlug,
      disableMultipliers: false,
      multiplierIncrement: 0,
      disableHealing: true,
      activeMultiplier: false,
      roundWinMultiplierIncrement: LOCAL_ROUND_WIN_MULTIPLIER_INCREMENT_TENTHS,
      guessMapType: 'roadmap'
    },
    createdAt: game.createdAtIso,
    shareLink: `https://www.geoguessr.com/duels/${game.gameId}`,
    teams: [],
    groupEventId: '',
    quizId: '',
    hostParticipates: true,
    isSinglePlayer: false,
    tripId: '',
    blueprintId: null,
    allowedCommunication: 'EmotesOnly',
    accessToken: game.accessToken,
    mapName: game.mapName || resolveMapName(game.mapSlug),
    gameContext: 'quickplay',
    competitiveGameMode: 'StandardDuels',
    tournament: null
  };
}

function buildPublicDuelState(game, viewerLocalPlayerId) {
  const teams = game.teams.map((team) => ({
    ...team,
    players: team.players.map((player) => ({
      ...player,
      playerId: outwardPlayerId(game, player.playerId, viewerLocalPlayerId)
    }))
  }));

  return {
    gameId: game.gameId,
    context: null,
    gameServerNodeId: game.gameServerNodeId,
    gameType: 'Duels',
    gameModeType: 'StandardDuels',
    status: game.finished ? 'Finished' : game.started ? 'Ongoing' : 'Created',
    currentRoundNumber: game.currentRoundNumber,
    version: game.version || 0,
    teams,
    rounds: game.rounds.map((round) => buildPublicRound(game, round, viewerLocalPlayerId)),
    winnerTeamId: game.winnerTeamId,
    options: {
      initialHealth: 6000,
      individualInitialHealth: false,
      initialHealthTeamOne: 6000,
      initialHealthTeamTwo: 6000,
      roundTime: LOCAL_ROUND_TIME_SECONDS,
      maxRoundTime: 0,
      gracePeriodTime: LOCAL_ROUND_GRACE_SECONDS,
      gameTimeOut: 7200,
      maxNumberOfRounds: 0,
      healingRounds: [5],
      movementOptions: { forbidMoving: false, forbidZooming: false, forbidRotating: false },
      mapSlug: game.mapSlug,
      isRated: true,
      map: {
        name: game.mapName || resolveMapName(game.mapSlug),
        slug: game.mapSlug,
        bounds: game.mapBounds,
        maxErrorDistance: 18499075
      },
      duelRoundOptions: [],
      roundsWithoutDamageMultiplier: 1,
      disableMultipliers: false,
      multiplierIncrement: 0,
      disableHealing: true,
      isTeamDuels: false,
      gameContext: { type: 'Quickplay', id: '' },
      roundStartingBehavior: 'Default',
      flashbackRounds: [],
      competitiveGameMode: 'StandardDuels',
      countAllGuesses: false,
      masterControlAutoStartRounds: false,
      consumedLocationsIdentifier: '',
      useCuratedLocations: false,
      extraWaitTimeBetweenRounds: 0,
      roundCountdownDelay: LOCAL_ROUND_COUNTDOWN_DELAY_SECONDS,
      guessMapType: 'roadmap',
      botBehaviors: {},
      progressionSystem: 5,
      activeMultiplier: false,
      roundWinMultiplierIncrement: LOCAL_ROUND_WIN_MULTIPLIER_INCREMENT_TENTHS
    },
    movementOptions: { forbidMoving: false, forbidZooming: false, forbidRotating: false },
    mapBounds: game.mapBounds,
    initialHealth: 6000,
    maxNumberOfRounds: 0,
    result: null,
    isPaused: false,
    tournamentId: '',
    gameOptions: {
      gameSettings: {
        rounds: LOCAL_ENGINE_ROUNDS,
        movement: false,
        panning: true,
        zooming: true,
        timeLimit: LOCAL_ROUND_TIME_SECONDS
      }
    }
  };
}

function patchDuelStartedWithRandomKml(message) {
  if (!message || message.code !== 'DuelStarted' || !message.duel || !message.duel.state || !Array.isArray(message.duel.state.rounds)) {
    return message;
  }

  // Keep local duel panoramas consistent with their pano IDs.
  if (message.duel.state.gameServerNodeId === 'local-node') {
    return message;
  }

  for (const round of message.duel.state.rounds) {
    if (!round || !round.panorama) {
      continue;
    }

    const point = randomPoint();
    round.panorama.lat = point.lat;
    round.panorama.lng = point.lon;
  }

  return message;
}

function markCurrentRoundStarted(game) {
  const round = game && game.rounds ? game.rounds[game.currentRoundNumber - 1] : null;
  if (!round) {
    return;
  }
  if (!round.startTime) {
    round.startTime = nowIso();
  }
  if (round.timerStartTime == null) {
    round.timerStartTime = null;
  }
}

function prepareRoundForCountdown(round) {
  if (!round) {
    return;
  }

  const nextRoundStartMs = Date.now() + (LOCAL_ROUND_COUNTDOWN_DELAY_SECONDS * 1000);
  round.startTime = new Date(nextRoundStartMs).toISOString();
  round.timerStartTime = null;
  round.endTime = null;
  round.hasProcessedRoundTimeout = false;
}

async function transitionGameToOngoing(game) {
  if (!game || game.finished) {
    return;
  }

  try {
    await hydrateRoundPanorama(game, game.currentRoundNumber);
  } catch (error) {
    // Keep game flow alive even if panorama lookup fails.
  }

  if (game.finished) {
    return;
  }

  game.status = 'Ongoing';
  markCurrentRoundStarted(game);
  game.version += 1;
  sendEventToGame(game, 'DuelStarted');
}

async function beginNextRound(game, winner) {
  if (!game || game.finished) {
    return;
  }

  if (winner) {
    winner.currentMultiplier = Number((winner.currentMultiplier + LOCAL_ROUND_WIN_MULTIPLIER_INCREMENT).toFixed(1));
  }

  game.currentRoundNumber += 1;
  const nextRound = game.rounds[game.currentRoundNumber - 1];
  if (nextRound) {
    try {
      await hydrateRoundPanorama(game, game.currentRoundNumber);
    } catch (error) {
      // Keep game flow alive even if panorama lookup fails.
    }

    prepareRoundForCountdown(nextRound);
  }

  game.version += 1;
  sendEventToGame(game, 'DuelNewRound');
}

function normalizeIsoTimestamp(value, fallbackIso) {
  const raw = toSafeString(value).trim();
  const millis = Date.parse(raw);
  if (!Number.isFinite(millis)) {
    return fallbackIso;
  }
  return new Date(millis).toISOString();
}

function findTeamPlayer(team, playerId) {
  if (!team || !Array.isArray(team.players)) {
    return null;
  }
  return team.players.find((player) => player.playerId === playerId) || null;
}

function upsertPlayerGuess(playerState, guess) {
  if (!playerState || !Array.isArray(playerState.guesses)) {
    return;
  }
  const index = playerState.guesses.findIndex((entry) => entry.roundNumber === guess.roundNumber);
  if (index === -1) {
    playerState.guesses.push(guess);
    return;
  }
  playerState.guesses[index] = guess;
}

function clearAllPins(game) {
  for (const team of game.teams) {
    for (const player of team.players) {
      player.pin = null;
    }
  }
}

function resolveTeamRoundScore(game, team, round) {
  const teamGuesses = [];
  for (const player of team.players) {
    const guess = round.guesses[player.playerId];
    if (!guess) {
      continue;
    }

    guess.distance = haversineMeters(guess.lat, guess.lng, round.panorama.lat, round.panorama.lng);
    guess.score = scoreFromDistance(guess.distance);
    guess.isTeamsBestGuessOnRound = false;
    teamGuesses.push(guess);
  }

  if (!teamGuesses.length) {
    return {
      score: 0,
      bestGuess: null,
      guesses: teamGuesses
    };
  }

  let bestGuess = teamGuesses[0];
  for (const guess of teamGuesses) {
    if ((guess.score || 0) > (bestGuess.score || 0)) {
      bestGuess = guess;
    }
  }

  bestGuess.isTeamsBestGuessOnRound = true;

  return {
    score: bestGuess.score || 0,
    bestGuess,
    guesses: teamGuesses
  };
}

function calculateRoundTimeoutDelayMs() {
  return Math.max(1000, (LOCAL_ROUND_TIME_SECONDS + LOCAL_ROUND_GRACE_SECONDS) * 1000);
}

function scheduleRoundTimeout(game, roundNumber, delayMs) {
  if (game.roundResolveTimer) {
    clearTimeout(game.roundResolveTimer);
  }

  const timeoutMs = Math.max(0, Number(delayMs) || 0);
  game.roundResolveTimer = setTimeout(() => {
    if (game.finished || game.currentRoundNumber !== roundNumber) {
      return;
    }
    resolveRound(game);
  }, timeoutMs);
}

function buildDuelEnvelope(game, viewerLocalPlayerId) {
  const viewerPlayerId = viewerLocalPlayerId
    ? outwardPlayerId(game, viewerLocalPlayerId, viewerLocalPlayerId)
    : null;

  return {
    gameId: game.gameId,
    playerId: viewerPlayerId,
    timestamp: nowIso(),
    countryGuess: null,
    coordinateGuess: null,
    lobby: null,
    battleRoyaleGameState: null,
    battleRoyalePlayer: null,
    bullseye: null,
    liveChallenge: null,
    duel: {
      state: buildPublicDuelState(game, viewerLocalPlayerId)
    }
  };
}

function sendEventToGame(game, code, extra) {
  for (const ws of game.gs2Clients) {
    if (ws.readyState === WebSocket.OPEN) {
      const viewerLocalPlayerId = ws.__localIdentity || '';
      const payload = Object.assign(buildDuelEnvelope(game, viewerLocalPlayerId), { code }, extra || {});
      ws.send(JSON.stringify(payload));
    }
  }
}

function teamForPlayer(game, playerId) {
  for (const team of game.teams) {
    if (team.players.some((player) => player.playerId === playerId)) {
      return team;
    }
  }
  return null;
}

function opponentTeam(game, teamId) {
  return game.teams.find((team) => team.id !== teamId) || null;
}

function resolveRound(game) {
  if (game.finished) {
    return;
  }
  if (game.roundResolveTimer) {
    clearTimeout(game.roundResolveTimer);
    game.roundResolveTimer = null;
  }

  const round = game.rounds[game.currentRoundNumber - 1];
  if (!round) {
    return;
  }
  if (round.hasProcessedRoundTimeout) {
    return;
  }

  const [playerA, playerB] = game.players;
  const teamA = teamForPlayer(game, playerA);
  const teamB = teamForPlayer(game, playerB);
  if (!teamA || !teamB) {
    return;
  }

  const teamAScoreData = resolveTeamRoundScore(game, teamA, round);
  const teamBScoreData = resolveTeamRoundScore(game, teamB, round);
  const scoreA = teamAScoreData.score;
  const scoreB = teamBScoreData.score;
  const scoreDiff = Math.abs(scoreA - scoreB);

  const healthBeforeA = teamA.health;
  const healthBeforeB = teamB.health;

  let winner = null;
  let loser = null;
  if (scoreA > scoreB) {
    winner = teamA;
    loser = teamB;
  } else if (scoreB > scoreA) {
    winner = teamB;
    loser = teamA;
  }

  let damageDealt = 0;
  const winnerMultiplierForRound = winner ? winner.currentMultiplier : 1;
  if (winner && loser) {
    damageDealt = Math.round(scoreDiff * winnerMultiplierForRound);
    loser.health = Math.max(0, loser.health - damageDealt);
  }

  teamA.roundResults.push({
    roundNumber: round.roundNumber,
    score: scoreA,
    healthBefore: healthBeforeA,
    healthAfter: teamA.health,
    bestGuess: teamAScoreData.bestGuess ? { ...teamAScoreData.bestGuess } : null,
    damageDealt: winner && winner.id === teamA.id ? damageDealt : 0,
    multiplier: teamA.currentMultiplier
  });

  teamB.roundResults.push({
    roundNumber: round.roundNumber,
    score: scoreB,
    healthBefore: healthBeforeB,
    healthAfter: teamB.health,
    bestGuess: teamBScoreData.bestGuess ? { ...teamBScoreData.bestGuess } : null,
    damageDealt: winner && winner.id === teamB.id ? damageDealt : 0,
    multiplier: teamB.currentMultiplier
  });

  round.hasProcessedRoundTimeout = true;
  if (!round.endTime) {
    round.endTime = nowIso();
  }
  clearAllPins(game);

  game.version += 1;

  sendEventToGame(game, 'DuelRoundTimedOut');

  const gameEnded = teamA.health <= 0 || teamB.health <= 0 || game.currentRoundNumber >= game.rounds.length;
  if (gameEnded) {
    game.finished = true;
    game.status = 'Finished';
    game.winnerTeamId = teamA.health === teamB.health ? null : (teamA.health > teamB.health ? teamA.id : teamB.id);

    if (game.roundTransitionTimer) {
      clearTimeout(game.roundTransitionTimer);
    }
    game.roundTransitionTimer = setTimeout(() => {
      game.version += 1;
      sendEventToGame(game, 'DuelFinished');
    }, LOCAL_ROUND_TRANSITION_DELAY_MS);
    return;
  }

  if (game.roundTransitionTimer) {
    clearTimeout(game.roundTransitionTimer);
  }
  game.roundTransitionTimer = setTimeout(() => {
    void beginNextRound(game, winner);
  }, LOCAL_ROUND_TRANSITION_DELAY_MS);
}

function handleLocalMatchmakingSubscribe(ctx, payload) {
  const req = ctx && ctx.clientToProxyWebSocket ? ctx.clientToProxyWebSocket.upgradeReq : null;
  const ip = originIpFromReq(req);
  const forwardedUsername = forwardedUsernameFromReq(req);
  const payloadPlayerId = payload && payload.playerId ? payload.playerId : null;
  const protocolPlayerId = payloadPlayerId || (ip && playersByIp.get(ip));
  if (!protocolPlayerId) {
    return;
  }

  const localIdentity = buildLocalIdentity(protocolPlayerId, forwardedUsername);
  if (!localIdentity) {
    return;
  }

  playerSessions.set(localIdentity, ctx);
  if (ip) {
    playersByIp.set(ip, localIdentity);
  }
  if (forwardedUsername) {
    localIdentityByUsername.set(forwardedUsername, localIdentity);
  }
  protocolPlayerIdByIdentity.set(localIdentity, protocolPlayerId);

  queueMatchmakingPlayer(localIdentity, {
    username: forwardedUsername || (payload && payload.username ? String(payload.username) : protocolPlayerId),
    ip,
    protocolPlayerId,
    lastSeenAt: nowIso()
  });

  sendWsJsonToClient(ctx, makeMatchmakingEvent('MatchmakingJoined', protocolPlayerId, {
    gameModes: Array.isArray(payload.gameModes) ? payload.gameModes : ['StandardDuels']
  }));

  tryAutoMatchmake();
}

function nowIso() {
  return new Date().toISOString();
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function toSafeString(value) {
  if (value === undefined || value === null) {
    return '';
  }

  if (Buffer.isBuffer(value)) {
    return value.toString('utf8');
  }

  return String(value);
}

function truncate(value, limit) {
  if (typeof value !== 'string') {
    value = toSafeString(value);
  }

  if (value.length <= limit) {
    return value;
  }

  return `${value.slice(0, limit)}… [truncated ${value.length - limit} chars]`;
}

function sanitizeHeaderValue(name, value) {
  if (SENSITIVE_HEADERS.has(name.toLowerCase())) {
    return '[redacted]';
  }

  if (Array.isArray(value)) {
    return value.map((entry) => sanitizeHeaderValue(name, entry));
  }

  return truncate(toSafeString(value), MAX_HEADER_VALUE);
}

function snapshotHeaders(headers) {
  if (!headers) {
    return {};
  }

  const result = {};
  for (const [name, value] of Object.entries(headers)) {
    result[name] = sanitizeHeaderValue(name, value);
  }
  return result;
}

function isTextLike(contentType) {
  if (!contentType) {
    return false;
  }

  const lower = String(contentType).toLowerCase();
  return (
    lower.startsWith('text/') ||
    lower.includes('json') ||
    lower.includes('xml') ||
    lower.includes('javascript') ||
    lower.includes('x-www-form-urlencoded') ||
    lower.includes('graphql')
  );
}

function isPrintableBuffer(buffer) {
  for (const byte of buffer) {
    if (byte === 9 || byte === 10 || byte === 13) {
      continue;
    }

    if (byte < 32 || byte > 126) {
      return false;
    }
  }

  return true;
}

function previewBody(buffer, contentType, limit) {
  if (!buffer || buffer.length === 0) {
    return '';
  }

  if (!isTextLike(contentType) && !isPrintableBuffer(buffer)) {
    return `<binary ${buffer.length} bytes>`;
  }

  const text = buffer.toString('utf8');
  return truncate(text, limit);
}

function appendLog(filePath, line) {
  fs.appendFileSync(filePath, `${line}\n`, 'utf8');
}

function buildRequestUrl(ctx) {
  const req = ctx.clientToProxyRequest;
  const host = req && req.headers ? req.headers.host || '' : '';
  const rawUrl = req && req.url ? req.url : '/';
  const scheme = ctx.isSSL ? 'https' : 'http';

  if (/^https?:\/\//i.test(rawUrl)) {
    return rawUrl;
  }

  if (!host) {
    return rawUrl;
  }

  return `${scheme}://${host}${rawUrl}`;
}

function formatDurationMs(startedAt) {
  return `${Date.now() - startedAt}ms`;
}

function formatHumanEntry(fields) {
  return fields.map((field) => (field === undefined || field === null || field === '' ? '-' : String(field))).join(' | ');
}

function writeEvent(logPaths, event, humanLine) {
  const jsonLine = JSON.stringify(event);
  appendLog(logPaths.jsonl, jsonLine);
  appendLog(logPaths.human, humanLine);
  console.log(humanLine);
}

function ensureTrafficState(ctx) {
  if (ctx.__trafficState) {
    return ctx.__trafficState;
  }

  const req = ctx.clientToProxyRequest;
  const state = {
    startedAt: Date.now(),
    protocol: ctx.isSSL ? 'https' : 'http',
    method: req ? req.method : '-',
    host: req && req.headers ? req.headers.host || '' : '',
    url: req ? req.url || '/' : '/',
    requestChunks: [],
    responseChunks: [],
    requestBytes: 0,
    responseBytes: 0,
    responseStatusCode: null,
    responseHeaders: null,
    websocketStartedAt: null
  };

  ctx.__trafficState = state;
  return state;
}

function registerRequestChunk(state, chunk) {
  state.requestBytes += chunk.length;
  const currentBytes = state.requestChunks.reduce((total, item) => total + item.length, 0);
  if (currentBytes < BODY_PREVIEW_LIMIT) {
    const remaining = BODY_PREVIEW_LIMIT - currentBytes;
    state.requestChunks.push(Buffer.from(chunk.slice(0, remaining)));
  }
}

function registerResponseChunk(state, chunk) {
  state.responseBytes += chunk.length;
  const currentBytes = state.responseChunks.reduce((total, item) => total + item.length, 0);
  if (currentBytes < BODY_PREVIEW_LIMIT) {
    const remaining = BODY_PREVIEW_LIMIT - currentBytes;
    state.responseChunks.push(Buffer.from(chunk.slice(0, remaining)));
  }
}

function buildBodyPreview(chunks, contentType, limit, totalBytes) {
  if (!chunks.length) {
    return '';
  }

  const buffer = Buffer.concat(chunks);
  const preview = previewBody(buffer, contentType, limit);
  if (typeof preview === 'string' && preview.startsWith('<binary ')) {
    return preview;
  }

  if (totalBytes > buffer.length) {
    return `${preview} [body preview ${buffer.length}/${totalBytes} bytes]`;
  }

  return preview;
}

function buildHeadersPreview(headers) {
  const entries = Object.entries(snapshotHeaders(headers));
  return entries.length ? JSON.stringify(Object.fromEntries(entries)) : '{}';
}

function createLogPaths() {
  ensureDir(LOG_DIR);
  return {
    human: path.join(LOG_DIR, 'traffic.log'),
    jsonl: path.join(LOG_DIR, 'traffic.jsonl')
  };
}

function readJsonBody(req) {
  return new Promise((resolve) => {
    const chunks = [];
    req.on('data', (chunk) => chunks.push(Buffer.from(chunk)));
    req.on('end', () => {
      if (!chunks.length) {
        resolve({});
        return;
      }
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      } catch (error) {
        resolve({});
      }
    });
    req.on('error', () => resolve({}));
  });
}

function applyCorsHeaders(res, req) {
  const requestOrigin = toSafeString(req && req.headers ? req.headers.origin : '').trim();
  const requestHeaders = toSafeString(req && req.headers ? req.headers['access-control-request-headers'] : '').trim();
  const requestMethod = toSafeString(req && req.headers ? req.headers['access-control-request-method'] : '').trim();

  res.setHeader('access-control-allow-origin', requestOrigin || '*');
  res.setHeader('access-control-allow-credentials', 'true');
  res.setHeader('access-control-allow-headers', requestHeaders || 'content-type,authorization,x-client');
  res.setHeader('access-control-allow-methods', requestMethod || 'GET,POST,DELETE,OPTIONS');
  res.setHeader('access-control-expose-headers', 'x-servertime-reception,x-servertime');
  res.setHeader('vary', 'Origin, Access-Control-Request-Headers, Access-Control-Request-Method');
}

function applyServerTimeHeaders(res) {
  const now = nowIso();
  res.setHeader('x-servertime-reception', now);
  res.setHeader('x-servertime', now);
}

function writeJson(res, statusCode, body, req) {
  const payload = JSON.stringify(body || {});
  res.statusCode = statusCode;
  res.setHeader('content-type', 'application/json; charset=utf-8');
  applyCorsHeaders(res, req);
  applyServerTimeHeaders(res);
  res.setHeader('cache-control', 'no-store');
  res.end(payload);
}

function endNoContent(res, req) {
  res.statusCode = 204;
  applyCorsHeaders(res, req);
  applyServerTimeHeaders(res);
  res.setHeader('cache-control', 'no-store');
  res.end();
}

function getGameFromRequest(req, urlObj) {
  const ip = originIpFromReq(req);
  const forwardedUsername = forwardedUsernameFromReq(req);
  const playerIdFromUsername = forwardedUsername ? (localIdentityByUsername.get(forwardedUsername) || '') : '';
  const playerId = playerIdFromUsername || (playersByIp.get(ip) || '');
  const gameIdFromPlayer = playerId ? localGameByPlayer.get(playerId) : '';
  const pathGameMatch = urlObj.pathname.match(/\/api\/duels\/([^/]+)/);
  const gs2GameMatch = urlObj.pathname.match(/^\/[^/]+\/([^/]+)\/(pin|guess|ws)$/);
  const gameId = (pathGameMatch && pathGameMatch[1]) || (gs2GameMatch && gs2GameMatch[1]) || gameIdFromPlayer;
  if (!gameId) {
    return { game: null, playerId };
  }
  return { game: localGames.get(gameId) || null, playerId };
}

function createLocalEngine() {
  const server = http.createServer(async (req, res) => {
    const host = String(req.headers['x-riogeo-original-host'] || req.headers.host || '').toLowerCase();
    const fullUrl = new URL(req.url || '/', 'http://local-engine');
    if (req.method === 'OPTIONS') {
      endNoContent(res, req);
      return;
    }

    const { game, playerId } = getGameFromRequest(req, fullUrl);

    if (host.includes(GAME_SERVER_HOST) && req.method === 'GET' && fullUrl.pathname === '/api/duels/ongoing/') {
      if (!game || game.finished) {
        endNoContent(res, req);
        return;
      }
      writeJson(res, 200, buildPublicDuelState(game, playerId), req);
      return;
    }

    if (host.includes(GAME_SERVER_HOST) && req.method === 'DELETE' && fullUrl.pathname === '/api/duels/ongoing/') {
      if (game && playerId) {
        game.finished = true;
        game.status = 'Finished';
        if (game.startTransitionTimer) {
          clearTimeout(game.startTransitionTimer);
          game.startTransitionTimer = null;
        }
        if (game.roundResolveTimer) {
          clearTimeout(game.roundResolveTimer);
          game.roundResolveTimer = null;
        }
        if (game.roundTransitionTimer) {
          clearTimeout(game.roundTransitionTimer);
          game.roundTransitionTimer = null;
        }
      }
      endNoContent(res, req);
      return;
    }

    if (host.includes(GAME_SERVER_HOST) && req.method === 'GET' && /\/api\/duels\/[^/]+$/.test(fullUrl.pathname)) {
      if (!game) {
        writeJson(res, 404, { message: 'Local game not found' }, req);
        return;
      }
      writeJson(res, 200, buildPublicDuelState(game, playerId), req);
      return;
    }

    if (host.includes(GAME_SERVER_HOST) && req.method === 'POST' && /\/api\/lobby\/[^/]+\/join$/.test(fullUrl.pathname)) {
      if (!game) {
        writeJson(res, 404, { message: 'Local game not found' }, req);
        return;
      }
      writeJson(res, 200, buildLobbyJoinState(game, playerId), req);
      return;
    }

    if (host.includes(GS2_HOST) && req.method === 'POST' && /\/(pin|guess)$/.test(fullUrl.pathname)) {
      if (!game || !playerId) {
        writeJson(res, 404, { message: 'Local game not found for player' }, req);
        return;
      }
      const body = await readJsonBody(req);
      const round = game.rounds[game.currentRoundNumber - 1];
      if (!round) {
        writeJson(res, 409, { message: 'Round not active' }, req);
        return;
      }

      const lat = Number(body.lat);
      const lng = Number(body.lng);
      if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
        writeJson(res, 400, { message: 'Invalid guess coordinates' }, req);
        return;
      }

      const playerTeam = teamForPlayer(game, playerId);
      const playerState = playerTeam ? findTeamPlayer(playerTeam, playerId) : null;
      if (!playerState) {
        writeJson(res, 409, { message: 'Player is not part of the local game' }, req);
        return;
      }

      if (fullUrl.pathname.endsWith('/pin')) {
        playerState.pin = { lat, lng };
        game.version += 1;
        sendEventToGame(game, 'DuelPinPlaced');
        endNoContent(res, req);
        return;
      }

      const createdAt = normalizeIsoTimestamp(body.time, nowIso());
      const roundGuess = {
        roundNumber: game.currentRoundNumber,
        lat,
        lng,
        distance: haversineMeters(lat, lng, round.panorama.lat, round.panorama.lng),
        created: createdAt,
        isTeamsBestGuessOnRound: false,
        score: null
      };
      round.guesses[playerId] = roundGuess;
      upsertPlayerGuess(playerState, roundGuess);
      playerState.pin = null;

      if (!round.timerStartTime) {
        const timerStartedAt = createdAt;
        round.timerStartTime = timerStartedAt;
        const timerEndsAt = new Date(Date.parse(timerStartedAt) + calculateRoundTimeoutDelayMs()).toISOString();
        round.endTime = timerEndsAt;
        scheduleRoundTimeout(game, round.roundNumber, Date.parse(timerEndsAt) - Date.now());
      }

      game.version += 1;
      sendEventToGame(game, 'DuelPlayerGuessed');

      const allGuessed = game.players.every((id) => !!round.guesses[id]);
      if (allGuessed) {
        resolveRound(game);
      }

      endNoContent(res, req);
      return;
    }

    writeJson(res, 404, { message: 'Unhandled local route', path: fullUrl.pathname, method: req.method, host }, req);
  });

  const wsServer = new WebSocket.Server({ noServer: true });
  server.on('upgrade', (req, socket, head) => {
    wsServer.handleUpgrade(req, socket, head, (ws) => {
      wsServer.emit('connection', ws, req);
    });
  });

  wsServer.on('connection', (ws, req) => {
    const host = String(req.headers['x-riogeo-original-host'] || req.headers.host || '').toLowerCase();
    const urlObj = new URL(req.url || '/', 'http://local-engine');
    ws.__remoteIp = originIpFromReq(req);
    ws.__forwardedUsername = forwardedUsernameFromReq(req);

    ws.on('message', (raw) => {
      const message = tryParseJson(raw);
      if (!message) {
        return;
      }

      if (host.includes(GAME_SERVER_WS_HOST) && message.code === 'SubscribeToMatchmaking') {
        const protocolPlayerId = message.playerId || (ws.__remoteIp && playersByIp.get(ws.__remoteIp)) || null;
        if (!protocolPlayerId) {
          return;
        }

        const localIdentity = buildLocalIdentity(protocolPlayerId, ws.__forwardedUsername);
        if (!localIdentity) {
          return;
        }

        cancelPendingQueueRemoval(localIdentity);

        if (ws.__remoteIp) {
          playersByIp.set(ws.__remoteIp, localIdentity);
        }
        if (ws.__forwardedUsername) {
          localIdentityByUsername.set(ws.__forwardedUsername, localIdentity);
        }
        ws.__localIdentity = localIdentity;
        ws.__protocolPlayerId = protocolPlayerId;
        localSocketsByIdentity.set(localIdentity, ws);
        protocolPlayerIdByIdentity.set(localIdentity, protocolPlayerId);

        const known = matchmakingPlayers.get(localIdentity);
        queueMatchmakingPlayer(localIdentity, {
          username: ws.__forwardedUsername || (known && known.username) || protocolPlayerId,
          ip: ws.__remoteIp || ((known && known.ip) || ''),
          protocolPlayerId,
          isSteam: true,
          lastSeenAt: nowIso()
        });

        ws.send(JSON.stringify(makeMatchmakingEvent('MatchmakingJoined', protocolPlayerId, {
          gameModes: Array.isArray(message.gameModes) ? message.gameModes : ['StandardDuels']
        })));

        tryAutoMatchmake();
        return;
      }

      if (host.includes(GS2_HOST)) {
        if (!ws.__localIdentity && message.playerId) {
          const inferredIdentity = buildLocalIdentity(message.playerId, ws.__forwardedUsername);
          if (inferredIdentity) {
            ws.__localIdentity = inferredIdentity;
            ws.__protocolPlayerId = message.playerId;
            protocolPlayerIdByIdentity.set(inferredIdentity, message.playerId);
            if (ws.__forwardedUsername) {
              localIdentityByUsername.set(ws.__forwardedUsername, inferredIdentity);
            }
          }
        }

        const gs2Match = urlObj.pathname.match(/^\/[^/]+\/([^/]+)\/ws$/);
        const gameId = gs2Match ? gs2Match[1] : null;
        const game = gameId ? localGames.get(gameId) : null;
        if (!game) {
          return;
        }

        game.gs2Clients.add(ws);

        if (message.code === 'SubscribeToLobby' || message.code === 'SubscribeToLiveStream') {
          if (!game.started) {
            game.started = true;
            sendEventToGame(game, 'DuelStarted');

            if (game.startTransitionTimer) {
              clearTimeout(game.startTransitionTimer);
            }
            game.startTransitionTimer = setTimeout(() => {
              void transitionGameToOngoing(game);
            }, 100);
          } else {
            sendEventToGame(game, 'DuelStarted');
          }
        }
      }
    });

    ws.on('close', () => {
      const identity = ws.__localIdentity || (ws.__remoteIp ? playersByIp.get(ws.__remoteIp) : '');
      if (identity && localSocketsByIdentity.get(identity) === ws) {
        localSocketsByIdentity.delete(identity);
      }
      if (identity && playerSessions.get(identity) && playerSessions.get(identity).clientToProxyWebSocket === ws) {
        playerSessions.delete(identity);
      }
      if (identity) {
        scheduleQueueRemoval(identity);
      }
      if (ws.__forwardedUsername && localIdentityByUsername.get(ws.__forwardedUsername) === identity) {
        localIdentityByUsername.delete(ws.__forwardedUsername);
      }
      for (const game of localGames.values()) {
        game.gs2Clients.delete(ws);
      }
    });
  });

  server.listen(LOCAL_ENGINE_PORT, '127.0.0.1');
  return { server, wsServer };
}

function createIdentityServer() {
  const server = http.createServer(async (req, res) => {
    const urlObj = new URL(req.url || '/', 'http://identity-server');
    const corsHeaders = {
      'access-control-allow-origin': '*',
      'access-control-allow-headers': 'content-type',
      'access-control-allow-methods': 'GET,POST,OPTIONS'
    };

    if (req.method === 'OPTIONS') {
      res.writeHead(204, corsHeaders);
      res.end();
      return;
    }

    if (req.method === 'POST' && urlObj.pathname === '/register') {
      const body = await readJsonBody(req);
      const registration = registerIdentityFromRequest(req, body.username || body.identity || body.playerName);
      if (!registration) {
        res.writeHead(400, Object.assign({ 'content-type': 'application/json; charset=utf-8' }, corsHeaders));
        res.end(JSON.stringify({ message: 'username is required' }));
        return;
      }

      res.writeHead(200, Object.assign({ 'content-type': 'application/json; charset=utf-8' }, corsHeaders));
      res.end(JSON.stringify(registration));
      return;
    }

    if (req.method === 'GET' && urlObj.pathname === '/whoami') {
      const ip = originIpFromReq(req);
      const playerId = ip ? playersByIp.get(ip) || null : null;
      const username = ip ? usernamesByIp.get(ip) || null : null;
      res.writeHead(200, Object.assign({ 'content-type': 'application/json; charset=utf-8' }, corsHeaders));
      res.end(JSON.stringify({ ip, username, playerId }));
      return;
    }

    if (req.method === 'GET' && (urlObj.pathname === '/matchmaking' || urlObj.pathname === '/api/matchmaking')) {
      res.writeHead(200, Object.assign({ 'content-type': 'application/json; charset=utf-8' }, corsHeaders));
      res.end(JSON.stringify({
        autoMode: matchmakingAutoMode.enabled,
        waitingPlayers: listWaitingPlayers()
      }));
      return;
    }

    if (req.method === 'GET' && (urlObj.pathname === '/matchmaking/ui' || urlObj.pathname === '/api/matchmaking/ui')) {
      res.writeHead(200, Object.assign({ 'content-type': 'text/html; charset=utf-8' }, corsHeaders));
      res.end(matchmakingAdminHtml());
      return;
    }

    if (req.method === 'POST' && (urlObj.pathname === '/matchmaking/auto' || urlObj.pathname === '/api/matchmaking/auto')) {
      const body = await readJsonBody(req);
      if (typeof body.enabled === 'boolean') {
        matchmakingAutoMode.enabled = body.enabled;
      }
      if (matchmakingAutoMode.enabled) {
        tryAutoMatchmake();
      }
      res.writeHead(200, Object.assign({ 'content-type': 'application/json; charset=utf-8' }, corsHeaders));
      res.end(JSON.stringify({ autoMode: matchmakingAutoMode.enabled }));
      return;
    }

    if (req.method === 'POST' && (urlObj.pathname === '/matchmaking/pair' || urlObj.pathname === '/api/matchmaking/pair')) {
      const body = await readJsonBody(req);
      const playerA = toSafeString(body.playerA).trim();
      const playerB = toSafeString(body.playerB).trim();
      if (!playerA || !playerB || playerA === playerB) {
        res.writeHead(400, Object.assign({ 'content-type': 'application/json; charset=utf-8' }, corsHeaders));
        res.end(JSON.stringify({ message: 'two different players are required' }));
        return;
      }

      if (!matchmakingPlayers.has(playerA) || !matchmakingPlayers.has(playerB)) {
        res.writeHead(404, Object.assign({ 'content-type': 'application/json; charset=utf-8' }, corsHeaders));
        res.end(JSON.stringify({ message: 'one or both players are not waiting' }));
        return;
      }

      const game = manualPairPlayers(playerA, playerB);
      res.writeHead(200, Object.assign({ 'content-type': 'application/json; charset=utf-8' }, corsHeaders));
      res.end(JSON.stringify({
        gameId: game.gameId,
        playerA,
        playerB,
        autoMode: matchmakingAutoMode.enabled
      }));
      return;
    }

    res.writeHead(404, Object.assign({ 'content-type': 'application/json; charset=utf-8' }, corsHeaders));
    res.end(JSON.stringify({ message: 'not found' }));
  });

  server.listen(ADMIN_PORT, '0.0.0.0');
  return server;
}

function main() {
  ensureDir(SSL_CA_DIR);
  ensureDir(LOG_DIR);

  const logPaths = createLogPaths();
  const identityServer = createIdentityServer();
  const localEngine = createLocalEngine();
  const proxy = new Proxy();

  proxy.use(Proxy.gunzip);

  proxy.onError((ctx, err, errorKind) => {
    const req = ctx && ctx.clientToProxyRequest ? ctx.clientToProxyRequest : null;
    const event = {
      ts: nowIso(),
      type: 'error',
      errorKind: errorKind || 'error',
      host: req && req.headers ? req.headers.host || '' : '',
      url: req ? req.url || '' : '',
      message: err ? err.message : 'Unknown error'
    };
    const humanLine = formatHumanEntry([
      event.ts,
      'ERROR',
      event.errorKind,
      event.host,
      event.url,
      event.message
    ]);
    writeEvent(logPaths, event, humanLine);
  });

  proxy.onRequest((ctx, callback) => {
    const state = ensureTrafficState(ctx);
    const req = ctx.clientToProxyRequest;
    const effectiveHost = req && req.headers
      ? String(req.headers['x-riogeo-original-host'] || state.host || '').toLowerCase()
      : state.host;

    if (shouldHandleLocalHttp(effectiveHost, req ? req.url : '/')) {
      state.host = effectiveHost;
      ctx.proxyToServerRequestOptions.host = '127.0.0.1';
      ctx.proxyToServerRequestOptions.port = LOCAL_ENGINE_PORT;
      ctx.proxyToServerRequestOptions.protocol = 'http:';
      ctx.proxyToServerRequestOptions.agent = proxy.httpAgent;
      ctx.proxyToServerRequestOptions.headers = ctx.proxyToServerRequestOptions.headers || {};
      ctx.proxyToServerRequestOptions.headers.host = `127.0.0.1:${LOCAL_ENGINE_PORT}`;
      ctx.proxyToServerRequestOptions.headers['x-riogeo-original-host'] = state.host || '';
      return callback();
    }

    const event = {
      ts: nowIso(),
      type: 'request',
      protocol: state.protocol,
      method: state.method,
      host: state.host,
      url: buildRequestUrl(ctx),
      headers: snapshotHeaders(req ? req.headers : {}),
      bodyPreview: ''
    };
    const humanLine = formatHumanEntry([
      event.ts,
      'REQUEST',
      event.protocol,
      event.method,
      event.host,
      event.url
    ]);
    writeEvent(logPaths, event, humanLine);
    return callback();
  });

  proxy.onRequestData((ctx, chunk, callback) => {
    const state = ensureTrafficState(ctx);
    registerRequestChunk(state, chunk);
    return callback(null, chunk);
  });

  proxy.onRequestEnd((ctx, callback) => {
    const state = ensureTrafficState(ctx);
    const req = ctx.clientToProxyRequest;
    const bodyPreview = buildBodyPreview(
      state.requestChunks,
      req && req.headers ? req.headers['content-type'] : '',
      BODY_PREVIEW_LIMIT,
      state.requestBytes
    );

    if (bodyPreview) {
      const event = {
        ts: nowIso(),
        type: 'request-body',
        protocol: state.protocol,
        method: state.method,
        host: state.host,
        url: buildRequestUrl(ctx),
        bytes: state.requestBytes,
        bodyPreview
      };
      const humanLine = formatHumanEntry([
        event.ts,
        'REQUEST_BODY',
        event.protocol,
        event.method,
        event.host,
        event.url,
        `${event.bytes} bytes`,
        bodyPreview
      ]);
      writeEvent(logPaths, event, humanLine);
    }

    return callback();
  });

  proxy.onResponse((ctx, callback) => {
    const state = ensureTrafficState(ctx);
    const res = ctx.serverToProxyResponse;
    state.responseStatusCode = res ? res.statusCode : null;
    state.responseHeaders = res ? snapshotHeaders(res.headers) : {};

    const event = {
      ts: nowIso(),
      type: 'response',
      protocol: state.protocol,
      method: state.method,
      host: state.host,
      url: buildRequestUrl(ctx),
      statusCode: state.responseStatusCode,
      headers: state.responseHeaders,
      durationMs: formatDurationMs(state.startedAt)
    };
    const humanLine = formatHumanEntry([
      event.ts,
      'RESPONSE',
      event.protocol,
      event.statusCode,
      event.method,
      event.host,
      event.url,
      event.durationMs
    ]);
    writeEvent(logPaths, event, humanLine);
    return callback();
  });

  proxy.onResponseData((ctx, chunk, callback) => {
    const state = ensureTrafficState(ctx);
    registerResponseChunk(state, chunk);
    return callback(null, chunk);
  });

  proxy.onResponseEnd((ctx, callback) => {
    const state = ensureTrafficState(ctx);
    const res = ctx.serverToProxyResponse;
    const bodyPreview = buildBodyPreview(
      state.responseChunks,
      res && res.headers ? res.headers['content-type'] : '',
      BODY_PREVIEW_LIMIT,
      state.responseBytes
    );

    if (bodyPreview) {
      const event = {
        ts: nowIso(),
        type: 'response-body',
        protocol: state.protocol,
        method: state.method,
        host: state.host,
        url: buildRequestUrl(ctx),
        statusCode: state.responseStatusCode,
        bytes: state.responseBytes,
        bodyPreview
      };
      const humanLine = formatHumanEntry([
        event.ts,
        'RESPONSE_BODY',
        event.protocol,
        event.statusCode,
        event.method,
        event.host,
        event.url,
        `${event.bytes} bytes`,
        bodyPreview
      ]);
      writeEvent(logPaths, event, humanLine);
    }

    return callback();
  });

  proxy.onWebSocketConnection((ctx, callback) => {
    const state = ensureTrafficState(ctx);
    state.websocketStartedAt = Date.now();
    const req = ctx.clientToProxyWebSocket && ctx.clientToProxyWebSocket.upgradeReq ? ctx.clientToProxyWebSocket.upgradeReq : null;
    if (req && req.headers && req.headers.host) {
      state.host = req.headers.host;
    }
    const url = req ? buildRequestUrl({ clientToProxyRequest: req, isSSL: ctx.isSSL }) : buildRequestUrl(ctx);
    const event = {
      ts: nowIso(),
      type: 'websocket-connect',
      protocol: ctx.isSSL ? 'wss' : 'ws',
      host: state.host,
      url,
      headers: snapshotHeaders(req ? req.headers : {})
    };
    const humanLine = formatHumanEntry([
      event.ts,
      'WS_CONNECT',
      event.protocol,
      event.host,
      event.url
    ]);
    writeEvent(logPaths, event, humanLine);

    const host = wsHost(ctx);
    if (shouldHandleLocalWs(host)) {
      const target = new URL('ws://127.0.0.1');
      target.port = String(LOCAL_ENGINE_PORT);
      target.pathname = req && req.url ? req.url.split('?')[0] : '/';
      target.search = req && req.url && req.url.includes('?') ? req.url.slice(req.url.indexOf('?')) : '';
      ctx.proxyToServerWebSocketOptions.url = target.toString();
      ctx.proxyToServerWebSocketOptions.headers = ctx.proxyToServerWebSocketOptions.headers || {};
      ctx.proxyToServerWebSocketOptions.headers.host = `127.0.0.1:${LOCAL_ENGINE_PORT}`;
      ctx.proxyToServerWebSocketOptions.headers['x-riogeo-original-host'] = state.host || '';
    }

    return callback();
  });

  proxy.onWebSocketFrame((ctx, type, fromServer, data, flags, callback) => {
    const state = ensureTrafficState(ctx);
    let nextData = data;
    let nextFlags = flags;

    const host = wsHost(ctx);
    const message = type === 'message' ? tryParseJson(data) : null;

    if (LOCAL_MATCHMAKING_ENABLED && type === 'message' && fromServer && host.includes(GS2_HOST) && message && message.code === 'DuelStarted') {
      const patched = patchDuelStartedWithRandomKml(message);
      nextData = Buffer.from(JSON.stringify(patched), 'utf8');
      nextFlags = false;
    }

    const buffer = Buffer.isBuffer(nextData) ? nextData : Buffer.from(toSafeString(nextData));
    const preview = previewBody(buffer, '', WS_PREVIEW_LIMIT);
    const direction = fromServer ? 'server->client' : 'client->server';
    const event = {
      ts: nowIso(),
      type: 'websocket-frame',
      frameType: type,
      direction,
      protocol: ctx.isSSL ? 'wss' : 'ws',
      host: state.host,
      url: buildRequestUrl(ctx),
      flags: flags || {},
      bytes: buffer.length,
      preview
    };
    const humanLine = formatHumanEntry([
      event.ts,
      'WS_FRAME',
      event.protocol,
      direction,
      type,
      event.host,
      event.url,
      `${event.bytes} bytes`,
      preview
    ]);
    writeEvent(logPaths, event, humanLine);
    return callback(null, nextData, nextFlags);
  });

  proxy.onWebSocketMessage((ctx, message, flags, callback) => {
    const state = ensureTrafficState(ctx);
    const buffer = Buffer.isBuffer(message) ? message : Buffer.from(toSafeString(message));
    const preview = previewBody(buffer, '', WS_PREVIEW_LIMIT);
    const event = {
      ts: nowIso(),
      type: 'websocket-message',
      protocol: ctx.isSSL ? 'wss' : 'ws',
      host: state.host,
      url: buildRequestUrl(ctx),
      bytes: buffer.length,
      preview
    };
    const humanLine = formatHumanEntry([
      event.ts,
      'WS_MESSAGE',
      event.protocol,
      event.host,
      event.url,
      `${event.bytes} bytes`,
      preview
    ]);
    writeEvent(logPaths, event, humanLine);
    return callback(null, message, flags);
  });

  proxy.onWebSocketClose((ctx, code, message, callback) => {
    const state = ensureTrafficState(ctx);
    const duration = state.websocketStartedAt ? formatDurationMs(state.websocketStartedAt) : '-';
    const event = {
      ts: nowIso(),
      type: 'websocket-close',
      protocol: ctx.isSSL ? 'wss' : 'ws',
      host: state.host,
      url: buildRequestUrl(ctx),
      code,
      message: toSafeString(message),
      durationMs: duration
    };
    const humanLine = formatHumanEntry([
      event.ts,
      'WS_CLOSE',
      event.protocol,
      code,
      event.host,
      event.url,
      duration,
      event.message
    ]);
    writeEvent(logPaths, event, humanLine);
    return callback(null, code, message);
  });

  proxy.listen({
    port: PROXY_PORT,
    host: PROXY_HOST,
    sslCaDir: SSL_CA_DIR,
    forceSNI: true
  });

  console.log(formatHumanEntry([nowIso(), 'LISTENING', `${PROXY_HOST}:${PROXY_PORT}`, 'sslCaDir', SSL_CA_DIR]));
  console.log(formatHumanEntry([nowIso(), 'IDENTITY_SERVER', `0.0.0.0:${ADMIN_PORT}`, identityServer ? 'running' : 'off']));
  console.log(formatHumanEntry([nowIso(), 'LOCAL_ENGINE', `127.0.0.1:${LOCAL_ENGINE_PORT}`, localEngine ? 'running' : 'off']));
  console.log(formatHumanEntry([nowIso(), 'LOCAL_MATCHMAKING', LOCAL_MATCHMAKING_ENABLED ? 'enabled' : 'disabled']));
  console.log(formatHumanEntry([nowIso(), 'KML_POINTS', kmlPoints.length]));
  console.log(formatHumanEntry([nowIso(), 'TIP', 'Configure your system proxy to point at this host:port']));
  console.log(formatHumanEntry([nowIso(), 'TIP', 'Import', path.join(SSL_CA_DIR, 'certs', 'ca.pem'), 'into Trusted Root CA to inspect HTTPS/WSS']));
  console.log(formatHumanEntry([nowIso(), 'TIP', 'Logs', logPaths.human, 'and', logPaths.jsonl]));
}

main();
