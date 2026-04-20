#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const http = require('http');
const https = require('https');
const { URL } = require('url');
const readline = require('readline');
const { Proxy } = require('http-mitm-proxy');

function parseArgs(argv) {
  const args = {
    serverHost: '127.0.0.1',
    serverPort: 8080,
    adminPort: 19081,
    listenPort: 8899,
    profileDir: '',
    username: '',
    captureJs: false,
    printFlags: true
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith('--')) {
      continue;
    }

    const next = argv[index + 1];
    switch (token) {
      case '--server':
        if (next && !next.startsWith('--')) {
          args.serverHost = next;
          index += 1;
        }
        break;
      case '--server-port':
        if (next && !next.startsWith('--')) {
          args.serverPort = Number(next);
          index += 1;
        }
        break;
      case '--admin-port':
        if (next && !next.startsWith('--')) {
          args.adminPort = Number(next);
          index += 1;
        }
        break;
      case '--listen-port':
        if (next && !next.startsWith('--')) {
          args.listenPort = Number(next);
          index += 1;
        }
        break;
      case '--profile-dir':
        if (next && !next.startsWith('--')) {
          args.profileDir = next;
          index += 1;
        }
        break;
      case '--username':
        if (next && !next.startsWith('--')) {
          args.username = next;
          index += 1;
        }
        break;
      case '--no-flags':
        args.printFlags = false;
        break;
      case '--capture-js':
        args.captureJs = true;
        break;
      default:
        break;
    }
  }

  return args;
}

function ask(question) {
  const interfaceHandle = readline.createInterface({ input: process.stdin, output: process.stdout });
  return new Promise((resolve) => {
    interfaceHandle.question(question, (answer) => {
      interfaceHandle.close();
      resolve(String(answer || '').trim());
    });
  });
}

function registerIdentity(serverHost, adminPort, username) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({ username });
    const request = http.request(
      {
        hostname: serverHost,
        port: adminPort,
        path: '/register',
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'content-length': Buffer.byteLength(body)
        }
      },
      (response) => {
        const chunks = [];
        response.on('data', (chunk) => chunks.push(chunk));
        response.on('end', () => {
          if (response.statusCode && response.statusCode >= 200 && response.statusCode < 300) {
            resolve(Buffer.concat(chunks).toString('utf8'));
            return;
          }
          reject(new Error(`registration failed with HTTP ${response.statusCode}`));
        });
      }
    );

    request.on('error', reject);
    request.write(body);
    request.end();
  });
}

function buildChromeFlags(listenPort, profileDirOverride) {
  const proxyValue = `127.0.0.1:${listenPort}`;
  const profileDir = profileDirOverride || path.join(process.env.LOCALAPPDATA || process.cwd(), 'RioGeo-Client');
  return [
    `--proxy-server=http=${proxyValue};https=${proxyValue}`,
    '--disable-quic',
    '--proxy-bypass-list=<-loopback>',
    `--user-data-dir=${profileDir}`
  ];
}

function shouldForwardToServer(host, url) {
  const h = String(host || '').toLowerCase();
  const u = String(url || '');
  return (
    (h.includes('game-server.geoguessr.com') && /\/api\/(duels|lobby)\//.test(u)) ||
    (h.includes('gs2.geoguessr.com') && /\/(pin|guess|ws)/.test(u))
  );
}

function shouldForwardWebSocketToServer(host) {
  const h = String(host || '').toLowerCase();
  return h.includes('game-server.geoguessr.com') || h.includes('gs2.geoguessr.com');
}

function isGeoGuessrRelatedRequest(headers) {
  const host = String((headers && headers.host) || '').toLowerCase();
  const origin = String((headers && headers.origin) || '').toLowerCase();
  const referer = String((headers && headers.referer) || '').toLowerCase();
  return host.includes('geoguessr') || origin.includes('geoguessr') || referer.includes('geoguessr');
}

function getPathnameFromRequestUrl(url, host) {
  const rawUrl = String(url || '');
  try {
    if (/^https?:\/\//i.test(rawUrl)) {
      return new URL(rawUrl).pathname || '/';
    }
    return new URL(rawUrl, `http://${host || 'localhost'}`).pathname || '/';
  } catch (error) {
    return rawUrl.split('?')[0] || '/';
  }
}

function sanitizeFilename(input) {
  return String(input || '')
    .replace(/[^a-zA-Z0-9._-]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 140) || 'asset';
}

function maybeInitJsCapture(ctx, options) {
  if (!options.captureJs) {
    return;
  }

  const request = ctx && ctx.clientToProxyRequest;
  if (!request || !request.headers) {
    return;
  }

  const host = String(request.headers.host || '').toLowerCase();
  const pathname = getPathnameFromRequestUrl(request.url, host);
  const lowerPath = pathname.toLowerCase();
  const isJsPath = lowerPath.endsWith('.js') || lowerPath.endsWith('.mjs') || lowerPath.endsWith('.cjs');
  if (!isJsPath) {
    return;
  }

  ctx.riogeoJsCapture = {
    host,
    pathname,
    startedAt: Date.now(),
    chunks: [],
    totalBytes: 0,
    dropped: false
  };
}

function ensureJsCaptureFromResponse(ctx) {
  if (!ctx || ctx.riogeoJsCapture) {
    return;
  }

  const request = ctx.clientToProxyRequest;
  const response = ctx.serverToProxyResponse;
  if (!request || !response) {
    return;
  }

  const contentType = String((response.headers && response.headers['content-type']) || '').toLowerCase();
  if (!contentType.includes('javascript') && !contentType.includes('ecmascript')) {
    return;
  }

  const host = String((request.headers && request.headers.host) || '').toLowerCase();
  const pathname = getPathnameFromRequestUrl(request.url, host);
  const capturePath = pathname.toLowerCase().endsWith('.js') ? pathname : `${pathname}.js`;

  ctx.riogeoJsCapture = {
    host,
    pathname: capturePath,
    startedAt: Date.now(),
    chunks: [],
    totalBytes: 0,
    dropped: false
  };
}

async function main() {
  const options = parseArgs(process.argv.slice(2));

  if (!options.serverHost) {
    options.serverHost = await ask('Enter server IP or host: ');
  }

  if (!options.username) {
    options.username = await ask('Enter username: ');
  }

  options.username = String(options.username || '').trim();
  if (!options.serverHost || !options.username) {
    throw new Error('server and username are required');
  }

  await registerIdentity(options.serverHost, options.adminPort, options.username);
  console.log(`Registered ${options.username} with ${options.serverHost}:${options.adminPort}`);

  const sslDir = path.resolve(process.env.CLIENT_SSL_CA_DIR || path.join(__dirname, '..', '.mitm-proxy-client'));
  const rawDir = path.resolve(process.env.CLIENT_RAW_DIR || path.join(__dirname, '..', 'raw'));
  const proxy = new Proxy();

  fs.mkdirSync(sslDir, { recursive: true });

  if (options.captureJs) {
    fs.mkdirSync(rawDir, { recursive: true });
    console.log(`JS capture enabled. Saving GeoGuessr JS assets to: ${rawDir}`);
  }

  proxy.use(Proxy.gunzip);

  proxy.onRequest((ctx, callback) => {
    const host = String(ctx.clientToProxyRequest.headers.host || '').toLowerCase();
    const url = String(ctx.clientToProxyRequest.url || '');

    if (shouldForwardToServer(host, url)) {
      const originalHost = ctx.clientToProxyRequest.headers.host || '';
      // For selected game endpoints, upstream is the RioGeo server proxy on plain HTTP.
      // http-mitm-proxy picks the request module from ctx.isSSL, so override it here.
      ctx.isSSL = false;
      ctx.proxyToServerRequestOptions.protocol = 'http:';
      ctx.proxyToServerRequestOptions.host = options.serverHost;
      ctx.proxyToServerRequestOptions.port = options.serverPort;
      ctx.proxyToServerRequestOptions.agent = proxy.httpAgent;
      ctx.proxyToServerRequestOptions.headers = ctx.proxyToServerRequestOptions.headers || {};
      ctx.proxyToServerRequestOptions.headers['x-riogeo-original-host'] = originalHost;
      ctx.proxyToServerRequestOptions.headers['x-riogeo-username'] = options.username;
    }

    maybeInitJsCapture(ctx, options);

    return callback();
  });

  proxy.onResponseData((ctx, chunk, callback) => {
    ensureJsCaptureFromResponse(ctx);
    const capture = ctx && ctx.riogeoJsCapture;
    if (capture && !capture.dropped) {
      capture.totalBytes += chunk.length;
      if (capture.totalBytes > 20 * 1024 * 1024) {
        capture.dropped = true;
        capture.chunks = [];
      } else {
        capture.chunks.push(Buffer.from(chunk));
      }
    }
    return callback(null, chunk);
  });

  proxy.onResponseEnd((ctx, callback) => {
    ensureJsCaptureFromResponse(ctx);
    const capture = ctx && ctx.riogeoJsCapture;
    if (capture && !capture.dropped && capture.chunks.length > 0) {
      const body = Buffer.concat(capture.chunks);
      const hash = crypto.createHash('sha1').update(body).digest('hex').slice(0, 10);
      const baseName = sanitizeFilename(path.basename(capture.pathname, '.js'));
      const hostName = sanitizeFilename(capture.host || 'unknown-host');
      const fileName = `${Date.now()}_${hostName}_${baseName}_${hash}.js`;
      const outPath = path.join(rawDir, fileName);
      fs.writeFile(outPath, body, (error) => {
        if (error) {
          console.error(`Failed to write JS capture ${outPath}: ${error.message || String(error)}`);
        } else {
          console.log(`Captured JS: ${outPath}`);
        }
      });
    }
    return callback();
  });

  proxy.onWebSocketConnection((ctx, callback) => {
    const host = String(ctx.clientToProxyWebSocket.upgradeReq.headers.host || '').toLowerCase();
    const url = String(ctx.clientToProxyWebSocket.upgradeReq.url || '');

    if (shouldForwardWebSocketToServer(host)) {
      const originalHost = ctx.clientToProxyWebSocket.upgradeReq.headers.host || '';
      // Upstream RioGeo server proxy listens on plain HTTP/WS.
      // Ensure http-mitm-proxy doesn't keep TLS agent settings for this leg.
      ctx.isSSL = false;
      ctx.proxyToServerWebSocketOptions.url = `ws://${options.serverHost}:${options.serverPort}${url.split('?')[0]}${url.includes('?') ? url.substring(url.indexOf('?')) : ''}`;
      ctx.proxyToServerWebSocketOptions.agent = proxy.httpAgent;
      ctx.proxyToServerWebSocketOptions.headers = ctx.proxyToServerWebSocketOptions.headers || {};
      ctx.proxyToServerWebSocketOptions.headers.host = originalHost;
      ctx.proxyToServerWebSocketOptions.headers['x-riogeo-original-host'] = originalHost;
      ctx.proxyToServerWebSocketOptions.headers['x-riogeo-username'] = options.username;
    }

    return callback();
  });

  proxy.listen({
    port: options.listenPort,
    host: '127.0.0.1',
    sslCaDir: sslDir,
    forceSNI: true
  });

  console.log(`\nLocal MITM proxy listening on 127.0.0.1:${options.listenPort}`);
  console.log(`Upstream server: ${options.serverHost}:${options.serverPort}`);
  console.log(`Connecting as: ${options.username}`);

  if (options.printFlags) {
    console.log('\nChrome flags:');
    for (const flag of buildChromeFlags(options.listenPort, options.profileDir)) {
      console.log(`  ${flag}`);
    }
  }

  console.log(`\nCA certificate: ${path.join(sslDir, 'certs', 'ca.pem')}`);
  console.log('Import this CA into your browser or system trust store.\n');
}

main().catch((error) => {
  console.error(error && error.stack ? error.stack : error.message || String(error));
  process.exit(1);
});
