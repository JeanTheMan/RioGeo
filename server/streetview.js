#!/usr/bin/env node

const https = require('node:https');
const { URL } = require('node:url');

const DEFAULT_LAT = 51.5074;
const DEFAULT_LNG = -0.1278;
const DEFAULT_RADIUS = 50;
const DEFAULT_TIMEOUT_MS = 15000;
const DEFAULT_SOURCE = 'outdoor';
const DEFAULT_API_KEY = 'AIzaSyDqRTXlnHXELLKn7645Q1L_5oc4CswKZK4';

function parseNumber(value, fallback) {
  if (value === undefined || value === null || value === '') {
    return fallback;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function getArgValue(name) {
  const prefix = `--${name}=`;
  const found = process.argv.find((argument) => argument.startsWith(prefix));
  if (found) {
    return found.slice(prefix.length);
  }

  const index = process.argv.indexOf(`--${name}`);
  if (index !== -1 && process.argv[index + 1] && !process.argv[index + 1].startsWith('--')) {
    return process.argv[index + 1];
  }

  return undefined;
}

function toSafeString(value) {
  if (value === undefined || value === null) {
    return '';
  }
  return String(value);
}

function resolveApiKey(explicitApiKey) {
  const key =
    toSafeString(explicitApiKey).trim() ||
    toSafeString(process.env.GOOGLE_MAPS_API_KEY).trim() ||
    toSafeString(process.env.MAPS_API_KEY).trim() ||
    DEFAULT_API_KEY;
  return toSafeString(key).trim();
}

function requestJson(url, timeoutMs = DEFAULT_TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    const request = https.get(url, (response) => {
      let body = '';

      response.setEncoding('utf8');
      response.on('data', (chunk) => {
        body += chunk;
      });

      response.on('end', () => {
        if (response.statusCode && response.statusCode >= 400) {
          reject(new Error(`HTTP ${response.statusCode}: ${body || response.statusMessage}`));
          return;
        }

        try {
          resolve(JSON.parse(body));
        } catch (error) {
          reject(new Error(`Failed to parse JSON response: ${error.message}`));
        }
      });
    });

    request.on('error', reject);
    request.setTimeout(Math.max(1000, parseNumber(timeoutMs, DEFAULT_TIMEOUT_MS)), () => {
      request.destroy(new Error('Street View metadata request timed out'));
    });
  });
}

async function snapPointToNearestRoad(lat, lng, options = {}) {
  const inputLat = parseNumber(lat, NaN);
  const inputLng = parseNumber(lng, NaN);
  if (!Number.isFinite(inputLat) || !Number.isFinite(inputLng)) {
    throw new Error('Invalid latitude/longitude supplied to snapPointToNearestRoad');
  }

  const roadsUrl = new URL('https://routing.openstreetmap.de/routed-car/route/v1/driving/' +
    `${inputLng},${inputLat};${inputLng},${inputLat}`);
  roadsUrl.searchParams.set('overview', 'false');
  roadsUrl.searchParams.set('geometries', 'polyline');
  roadsUrl.searchParams.set('steps', 'true');

  const roadsData = await requestJson(roadsUrl, options.timeoutMs);
  const waypoints = Array.isArray(roadsData && roadsData.waypoints) ? roadsData.waypoints : [];
  const waypoint = waypoints[0];
  if (!waypoint || !Array.isArray(waypoint.location) || waypoint.location.length < 2) {
    return null;
  }

  const snappedLng = parseNumber(waypoint.location[0], NaN);
  const snappedLat = parseNumber(waypoint.location[1], NaN);
  if (!Number.isFinite(snappedLat) || !Number.isFinite(snappedLng)) {
    return null;
  }

  return {
    lat: snappedLat,
    lng: snappedLng,
    placeId: ''
  };
}

async function getStreetViewPanorama(lat, lng, options = {}) {
  const apiKey = resolveApiKey(options.apiKey);
  if (!apiKey) {
    throw new Error('Missing Google Maps API key. Set GOOGLE_MAPS_API_KEY or MAPS_API_KEY.');
  }

  const inputLat = parseNumber(lat, NaN);
  const inputLng = parseNumber(lng, NaN);
  if (!Number.isFinite(inputLat) || !Number.isFinite(inputLng)) {
    throw new Error('Invalid latitude/longitude supplied to getStreetViewPanorama');
  }

  const roadOnly = options.roadOnly !== false;
  const strictRoadOnly = options.strictRoadOnly === true;
  const source = toSafeString(options.source).trim() || DEFAULT_SOURCE;
  let lookupLat = inputLat;
  let lookupLng = inputLng;

  if (roadOnly) {
    try {
      const snappedRoadPoint = await snapPointToNearestRoad(inputLat, inputLng, {
        timeoutMs: options.timeoutMs
      });
      if (!snappedRoadPoint) {
        if (strictRoadOnly) {
          return {
            found: false,
            status: 'NO_ROAD',
            panoId: '',
            lat: inputLat,
            lng: inputLng,
            raw: null
          };
        }
      } else {
        lookupLat = snappedRoadPoint.lat;
        lookupLng = snappedRoadPoint.lng;
      }
    } catch (error) {
      if (strictRoadOnly) {
        return {
          found: false,
          status: 'ROAD_SNAP_FAILED',
          panoId: '',
          lat: inputLat,
          lng: inputLng,
          raw: { message: toSafeString(error && error.message) }
        };
      }
      // Best-effort mode: continue with outdoor-only Street View lookup.
    }
  }

  const url = new URL('https://maps.googleapis.com/maps/api/streetview/metadata');
  url.searchParams.set('location', `${lookupLat},${lookupLng}`);
  url.searchParams.set('key', apiKey);
  url.searchParams.set('radius', String(Math.max(1, Math.round(parseNumber(options.radius, DEFAULT_RADIUS)))));
  url.searchParams.set('source', source);

  const data = await requestJson(url, options.timeoutMs);
  const panoId = toSafeString(data && (data.pano_id || data.panoId)).trim();
  const panoLat = parseNumber(data && data.location ? data.location.lat : undefined, NaN);
  const panoLng = parseNumber(data && data.location ? data.location.lng : undefined, NaN);

  if (data.status !== 'OK' || !panoId || !Number.isFinite(panoLat) || !Number.isFinite(panoLng)) {
    return {
      found: false,
      status: toSafeString(data && data.status).trim() || 'UNKNOWN',
      panoId: '',
      lat: inputLat,
      lng: inputLng,
      raw: data
    };
  }

  return {
    found: true,
    status: data.status,
    panoId,
    lat: panoLat,
    lng: panoLng,
    countryCode: toSafeString(options.countryCode).trim(),
    heading: parseNumber(options.heading, 0),
    pitch: parseNumber(options.pitch, 0),
    zoom: parseNumber(options.zoom, 0),
    raw: data
  };
}

module.exports = {
  getStreetViewPanorama,
  snapPointToNearestRoad,
  requestJson,
  parseNumber
};

async function main() {
  const apiKey = resolveApiKey(getArgValue('api-key'));
  if (!apiKey) {
    console.error('Missing API key. Set GOOGLE_MAPS_API_KEY or MAPS_API_KEY.');
    process.exitCode = 1;
    return;
  }

  const lat = parseNumber(getArgValue('lat') ?? process.env.LAT, DEFAULT_LAT);
  const lng = parseNumber(getArgValue('lng') ?? process.env.LNG, DEFAULT_LNG);

  const pano = await getStreetViewPanorama(lat, lng, {
    apiKey,
    radius: parseNumber(getArgValue('radius') ?? process.env.RADIUS, DEFAULT_RADIUS),
    source: toSafeString(getArgValue('source') ?? process.env.SOURCE).trim() || DEFAULT_SOURCE,
    roadOnly: toSafeString(getArgValue('road-only') ?? process.env.ROAD_ONLY).trim().toLowerCase() !== 'false',
    strictRoadOnly: toSafeString(getArgValue('strict-road-only') ?? process.env.STRICT_ROAD_ONLY).trim() === 'true',
    timeoutMs: parseNumber(getArgValue('timeout-ms') ?? process.env.TIMEOUT_MS, DEFAULT_TIMEOUT_MS)
  });

  console.log(JSON.stringify(pano, null, 2));
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error.message);
    process.exitCode = 1;
  });
}
