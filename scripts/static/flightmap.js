"use strict";

// Live flight map: approximates each plane's position along its route.
//
// flight_status only carries ONE meaningful timestamp per flight (see
// airport_datagen.py Flight.estimated_at): the arrival time for a flight
// landing at the SFO hub, or the departure time for a flight leaving it.
// There is no real position feed. So this estimates the missing endpoint's
// time from the real distance between the two airports at a plausible
// cruise speed, then flies the plane along its route on a climb, cruise, and
// descent profile for "now" (planePosition). It's an honest approximation,
// not real tracking data.

const HUB = "SFO";
const CRUISE_KMH = 850; // typical narrow-body cruise speed
const GROUND_OVERHEAD_MIN = 25; // taxi + climb/descent, roughly constant regardless of distance

// Real, approximate airport coordinates for River Air's hub + 15 spokes
// (airport_datagen.py HUB/SPOKES).
const AIRPORTS = {
  SFO: [37.6, -122.4], ORD: [41.9, -87.9], SEA: [47.4, -122.3], LAX: [33.9, -118.4],
  DEN: [39.9, -104.7], PHX: [33.4, -112.0], DFW: [32.9, -97.0], ATL: [33.6, -84.4],
  JFK: [40.6, -73.8], BOS: [42.4, -71.0], MSP: [44.9, -93.2], LAS: [36.1, -115.2],
  SAN: [32.7, -117.2], PDX: [45.6, -122.6], SLC: [40.8, -111.9], IAH: [30.0, -95.3],
};

// Simple lat/lon bounding-box projection, calibrated to the continental US
// and to MAP_W/MAP_H's aspect ratio — good enough for a stylized backdrop,
// not a survey-grade map.
const BOUNDS = { lonMin: -125, lonMax: -66, latMin: 24, latMax: 49 };
const MAP_W = 900, MAP_H = 484;

function project([lat, lon]) {
  const x = ((lon - BOUNDS.lonMin) / (BOUNDS.lonMax - BOUNDS.lonMin)) * MAP_W;
  const y = ((BOUNDS.latMax - lat) / (BOUNDS.latMax - BOUNDS.latMin)) * MAP_H;
  return [x, y];
}

function unproject(x, y) {
  return [BOUNDS.latMax - (y / MAP_H) * (BOUNDS.latMax - BOUNDS.latMin),
    BOUNDS.lonMin + (x / MAP_W) * (BOUNDS.lonMax - BOUNDS.lonMin)];
}

// A simplified continental-US outline (real landmark coordinates, low-poly)
// projected through the same function used for the planes, so airports land
// in the right place relative to it even though the outline itself is a
// stylized approximation, not precise cartography.
const US_OUTLINE = [
  [48.4, -124.7], [46.9, -124.1], [44.6, -124.1], [42.8, -124.4], [41.8, -124.2],
  [39.4, -123.8], [37.8, -122.5], [36.6, -121.9], [34.4, -120.5], [33.8, -118.4],
  [32.7, -117.2], [32.5, -114.8], [31.9, -111.0], [31.3, -108.2], [31.8, -106.5],
  [29.8, -104.8], [29.4, -101.4], [28.4, -100.5], [26.4, -99.1], [25.9, -97.5],
  [27.8, -97.4], [29.3, -94.8], [29.9, -93.9], [29.7, -91.9], [29.2, -89.4],
  [30.3, -87.2], [30.4, -86.5], [30.1, -85.7], [29.7, -85.0], [29.9, -84.6],
  [28.9, -82.6], [26.1, -81.8], [25.5, -80.5], [25.1, -80.4], [26.7, -80.0],
  [28.5, -80.6], [30.7, -81.5], [32.0, -80.9], [33.9, -78.0], [35.2, -75.5],
  [36.9, -76.0], [38.3, -75.1], [39.4, -74.2], [40.6, -73.8], [41.3, -71.9],
  [41.6, -70.2], [43.6, -70.2], [44.8, -66.9], [45.9, -67.8], [46.7, -70.3],
  [45.0, -73.3], [44.5, -76.5], [43.3, -79.0], [42.0, -83.1], [45.8, -84.7],
  [46.9, -90.0], [48.0, -89.5], [49.0, -95.2], [49.0, -104.0], [49.0, -114.0],
  [49.0, -123.0], [48.4, -124.7],
];

function outlinePath() {
  return US_OUTLINE.map(([lat, lon], i) => {
    const [x, y] = project([lat, lon]);
    return `${i === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ") + " Z";
}

function haversineKm([lat1, lon1], [lat2, lon2]) {
  const R = 6371;
  const rad = Math.PI / 180;
  const dLat = (lat2 - lat1) * rad;
  const dLon = (lon2 - lon1) * rad;
  const a = Math.sin(dLat / 2) ** 2
    + Math.cos(lat1 * rad) * Math.cos(lat2 * rad) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(a));
}

function estimateDurationMs(origin, destination) {
  const a = AIRPORTS[origin], b = AIRPORTS[destination];
  if (!a || !b) return null;
  const hours = haversineKm(a, b) / CRUISE_KMH + GROUND_OVERHEAD_MIN / 60;
  return hours * 3600 * 1000;
}

// Lightning returns "2026-09-28 17:50:00.000000" (UTC, per AGENTS.md) — not
// full ISO 8601, so browsers may otherwise parse it as local time.
function parseTsMs(value) {
  if (value == null) return null;
  if (typeof value === "number") return value;
  const iso = String(value).trim().replace(" ", "T");
  const ms = Date.parse(/Z$|[+-]\d\d:?\d\d$/.test(iso) ? iso : `${iso}Z`);
  return Number.isNaN(ms) ? null : ms;
}

// Spelled out rather than the ISO "Z" suffix, which reads as an unexplained
// stray letter to anyone who doesn't already know Zulu-time notation.
function hhmmUTC(ms) {
  return ms == null ? "—" : `${new Date(ms).toISOString().slice(11, 16)} UTC`;
}

// The route curve, shared by the plane's own motion and the trajectory line
// drawn for a focused flight, so the plane always sits exactly on the line
// instead of cutting the corner across it. A quadratic Bezier bowed toward
// the top of the map (roughly how a great circle bows north on a flat
// US-scale projection) — not a precise geodesic.
function routeCurve(x0, y0, x1, y1) {
  const dx = x1 - x0, dy = y1 - y0;
  const len = Math.hypot(dx, dy) || 1;
  let px = -dy / len, py = dx / len;
  if (py > 0) { px = -px; py = -py; } // always bow "up" regardless of travel direction
  const bulge = len * 0.14;
  return { cx: (x0 + x1) / 2 + px * bulge, cy: (y0 + y1) / 2 + py * bulge };
}

function bezierPoint(x0, y0, cx, cy, x1, y1, t) {
  const mt = 1 - t;
  return [
    mt * mt * x0 + 2 * mt * t * cx + t * t * x1,
    mt * mt * y0 + 2 * mt * t * cy + t * t * y1,
  ];
}

function bezierTangentAngle(x0, y0, cx, cy, x1, y1, t) {
  const mt = 1 - t;
  const dx = 2 * mt * (cx - x0) + 2 * t * (x1 - cx);
  const dy = 2 * mt * (cy - y0) + 2 * t * (y1 - cy);
  return Math.atan2(dy, dx) * (180 / Math.PI);
}

// --- Flight profile ----------------------------------------------------------
// Every plane takes off at RUNWAY_KT, speeds up to cruise over its CLIMB_MIN
// climb, cruises, and slows back to RUNWAY_KT over the DESCENT_MIN to
// touchdown. The map places the plane by the distance this profile has flown
// along its route, and the info box reads the same profile's speed and
// altitude, so the two always agree.

const CLIMB_MIN = 20; // takeoff to cruise
const DESCENT_MIN = 25; // top of descent to landing
const RUNWAY_KT = 160; // ground speed at takeoff and touchdown
const HOLD_LAP_MIN = 4; // about one lap of the circle a late arrival flies before its descent
const ROUTE_SAMPLES = 48; // points along each route curve, for measuring distance on it
const KM_PER_NM = 1.852;
// Map units per nautical mile. The projection is true to scale north-south,
// and east-west too near SFO's latitude, where the holding circles are.
const NM_PX = MAP_H / (BOUNDS.latMax - BOUNDS.latMin) / 60;
// Each plane surges ±GUST_NM along its path, so its ground speed drifts a few
// knots like a real feed while still being exactly the rate it moves.
const GUST_NM = 0.02, GUST_S = 13;
const routeCache = new Map(); // "ORD-SFO" -> the route's curve and the distance along it

// A steady 0..1 value per flight, so each keeps its own cruise level and gusts.
function flightSeed(key) {
  let hash = 0;
  for (const char of key) hash = (hash * 31 + char.charCodeAt(0)) >>> 0;
  return hash / 2 ** 32;
}

// The route's curve (routeCurve) and the ground distance from the origin to
// each of ROUTE_SAMPLES points along it, measured once per route.
function routeGeometry(origin, destination) {
  const id = `${origin}-${destination}`;
  if (!routeCache.has(id)) {
    const [x0, y0] = project(AIRPORTS[origin]), [x1, y1] = project(AIRPORTS[destination]);
    const { cx, cy } = routeCurve(x0, y0, x1, y1);
    const points = Array.from({ length: ROUTE_SAMPLES + 1 }, (_, i) =>
      unproject(...bezierPoint(x0, y0, cx, cy, x1, y1, i / ROUTE_SAMPLES)));
    const nm = [0];
    for (let i = 1; i <= ROUTE_SAMPLES; i++) nm.push(nm[i - 1] + haversineKm(points[i - 1], points[i]) / KM_PER_NM);
    routeCache.set(id, { x0, y0, cx, cy, x1, y1, nm, lengthNm: nm[ROUTE_SAMPLES] });
  }
  return routeCache.get(id);
}

// The point `alongNm` from the origin, and the direction of travel there.
function pointAlong(route, alongNm) {
  const { x0, y0, cx, cy, x1, y1, nm } = route;
  let i = 1;
  while (i < ROUTE_SAMPLES && nm[i] < alongNm) i++;
  const t = Math.max(0, Math.min(1, (i - 1 + (alongNm - nm[i - 1]) / (nm[i] - nm[i - 1])) / ROUTE_SAMPLES));
  const [x, y] = bezierPoint(x0, y0, cx, cy, x1, y1, t);
  return { x, y, angleDeg: bezierTangentAngle(x0, y0, cx, cy, x1, y1, t) };
}

// Returns null if the flight can't be placed (unknown airport code), else the
// full timing/position picture for "now": where to draw the plane (x, y,
// angleDeg, phase — "waiting" at the gate, "enroute", or "done"), where it is
// on the flight profile (stage — "climb", "cruise", "hold", or "descent" —
// knots, level, toGoNm), and the three moments the info box shows. Only the
// departure OR arrival instant is real data (see file header); the other two
// are derived from the estimated duration, so they're approximations, not
// authoritative times.
//
// A late arrival keeps its scheduled takeoff and circles at the top of its
// descent until it can land at its estimated time. flight_status announces an
// arrival's delay while the plane would already be in the air, so moving its
// takeoff later instead would slide it backward along its route each time
// the delay grew.
function planePosition(row, nowMs) {
  const a = AIRPORTS[row.origin], b = AIRPORTS[row.destination];
  if (!a || !b) return null;
  const duration = estimateDurationMs(row.origin, row.destination);
  if (duration == null) return null;
  const isArrival = row.destination === HUB;
  const known = parseTsMs(row.scheduled_time);
  if (known == null) return null;
  const delayMs = (Number(row.delay_minutes) || 0) * 60000;

  let departureMs, scheduledArrivalMs, estimatedArrivalMs;
  if (isArrival) {
    scheduledArrivalMs = known;
    estimatedArrivalMs = known + delayMs;
    departureMs = Math.min(scheduledArrivalMs, estimatedArrivalMs) - duration;
  } else {
    departureMs = known + delayMs;
    scheduledArrivalMs = known + duration;
    estimatedArrivalMs = departureMs + duration;
  }

  let phase = "enroute";
  if (isArrival && row.status === "LANDED") phase = "done";
  else if (!isArrival && nowMs >= estimatedArrivalMs) phase = "done";
  else if (!isArrival && row.status !== "DEPARTED") phase = "waiting";

  // Minutes since takeoff, out of the flight time plus any hold.
  const route = routeGeometry(row.origin, row.destination);
  const flightMin = duration / 60000, totalMin = (estimatedArrivalMs - departureMs) / 60000;
  const minutes = phase === "done" ? totalMin
    : phase === "waiting" ? 0 : Math.max(0, Math.min(totalMin, (nowMs - departureMs) / 60000));
  const toLand = totalMin - minutes;
  // The cruise speed that covers the route in flightMin around the slower climb and descent.
  const ramps = (CLIMB_MIN + DESCENT_MIN) / 2;
  const cruiseKt = (route.lengthNm * 60 - RUNWAY_KT * ramps) / (flightMin - ramps);
  // Distance (nm) covered `m` minutes into a climb, or in the last `m` minutes of a descent, and the speed there.
  const rampNm = (m, rampMin) => (RUNWAY_KT * m + ((cruiseKt - RUNWAY_KT) * m * m) / (2 * rampMin)) / 60;
  const rampKt = (m, rampMin) => RUNWAY_KT + ((cruiseKt - RUNWAY_KT) * m) / rampMin;
  const holdNm = (cruiseKt * (totalMin - flightMin)) / 60;
  let stage = minutes < CLIMB_MIN ? "climb" : toLand < DESCENT_MIN ? "descent" : "cruise";
  let flownNm = stage === "climb" ? rampNm(minutes, CLIMB_MIN)
    : stage === "descent" ? route.lengthNm + holdNm - rampNm(toLand, DESCENT_MIN)
    : rampNm(CLIMB_MIN, CLIMB_MIN) + (cruiseKt * (minutes - CLIMB_MIN)) / 60;
  let knots = stage === "climb" ? rampKt(minutes, CLIMB_MIN)
    : stage === "descent" ? rampKt(toLand, DESCENT_MIN) : cruiseKt;
  const level = Math.max(0, Math.min(1, minutes / CLIMB_MIN, toLand / DESCENT_MIN)); // share of cruise altitude
  const gust = nowMs / 1000 / GUST_S + flightSeed(row.key) * 2 * Math.PI;
  flownNm += GUST_NM * Math.sin(gust) * level;
  knots += (GUST_NM / GUST_S) * 3600 * Math.cos(gust) * level;

  // The hold: whole laps, turning right as holds are flown, from the top of
  // descent, so the plane leaves the circle where it joined it.
  const holdAtNm = route.lengthNm - rampNm(DESCENT_MIN, DESCENT_MIN);
  const holding = flownNm > holdAtNm && flownNm < holdAtNm + holdNm;
  const alongNm = holding ? holdAtNm : flownNm > holdAtNm ? flownNm - holdNm : flownNm;
  let { x, y, angleDeg } = pointAlong(route, alongNm);
  if (holding) {
    const laps = Math.max(1, Math.round((totalMin - flightMin) / HOLD_LAP_MIN));
    const radius = holdNm / laps / (2 * Math.PI);
    const entry = (angleDeg * Math.PI) / 180, heading = entry + (flownNm - holdAtNm) / radius;
    x += radius * NM_PX * (Math.sin(heading) - Math.sin(entry));
    y += radius * NM_PX * (Math.cos(entry) - Math.cos(heading));
    angleDeg = ((((heading * 180) / Math.PI) % 360) + 540) % 360 - 180;
    stage = "hold";
  }
  return {
    x, y, angleDeg, phase, stage, knots, level, toGoNm: route.lengthNm - alongNm,
    x0: route.x0, y0: route.y0, x1: route.x1, y1: route.y1, cx: route.cx, cy: route.cy,
    departureMs, scheduledArrivalMs, estimatedArrivalMs, holdEndMs: estimatedArrivalMs - DESCENT_MIN * 60000,
  };
}

function statusColorClass(row, phase) {
  if (phase === "done") return "plane-done";
  if (phase === "waiting") return "plane-waiting";
  if (row.status === "DELAYED") return (row.delay_minutes ?? 0) >= 45 ? "plane-miss" : "plane-tight";
  return "plane-ok";
}

// The same color class for callers outside the map (the Operations table's
// status pills), so a flight's status reads the same color in both places.
function flightColorClass(row, nowMs = Date.now()) {
  const pos = planePosition(row, nowMs);
  return statusColorClass(row, pos ? pos.phase : "enroute");
}

// Simplified top-down airplane silhouette (nose points "up", i.e. -y, before
// rotation) plus a halo behind it for contrast against the map.
const PLANE_SVG = `<circle class="plane-halo" r="10"/><path d="
  M0,-13 L1.6,-9 L1.6,-1.5 L11,4 L11,6.2 L1.8,3.6 L1.8,8.5 L5,11 L5,12.6
  L0,11.2 L-5,12.6 L-5,11 L-1.8,8.5 L-1.8,3.6 L-11,6.2 L-11,4 L-1.6,-1.5 L-1.6,-9 Z
"/>`;

// Applying CSS transform-origin/transition to an SVG element with an
// attribute-based `transform` corrupts the attribute's coordinate space in
// Chromium (confirmed: planes render hundreds of px outside the map). So
// movement is animated here in plain JS/rAF instead of CSS, writing the
// `transform` attribute directly every frame — the same tween technique as
// animateNumber() in app.js, just for x/y/angle instead of a number.
const planeAnim = new Map(); // key -> { x, y, angleDeg, raf }

function angleLerp(from, to, t) {
  let delta = ((to - from + 540) % 360) - 180;
  return from + delta * t;
}

function setPlaneTransform(g, x, y, angleDeg) {
  g.setAttribute("transform", `translate(${x.toFixed(1)},${y.toFixed(1)}) rotate(${(angleDeg + 90).toFixed(1)})`);
}

function ensurePlaneEl(layer, key, onSelect) {
  let g = layer.querySelector(`[data-plane="${CSS.escape(key)}"]`);
  if (!g) {
    g = document.createElementNS("http://www.w3.org/2000/svg", "g");
    g.setAttribute("data-plane", key);
    g.className.baseVal = "plane";
    g.innerHTML = PLANE_SVG;
    if (onSelect) g.addEventListener("click", (e) => { e.stopPropagation(); onSelect(key); });
    layer.appendChild(g);
  }
  return g;
}

function animatePlaneTo(g, key, target, duration = 2200) {
  const from = planeAnim.get(key) || target;
  if (from.raf) cancelAnimationFrame(from.raf);
  const start = performance.now();
  const state = { ...target, raf: 0 };
  planeAnim.set(key, state);
  (function step(now) {
    const t = Math.min(1, (now - start) / duration);
    const eased = 1 - Math.pow(1 - t, 2);
    setPlaneTransform(
      g,
      from.x + (target.x - from.x) * eased,
      from.y + (target.y - from.y) * eased,
      angleLerp(from.angleDeg, target.angleDeg, eased),
    );
    if (t < 1) state.raf = requestAnimationFrame(step);
  })(start);
}

// Same curve planePosition() moves the plane along (routeCurve), so the
// dashed line and the plane never diverge, apart from a late arrival's
// holding circle a few miles beside it.
function trajectoryPath(x0, y0, cx, cy, x1, y1) {
  return `M${x0.toFixed(1)},${y0.toFixed(1)} Q${cx.toFixed(1)},${cy.toFixed(1)} ${x1.toFixed(1)},${y1.toFixed(1)}`;
}

// --- Simulated telemetry for the focused flight's info box -----------------
// There is no position feed (see the file header), so the readings come from
// the flight profile planePosition flies the plane on, refreshed every second
// (tickTelemetry): ground speed is how fast the plane moves on the map, and
// vertical speed is how fast its altitude changes.

const SWAY_FT = 20, SWAY_S = 10; // at cruise the altitude drifts ±SWAY_FT, about once a minute
const MI_PER_NM = 1.15078;

function el(className, text) {
  const node = document.createElement("div");
  node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function countdown(ms) {
  const s = Math.max(0, Math.round(ms / 1000));
  const two = (n) => String(n).padStart(2, "0");
  return `${Math.floor(s / 3600)}:${two(Math.floor(s / 60) % 60)}:${two(s % 60)}`;
}

function signed(value, unit) {
  const rounded = Math.round(value / 10) * 10;
  return `${rounded > 0 ? "+" : rounded < 0 ? "−" : ""}${Math.abs(rounded).toLocaleString("en-US")} ${unit}`;
}

// [label, value, fullWidth] rows for the focused flight at `nowMs`.
function telemetry(row, nowMs) {
  const pos = planePosition(row, nowMs);
  if (!pos) return [];
  // On the ground once past its estimated arrival: an arrival's LANDED update
  // can trail that by up to a minute, and never comes if the stream stops first.
  if (pos.phase === "done" || nowMs >= pos.estimatedArrivalMs) {
    return [["Altitude", "0 ft"], ["Ground speed", "0 kt"], ["Arrived", hhmmUTC(pos.estimatedArrivalMs), true]];
  }
  if (pos.phase === "waiting" || nowMs < pos.departureMs) {
    return [["Altitude", "0 ft"], ["Ground speed", "0 kt"], ["Departs in", countdown(pos.departureMs - nowMs), true]];
  }
  const seed = flightSeed(row.key);
  const cruiseFt = [33000, 35000, 37000, 39000][Math.floor(seed * 4)];
  const sway = nowMs / 1000 / SWAY_S + seed * 2 * Math.PI;
  const cruising = pos.stage === "cruise" || pos.stage === "hold";
  const altitude = cruiseFt * pos.level + (cruising ? SWAY_FT * Math.sin(sway) : 0);
  // ft/min: steady in the climb and descent, and the sway's own rate at cruise.
  const climbRate = cruising ? (SWAY_FT / SWAY_S) * 60 * Math.cos(sway)
    : pos.stage === "climb" ? cruiseFt / CLIMB_MIN : -cruiseFt / DESCENT_MIN;
  const [lat, lon] = unproject(pos.x, pos.y);
  const rows = [
    ["Altitude", `${(Math.round(altitude / 10) * 10).toLocaleString("en-US")} ft`],
    ["Ground speed", `${Math.round(pos.knots)} kt`],
    ["Vertical speed", signed(climbRate, "ft/min")],
    ["Heading", `${String((((Math.round(pos.angleDeg) + 90) % 360) + 360) % 360).padStart(3, "0")}°`],
    ["Position", `${lat.toFixed(2)}° N, ${Math.abs(lon).toFixed(2)}° W`, true],
    ["To go", `${Math.round(pos.toGoNm * MI_PER_NM).toLocaleString("en-US")} mi`],
    ["Arrives in", countdown(pos.estimatedArrivalMs - nowMs)],
  ];
  if (pos.stage === "hold") rows.push(["Holding for", countdown(pos.holdEndMs - nowMs), true]);
  return rows;
}

function telemetryBlock(row, nowMs) {
  const rows = telemetry(row, nowMs);
  const block = el("infobox-telemetry");
  block.dataset.labels = rows.map(([label]) => label).join("|");
  const head = el("infobox-telemetry-head", "Live telemetry");
  head.prepend(el("live-dot"));
  block.appendChild(head);
  for (const [label, value, fullWidth] of rows) {
    const cell = el(`infobox-tm${fullWidth ? " wide" : ""}`);
    cell.appendChild(el("infobox-label", label));
    cell.appendChild(el("infobox-value", value));
    block.appendChild(cell);
  }
  return block;
}

// Updates the readings in place each second, so the live dot keeps pulsing;
// rebuilds only when the set of readings changes (takeoff, a hold, landing).
function tickTelemetry(svg) {
  const block = document.querySelector("#map-infobox:not(.hidden) .infobox-telemetry");
  if (!block || !svg.__lastFocused) return;
  const rows = telemetry(svg.__lastFocused.row, Date.now());
  if (block.dataset.labels !== rows.map(([label]) => label).join("|")) {
    block.replaceWith(telemetryBlock(svg.__lastFocused.row, Date.now()));
    return;
  }
  block.querySelectorAll(".infobox-value").forEach((value, i) => { value.textContent = rows[i][1]; });
}

// A real HTML element (a sibling of the <svg> in .map-wrap, positioned
// absolute), not SVG text — SVG content is squeezed into whatever pixel box
// the <svg> actually renders at (both the base fit-to-panel scale AND the
// current zoom), so text set in "user units" never reliably matches the
// rest of the page's normal CSS pixel sizes no matter what value is picked.
// Plain HTML/CSS text sidesteps that entirely and always matches the
// dashboard's own type sizes exactly, at any zoom level or panel width.
function renderInfoBox(svg, row, pos) {
  const box = document.getElementById("map-infobox");
  const mapWrap = svg.closest(".map-wrap");
  const svgRect = svg.getBoundingClientRect();
  const wrapRect = mapWrap.getBoundingClientRect();
  const view = svg.__view;
  const pxPerUnitX = svgRect.width / view.w;
  const pxPerUnitY = svgRect.height / view.h;
  // The plane's position in CSS pixels relative to .map-wrap — the same
  // math for both the panel's fit-to-box scale and the current zoom/pan, so
  // nothing needs separate compensation the way the SVG-based version did.
  const planeX = (svgRect.left - wrapRect.left) + (pos.x - view.x) * pxPerUnitX;
  const planeY = (svgRect.top - wrapRect.top) + (pos.y - view.y) * pxPerUnitY;

  // Same thresholds as the rest of the app: delayed from 15 min (the
  // "Delayed" count), at risk of missed connections from 45.
  const delay = Number(row.delay_minutes) || 0;
  const lateClass = delay >= 45 ? "miss" : delay >= 15 ? "tight" : "";

  box.innerHTML = "";
  box.appendChild(el("infobox-title", row.key));
  box.appendChild(el("infobox-route", `${row.origin} → ${row.destination}`));
  for (const [label, ms, valueClass] of [
    ["Departure", pos.departureMs, ""],
    ["Sched. arrival", pos.scheduledArrivalMs, ""],
    ["Est. arrival", pos.estimatedArrivalMs, lateClass],
  ]) {
    const line = el("infobox-row");
    line.appendChild(el("infobox-label", label));
    line.appendChild(el(`infobox-value ${valueClass}`.trim(), hhmmUTC(ms)));
    box.appendChild(line);
  }
  box.appendChild(telemetryBlock(row, Date.now()));
  box.classList.remove("hidden");

  // Measure the box now that it has real content, then take the first spot
  // around the plane (right-above by default) that stays inside .map-wrap
  // and doesn't cover the zoom buttons, "Show all flights", or the legend.
  const w = box.offsetWidth, h = box.offsetHeight, gap = 18;
  const blockers = [...mapWrap.querySelectorAll(".map-controls, .map-show-all:not(.hidden), .map-legend")]
    .map((node) => {
      const r = node.getBoundingClientRect();
      return { l: r.left - wrapRect.left, t: r.top - wrapRect.top, r: r.right - wrapRect.left, b: r.bottom - wrapRect.top };
    });
  const fits = ([x, y]) => x >= 0 && y >= 0 && x + w <= wrapRect.width && y + h <= wrapRect.height
    && blockers.every((k) => x + w <= k.l || x >= k.r || y + h <= k.t || y >= k.b);
  const spots = [
    [planeX + gap, planeY - gap - h],
    [planeX + gap, planeY + gap],
    [planeX - gap - w, planeY - gap - h],
    [planeX - gap - w, planeY + gap],
  ];
  const [x, y] = spots.find(fits) || spots[0];
  // On a map too short for any spot beside the plane, keep the box inside the map anyway.
  box.style.left = `${Math.max(0, Math.min(x, wrapRect.width - w))}px`;
  box.style.top = `${Math.max(0, Math.min(y, wrapRect.height - h))}px`;
}

// Renders/updates plane markers for `flights` (the same rows /api/state
// returns) into the <svg> at `svgSelector`. Safe to call on every poll —
// markers are keyed by flight id and glide to their new position/heading.
// opts.focusedKey, when set, hides every other plane and draws that one
// flight's trajectory + info box instead.
function renderFlightMap(svgSelector, flights, opts = {}) {
  const svg = document.querySelector(svgSelector);
  if (!svg) return;
  const layer = svg.querySelector("#map-planes");
  const nowMs = Date.now();
  const seen = new Set();
  const highlightKey = opts.highlightKey;
  const focusedKey = opts.focusedKey;
  let focusedPos = null;

  for (const row of flights) {
    const pos = planePosition(row, nowMs);
    if (!pos) continue;
    seen.add(row.key);
    const g = ensurePlaneEl(layer, row.key, opts.onSelect);
    const target = { x: pos.x, y: pos.y, angleDeg: pos.angleDeg };
    if (planeAnim.has(row.key)) {
      animatePlaneTo(g, row.key, target);
    } else {
      setPlaneTransform(g, target.x, target.y, target.angleDeg);
      planeAnim.set(row.key, target);
    }
    const hidden = focusedKey && row.key !== focusedKey;
    g.setAttribute("class", [
      "plane", statusColorClass(row, pos.phase), pos.phase,
      row.key === highlightKey ? "plane-highlight" : "",
      row.key === focusedKey ? "plane-selected plane-focused" : "",
      hidden ? "plane-hidden" : "",
    ].join(" "));
    g.setAttribute("data-title", `${row.key}: ${row.origin} → ${row.destination} · ${row.status}`);
    if (row.key === focusedKey) focusedPos = { row, pos };
  }
  layer.querySelectorAll("[data-plane]").forEach((g) => {
    if (!seen.has(g.dataset.plane)) { planeAnim.delete(g.dataset.plane); g.remove(); }
  });

  const trajectory = svg.querySelector("#map-trajectory");
  const infobox = document.getElementById("map-infobox");
  // Cached so zoom/pan (setView, below) can re-run just the info box's own
  // sizing/position between polls, without waiting on the next /api/state.
  svg.__lastFocused = focusedPos;
  if (focusedPos) {
    trajectory.setAttribute("d", trajectoryPath(focusedPos.pos.x0, focusedPos.pos.y0, focusedPos.pos.cx, focusedPos.pos.cy, focusedPos.pos.x1, focusedPos.pos.y1));
    trajectory.classList.remove("hidden");
    renderInfoBox(svg, focusedPos.row, focusedPos.pos);
  } else {
    trajectory.classList.add("hidden");
    infobox.classList.add("hidden");
  }
}

// --- Zoom / pan: drives the viewBox directly so plane/airport/outline
// coordinates need no separate scaling logic. Buttons are the reliable path
// for a live demo; wheel-zoom and drag-pan are there for exploration. ---

const ZOOM_MIN_W = 90; // most zoomed-in (~10x)

function setView(svg, view) {
  view.w = Math.max(ZOOM_MIN_W, Math.min(MAP_W, view.w));
  view.h = view.w * (MAP_H / MAP_W);
  view.x = Math.max(0, Math.min(MAP_W - view.w, view.x));
  view.y = Math.max(0, Math.min(MAP_H - view.h, view.y));
  svg.__view = view;
  svg.setAttribute("viewBox", `${view.x.toFixed(1)} ${view.y.toFixed(1)} ${view.w.toFixed(1)} ${view.h.toFixed(1)}`);
  // The info box's constant-screen-size compensation (renderInfoBox) depends
  // on the current view, so it has to be redone right here on every zoom/pan
  // step — otherwise it only catches up at the next poll (up to 2.5s later),
  // during which it visibly scales with the map exactly like before.
  if (svg.__lastFocused) renderInfoBox(svg, svg.__lastFocused.row, svg.__lastFocused.pos);
}

function zoomAt(svg, factor, clientX, clientY) {
  const view = svg.__view;
  const rect = svg.getBoundingClientRect();
  const fx = rect.width ? (clientX - rect.left) / rect.width : 0.5;
  const fy = rect.height ? (clientY - rect.top) / rect.height : 0.5;
  const px = view.x + fx * view.w, py = view.y + fy * view.h;
  const w = view.w * factor;
  setView(svg, { x: px - fx * w, y: py - fy * (w * (MAP_H / MAP_W)), w });
}

function wireZoomPan(svg, container) {
  svg.addEventListener("wheel", (e) => {
    e.preventDefault();
    zoomAt(svg, e.deltaY > 0 ? 1.18 : 1 / 1.18, e.clientX, e.clientY);
  }, { passive: false });

  let dragging = null;
  svg.addEventListener("pointerdown", (e) => {
    if (e.target.closest(".plane")) return;
    dragging = { x: e.clientX, y: e.clientY, view: { ...svg.__view } };
    svg.setPointerCapture(e.pointerId);
    svg.classList.add("dragging");
  });
  svg.addEventListener("pointermove", (e) => {
    if (!dragging) return;
    const rect = svg.getBoundingClientRect();
    const scale = svg.__view.w / (rect.width || 1);
    setView(svg, {
      ...dragging.view,
      x: dragging.view.x - (e.clientX - dragging.x) * scale,
      y: dragging.view.y - (e.clientY - dragging.y) * scale,
    });
  });
  const endDrag = () => { dragging = null; svg.classList.remove("dragging"); };
  svg.addEventListener("pointerup", endDrag);
  svg.addEventListener("pointerleave", endDrag);

  container.querySelectorAll("[data-zoom]").forEach((button) =>
    button.addEventListener("click", () => {
      const rect = svg.getBoundingClientRect();
      const cx = rect.left + rect.width / 2, cy = rect.top + rect.height / 2;
      if (button.dataset.zoom === "reset") setView(svg, { x: 0, y: 0, w: MAP_W });
      else zoomAt(svg, button.dataset.zoom === "in" ? 1 / 1.5 : 1.5, cx, cy);
    }));
}

// Lat/lon lines every 5°, like an aeronautical chart. Drawn in map space so
// they pan and zoom with everything else.
function graticulePath() {
  const d = [];
  for (let lon = -125; lon <= -65; lon += 5) {
    const [x] = project([0, lon]);
    d.push(`M${x.toFixed(1)},0V${MAP_H}`);
  }
  for (let lat = 25; lat <= 45; lat += 5) {
    const [, y] = project([lat, 0]);
    d.push(`M0,${y.toFixed(1)}H${MAP_W}`);
  }
  return d.join("");
}

function buildMapSvg(svgSelector) {
  const svg = document.querySelector(svgSelector);
  if (!svg || svg.dataset.built) return;
  const outline = outlinePath();
  const [hx, hy] = project(AIRPORTS[HUB]);
  svg.innerHTML = `
    <defs>
      <linearGradient id="map-land" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0" stop-color="#13264a"/>
        <stop offset="1" stop-color="#0b1730"/>
      </linearGradient>
      <linearGradient id="map-route" x1="0" y1="0" x2="1" y2="0">
        <stop offset="0" stop-color="#22d3ee"/>
        <stop offset="1" stop-color="#a78bfa"/>
      </linearGradient>
    </defs>
    <path class="map-graticule" d="${graticulePath()}"></path>
    <path class="map-outline-glow" d="${outline}"></path>
    <path class="map-outline" d="${outline}"></path>
    ${Object.entries(AIRPORTS).map(([code, coord]) => {
      const [x, y] = project(coord);
      return `<circle class="map-airport ${code === HUB ? "map-hub" : ""}" cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="${code === HUB ? 4 : 2.2}"><title>${code}</title></circle>`;
    }).join("")}
    <circle class="map-hub-ring" cx="${hx.toFixed(1)}" cy="${hy.toFixed(1)}" r="4">
      <animate attributeName="r" values="4;16" dur="2.4s" repeatCount="indefinite"/>
      <animate attributeName="opacity" values=".8;0" dur="2.4s" repeatCount="indefinite"/>
    </circle>
    <path id="map-trajectory" class="map-trajectory hidden"></path>
    <g id="map-planes"></g>
  `;
  setView(svg, { x: 0, y: 0, w: MAP_W });
  const container = svg.closest(".map-wrap") || svg.parentElement;
  wireZoomPan(svg, container);
  window.setInterval(() => tickTelemetry(svg), 1000);
  svg.dataset.built = "1";
}

window.FlightMap = { buildMapSvg, renderFlightMap, flightColorClass };
