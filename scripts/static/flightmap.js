"use strict";

// Live flight map: draws each plane from its streamed telemetry.
//
// The generator publishes a reading per airborne flight every 5 seconds to
// the flight_telemetry topic (position, altitude, speeds, heading, stage,
// distance to go; see flight_profile.py), and /api/state returns each
// flight's latest one, read through Lightning. Between polls the browser
// flies the plane on from that reading (planeState). The readings are
// synthetic, not real tracking data.
//
// flight_status only carries ONE meaningful timestamp per flight (see
// airport_datagen.py Flight.estimated_at): the arrival time for a flight
// landing at the SFO hub, or the departure time for a flight leaving it. The
// info box's other times come from the real distance between the two
// airports at a plausible cruise speed (estimateDurationMs).

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

function bezierTangentAngle(x0, y0, cx, cy, x1, y1, t) {
  const mt = 1 - t;
  const dx = 2 * mt * (cx - x0) + 2 * t * (x1 - cx);
  const dy = 2 * mt * (cy - y0) + 2 * t * (y1 - cy);
  return Math.atan2(dy, dx) * (180 / Math.PI);
}

// --- Flight placement --------------------------------------------------------
// Where a plane is comes from its latest flight_telemetry reading (streamed by
// the generator, read through Lightning in /api/state). Between polls the
// plane flies on from that reading at its ground speed and heading, and climbs
// or descends at its vertical speed, for at most FRESH_MS. A flight with no
// reading, or a LANDED one, sits at its origin or destination.

// Map units per nautical mile. The projection is true to scale north-south,
// and east-west too near SFO's latitude, where the holding circles are.
const NM_PX = MAP_H / (BOUNDS.latMax - BOUNDS.latMin) / 60;
// How long a reading carries the plane on before it stops and the box's live
// dot goes stale. The generator sends one every 5 seconds.
const FRESH_MS = 15000;
const clampAge = (ms) => Math.max(0, Math.min(FRESH_MS, ms));

// The route's curve and the three moments the info box shows. Only the
// departure OR arrival instant is real data (see file header); the other two
// are derived from the estimated duration, so they're approximations, not
// authoritative times. A late arrival keeps its scheduled takeoff and circles
// until it can land at its estimated time, the same rule the generator flies.
function flightTimes(row) {
  const a = AIRPORTS[row.origin], b = AIRPORTS[row.destination];
  if (!a || !b) return null;
  const duration = estimateDurationMs(row.origin, row.destination);
  const known = parseTsMs(row.scheduled_time);
  if (known == null) return null;
  const isArrival = row.destination === HUB;
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
  const [x0, y0] = project(a), [x1, y1] = project(b);
  const { cx, cy } = routeCurve(x0, y0, x1, y1);
  return { isArrival, x0, y0, cx, cy, x1, y1, departureMs, scheduledArrivalMs, estimatedArrivalMs };
}

// "waiting" at the gate, "enroute", or "done", from flight_status alone.
function groundPhase(row, times, nowMs) {
  if (times.isArrival && row.status === "LANDED") return "done";
  if (!times.isArrival && nowMs >= times.estimatedArrivalMs) return "done";
  if (!times.isArrival && row.status !== "DEPARTED") return "waiting";
  return "enroute";
}

// A reading as the browser keeps it: the row plus when it was taken and how
// old it was when it arrived. `as_of` can run ahead of the server's clock
// (the generator's --speed), so the age is clamped at 0 rather than going
// negative and flying the plane backward.
function telemetrySample(reading, serverNowMs, receivedAtMs) {
  const asOfMs = parseTsMs(reading.as_of);
  return { reading, asOfMs, ageAtPollMs: clampAge((serverNowMs ?? receivedAtMs) - asOfMs), receivedAtMs };
}

// Returns null if the flight can't be placed (unknown airport code), else
// everything the map and the info box draw for "now": where the plane is
// (x, y, angleDeg, phase, onGround), how it is flying (stage, knots, altitudeFt,
// verticalFpm, headingDeg, toGoNm, holdEndMs), whether its reading has gone
// stale, the clock the box counts down on (frozen with the plane), and the
// flight's times and route curve.
function planeState(row, sample, nowMs) {
  const times = flightTimes(row);
  if (!times) return null;
  const reading = sample && sample.reading;
  const stage = reading ? String(reading.stage) : null;

  // flight_status saying it's down outranks a reading that hasn't caught up.
  let phase = groundPhase(row, times, nowMs);
  if (!reading || stage === "LANDED" || phase === "done" || nowMs >= times.estimatedArrivalMs) {
    if (stage === "LANDED") phase = "done";
    const atDestination = phase === "done" || nowMs >= times.estimatedArrivalMs;
    const t = atDestination ? 1 : 0;
    const [x, y] = atDestination ? [times.x1, times.y1] : [times.x0, times.y0];
    // In the air by its schedule, but no reading has arrived: the feed isn't readable yet.
    const awaitingTelemetry = !reading && phase === "enroute" && !atDestination && nowMs >= times.departureMs;
    return {
      ...times, x, y, phase, onGround: true, awaitingTelemetry, stale: false, clockMs: nowMs,
      angleDeg: bezierTangentAngle(times.x0, times.y0, times.cx, times.cy, times.x1, times.y1, t),
    };
  }

  // Seconds flown since the reading, stopping at FRESH_MS.
  const sinceMs = sample.ageAtPollMs + Math.max(0, nowMs - sample.receivedAtMs);
  const elapsedS = Math.min(FRESH_MS, sinceMs) / 1000;
  const knots = Number(reading.ground_speed_kt);
  const verticalFpm = Number(reading.vertical_speed_fpm);
  const headingDeg = Number(reading.heading_deg);
  const angleDeg = headingDeg - 90;
  const angle = (angleDeg * Math.PI) / 180;
  const movedNm = (knots * elapsedS) / 3600;
  const [rx, ry] = project([Number(reading.latitude), Number(reading.longitude)]);
  return {
    ...times,
    x: rx + movedNm * NM_PX * Math.cos(angle),
    y: ry + movedNm * NM_PX * Math.sin(angle),
    angleDeg, phase: "enroute", onGround: false, stage, knots, verticalFpm, headingDeg,
    altitudeFt: Math.max(0, Number(reading.altitude_ft) + (verticalFpm / 60) * elapsedS),
    // A hold circles beside the route, so it doesn't bring the plane closer.
    toGoNm: Math.max(0, Number(reading.distance_to_go_nm) - (stage === "HOLDING" ? 0 : movedNm)),
    holdEndMs: parseTsMs(reading.hold_ends_at),
    stale: sinceMs >= FRESH_MS,
    clockMs: nowMs - Math.max(0, sinceMs - FRESH_MS),
  };
}

// A flight is at risk (red) when it's 45+ minutes late or strands more than
// AT_RISK_PASSENGERS connections (airport_datagen.py AT_RISK_PASSENGERS).
const AT_RISK_PASSENGERS = 10;

function statusColorClass(row, phase) {
  if (phase === "done") return "plane-done";
  if (phase === "waiting") return "plane-waiting";
  if ((Number(row.affected_passengers) || 0) > AT_RISK_PASSENGERS) return "plane-miss";
  if (row.status === "DELAYED") return (row.delay_minutes ?? 0) >= 45 ? "plane-miss" : "plane-tight";
  return "plane-ok";
}

// The same color class for callers outside the map (the Operations table's
// status pills), so a flight's status reads the same color in both places.
function flightColorClass(row, nowMs = Date.now()) {
  const times = flightTimes(row);
  return statusColorClass(row, times ? groundPhase(row, times, nowMs) : "enroute");
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

// The same curve the generator flies the plane along (flight_profile.py), so
// the dashed line and the plane never diverge, apart from a late arrival's
// holding circle a few miles beside it.
function trajectoryPath(x0, y0, cx, cy, x1, y1) {
  return `M${x0.toFixed(1)},${y0.toFixed(1)} Q${cx.toFixed(1)},${cy.toFixed(1)} ${x1.toFixed(1)},${y1.toFixed(1)}`;
}

// --- Live telemetry for the focused flight's info box ------------------------
// The values come from the flight's flight_telemetry reading, flown on to
// "now" by planeState and refreshed every second (tickTelemetry). After
// FRESH_MS without a new reading they stop, and the live dot goes stale.

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

// [label, value, fullWidth] rows for the focused flight, from planeState.
function telemetry(pos) {
  if (!pos) return [];
  const nowMs = pos.clockMs;
  if (pos.awaitingTelemetry) {
    return [["Telemetry", "Waiting for telemetry", true], ["Arrives in", countdown(pos.estimatedArrivalMs - nowMs)]];
  }
  if (pos.onGround) {
    // On the ground once landed or past its estimated arrival: an arrival's
    // LANDED update can trail that by up to a minute, and never comes if the
    // stream stops first.
    if (pos.phase === "done" || nowMs >= pos.estimatedArrivalMs) {
      return [["Altitude", "0 ft"], ["Ground speed", "0 kt"], ["Arrived", hhmmUTC(pos.estimatedArrivalMs), true]];
    }
    return [["Altitude", "0 ft"], ["Ground speed", "0 kt"], ["Departs in", countdown(pos.departureMs - nowMs), true]];
  }
  const [lat, lon] = unproject(pos.x, pos.y);
  const rows = [
    ["Altitude", `${(Math.round(pos.altitudeFt / 10) * 10).toLocaleString("en-US")} ft`],
    ["Ground speed", `${Math.round(pos.knots)} kt`],
    ["Vertical speed", signed(pos.verticalFpm, "ft/min")],
    ["Heading", `${String(((Math.round(pos.headingDeg) % 360) + 360) % 360).padStart(3, "0")}°`],
    ["Position", `${lat.toFixed(2)}° N, ${Math.abs(lon).toFixed(2)}° W`, true],
    ["To go", `${Math.round(pos.toGoNm * MI_PER_NM).toLocaleString("en-US")} mi`],
    ["Arrives in", countdown(pos.estimatedArrivalMs - nowMs)],
  ];
  if (pos.stage === "HOLDING" && pos.holdEndMs != null) rows.push(["Holding for", countdown(pos.holdEndMs - nowMs), true]);
  return rows;
}

function telemetryBlock(pos) {
  const rows = telemetry(pos);
  const block = el("infobox-telemetry");
  block.dataset.labels = rows.map(([label]) => label).join("|");
  const head = el("infobox-telemetry-head", "Live telemetry");
  head.prepend(el(`live-dot${pos && pos.stale ? " stale" : ""}`));
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
  const focused = svg.__lastFocused;
  if (!block || !focused) return;
  const pos = planeState(focused.row, focused.sample, Date.now());
  const rows = telemetry(pos);
  if (block.dataset.labels !== rows.map(([label]) => label).join("|")) {
    block.replaceWith(telemetryBlock(pos));
    return;
  }
  block.querySelector(".live-dot").classList.toggle("stale", pos.stale);
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
  box.appendChild(telemetryBlock(planeState(row, svg.__lastFocused.sample, Date.now())));
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
// flight's trajectory + info box instead. opts.telemetry is /api/state's
// latest reading per flight, opts.serverNowMs its `now`, and opts.receivedAtMs
// when the browser got it.
// The reading each flight is drawn from. A reading already seen on an
// earlier poll keeps the time it first arrived, so a stopped stream lets the
// plane run out of FRESH_MS and freeze. When /api/state stops returning a
// flight's reading (it keeps two minutes), the last one stays, frozen, until
// the flight lands.
const samples = new Map(); // key -> telemetrySample

function latestSample(key, reading, serverNowMs, receivedAtMs) {
  const kept = samples.get(key);
  if (reading && (!kept || parseTsMs(reading.as_of) !== kept.asOfMs)) {
    samples.set(key, telemetrySample(reading, serverNowMs, receivedAtMs));
  } else if (!reading && kept && String(kept.reading.stage) === "LANDED") {
    samples.delete(key);
  }
  return samples.get(key) || null;
}

function renderFlightMap(svgSelector, flights, opts = {}) {
  const svg = document.querySelector(svgSelector);
  if (!svg) return;
  const layer = svg.querySelector("#map-planes");
  const nowMs = Date.now();
  const seen = new Set();
  const highlightKey = opts.highlightKey;
  const focusedKey = opts.focusedKey;
  const readings = opts.telemetry || {};
  const receivedAtMs = opts.receivedAtMs ?? nowMs;
  let focusedPos = null;

  for (const row of flights) {
    const sample = latestSample(row.key, readings[row.key], opts.serverNowMs, receivedAtMs);
    const pos = planeState(row, sample, nowMs);
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
    if (row.key === focusedKey) focusedPos = { row, sample, pos };
  }
  layer.querySelectorAll("[data-plane]").forEach((g) => {
    if (!seen.has(g.dataset.plane)) { planeAnim.delete(g.dataset.plane); g.remove(); }
  });
  for (const key of samples.keys()) if (!seen.has(key)) samples.delete(key);

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
