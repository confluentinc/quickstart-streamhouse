"use strict";

// ---- config ----
const OUR_CAR = 88;
// One orbit of the track == one race lap. We don't hardcode the duration because
// it varies by feed (live = seconds_per_lap, default 20s; mock replays much faster)
// and the live dashboard can't read seconds_per_lap from the Kafka data. Instead we
// measure the wall-clock time between lap-number changes and orbit at that rate.
const DEFAULT_LAP_MS = 20000; // orbit period until the feed's lap cadence is observed
const LAP_MS_MIN = 800;       // clamp measured cadence to sane bounds
const LAP_MS_MAX = 120000;
const GAP_SCALE = 0.012;      // seconds of gap -> fraction of a lap on the map
// Corrections (lap sync, gap changes) may change a car's speed by at most this share of
// normal pace, so they read as a car speeding up or easing off — never running backwards.
const MAX_CORRECTION = 0.4;

const TEAM_COLORS = {
  "River Racing": "#1fc77b",
  "Titan Dynamics": "#3b7bff",
  "Apex Motorsport": "#ff7a1a",
  "Scuderia Rossa": "#ff2b2b",
  "Sterling GP": "#00c8c8",
  "Aston Verde": "#1f8f6a",
  "Alpine Force": "#2f6bd8",
  "Sauber Spirit": "#7cff5a",
  "Haas Velocity": "#c9ced6",
  "Williams Heritage": "#5aa9ff",
  "Racing Bulls": "#5b6bff",
};
const teamColor = (t) => TEAM_COLORS[t] || "#9aa7b8";

// ---- state ----
let latest = null;
const carOffsets = new Map();  // car_number -> displayed lap fraction behind the leader

const mapEl = document.getElementById("map");
const mapMsg = document.getElementById("map-msg");

// Places a car from its lap fraction (0..1 from the start/finish line, in race
// direction); set once the circuit map is up.
let placeCarAt = null;

window.addEventListener("load", async () => {
  try {
    placeCarAt = await initMap();
  } catch (err) {
    // Leaflet comes from cdnjs, so an offline laptop or a venue that blocks the CDN
    // ends up here; the rest of the pit wall keeps working without the map.
    console.warn("Circuit map unavailable:", err);
    mapEl.classList.add("hidden");
    mapMsg.classList.remove("hidden");
  }
});

// Visual orbit state — one lap of the track per race lap, paced by the feed.
let lapPeriodMs = DEFAULT_LAP_MS;       // measured time for one race lap
let orbitPhase = 0;                     // 0..1 position around the track
let lastFrameMs = performance.now();
let lapSeenNum = null;                  // last lap number observed
let lapSeenAt = 0;                      // when we observed it (performance.now)
let phaseNudge = 0;                     // lap-sync correction still to be applied

// Track how fast the feed advances laps so the animation matches race speed, and
// phase-lock the leader to the start/finish line whenever a new lap begins: the lap
// number is clock-driven by the simulator, so this is the only moment we know where
// the leader "is". The correction is spread out (see animate) rather than snapped.
function trackLapCadence(lap) {
  if (lap == null) return;
  const now = performance.now();
  if (lapSeenNum != null && lap > lapSeenNum) {
    const observed = (now - lapSeenAt) / (lap - lapSeenNum);
    lapPeriodMs = clamp(0.5 * lapPeriodMs + 0.5 * observed, LAP_MS_MIN, LAP_MS_MAX);
    lapSeenNum = lap;
    lapSeenAt = now;
    phaseNudge = wrapHalf(-orbitPhase);
  } else if (lapSeenNum == null || lap < lapSeenNum) {
    // First snapshot (we joined mid-lap, so don't sync yet), or the race looped back
    // to lap 1 — reset the baseline.
    if (lapSeenNum != null) phaseNudge = wrapHalf(-orbitPhase);
    lapSeenNum = lap;
    lapSeenAt = now;
  }
}

// ---- websocket ----
function connect() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws`);
  const conn = document.getElementById("conn");
  ws.onopen = () => { conn.textContent = "● LIVE"; conn.className = "conn online"; };
  ws.onclose = () => {
    conn.textContent = "● RECONNECTING"; conn.className = "conn offline";
    setTimeout(connect, 1500);
  };
  ws.onmessage = (e) => { latest = JSON.parse(e.data); render(latest); };
}
connect();

// ---- render on each snapshot ----
function render(s) {
  document.getElementById("lap").textContent = s.lap || "--";
  trackLapCadence(s.lap);
  renderBoard(s.standings);
  renderTelemetry(s.telemetry, s.standings);
  renderAnomaly(s);
  renderAgent(s);
}

function renderBoard(standings) {
  const body = document.getElementById("board-body");
  body.innerHTML = "";
  for (const c of standings) {
    const tr = document.createElement("tr");
    if (c.car_number === OUR_CAR) tr.className = "us";
    const pit = c.in_pit_lane ? '<span class="pit-flag">PIT</span>' : "";
    tr.innerHTML =
      `<td>${c.position ?? "-"}</td>` +
      `<td>${c.car_number}</td>` +
      `<td>${c.driver ?? ""}${pit}</td>` +
      `<td><span class="dot ${c.tire_compound}"></span>${c.tire_compound ?? ""}</td>` +
      `<td>${c.tire_age_laps ?? "-"}</td>` +
      `<td>${c.position === 1 ? "LEADER" : "+" + fmt(c.gap_to_leader_sec, 1)}</td>` +
      `<td>${c.pit_stops ?? 0}</td>`;
    body.appendChild(tr);
  }
}

function renderTelemetry(t, standings) {
  const us = (standings || []).find((c) => c.car_number === OUR_CAR);
  const posPill = document.getElementById("telem-pos");
  posPill.textContent = us && us.position ? "P" + us.position : "";
  if (!t) return;
  const temps = { fl: t.tire_temp_fl_c, fr: t.tire_temp_fr_c, rl: t.tire_temp_rl_c, rr: t.tire_temp_rr_c };
  const anomaly = !!(latest && latest.car_state && latest.car_state.anomaly_tire_temp_fl);
  for (const el of document.querySelectorAll(".tyre")) {
    const k = el.dataset.k;
    const temp = temps[k];
    const bar = el.querySelector("i");
    const val = el.querySelector(".tyre-val");
    bar.style.width = clamp((temp - 90) / 60 * 100, 6, 100) + "%";
    bar.style.background = heatColor(temp);
    val.textContent = temp != null ? Math.round(temp) + "°" : "--";
    el.classList.toggle("alert", k === "fl" && anomaly);
  }
  setBar("m-fuel", "m-fuel-v", t.fuel_remaining_kg / 44 * 100, fmt(t.fuel_remaining_kg, 1) + "kg");
  setBar("m-batt", "m-batt-v", t.battery_charge_pct, Math.round(t.battery_charge_pct) + "%");
  document.getElementById("ro-speed").textContent = Math.round(t.speed_kph ?? 0);
  document.getElementById("ro-throttle").textContent = Math.round(t.throttle_pct ?? 0) + "%";
  document.getElementById("ro-drs").textContent = t.drs_active ? "ON" : "off";
}

function renderAnomaly(s) {
  const locked = document.getElementById("anomaly-locked");
  const live = document.getElementById("anomaly-live");
  if (!s.reveal.car_state || !s.car_state) {
    locked.classList.remove("hidden"); live.classList.add("hidden"); return;
  }
  locked.classList.add("hidden"); live.classList.remove("hidden");
  const cs = s.car_state;
  const status = document.getElementById("anom-status");
  const alert = !!cs.anomaly_tire_temp_fl;
  status.textContent = alert ? "⚠ ANOMALY" : "NOMINAL";
  status.classList.toggle("alert", alert);
  document.getElementById("anom-fl").textContent = fmt(cs.tire_temp_fl_c, 0) + "°C";
  document.getElementById("anom-compound").textContent = cs.tire_compound ?? "--";
  document.getElementById("anom-age").textContent = (cs.tire_age_laps ?? "--") + " laps";
}

function renderAgent(s) {
  const locked = document.getElementById("agent-locked");
  const live = document.getElementById("agent-live");
  if (!s.reveal.pit_decisions || !s.decisions.length) {
    locked.classList.remove("hidden"); live.classList.add("hidden"); return;
  }
  locked.classList.add("hidden"); live.classList.remove("hidden");
  const d = s.decisions[s.decisions.length - 1];
  const banner = document.getElementById("agent-banner");
  banner.textContent = d.suggestion || "--";
  banner.className = "agent-banner " + sugClass(d.suggestion);
  const feed = document.getElementById("agent-feed");
  feed.innerHTML = "";
  for (const x of [...s.decisions].reverse()) {
    const li = document.createElement("li");
    li.innerHTML = `<span class="f-lap">L${x.lap}</span>` +
      `<span class="f-sug ${(x.suggestion || "").replace(/ /g, "-")}">${x.suggestion}</span>` +
      `<span class="f-ctx">${x.condition_summary || ""}</span>`;
    feed.appendChild(li);
  }
}

// ---- track animation loop ----
function animate() {
  const now = performance.now();
  const dt = now - lastFrameMs;
  lastFrameMs = now;
  if (latest && placeCarAt) {
    // Advance the orbit by the fraction of a lap elapsed since the last frame, folding
    // in a rate-limited slice of any pending lap-sync correction.
    const pace = dt / lapPeriodMs;
    const limit = MAX_CORRECTION * pace;
    const fix = clamp(phaseNudge, -limit, limit);
    phaseNudge -= fix;
    orbitPhase = (((orbitPhase + pace + fix) % 1) + 1) % 1;
    for (const c of latest.standings) {
      // Gaps only update once per lap (and jump at pit stops); glide to the new one
      // instead of teleporting. Worst case pace - limit - limit stays above zero.
      const target = (c.gap_to_leader_sec || 0) * GAP_SCALE;
      const prev = carOffsets.get(c.car_number);
      const offset = prev == null ? target : prev + clamp(target - prev, -limit, limit);
      carOffsets.set(c.car_number, offset);
      let frac = orbitPhase - offset;
      frac = ((frac % 1) + 1) % 1;
      placeCarAt(c, frac);
    }
  }
  requestAnimationFrame(animate);
}
requestAnimationFrame(animate);

// ---- circuit map (Leaflet) ----
// circuits/<name>.geojson is the GP centreline, starting at the start/finish line and
// ordered in race direction, so a lap fraction maps straight onto distance along it.
// The server picks the default circuit (PITWALL_CIRCUIT / --circuit) and renders it as
// the selected option of #circuit-select; the dropdown swaps circuits live, and a
// viewer's pick is remembered in this browser and in the URL (?circuit=).
const circuitSelect = document.getElementById("circuit-select");
const CIRCUIT_STORE_KEY = "pitwall.circuit";

function initialCircuit() {
  const known = new Set([...circuitSelect.options].map((o) => o.value));
  let saved = null;
  try { saved = localStorage.getItem(CIRCUIT_STORE_KEY); } catch (_) { /* storage blocked */ }
  for (const k of [new URLSearchParams(location.search).get("circuit"), saved]) {
    if (k && known.has(k)) return k;
  }
  return circuitSelect.value;
}

async function fetchCircuit(key) {
  const res = await fetch(`/static/circuits/${encodeURIComponent(key)}.geojson`);
  if (!res.ok) throw new Error(`circuit ${key} HTTP ${res.status}`);
  const line = (await res.json()).features[0].geometry.coordinates.map(([lon, lat]) => [lat, lon]);
  // Cumulative distance along the line; equirectangular metres are plenty at this scale.
  const kx = Math.cos(line[0][0] * Math.PI / 180);
  const cum = [0];
  for (let i = 1; i < line.length; i++) {
    const dy = line[i][0] - line[i - 1][0];
    const dx = (line[i][1] - line[i - 1][1]) * kx;
    cum.push(cum[i - 1] + Math.hypot(dx, dy));
  }
  return { line, cum, total: cum[cum.length - 1] };
}

async function initMap() {
  if (!window.L) throw new Error("Leaflet not loaded");
  let key = initialCircuit();
  let geo = await fetchCircuit(key);
  const map = L.map(mapEl, { zoomSnap: 0.25, scrollWheelZoom: false, attributionControl: true });
  // Standard OSM tiles (no API key), darkened in CSS to match the pit wall theme.
  L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    className: "map-tiles",
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors · circuit: ' +
      '<a href="https://github.com/bacinger/f1-circuits">bacinger/f1-circuits</a>',
  }).addTo(map);
  window.addEventListener("resize", () => map.invalidateSize());
  // Own pane under the cars (overlayPane is z 400), so a swapped-in circuit never covers them.
  map.createPane("circuit").style.zIndex = 350;

  let trackLine = null, startMark = null;
  function drawCircuit() {
    if (trackLine) trackLine.remove();
    if (startMark) startMark.remove();
    trackLine = L.polyline(geo.line, { pane: "circuit", className: "map-track", weight: 7, interactive: false })
      .addTo(map);
    startMark = L.circleMarker(geo.line[0], { pane: "circuit", className: "map-start", radius: 4, weight: 2, interactive: false })
      .addTo(map);
    map.fitBounds(trackLine.getBounds(), { padding: [16, 16] });
    circuitSelect.value = key;
    document.title = `River Racing · Pit Wall · ${circuitSelect.selectedOptions[0].textContent}`;
  }
  drawCircuit();

  circuitSelect.disabled = false;
  circuitSelect.addEventListener("change", async () => {
    const next = circuitSelect.value;
    try {
      geo = await fetchCircuit(next);
    } catch (err) {
      console.warn(err);
      circuitSelect.value = key;
      return;
    }
    key = next;
    seg = 0;
    drawCircuit();
    try { localStorage.setItem(CIRCUIT_STORE_KEY, key); } catch (_) { /* storage blocked */ }
    const url = new URL(location.href);
    url.searchParams.set("circuit", key);
    history.replaceState(null, "", url);
  });

  const markers = new Map();
  let seg = 0; // last segment hit; cars are drawn in a sweep so this rarely moves far
  function pointAtFraction(frac) {
    const { line, cum, total } = geo;
    const d = frac * total;
    if (seg > cum.length - 2 || d < cum[seg] || d > cum[seg + 1]) {
      seg = 0;
      while (seg < cum.length - 2 && cum[seg + 1] < d) seg++;
    }
    const t = (d - cum[seg]) / ((cum[seg + 1] - cum[seg]) || 1);
    const a = line[seg], b = line[seg + 1];
    return [a[0] + t * (b[0] - a[0]), a[1] + t * (b[1] - a[1])];
  }

  return (c, frac) => {
    const ll = pointAtFraction(frac);
    let m = markers.get(c.car_number);
    if (!m) {
      const tip = `#${c.car_number} ${c.driver ?? ""}`;
      if (c.car_number === OUR_CAR) {
        // A DOM marker so #88 gets its label and glow, and sits above the pack's pane.
        m = L.marker(ll, {
          icon: L.divIcon({ className: "map-car-ours", html: "<span>88</span>", iconSize: [24, 24] }),
          keyboard: false,
        }).bindTooltip(tip, { className: "map-tip" });
      } else {
        m = L.circleMarker(ll, {
          radius: 6, weight: 1, color: "#06121a", fillColor: teamColor(c.team), fillOpacity: 1,
        }).bindTooltip(tip, { className: "map-tip" });
      }
      m.addTo(map);
      markers.set(c.car_number, m);
    } else {
      m.setLatLng(ll);
    }
  };
}

// ---- helpers ----
function setBar(barId, valId, pct, text) {
  document.getElementById(barId).style.width = clamp(pct, 0, 100) + "%";
  document.getElementById(valId).textContent = text;
}
function clamp(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }
function wrapHalf(v) { return v - Math.round(v); }   // wrap a phase delta into [-0.5, 0.5]
function fmt(v, d) { return v == null ? "--" : Number(v).toFixed(d); }
function heatColor(t) {
  if (t == null) return "#2a3a4e";
  const p = clamp((t - 95) / 50, 0, 1);   // 95°C green -> 145°C red
  const hue = 120 * (1 - p);
  return `hsl(${hue}, 80%, 48%)`;
}
function sugClass(s) {
  if (s === "PIT NOW") return "pit-now";
  if (s === "PIT SOON") return "pit-soon";
  return "stay-out";
}
