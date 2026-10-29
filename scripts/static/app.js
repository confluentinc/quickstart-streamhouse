"use strict";

FlightMap.buildMapSvg("#flight-map");

const $ = (id) => document.getElementById(id);
let current = { flights: [], delays: [], counts: {} };
let openFlight = null;
let selected = null;
let lastUpdated = null;
let noticeUntil = 0; // an offer action's message holds the status line until then, instead of the next poll's
let scrollToNow = true; // on load and after "Show all", scroll Operations to the flights around now
const rowWatermarks = new Map(); // key -> "status|delay_minutes|affected_passengers", for flash-on-change
const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (char) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[char]));

// Tweens a big number from its last rendered value to `to` so every 2.5s poll
// feels alive rather than silently jumping — the "engaging, real-time" cue.
function animateNumber(el, to, duration = 600) {
  to = Number(to) || 0;
  const from = Number(el.dataset.value || 0);
  if (from === to) { el.textContent = to; el.dataset.value = to; return; }
  const start = performance.now();
  el.dataset.value = to;
  (function step(now) {
    const progress = Math.min(1, (now - start) / duration);
    el.textContent = Math.round(from + (to - from) * (1 - Math.pow(1 - progress, 3)));
    if (progress < 1) requestAnimationFrame(step);
  })(start);
}

// Briefly flashes a row when a watched field changes between polls, so a
// status/delay/at-risk update is visible, not just quietly redrawn.
function flashIfChanged(rowEl, watchKey, watermark) {
  const previous = rowWatermarks.get(watchKey);
  if (previous !== undefined && previous !== watermark) {
    rowEl.classList.remove("flash");
    // eslint-disable-next-line no-unused-expressions
    rowEl.offsetWidth; // restart the CSS animation
    rowEl.classList.add("flash");
  }
  rowWatermarks.set(watchKey, watermark);
}

// Lightning returns timestamps like "2026-09-25 01:00:00.000000" (UTC); show HH:MM.
function hhmm(value) {
  if (value === null || value === undefined || value === "") return "";
  if (/^\d+$/.test(String(value))) return new Date(Number(value)).toISOString().slice(11, 16);
  const match = String(value).match(/[ T](\d{2}:\d{2})/);
  return match ? match[1] : String(value);
}

// Lightning's "2026-09-25 01:00:00.000000" and the API's ISO "now" are both UTC.
const utcMs = (value) => Date.parse(`${String(value).slice(0, 19).replace(" ", "T")}Z`);

const riskClass = (risk) => ({ HIGH: "MISS", OK: "OK" }[risk] || "other");
const label = (value) => String(value ?? "").replace(/_/g, " "); // ON_TIME -> ON TIME, display only

async function request(path, method = "GET") {
  const response = await fetch(path, { method });
  const body = await response.json();
  if (!response.ok) throw new Error(body.detail || response.statusText);
  return body;
}

async function refresh() {
  try {
    current = await request("/api/state");
    lastUpdated = Date.now();
    render();
    if (Date.now() >= noticeUntil) $("status-line").textContent = "Live from Lightning Tables";
  } catch (error) {
    $("status-line").textContent = error.message;
  }
}

function flightButton(id) {
  return `<button class="link" data-flight="${esc(id)}">${esc(id)}</button>`;
}

function selectFlight(flightId) {
  // Clicking the already-selected flight again (from the table or the map),
  // or an explicit null (the map's "Show all flights" button), deselects.
  clearPassenger();
  if (flightId === null || flightId === openFlight) {
    openFlight = null;
    scrollToNow = true;
    $("flight-empty").classList.remove("hidden");
    $("flight-table").classList.add("hidden");
    $("seat-map").classList.add("hidden");
    $("seat-tip").classList.add("hidden");
    render();
    return;
  }
  openFlight = flightId;
  render();
  refreshFlight(true);
}

// Selecting a passenger swaps Biggest delays for Hotel options (in
// renderPassenger, once the offers load); clicking the same passenger again,
// or changing flight, clears it and swaps Biggest delays back.
function selectPassenger(passengerId) {
  if (passengerId === selected) clearPassenger();
  else { selected = passengerId; renderPassenger(); }
  document.querySelectorAll("[data-passenger]").forEach((button) =>
    button.closest("tr").classList.toggle("selected", button.dataset.passenger === selected));
  document.querySelectorAll("#cabin .seat").forEach((seat) =>
    seat.classList.toggle("selected", seatHolders.get(seat.dataset.seat)?.key === selected));
}

function clearPassenger() {
  selected = null;
  $("passenger-card").className = "pax-empty";
  $("passenger-card").textContent = "Select a passenger.";
  $("hotels-panel").classList.add("hidden");
  $("delays-panel").classList.remove("hidden");
}

function bindFlights() {
  document.querySelectorAll("[data-flight]").forEach((button) =>
    button.onclick = () => selectFlight(button.dataset.flight));
}

function render() {
  const counts = current.counts || {};
  if (!$("counts").dataset.built) {
    $("counts").innerHTML = `
      <div class="count"><b id="count-flights" data-value="0">0</b><span>Flights today</span></div>
      <div class="count tight"><b id="count-delayed" data-value="0">0</b><span>Delayed</span></div>
      <div class="count miss"><b id="count-affected" data-value="0">0</b><span>Passengers at risk</span></div>
      <div class="count ok"><b id="count-booked" data-value="0">0</b><span>Booked</span></div>`;
    $("counts").dataset.built = "1";
  }
  animateNumber($("count-flights"), counts.flights ?? 0);
  animateNumber($("count-delayed"), counts.delayed ?? 0);
  animateNumber($("count-affected"), counts.affected ?? 0);
  animateNumber($("count-booked"), counts.booked ?? 0);

  // Selecting a flight (map or table) filters this table down to it, same
  // as the map hiding every other plane — both reflect the one shared
  // openFlight selection, not two independent states.
  const shownFlights = openFlight ? current.flights.filter((row) => row.key === openFlight) : current.flights;
  $("flights-src").innerHTML = openFlight
    ? `Filtered to 1 of ${current.flights.length} · <button class="link" id="flights-show-all">Show all</button>`
    : "Lightning Tables";
  if (openFlight) $("flights-show-all").onclick = () => selectFlight(null);

  // "Biggest impact" is the flight putting the most passengers at risk — not
  // simply the first row of Biggest delays, which is sorted by minutes and is
  // often a departure with nobody connecting (0 at risk).
  const impactKey = current.flights.reduce((best, row) =>
    row.affected_passengers > (best?.affected_passengers ?? 0) ? row : best, null)?.key;
  const impactTag = (key) => (key === impactKey ? '<span class="impact-tag">Biggest impact</span>' : "");
  const rowClass = (key) => [key === openFlight ? "selected" : "", key === impactKey ? "top-delay" : ""].join(" ").trim();

  $("flights").innerHTML = shownFlights.map((row) => `<tr id="flight-row-${esc(row.key)}" class="${rowClass(row.key)}">
    <td>${flightButton(row.key)}${impactTag(row.key)}</td><td>${esc(row.origin)} → ${esc(row.destination)}</td>
    <td>${esc(hhmm(row.scheduled_time))}</td>
    <td><span class="risk ${FlightMap.flightColorClass(row)}">${esc(label(row.status))}</span></td>
    <td>${esc(row.delay_minutes)}</td><td>${esc(row.affected_passengers)}</td></tr>`).join("");
  shownFlights.forEach((row) =>
    flashIfChanged($(`flight-row-${row.key}`), `flight:${row.key}`, `${row.status}|${row.delay_minutes}|${row.affected_passengers}`));
  if (scrollToNow && !openFlight) scrollFlightsToNow();

  $("delays").innerHTML = current.delays.map((row) => `<tr id="delay-row-${esc(row.key)}" class="${rowClass(row.key)}">
    <td>${flightButton(row.key)}${impactTag(row.key)}</td>
    <td>${esc(row.origin)} → ${esc(row.destination)}</td>
    <td>${esc(row.delay_minutes)}</td><td>${esc(row.affected_passengers)}</td></tr>`).join("");
  current.delays.forEach((row) =>
    flashIfChanged($(`delay-row-${row.key}`), `delay:${row.key}`, `${row.delay_minutes}|${row.affected_passengers}`));

  bindFlights();
  $("map-show-all").classList.toggle("hidden", !openFlight);
  FlightMap.renderFlightMap("#flight-map", current.flights, { highlightKey: impactKey, focusedKey: openFlight, onSelect: selectFlight });
}

// The list starts 18 hours back, so put the first flight scheduled from 30 minutes ago at the top.
function scrollFlightsToNow() {
  const since = utcMs(current.now) - 30 * 60 * 1000;
  const row = $("flights").rows[current.flights.findIndex((flight) => utcMs(flight.scheduled_time) >= since)];
  if (!row) return;
  const wrap = row.closest(".table-wrap");
  wrap.scrollTop = row.offsetTop - wrap.querySelector("thead").offsetHeight;
  scrollToNow = false;
}

async function refreshFlight(opening = false) {
  if (!openFlight) return;
  const flightId = openFlight;
  let body;
  try {
    body = await request(`/api/flight/${encodeURIComponent(flightId)}`);
  } catch (error) {
    $("status-line").textContent = error.message;
    return;
  }
  if (flightId !== openFlight) return;
  const { flight, passengers, seats } = body;
  $("flight-title").textContent = `${flight.key} passengers`;
  $("flight-sub").textContent = `${flight.origin} → ${flight.destination} · ${flight.status} · `
    + `${flight.delay_minutes} min delay · est. ${hhmm(flight.estimated_time)}`;
  $("flight-empty").classList.add("hidden");
  $("seat-map").classList.remove("hidden");
  $("flight-table").classList.remove("hidden");
  if (opening) {
    ["seat-map", "flight-table"].forEach((id) => {
      $(id).classList.remove("reveal");
      // eslint-disable-next-line no-unused-expressions
      $(id).offsetWidth; // restart the CSS animation
      $(id).classList.add("reveal");
    });
  }
  // A departure's passengers are the ones connecting onto it, so show where they fly in from.
  $("other-flight-head").textContent = flight.arrival ? "Connection" : "Arriving on";
  $("passengers").innerHTML = passengers.map((row) => `<tr class="${row.key === selected ? "selected" : ""}">
    <td><button class="link" data-passenger="${esc(row.key)}">${esc(row.key)}</button></td>
    <td>${esc(row.seat ?? "—")}</td><td>${esc(row.final_destination ?? "—")}</td>
    <td>${esc((flight.arrival ? row.connecting_flight_id : row.inbound_flight_id) ?? "—")}</td>
    <td>${esc(row.connection_minutes ?? "—")}</td>
    <td><span class="risk ${riskClass(row.risk)}">${esc(label(row.risk))}</span></td></tr>`).join("");
  document.querySelectorAll("[data-passenger]").forEach((button) =>
    button.onclick = () => selectPassenger(button.dataset.passenger));
  renderSeats(flight.key, seats, passengers);
}

// --- Seat map ----------------------------------------------------------------
// Every flight uses the generator's one seat layout (SEATS, sent with each
// flight), drawn top-down with the nose on the left: D-F above the aisle and
// A-C below it, as seen from above. First rows seat 2-2 in the space of three.

let seatHolders = new Map(); // seat -> passenger_state row on the open flight
let seatFlight = null; // the flight seatHolders belongs to

function buildCabin(seats) {
  const rows = new Map();
  seats.forEach((seat) => {
    const row = parseInt(seat, 10);
    rows.set(row, [...(rows.get(row) || []), seat]);
  });
  const side = (content) => `<div class="seat-side">${content}</div>`;
  const seatCells = (rowSeats, letters) => rowSeats.filter((seat) => letters.includes(seat.slice(-1))).reverse()
    .map((seat) => `<div class="seat" data-seat="${esc(seat)}"></div>`).join("");
  const letterCells = (letters) => letters.map((letter) => `<span>${letter}</span>`).join("");
  $("cabin").innerHTML = `<div class="seat-row cabin-letters" aria-hidden="true">
      ${side(letterCells(["F", "E", "D"]))}<span class="row-num"></span>${side(letterCells(["C", "B", "A"]))}</div>`
    + [...rows].map(([row, rowSeats]) => `<div class="seat-row${rowSeats.length === 4 ? " first" : ""}">
      ${side(seatCells(rowSeats, "DEF"))}<span class="row-num">${row}</span>${side(seatCells(rowSeats, "ABC"))}</div>`)
      .join("");
  $("cabin").dataset.built = "1";
}

function renderSeats(flightId, seats, passengers) {
  if (!$("cabin").dataset.built) buildCabin(seats);
  const sameFlight = seatFlight === flightId;
  seatFlight = flightId;
  seatHolders = new Map(passengers.filter((row) => row.seat).map((row) => [row.seat, row]));
  const counts = { miss: 0, ok: 0, open: 0 };
  document.querySelectorAll("#cabin .seat").forEach((el) => {
    const holder = seatHolders.get(el.dataset.seat);
    const state = !holder ? "open" : holder.risk === "HIGH" ? "miss" : "ok";
    counts[state] += 1;
    // Pulse a seat whose risk changed since the last poll, like the flashing table rows.
    const flip = sameFlight && el.dataset.state !== state;
    el.dataset.state = state;
    el.className = ["seat", state, holder && holder.key === selected && "selected", flip && "flip"]
      .filter(Boolean).join(" ");
    el.setAttribute("aria-label", holder ? `Seat ${el.dataset.seat}, ${holder.key}, ${label(holder.risk)}`
      : `Seat ${el.dataset.seat}, open`);
  });
  Object.entries(counts).forEach(([state, count]) => { $(`seats-${state}`).textContent = count; });
  const hovered = document.querySelector("#cabin .seat:hover");
  if (hovered && !$("seat-tip").classList.contains("hidden")) showSeatTip(hovered);
}

// Styled like the flight map's info box; fixed to the viewport so the
// scrolling rail and panel edges never clip it.
function showSeatTip(el) {
  const seat = el.dataset.seat;
  const holder = seatHolders.get(seat);
  const cabin = el.closest(".seat-row").classList.contains("first") ? "First" : "Economy";
  const line = (name, value, valueClass = "") => `<div class="infobox-row"><span class="infobox-label">${esc(name)}</span>
    <span class="infobox-value ${valueClass}">${value}</span></div>`;
  const flightSeat = (flight, flightSeatId) => `${esc(flight)} · ${esc(flightSeatId ?? "—")}`;
  const tip = $("seat-tip");
  tip.innerHTML = !holder
    ? `<div class="infobox-title">Seat ${esc(seat)}</div><div class="infobox-route">${cabin} · Open</div>`
    : `<div class="infobox-title">${esc(holder.key)}</div>
      <div class="infobox-route">Seat ${esc(seat)} · ${cabin}</div>
      ${line("Inbound", flightSeat(holder.inbound_flight_id, holder.inbound_seat))}
      ${line("Connection", holder.connecting_flight_id
        ? flightSeat(holder.connecting_flight_id, holder.connecting_seat) : "Trip ends at SFO")}
      ${line("Going to", esc(holder.final_destination ?? "—"))}
      ${holder.connecting_flight_id ? line("Time to connect", `${esc(holder.connection_minutes ?? "—")} min`,
        holder.risk === "HIGH" ? "miss" : "") : ""}
      ${line("Risk", `<span class="risk ${riskClass(holder.risk)}">${esc(label(holder.risk))}</span>`)}`;
  tip.classList.remove("hidden");
  const rect = el.getBoundingClientRect(), gap = 10;
  const width = tip.offsetWidth, height = tip.offsetHeight;
  tip.style.left = `${Math.max(8, Math.min(rect.left + rect.width / 2 - width / 2, window.innerWidth - width - 8))}px`;
  tip.style.top = `${rect.top - gap - height >= 8 ? rect.top - gap - height : rect.bottom + gap}px`;
}

$("cabin").addEventListener("mouseover", (event) => {
  const seat = event.target.closest(".seat");
  if (seat) showSeatTip(seat);
  else $("seat-tip").classList.add("hidden");
});
$("cabin").addEventListener("mouseleave", () => $("seat-tip").classList.add("hidden"));
$("cabin").addEventListener("click", (event) => {
  const holder = seatHolders.get(event.target.closest(".seat")?.dataset.seat);
  if (holder) selectPassenger(holder.key);
});

async function renderPassenger() {
  const card = $("passenger-card");
  if (!selected) return;
  const passengerId = selected;
  let body;
  try {
    body = await request(`/api/passenger/${encodeURIComponent(passengerId)}`);
  } catch (error) {
    $("status-line").textContent = error.message;
    return;
  }
  if (passengerId !== selected) return;
  const { passenger, offers, hotels } = body;
  card.className = "pax";
  const facts = [
    ["Inbound", passenger.inbound_flight_id],
    ["Connection", passenger.connecting_flight_id ?? "—"],
    ["Final destination", passenger.final_destination],
    ["Connection minutes", passenger.connection_minutes ?? "—"],
  ].map(([label, value]) => `<div class="fact"><span>${esc(label)}</span><b>${esc(value)}</b></div>`).join("");
  const offerCards = offers.length ? offers.map((offer) => `<div class="offer">
      <div class="kicker">${esc(offer.recommended_flight_id)} · ${esc(label(offer.status))}</div>
      <div class="msg">${offer.hotel_name ? esc(offer.hotel_name) : "No hotel needed"}
        ${offer.hotel_cost !== null && offer.hotel_cost !== undefined && offer.hotel_cost !== ""
          ? `<span class="cost"> · $${esc(offer.hotel_cost)}</span>` : ""}</div>
      <div class="foot-row"><span class="cost">Offered ${esc(hhmm(offer.recommended_at))}</span>
      ${offer.status === "OFFERED" ? `<button class="btn" data-select="${esc(offer.key)}">Select</button>` : ""}
      ${offer.status === "SELECTED" ? `<button class="btn" data-book="${esc(offer.key)}">Book</button>` : ""}</div></div>`).join("")
    : `<div class="pax-empty">No recovery offers.</div>`;
  card.innerHTML = `<div class="pax-id">${esc(passenger.key)}</div>
    <div class="pax-route"><span class="risk ${riskClass(passenger.risk)}">${esc(label(passenger.risk))}</span></div>
    <div class="facts">${facts}</div>${offerCards}`;
  renderHotels(hotels, offers);
  $("delays-panel").classList.add("hidden");
  $("hotels-panel").classList.remove("hidden");
  document.querySelectorAll("[data-select]").forEach((button) =>
    button.onclick = () => act("select", button.dataset.select));
  document.querySelectorAll("[data-book]").forEach((button) =>
    button.onclick = () => act("book", button.dataset.book));
}

// Live hotel_inventory rows, cheapest first, each tagged with this passenger's
// furthest-along offer there, so the Grand Hyatt counting down to sold out (+40)
// is visible before Select replaces it with the next hotel that has rooms.
function renderHotels(hotels, offers) {
  const rank = { OFFERED: 1, SELECTED: 2, BOOKED: 3 };
  const offerAt = {};
  offers.forEach((offer) => {
    if (offer.hotel_name && (rank[offer.status] || 0) > (rank[offerAt[offer.hotel_name]] || 0)) {
      offerAt[offer.hotel_name] = offer.status;
    }
  });
  $("hotels").innerHTML = hotels.map((row) => {
    const rooms = row.available_rooms;
    const [pill, text] = rooms < 1 ? ["MISS", "Sold out"] : rooms <= 10 ? ["TIGHT", "Few left"] : ["OK", "Available"];
    const tag = offerAt[row.key] ? `<span class="offer-tag">${esc(label(offerAt[row.key]))}</span>` : "";
    return `<tr><td>${esc(row.key)}${tag}</td><td>${esc(rooms)}</td><td>$${esc(row.nightly_rate)}</td>
      <td><span class="risk ${pill}">${text}</span></td></tr>`;
  }).join("");
  hotels.forEach((row, index) =>
    flashIfChanged($("hotels").rows[index], `hotel:${row.key}`, String(row.available_rooms)));
}

// Shows what Select or Book did for 10 seconds, so a sold-out swap can be read before polls reclaim the line.
function notice(message) {
  $("status-line").textContent = message;
  noticeUntil = Date.now() + 10000;
}

async function act(verb, offerId) {
  try {
    const changed = await request(`/api/passenger/${encodeURIComponent(selected)}/${verb}/${encodeURIComponent(offerId)}`, "POST");
    notice(changed.sold_out_hotel
      ? `${changed.sold_out_hotel} is sold out. Now offering ${changed.hotel_name}.`
      : `${changed.status}: ${changed.hotel_name || "no hotel needed"}`);
    setTimeout(() => { refresh(); renderPassenger(); }, 1000);
  } catch (error) {
    notice(error.message);
  }
}

function tickLiveDot() {
  if (!lastUpdated) return;
  $("live-dot").classList.toggle("stale", Date.now() - lastUpdated > 6000);
}

// Skips a poll while the previous one is still waiting, so slow Lightning reads can't pile up.
function onePending(poll) {
  let pending = false;
  return async () => {
    if (pending) return;
    pending = true;
    try { await poll(); } finally { pending = false; }
  };
}

$("refresh").onclick = () => { refresh(); refreshFlight(); renderPassenger(); };
$("map-show-all").onclick = () => selectFlight(null);
refresh();
window.setInterval(onePending(refresh), 2500);
window.setInterval(onePending(() => Promise.all([refreshFlight(), renderPassenger()])), 2500);
window.setInterval(tickLiveDot, 1000);
