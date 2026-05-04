const express = require("express");
const path = require("path");
const { Pool } = require("pg");

const app = express();
app.use(express.json());

// ── Password protection ───────────────────────────────────────────────────────
const SITE_PASSWORD = process.env.SITE_PASSWORD || "edpd";
const AUTH_COOKIE = "ops_auth";
const authenticated = new Set(); // in-memory session store

function requireAuth(req, res, next) {
  // Skip auth for API routes and static assets
  if (req.path.startsWith("/api/")) return next();
  // Check cookie
  const cookies = req.headers.cookie || "";
  const token = cookies.split(";").map(c => c.trim())
    .find(c => c.startsWith(AUTH_COOKIE + "="))?.split("=")[1];
  if (token && authenticated.has(token)) return next();
  // Serve login page
  if (req.method === "POST" && req.path === "/login") return next();
  if (req.path === "/login") return next();
  res.send(`<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<title>LifeLines Ops — Login</title>
<link rel="icon" type="image/svg+xml" href="/favicon.svg">
<style>
@import url('https://api.fontshare.com/v2/css?f[]=satoshi@400,500,700&display=swap');
*{box-sizing:border-box;margin:0;padding:0}
body{background:#f0f0eb;display:flex;align-items:center;justify-content:center;min-height:100vh;font-family:'Satoshi',sans-serif}
.box{background:#f7f7f3;border:1px solid #d4d4cc;border-radius:4px;padding:40px;width:320px;box-shadow:0 2px 8px rgba(0,0,0,0.08)}
.brand{font-size:11px;font-weight:700;letter-spacing:.14em;text-transform:uppercase;color:#4a6741;margin-bottom:24px}
h1{font-size:16px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:#2a2a28;margin-bottom:24px}
input{width:100%;background:#fff;border:1px solid #c8c8c0;color:#2a2a28;font-family:'Satoshi',sans-serif;font-size:14px;padding:10px 12px;border-radius:2px;outline:none;margin-bottom:12px}
input:focus{border-color:#4a6741}
button{width:100%;background:#4a6741;color:#fff;border:none;font-family:'Satoshi',sans-serif;font-size:12px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;padding:10px;border-radius:2px;cursor:pointer}
button:hover{opacity:.85}
.err{color:#8a2a2a;font-size:12px;margin-top:8px;display:none}
</style></head>
<body><div class="box">
<div class="brand">LifeLines</div>
<h1>Ops Tools</h1>
<form method="POST" action="/login">
<input type="password" name="password" placeholder="Password" autofocus>
<button type="submit">Enter</button>
<div class="err" id="err">${req.query.err ? "Incorrect password" : ""}</div>
</form>
</div>
<script>document.querySelector('.err').style.display='${req.query.err ? "block" : "none"}'</script>
</body></html>`);
}

app.post("/login", express.urlencoded({ extended: false }), (req, res) => {
  if (req.body.password === SITE_PASSWORD) {
    const token = Math.random().toString(36).slice(2) + Date.now().toString(36);
    authenticated.add(token);
    res.setHeader("Set-Cookie", AUTH_COOKIE + "=" + token + "; Path=/; HttpOnly; Max-Age=" + (60*60*24*30));
    res.redirect("/");
  } else {
    res.redirect("/login?err=1");
  }
});

app.get('/favicon.svg', (req, res) => res.sendFile(path.join(__dirname, 'public', 'favicon.svg')));
app.use(requireAuth);
app.use(express.static(path.join(__dirname, "public")));


// ── Credentials ───────────────────────────────────────────────────────────────
const CREDS = {
  dtcStore: process.env.DTC_STORE,
  dtcToken: process.env.DTC_TOKEN,
  b2bStore: process.env.B2B_STORE,
  b2bToken: process.env.B2B_TOKEN,
};

// ── Database ──────────────────────────────────────────────────────────────────
const db = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });

async function initDB() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS orders (
      id TEXT PRIMARY KEY,
      store TEXT NOT NULL,
      order_number TEXT,
      created_at TIMESTAMPTZ,
      email TEXT,
      units INTEGER DEFAULT 0,
      fulfillment_hours REAL,
      delivery_hours REAL,
      processing_hours REAL,
      is_flagged BOOLEAN DEFAULT FALSE,
      flag_types TEXT,
      tracking_json TEXT,
      customer_name TEXT,
      updated_at TIMESTAMPTZ,
      synced_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_orders_store_created ON orders(store, created_at);
    CREATE INDEX IF NOT EXISTS idx_orders_email ON orders(email);
    CREATE INDEX IF NOT EXISTS idx_orders_flagged ON orders(is_flagged) WHERE is_flagged = TRUE;

    CREATE TABLE IF NOT EXISTS sync_state (
      key TEXT PRIMARY KEY,
      value TEXT,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS stale_fulfillments (
      fulfillment_id TEXT PRIMARY KEY,
      order_id TEXT,
      order_name TEXT,
      order_created_at TIMESTAMPTZ,
      email TEXT,
      customer_name TEXT,
      shipping_address TEXT,
      order_total TEXT,
      fulfilled_at TIMESTAMPTZ,
      display_status TEXT,
      has_tracking BOOLEAN DEFAULT FALSE,
      tracking_json TEXT,
      skus TEXT,
      tags TEXT,
      latest_event_json TEXT,
      all_events_json TEXT,
      days_since_fulfilled REAL,
      synced_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_stale_display_status ON stale_fulfillments(display_status);
    CREATE INDEX IF NOT EXISTS idx_stale_fulfilled_at ON stale_fulfillments(fulfilled_at);
  `);
  console.log("DB initialized");
}

// ── GQL helpers ───────────────────────────────────────────────────────────────
async function gql(store, token, query, variables = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15000);
  try {
    const res = await fetch(`https://${store}/admin/api/2024-01/graphql.json`, {
      method: "POST",
      headers: { "X-Shopify-Access-Token": token, "Content-Type": "application/json" },
      body: JSON.stringify({ query, variables }),
      signal: controller.signal,
    });
    clearTimeout(timeout);
    if (!res.ok) throw new Error(`GQL HTTP ${res.status}`);
    const json = await res.json();
    if (json.errors) throw new Error(json.errors.map(e => e.message).join("; "));
    const cost = json.extensions?.cost;
    if (cost?.throttleStatus) {
      const { currentlyAvailable, restoreRate } = cost.throttleStatus;
      const needed = (cost.actualQueryCost || 0) * 1.2;
      if (currentlyAvailable < needed && restoreRate > 0) {
        await new Promise(r => setTimeout(r, Math.min(Math.ceil((needed - currentlyAvailable) / restoreRate) * 1000, 2000)));
      }
    }
    return json.data;
  } catch (err) {
    clearTimeout(timeout);
    if (err.name === "AbortError") throw new Error("GQL timeout");
    throw err;
  }
}

async function gqlAll(store, token, query, variables, getEdges, getPageInfo, deadlineMs = 120000) {
  let results = [], cursor = null, pages = 0;
  const DEADLINE = Date.now() + deadlineMs;
  while (pages < 100) {
    if (Date.now() > DEADLINE) { console.warn(`gqlAll deadline at ${pages} pages, ${results.length} results`); break; }
    const data = await gql(store, token, query, { ...variables, after: cursor });
    results = results.concat(getEdges(data).map(e => e.node));
    const pi = getPageInfo(data);
    pages++;
    if (!pi.hasNextPage) break;
    cursor = pi.endCursor;
  }
  return results;
}

// ── REST (inventory only) ─────────────────────────────────────────────────────
async function restFetchAll(store, token, endpoint, key) {
  let results = [], url = `https://${store}/admin/api/2024-01${endpoint}`, pages = 0;
  while (url && pages < 20) {
    const res = await fetch(url, { headers: { "X-Shopify-Access-Token": token } });
    if (!res.ok) throw new Error(`REST ${res.status}`);
    const data = await res.json();
    results = results.concat(data[key] || []);
    pages++;
    const link = res.headers.get("Link");
    url = null;
    if (link) { const m = link.match(/<([^>]+)>;\s*rel="next"/); if (m) url = m[1]; }
    if (url) await new Promise(r => setTimeout(r, 250));
  }
  return results;
}

// ── Math ──────────────────────────────────────────────────────────────────────
function hoursBetween(a, b) {
  if (!a || !b) return null;
  return (new Date(b) - new Date(a)) / 36e5;
}
function avg(arr) {
  const v = arr.filter(x => x !== null && x !== undefined && !isNaN(x) && x >= 0);
  return v.length ? v.reduce((a, b) => a + b, 0) / v.length : null;
}
function formatDuration(h) {
  if (h === null || h === undefined) return "—";
  if (h < 1) return `${Math.round(h * 60)}m`;
  if (h < 24) return `${h.toFixed(1)}h`;
  return `${(h / 24).toFixed(1)}d`;
}

// ── GQL Query ─────────────────────────────────────────────────────────────────
const ORDERS_QUERY = `
query Orders($first: Int!, $after: String, $query: String!) {
  orders(first: $first, after: $after, query: $query, sortKey: UPDATED_AT, reverse: true) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id name createdAt updatedAt email
        shippingAddress { firstName lastName }
        billingAddress { firstName lastName }
        lineItems(first: 20) { edges { node { quantity } } }
        fulfillments(first: 5) {
          createdAt updatedAt displayStatus
          trackingInfo(first: 1) { company number url }
        }
      }
    }
  }
}`;

const DRAFT_ORDERS_QUERY = `
query DraftOrders($first: Int!, $after: String, $query: String!) {
  draftOrders(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id name createdAt completedAt status email tags
        totalPrice subtotalPrice
        lineItems(first: 50) {
          edges {
            node {
              title variantTitle sku quantity originalUnitPrice
              variant { id inventoryItem { id } }
            }
          }
        }
      }
    }
  }
}`;

const DTC_STALE_QUERY = `
query StaleFulfillments($first: Int!, $after: String, $query: String!) {
  orders(first: $first, after: $after, query: $query, sortKey: CREATED_AT, reverse: true) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id name createdAt cancelledAt email displayFulfillmentStatus
        totalPriceSet { shopMoney { amount currencyCode } }
        shippingAddress { name address1 address2 city province zip country }
        tags
        fulfillments(first: 10) {
          id name createdAt updatedAt status displayStatus
          trackingInfo(first: 5) { company number url }
          events(first: 50) { edges { node { status happenedAt message } } }
          fulfillmentLineItems(first: 50) { edges { node { quantity lineItem { sku title } } } }
        }
      }
    }
  }
}`;

// ── Order processor (Shopify node → DB row) ───────────────────────────────────
function processOrderNode(node, store, draftCompletedAt) {
  const units = (node.lineItems?.edges || []).reduce((s, e) => s + (e.node.quantity || 0), 0);
  const addr = node.shippingAddress || node.billingAddress || {};
  const customerName = [addr.firstName, addr.lastName].filter(Boolean).join(" ") || node.email || "—";
  const fulfs = node.fulfillments || [];

  let fulfillmentHours = null, deliveryHours = null;
  let trackingInfo = null;

  if (fulfs.length > 0) {
    const sorted = [...fulfs].sort((a, b) => new Date(a.createdAt) - new Date(b.createdAt));
    const first = sorted[0];
    const fh = hoursBetween(node.createdAt, first.createdAt);
    if (fh !== null && fh >= 0) fulfillmentHours = fh;

    const delivered = fulfs.find(f => (f.displayStatus || "").toUpperCase() === "DELIVERED");
    if (delivered) {
      const dh = hoursBetween(first.createdAt, delivered.updatedAt);
      if (dh !== null && dh >= 0) deliveryHours = dh;
    }

    const wt = [...fulfs].reverse().find(f => f.trackingInfo?.length > 0);
    if (wt) trackingInfo = {
      number: wt.trackingInfo[0].number,
      company: wt.trackingInfo[0].company,
      url: wt.trackingInfo[0].url,
      shipmentStatus: wt.displayStatus,
      updatedAt: wt.updatedAt,
    };
  }

  const processingHours = draftCompletedAt
    ? hoursBetween(node.createdAt, draftCompletedAt)
    : null;

  // Flag logic
  const THRESHOLD = 10 * 24;
  const EXCLUDED = new Set(["inquiries@lifelines.com", "care@lifelines.com"]);
  const flags = [];
  if (fulfillmentHours !== null && fulfillmentHours > THRESHOLD) flags.push("fulfillment");
  if (deliveryHours !== null && deliveryHours > THRESHOLD) flags.push("delivery");
  if (fulfs.length > 0 && !fulfs.find(f => (f.displayStatus || "").toUpperCase() === "DELIVERED")) {
    const first = [...fulfs].sort((a, b) => new Date(a.createdAt) - new Date(b.createdAt))[0];
    const stalled = hoursBetween(first.createdAt, new Date().toISOString());
    if (stalled !== null && stalled > THRESHOLD) flags.push("delivery_stalled");
  }

  const isFlagged = flags.length > 0 && !EXCLUDED.has((node.email || "").toLowerCase());

  return {
    id: node.id,
    store,
    order_number: node.name,
    created_at: node.createdAt,
    updated_at: node.updatedAt,
    email: node.email || null,
    units,
    fulfillment_hours: fulfillmentHours,
    delivery_hours: deliveryHours,
    processing_hours: processingHours,
    is_flagged: isFlagged,
    flag_types: flags.join(","),
    tracking_json: trackingInfo ? JSON.stringify(trackingInfo) : null,
    customer_name: customerName,
  };
}

// ── In-memory caches ──────────────────────────────────────────────────────────
let b2bDraftsCache = null;
let b2bDraftsCacheTime = 0;
let dtcStaleCache = null;
let dtcStaleCacheTime = 0;
const B2B_CACHE_TTL = 5 * 60 * 1000;
const DTC_STALE_TTL = 5 * 60 * 1000;

// ── Sync logic ────────────────────────────────────────────────────────────────
let syncInProgress = false;
async function syncStore(store, token, label, since, draftsMap = {}) {
  const query = since
    ? `updated_at:>=${since}`
    : `created_at:>=${new Date(Date.now() - 365 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10)}`;

  console.log(`[sync] ${label}: fetching orders since ${since || "12mo ago"}`);
  const orders = await gqlAll(store, token, ORDERS_QUERY, { first: 250, query },
    d => d.orders.edges, d => d.orders.pageInfo, 600000); // 10min for sync
  console.log(`[sync] ${label}: got ${orders.length} orders`);

  if (!orders.length) return 0;

  // Upsert in batches of 50
  const BATCH = 50;
  let upserted = 0;
  for (let i = 0; i < orders.length; i += BATCH) {
    const batch = orders.slice(i, i + BATCH);
    const values = [], params = [];
    let pi = 1;
    for (const node of batch) {
      const row = processOrderNode(node, label, null); // processing time stored separately via draftsMap
      values.push(`($${pi},$${pi+1},$${pi+2},$${pi+3},$${pi+4},$${pi+5},$${pi+6},$${pi+7},$${pi+8},$${pi+9},$${pi+10},$${pi+11},$${pi+12},$${pi+13})`);
      params.push(row.id, row.store, row.order_number, row.created_at, row.updated_at,
        row.email, row.units, row.fulfillment_hours, row.delivery_hours, row.processing_hours,
        row.is_flagged, row.flag_types, row.tracking_json, row.customer_name);
      pi += 14;
    }
    await db.query(`
      INSERT INTO orders (id, store, order_number, created_at, updated_at, email, units,
        fulfillment_hours, delivery_hours, processing_hours, is_flagged, flag_types, tracking_json, customer_name)
      VALUES ${values.join(",")}
      ON CONFLICT (id) DO UPDATE SET
        updated_at = EXCLUDED.updated_at,
        units = EXCLUDED.units,
        fulfillment_hours = EXCLUDED.fulfillment_hours,
        delivery_hours = EXCLUDED.delivery_hours,
        processing_hours = EXCLUDED.processing_hours,
        is_flagged = EXCLUDED.is_flagged,
        flag_types = EXCLUDED.flag_types,
        tracking_json = EXCLUDED.tracking_json,
        customer_name = EXCLUDED.customer_name,
        synced_at = NOW()
    `, params);
    upserted += batch.length;
  }

  return upserted;
}

async function buildDraftsMap(store, token, since, deadlineMs = 120000) {
  const query = since
    ? `status:completed updated_at:>=${since}`
    : `status:completed updated_at:>=${new Date(Date.now() - 365 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10)}`;

  const drafts = await gqlAll(store, token, DRAFT_ORDERS_QUERY, { first: 250, query },
    d => d.draftOrders.edges, d => d.draftOrders.pageInfo, deadlineMs);

  // Build monthly avg processing hours: draft created_at → completed_at
  // Keyed by "YYYY-MM" for use in scorecard aggregation
  const byMonth = {};
  for (const d of drafts) {
    if (d.completedAt) {
      const h = hoursBetween(d.createdAt, d.completedAt);
      if (h !== null && h >= 0 && h < 720) { // cap at 30 days to exclude outliers
        const key = d.completedAt.slice(0, 7); // "2026-04"
        if (!byMonth[key]) byMonth[key] = [];
        byMonth[key].push(h);
      }
    }
  }
  // Convert to averages
  const result = {};
  for (const [month, hours] of Object.entries(byMonth)) {
    result[month] = avg(hours);
  }
  return result;
}


// ── Sync stale fulfillments to DB ────────────────────────────────────────────
async function syncStaleFulfillments(store, token) {
  const TARGET = new Set(["IN_TRANSIT", "CONFIRMED"]);
  const ninetyDaysAgo = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

  const orders = await gqlAll(store, token, DTC_STALE_QUERY,
    { first: 250, query: `fulfillment_status:fulfilled -status:cancelled created_at:>=${ninetyDaysAgo}` },
    d => d.orders.edges, d => d.orders.pageInfo, 120000);

  // Clear old stale data and rebuild
  await db.query("DELETE FROM stale_fulfillments");

  const rows = [];
  for (const order of orders) {
    if (order.cancelledAt) continue;
    for (const f of order.fulfillments || []) {
      const status = (f.displayStatus || "").toUpperCase().replace(/ /g, "_");
      if (!TARGET.has(status)) continue;
      const days = (Date.now() - new Date(f.createdAt).getTime()) / 864e5;
      if (days < 10) continue;

      const events = (f.events.edges || []).map(e => e.node).filter(e => e.happenedAt)
        .sort((a, b) => new Date(b.happenedAt) - new Date(a.happenedAt));
      const skus = (f.fulfillmentLineItems.edges || [])
        .map(e => `${e.node.lineItem.sku || e.node.lineItem.title} x${e.node.quantity}`);
      const addr = order.shippingAddress || {};
      const total = order.totalPriceSet?.shopMoney || {};
      const tracking = (f.trackingInfo || []).map(t => ({ company: t.company, number: t.number, url: t.url }));

      rows.push({
        fulfillment_id: f.id,
        order_id: order.id,
        order_name: order.name,
        order_created_at: order.createdAt,
        email: order.email || null,
        customer_name: addr.name || order.email || "",
        shipping_address: [addr.address1, addr.address2, addr.city, addr.province, addr.zip, addr.country].filter(Boolean).join(", "),
        order_total: total.amount ? `${total.currencyCode} ${parseFloat(total.amount).toFixed(2)}` : "",
        fulfilled_at: f.createdAt,
        display_status: f.displayStatus,
        has_tracking: tracking.length > 0,
        tracking_json: JSON.stringify(tracking),
        skus: skus.join(" | "),
        tags: (order.tags || []).join(", "),
        latest_event_json: events[0] ? JSON.stringify(events[0]) : null,
        all_events_json: JSON.stringify(events),
        days_since_fulfilled: Math.floor(days),
      });
    }
  }

  // Upsert in batches
  const BATCH = 50;
  for (let i = 0; i < rows.length; i += BATCH) {
    const batch = rows.slice(i, i + BATCH);
    const values = [], params = [];
    let pi = 1;
    for (const r of batch) {
      values.push(`($${pi},$${pi+1},$${pi+2},$${pi+3},$${pi+4},$${pi+5},$${pi+6},$${pi+7},$${pi+8},$${pi+9},$${pi+10},$${pi+11},$${pi+12},$${pi+13},$${pi+14},$${pi+15},$${pi+16})`);
      params.push(r.fulfillment_id, r.order_id, r.order_name, r.order_created_at, r.email,
        r.customer_name, r.shipping_address, r.order_total, r.fulfilled_at, r.display_status,
        r.has_tracking, r.tracking_json, r.skus, r.tags, r.latest_event_json, r.all_events_json, r.days_since_fulfilled);
      pi += 17;
    }
    await db.query(`
      INSERT INTO stale_fulfillments (fulfillment_id, order_id, order_name, order_created_at, email,
        customer_name, shipping_address, order_total, fulfilled_at, display_status,
        has_tracking, tracking_json, skus, tags, latest_event_json, all_events_json, days_since_fulfilled)
      VALUES ${values.join(",")}
      ON CONFLICT (fulfillment_id) DO UPDATE SET
        display_status = EXCLUDED.display_status,
        has_tracking = EXCLUDED.has_tracking,
        tracking_json = EXCLUDED.tracking_json,
        latest_event_json = EXCLUDED.latest_event_json,
        all_events_json = EXCLUDED.all_events_json,
        days_since_fulfilled = EXCLUDED.days_since_fulfilled,
        synced_at = NOW()
    `, params);
  }

  console.log(`[sync] DTC stale: ${rows.length} stale fulfillments synced`);
  return rows.length;
}

async function runSync(isFullBackfill = false) {
  if (syncInProgress) { console.log('[sync] Skipping — sync already in progress'); return; }
  const { dtcStore, dtcToken, b2bStore, b2bToken } = CREDS;
  if (!dtcStore || !b2bStore) return;
  syncInProgress = true;

  try {
    // Get last sync time
    const stateRes = await db.query("SELECT value FROM sync_state WHERE key = 'last_sync'");
    const lastSync = stateRes.rows[0]?.value || null;
    const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
    const since = isFullBackfill ? null : (lastSync ? (new Date(lastSync) < new Date(thirtyDaysAgo) ? thirtyDaysAgo : lastSync) : thirtyDaysAgo);
    const syncStart = new Date().toISOString();

    console.log(`[sync] Starting ${isFullBackfill ? "BACKFILL" : "incremental"} sync`);

    // Build draft maps for processing times
    const [dtcDrafts, b2bDrafts] = await Promise.all([
      buildDraftsMap(dtcStore, dtcToken, since, isFullBackfill ? 600000 : 60000).catch(e => { console.warn("DTC drafts:", e.message); return {}; }),
      buildDraftsMap(b2bStore, b2bToken, since, isFullBackfill ? 600000 : 60000).catch(e => { console.warn("B2B drafts:", e.message); return {}; }),
    ]);

    // Store monthly processing averages in sync_state
    for (const [month, avgHours] of Object.entries(dtcDrafts)) {
      await db.query(`INSERT INTO sync_state (key, value, updated_at) VALUES ($1, $2, NOW())
        ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = NOW()`,
        [`processing_dtc_${month}`, String(avgHours)]);
    }
    for (const [month, avgHours] of Object.entries(b2bDrafts)) {
      await db.query(`INSERT INTO sync_state (key, value, updated_at) VALUES ($1, $2, NOW())
        ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = NOW()`,
        [`processing_b2b_${month}`, String(avgHours)]);
    }

    // Sync both stores in parallel (different rate limit buckets)
    const [dtcCount, b2bCount] = await Promise.all([
      syncStore(dtcStore, dtcToken, "DTC", since, {}),
      syncStore(b2bStore, b2bToken, "B2B", since, {}),
    ]);

    // Update sync state
    await db.query(`
      INSERT INTO sync_state (key, value, updated_at) VALUES ('last_sync', $1, NOW())
      ON CONFLICT (key) DO UPDATE SET value = $1, updated_at = NOW()
    `, [syncStart]);

    console.log(`[sync] Done — DTC: ${dtcCount}, B2B: ${b2bCount} orders upserted`);

    // Sync DTC stale fulfillments to DB
    try {
      await syncStaleFulfillments(dtcStore, dtcToken);
    } catch (err) {
      console.warn("[sync] DTC stale fulfillments error:", err.message);
    }
  } catch (err) {
    console.error("[sync] Error:", err.message);
  } finally {
    syncInProgress = false;
  }
}

// ── Scorecard endpoint (reads from DB) ────────────────────────────────────────
app.post("/api/scorecard", async (req, res) => {
  const { year, month } = req.body;
  const start = new Date(Date.UTC(year, month - 1, 1));
  const end   = new Date(Date.UTC(year, month, 1));

  const monthKey = `${year}-${String(month).padStart(2, '0')}`;

  try {
    const [statsRes, flaggedRes, syncRes, processingRes] = await Promise.all([
      db.query(`
        SELECT
          store,
          COUNT(*) AS total_orders,
          SUM(units) AS total_units,
          AVG(fulfillment_hours) AS avg_fulfillment,
          AVG(delivery_hours) AS avg_delivery
        FROM orders
        WHERE created_at >= $1 AND created_at < $2
        GROUP BY store
      `, [start, end]),
      db.query(`
        SELECT id, store, order_number, customer_name, email, units,
               created_at, flag_types, fulfillment_hours, delivery_hours,
               processing_hours, tracking_json
        FROM orders
        WHERE created_at >= $1 AND created_at < $2
          AND is_flagged = TRUE
        ORDER BY GREATEST(COALESCE(fulfillment_hours,0), COALESCE(delivery_hours,0)) DESC
        LIMIT 500
      `, [start, end]),
      db.query("SELECT value FROM sync_state WHERE key = 'last_sync'"),
      db.query("SELECT key, value FROM sync_state WHERE key = ANY($1)", [[`processing_dtc_${monthKey}`, `processing_b2b_${monthKey}`]]),
    ]);
    const processingMap = {};
    for (const r of processingRes.rows) {
      if (r.key.includes('_dtc_')) processingMap['DTC'] = parseFloat(r.value);
      if (r.key.includes('_b2b_')) processingMap['B2B'] = parseFloat(r.value);
    }

    // Daily time series
    const dailyRes = await db.query(`
      SELECT
        store,
        DATE(created_at) AS day,
        COUNT(*) AS orders,
        SUM(units) AS units
      FROM orders
      WHERE created_at >= $1 AND created_at < $2
      GROUP BY store, DATE(created_at)
      ORDER BY day
    `, [start, end]);

    const fulfillRes = await db.query(`
      SELECT
        store,
        DATE(created_at) AS day,
        COUNT(*) AS fulfilled
      FROM orders
      WHERE created_at >= $1 AND created_at < $2
        AND fulfillment_hours IS NOT NULL
      GROUP BY store, DATE(created_at)
      ORDER BY day
    `, [start, end]);

    // Shape response to match frontend expectations
    const daysInMonth = new Date(year, month, 0).getDate();
    const storeLabels = ["DTC", "B2B"];

    const stores = storeLabels.map(label => {
      const stats = statsRes.rows.find(r => r.store === label) || {};
      const ordersByDay = buildDayArray(daysInMonth, dailyRes.rows.filter(r => r.store === label), "orders", "units");
      const fulfillmentsByDay = buildDayArray(daysInMonth, fulfillRes.rows.filter(r => r.store === label), "fulfilled");

      return {
        label,
        totalOrders: parseInt(stats.total_orders || 0),
        totalUnits: parseInt(stats.total_units || 0),
        avgProcessingFormatted: formatDuration(processingMap[label] || null),
        avgFulfillmentFormatted: formatDuration(parseFloat(stats.avg_fulfillment) || null),
        avgDeliveryFormatted: formatDuration(parseFloat(stats.avg_delivery) || null),
        avgFulfillmentHours: parseFloat(stats.avg_fulfillment) || null,
        avgDeliveryHours: parseFloat(stats.avg_delivery) || null,
        rawProcessingTimes: [],
        rawFulfillmentTimes: [],
        ordersByDay,
        fulfillmentsByDay,
        flaggedOrders: [],
      };
    });

    const allStats = statsRes.rows;
    const combined = {
      totalOrders: allStats.reduce((s, r) => s + parseInt(r.total_orders || 0), 0),
      totalUnits: allStats.reduce((s, r) => s + parseInt(r.total_units || 0), 0),
      avgProcessingFormatted: formatDuration(avg(Object.values(processingMap).filter(v => v > 0))),
      avgFulfillmentFormatted: formatDuration(avg(allStats.map(r => parseFloat(r.avg_fulfillment)).filter(v => v > 0))),
      avgDeliveryFormatted: formatDuration(avg(allStats.map(r => parseFloat(r.avg_delivery)).filter(v => v > 0))),
    };

    const flaggedOrders = flaggedRes.rows.map(r => ({
      store: r.store,
      orderNumber: r.order_number,
      customerName: r.customer_name,
      email: r.email,
      units: r.units,
      createdAt: r.created_at,
      issues: (r.flag_types || "").split(",").filter(Boolean).map(type => ({
        type,
        hours: type === "fulfillment" ? r.fulfillment_hours :
               type === "delivery" ? r.delivery_hours :
               type === "delivery_stalled" ? (r.delivery_hours || r.fulfillment_hours || 0) : 0,
      })),
      processingHours: r.processing_hours,
      fulfillmentHours: r.fulfillment_hours,
      deliveryHours: r.delivery_hours,
      tracking: r.tracking_json ? JSON.parse(r.tracking_json) : null,
    }));

    res.json({
      stores, combined, flaggedOrders, year, month,
      lastSync: syncRes.rows[0]?.value || null,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

function buildDayArray(daysInMonth, rows, countField, unitsField) {
  const arr = Array.from({ length: daysInMonth }, (_, i) => ({ day: i + 1, orders: 0, units: 0, fulfilled: 0 }));
  for (const row of rows) {
    const d = new Date(row.day).getUTCDate();
    if (arr[d - 1]) {
      arr[d - 1][countField] = parseInt(row[countField] || 0);
      if (unitsField) arr[d - 1][unitsField] = parseInt(row[unitsField] || 0);
    }
  }
  return arr;
}

// ── Care@ endpoint (reads from DB) ────────────────────────────────────────────
app.post("/api/care-scorecard", async (req, res) => {
  const { year, month } = req.body;
  const start = new Date(Date.UTC(year, month - 1, 1));
  const end   = new Date(Date.UTC(year, month, 1));

  try {
    const [statsRes, syncRes] = await Promise.all([
      db.query(`
        SELECT
          COUNT(*) AS total_orders,
          SUM(units) AS total_units,
          AVG(fulfillment_hours) AS avg_fulfillment,
          AVG(delivery_hours) AS avg_delivery
        FROM orders
        WHERE created_at >= $1 AND created_at < $2
          AND LOWER(email) = 'care@lifelines.com'
      `, [start, end]),
      db.query("SELECT value FROM sync_state WHERE key = 'last_sync'"),
    ]);

    const dailyRes = await db.query(`
      SELECT DATE(created_at) AS day, COUNT(*) AS orders, SUM(units) AS units
      FROM orders
      WHERE created_at >= $1 AND created_at < $2
        AND LOWER(email) = 'care@lifelines.com'
      GROUP BY DATE(created_at) ORDER BY day
    `, [start, end]);

    const fulfillRes = await db.query(`
      SELECT DATE(created_at) AS day, COUNT(*) AS fulfilled
      FROM orders
      WHERE created_at >= $1 AND created_at < $2
        AND LOWER(email) = 'care@lifelines.com'
        AND fulfillment_hours IS NOT NULL
      GROUP BY DATE(created_at) ORDER BY day
    `, [start, end]);

    const daysInMonth = new Date(year, month, 0).getDate();
    const stats = statsRes.rows[0] || {};

    res.json({
      totalOrders: parseInt(stats.total_orders || 0),
      totalUnits: parseInt(stats.total_units || 0),
      avgFulfillmentFormatted: formatDuration(parseFloat(stats.avg_fulfillment) || null),
      avgDeliveryFormatted: formatDuration(parseFloat(stats.avg_delivery) || null),
      ordersByDay: buildDayArray(daysInMonth, dailyRes.rows, "orders", "units"),
      fulfillmentsByDay: buildDayArray(daysInMonth, fulfillRes.rows, "fulfilled"),
      year, month,
      lastSync: syncRes.rows[0]?.value || null,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// ── DTC Stale (direct GQL — needs fresh data) ─────────────────────────────────
app.post("/api/dtc-stale", async (req, res) => {
  try {
    const result = await db.query(`
      SELECT * FROM stale_fulfillments
      ORDER BY days_since_fulfilled DESC
    `);

    const rows = result.rows.map(r => ({
      orderName: r.order_name,
      orderId: r.order_id,
      orderCreatedAt: r.order_created_at,
      email: r.email,
      customerName: r.customer_name,
      shippingAddress: r.shipping_address,
      orderTotal: r.order_total,
      fulfillmentId: r.fulfillment_id,
      fulfilledAt: r.fulfilled_at,
      daysSinceFulfilled: Math.round(r.days_since_fulfilled),
      displayStatus: r.display_status,
      hasTracking: r.has_tracking,
      tracking: r.tracking_json ? JSON.parse(r.tracking_json) : [],
      skus: r.skus ? r.skus.split(" | ") : [],
      tags: r.tags || "",
      latestEvent: r.latest_event_json ? JSON.parse(r.latest_event_json) : null,
      allEvents: r.all_events_json ? JSON.parse(r.all_events_json) : [],
    }));

    res.json({ rows, total: rows.length, fromDB: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// ── B2B Drafts (direct GQL — always needs fresh) ──────────────────────────────
app.post("/api/b2b-drafts", async (req, res) => {
  const { b2bStore, b2bToken } = CREDS;
  if (!b2bStore || !b2bToken) return res.status(400).json({ error: "Missing B2B credentials." });

  try {
    let drafts;
    const forceRefresh = req.body.refresh === true;
    if (!forceRefresh && b2bDraftsCache && Date.now() - b2bDraftsCacheTime < B2B_CACHE_TTL) {
      drafts = b2bDraftsCache;
    } else {
      drafts = await gqlAll(b2bStore, b2bToken, DRAFT_ORDERS_QUERY,
        { first: 250, query: "status:open" },
        d => d.draftOrders.edges, d => d.draftOrders.pageInfo, 120000); // 2min
      b2bDraftsCache = drafts;
      b2bDraftsCacheTime = Date.now();
    }

    const needsReview = drafts.filter(d => (d.tags || []).map(t => t.toLowerCase()).includes("needs-review"));

    const customerMap = {};
    for (const d of drafts) {
      const email = d.email || null;
      const key = email || "Unknown";
      if (!customerMap[key]) customerMap[key] = { customer: key, email: email || "—", draftCount: 0, totalValue: 0 };
      customerMap[key].draftCount++;
      customerMap[key].totalValue += parseFloat(d.totalPrice || 0);
    }
    const byCustomer = Object.values(customerMap).sort((a, b) => b.draftCount - a.draftCount);

    const invItemIds = [...new Set(
      drafts.flatMap(d => (d.lineItems.edges || []).map(e => e.node.variant?.inventoryItem?.id).filter(Boolean))
    )];

    const inventoryMap = {};
    if (invItemIds.length > 0) {
      const numericIds = invItemIds.map(id => id.replace("gid://shopify/InventoryItem/", ""));
      for (let i = 0; i < numericIds.length; i += 50) {
        const batch = numericIds.slice(i, i + 50).join(",");
        const levels = await restFetchAll(b2bStore, b2bToken,
          `/inventory_levels.json?inventory_item_ids=${batch}&limit=250`, "inventory_levels");
        for (const lvl of levels) {
          const gid = `gid://shopify/InventoryItem/${lvl.inventory_item_id}`;
          inventoryMap[gid] = (inventoryMap[gid] || 0) + (lvl.available || 0);
        }
      }
    }

    const oosMap = {};
    for (const draft of drafts) {
      for (const edge of draft.lineItems.edges || []) {
        const li = edge.node;
        if (!li.variant?.inventoryItem?.id) continue;
        const available = inventoryMap[li.variant.inventoryItem.id] ?? null;
        if (available === null || available > 0) continue;
        const vid = li.variant.id;
        if (!oosMap[vid]) {
          oosMap[vid] = { sku: li.sku || "—", productTitle: li.title || "Unknown",
            variantTitle: li.variantTitle || "", available,
            draftCount: 0, totalUnitsRequested: 0, affectedDrafts: [] };
        }
        oosMap[vid].draftCount++;
        oosMap[vid].totalUnitsRequested += li.quantity || 0;
        oosMap[vid].affectedDrafts.push(draft.name);
      }
    }
    const oosItems = Object.values(oosMap).sort((a, b) => b.draftCount - a.draftCount);

    const needsReviewExport = needsReview.map(d => ({
      name: d.name, createdAt: d.createdAt, tags: (d.tags || []).join(", "),
      customerName: d.email || "—",
      email: d.email || "",
      subtotal: d.subtotalPrice, total: d.totalPrice,
      lineItems: (d.lineItems.edges || []).map(e => ({
        title: e.node.title, variantTitle: e.node.variantTitle || "",
        sku: e.node.sku || "", quantity: e.node.quantity, price: e.node.originalUnitPrice,
      })),
    }));

    res.json({ totalDrafts: drafts.length, needsReviewCount: needsReview.length, needsReviewExport, byCustomer, oosItems });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});


// ── NPI endpoint ─────────────────────────────────────────────────────────────
app.post("/api/npi", async (req, res) => {
  const { b2bStore, b2bToken } = CREDS;
  if (!b2bStore || !b2bToken) return res.status(400).json({ error: "Missing B2B credentials." });

  try {
    let drafts;
    if (b2bDraftsCache && Date.now() - b2bDraftsCacheTime < B2B_CACHE_TTL) {
      drafts = b2bDraftsCache;
    } else {
      drafts = await gqlAll(b2bStore, b2bToken, DRAFT_ORDERS_QUERY,
        { first: 250, query: "status:open" },
        d => d.draftOrders.edges, d => d.draftOrders.pageInfo, 120000); // 2min
      b2bDraftsCache = drafts;
      b2bDraftsCacheTime = Date.now();
    }

    const LAUNCH_RE = /^launch-([a-z]{3})-26$/i;
    const launchMap = {};

    for (const d of drafts) {
      const tags = d.tags || [];
      const launchTag = tags.find(t => LAUNCH_RE.test(t));
      if (!launchTag) continue;
      const match = launchTag.match(LAUNCH_RE);
      const monthCode = match[1].toLowerCase();
      const key = monthCode + "-26";
      if (!launchMap[key]) {
        launchMap[key] = {
          tag: launchTag, monthCode,
          label: monthCode.charAt(0).toUpperCase() + monthCode.slice(1) + " 2026",
          draftCount: 0, totalUnits: 0, totalValue: 0,
          customers: new Set(), drafts: [],
        };
      }
      const units = (d.lineItems && d.lineItems.edges || []).reduce((s, e) => s + (e.node.quantity || 0), 0);
      launchMap[key].draftCount++;
      launchMap[key].totalUnits += units;
      launchMap[key].totalValue += parseFloat(d.totalPrice || 0);
      if (d.email) launchMap[key].customers.add(d.email.toLowerCase());
      launchMap[key].drafts.push({
        name: d.name, email: d.email || "—", units,
        value: parseFloat(d.totalPrice || 0),
        createdAt: d.createdAt, tags: d.tags,
        lineItems: (d.lineItems && d.lineItems.edges || []).map(function(e) {
          return { title: e.node.title, sku: e.node.sku || "", quantity: e.node.quantity };
        }),
      });
    }

    const MONTH_ORDER = { jan:1,feb:2,mar:3,apr:4,may:5,jun:6,jul:7,aug:8,sep:9,oct:10,nov:11,dec:12 };
    const launches = Object.values(launchMap)
      .map(l => Object.assign({}, l, { customerCount: l.customers.size, customers: undefined }))
      .sort((a, b) => (MONTH_ORDER[a.monthCode] || 99) - (MONTH_ORDER[b.monthCode] || 99));

    res.json({ launches, totalDrafts: launches.reduce((s, l) => s + l.draftCount, 0) });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// ── Delivery trend endpoint ───────────────────────────────────────────────────
app.post("/api/delivery-trend", async (req, res) => {
  try {
    const result = await db.query(`
      SELECT
        store,
        TO_CHAR(DATE_TRUNC('month', created_at), 'YYYY-MM') AS month,
        AVG(fulfillment_hours) AS avg_fulfillment,
        AVG(delivery_hours) AS avg_delivery,
        COUNT(*) AS orders
      FROM orders
      WHERE created_at >= NOW() - INTERVAL '9 months'
        AND fulfillment_hours IS NOT NULL
      GROUP BY store, DATE_TRUNC('month', created_at)
      ORDER BY DATE_TRUNC('month', created_at)
    `);
    const byStore = {};
    for (const row of result.rows) {
      if (!byStore[row.store]) byStore[row.store] = [];
      byStore[row.store].push({
        month: row.month,
        avgFulfillmentHours: parseFloat(row.avg_fulfillment) || null,
        avgDeliveryHours: parseFloat(row.avg_delivery) || null,
        orders: parseInt(row.orders),
      });
    }
    res.json({ byStore });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});



// ── Care@ SKU breakdown (live from Shopify - small dataset) ──────────────────
app.post("/api/care-skus", async (req, res) => {
  const { year, month } = req.body;
  const { b2bStore, b2bToken } = CREDS;
  if (!b2bStore || !b2bToken) return res.status(400).json({ error: "Missing B2B credentials." });

  const CARE_SKU_QUERY = `
query CareOrders($first: Int!, $after: String, $query: String!) {
  orders(first: $first, after: $after, query: $query, sortKey: CREATED_AT) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id name createdAt
        lineItems(first: 50) {
          edges { node { sku title quantity } }
        }
      }
    }
  }
}`;

  const start = `${year}-${String(month).padStart(2, "0")}-01`;
  const endDate = new Date(Date.UTC(year, month, 1));
  const end = `${endDate.getFullYear()}-${String(endDate.getMonth() + 1).padStart(2, "0")}-01`;

  try {
    const orders = await gqlAll(b2bStore, b2bToken, CARE_SKU_QUERY,
      { first: 250, query: `email:care@lifelines.com created_at:>=${start} created_at:<${end}` },
      d => d.orders.edges, d => d.orders.pageInfo, 30000);

    // Tally SKUs
    const skuMap = {};
    for (const order of orders) {
      for (const edge of order.lineItems.edges || []) {
        const li = edge.node;
        const key = li.sku || li.title;
        if (!skuMap[key]) skuMap[key] = { sku: li.sku || "", title: li.title, qty: 0, orderCount: 0 };
        skuMap[key].qty += li.quantity || 0;
        skuMap[key].orderCount++;
      }
    }

    const topSkus = Object.values(skuMap)
      .sort((a, b) => b.qty - a.qty)
      .slice(0, 20);

    res.json({ topSkus, totalOrders: orders.length });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// ── Quality SKU endpoint (YTD top replacement SKUs from flagged orders) ────────
app.post("/api/quality-skus", async (req, res) => {
  try {
    const result = await db.query(`
      SELECT
        tracking_json,
        order_number,
        store,
        created_at,
        flag_types,
        fulfillment_hours,
        delivery_hours
      FROM orders
      WHERE is_flagged = TRUE
        AND created_at >= DATE_TRUNC('year', NOW())
        AND tracking_json IS NOT NULL
      ORDER BY created_at DESC
      LIMIT 2000
    `);

    // Parse tracking_json and tally by SKU/company (carrier)
    const carrierMap = {};
    for (const row of result.rows) {
      let tracking = null;
      try { tracking = JSON.parse(row.tracking_json); } catch(e) { continue; }
      if (!tracking || !tracking.company) continue;
      const carrier = tracking.company;
      if (!carrierMap[carrier]) carrierMap[carrier] = { carrier, count: 0, orders: [] };
      carrierMap[carrier].count++;
      carrierMap[carrier].orders.push(row.order_number);
    }

    // Also get top flagged order counts by month/store for YTD
    const ytdRes = await db.query(`
      SELECT
        store,
        TO_CHAR(DATE_TRUNC('month', created_at), 'YYYY-MM') AS month,
        COUNT(*) AS flagged_count,
        SUM(CASE WHEN flag_types LIKE '%fulfillment%' THEN 1 ELSE 0 END) AS fulfillment_flags,
        SUM(CASE WHEN flag_types LIKE '%delivery%' THEN 1 ELSE 0 END) AS delivery_flags
      FROM orders
      WHERE is_flagged = TRUE
        AND created_at >= DATE_TRUNC('year', NOW())
      GROUP BY store, DATE_TRUNC('month', created_at)
      ORDER BY DATE_TRUNC('month', created_at)
    `);

    res.json({
      topCarriers: Object.values(carrierMap).sort((a,b) => b.count - a.count).slice(0,10),
      byMonth: ytdRes.rows,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});


// ── Debug: inspect draft tags ─────────────────────────────────────────────────
app.get("/api/debug-tags", async (req, res) => {
  const { b2bStore, b2bToken } = CREDS;
  try {
    let drafts;
    if (b2bDraftsCache && Date.now() - b2bDraftsCacheTime < B2B_CACHE_TTL) {
      drafts = b2bDraftsCache;
    } else {
      drafts = await gqlAll(b2bStore, b2bToken, DRAFT_ORDERS_QUERY,
        { first: 250, query: "status:open" },
        d => d.draftOrders.edges, d => d.draftOrders.pageInfo, 120000);
      b2bDraftsCache = drafts;
      b2bDraftsCacheTime = Date.now();
    }
    // Count all tags and find launch tags
    const tagCounts = {};
    const launchTags = new Set();
    for (const d of drafts) {
      for (const t of (d.tags || [])) {
        tagCounts[t] = (tagCounts[t] || 0) + 1;
        if (/launch/i.test(t)) launchTags.add(t);
      }
    }
    res.json({
      totalDrafts: drafts.length,
      launchTags: [...launchTags].sort(),
      sampleDraftTags: drafts.slice(0, 5).map(d => ({ name: d.name, tags: d.tags })),
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Manual backfill trigger ───────────────────────────────────────────────────
app.post("/api/trigger-backfill", async (req, res) => {
  // Reset last_sync so next runSync treats it as a fresh backfill
  await db.query("DELETE FROM sync_state WHERE key = 'last_sync'");
  res.json({ ok: true, message: "Backfill will start within 5 seconds" });
  setTimeout(() => runSync(true), 2000);
});

// ── Sync status endpoint ──────────────────────────────────────────────────────
app.get("/api/sync-status", async (req, res) => {
  try {
    const [syncRes, countRes] = await Promise.all([
      db.query("SELECT * FROM sync_state"),
      db.query("SELECT store, COUNT(*) as count FROM orders GROUP BY store"),
    ]);
    res.json({
      state: Object.fromEntries(syncRes.rows.map(r => [r.key, r.value])),
      counts: Object.fromEntries(countRes.rows.map(r => [r.store, parseInt(r.count)])),
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Boot ──────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;

async function boot() {
  await initDB();

  // Check if we need a backfill
  const stateRes = await db.query("SELECT value FROM sync_state WHERE key = 'last_sync'");
  const needsBackfill = !stateRes.rows[0];

  app.listen(PORT, () => console.log(`Ops Scorecard running on :${PORT}`));

  // Run initial sync/backfill after server is up
  if (needsBackfill) {
    console.log("[sync] No previous sync found — running 12-month backfill in background");
    setTimeout(() => runSync(true), 2000);
  } else {
    console.log("[sync] Running incremental sync on startup");
    setTimeout(() => runSync(false), 2000);
  }

  // Schedule incremental sync every 5 minutes, but only during business hours (7AM-8PM MT)
  setInterval(() => {
    const now = new Date();
    // Convert to Mountain Time (UTC-6 MDT / UTC-7 MST)
    const utcHour = now.getUTCHours();
    const month = now.getUTCMonth(); // 0=Jan
    // MDT (UTC-6) March-Nov, MST (UTC-7) Nov-Mar
    const isDST = month >= 2 && month <= 10;
    const mtHour = (utcHour - (isDST ? 6 : 7) + 24) % 24;
    if (mtHour >= 7 && mtHour < 20) {
      runSync(false);
    } else {
      console.log(`[sync] Outside business hours (${mtHour}:00 MT) — skipping`);
    }
  }, 5 * 60 * 1000);
}

boot().catch(err => {
  console.error("Boot failed:", err);
  process.exit(1);
});
