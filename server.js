const express = require("express");
const path = require("path");
const { Pool } = require("pg");

const app = express();
app.use(express.json());
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
      const row = processOrderNode(node, label, draftsMap[node.name] || null);
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

async function buildDraftsMap(store, token, since) {
  const query = since
    ? `status:completed updated_at:>=${since}`
    : `status:completed updated_at:>=${new Date(Date.now() - 365 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10)}`;

  const drafts = await gqlAll(store, token, DRAFT_ORDERS_QUERY, { first: 250, query },
    d => d.draftOrders.edges, d => d.draftOrders.pageInfo, 600000); // 10min for sync

  // Map: shopify order GID isn't on draft, use completedAt as proxy — store by draft name
  // Actually we store processing_hours on the draft itself: created_at → completed_at
  const map = {}; // draft.name → processing_hours (we'll match by order name later)
  for (const d of drafts) {
    if (d.completedAt) {
      const h = hoursBetween(d.createdAt, d.completedAt);
      if (h !== null && h >= 0) map[d.name] = h;
    }
  }
  return map;
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
      buildDraftsMap(dtcStore, dtcToken, since).catch(e => { console.warn("DTC drafts:", e.message); return {}; }),
      buildDraftsMap(b2bStore, b2bToken, since).catch(e => { console.warn("B2B drafts:", e.message); return {}; }),
    ]);

    // Sync both stores in parallel (different rate limit buckets)
    const [dtcCount, b2bCount] = await Promise.all([
      syncStore(dtcStore, dtcToken, "DTC", since, dtcDrafts),
      syncStore(b2bStore, b2bToken, "B2B", since, b2bDrafts),
    ]);

    // Update sync state
    await db.query(`
      INSERT INTO sync_state (key, value, updated_at) VALUES ('last_sync', $1, NOW())
      ON CONFLICT (key) DO UPDATE SET value = $1, updated_at = NOW()
    `, [syncStart]);

    console.log(`[sync] Done — DTC: ${dtcCount}, B2B: ${b2bCount} orders upserted`);
  } catch (err) {
    console.error("[sync] Error:", err.message);
  } finally {
    syncInProgress = false;
  }
}

// ── Scorecard endpoint (reads from DB) ────────────────────────────────────────
app.post("/api/scorecard", async (req, res) => {
  const { year, month } = req.body;
  const start = new Date(year, month - 1, 1);
  const end   = new Date(year, month, 1);

  try {
    const [statsRes, flaggedRes, syncRes] = await Promise.all([
      db.query(`
        SELECT
          store,
          COUNT(*) AS total_orders,
          SUM(units) AS total_units,
          AVG(processing_hours) AS avg_processing,
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
    ]);

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
        avgProcessingFormatted: formatDuration(parseFloat(stats.avg_processing) || null),
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
      avgProcessingFormatted: formatDuration(avg(allStats.map(r => parseFloat(r.avg_processing)).filter(v => v > 0))),
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
  const start = new Date(year, month - 1, 1);
  const end   = new Date(year, month, 1);

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
  const { dtcStore, dtcToken } = CREDS;
  if (!dtcStore || !dtcToken) return res.status(400).json({ error: "Missing DTC credentials." });

  const ninetyDaysAgo = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
  const TARGET = new Set(["IN_TRANSIT", "CONFIRMED"]);

  try {
    const orders = await gqlAll(dtcStore, dtcToken, DTC_STALE_QUERY,
      { first: 250, query: `fulfillment_status:fulfilled -status:cancelled created_at:>=${ninetyDaysAgo}` },
      d => d.orders.edges, d => d.orders.pageInfo, 30000);

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

        rows.push({
          orderName: order.name, orderId: order.id, orderCreatedAt: order.createdAt,
          email: order.email,
          customerName: addr.name || order.email || "",
          shippingAddress: [addr.address1, addr.address2, addr.city, addr.province, addr.zip, addr.country].filter(Boolean).join(", "),
          orderTotal: total.amount ? `${total.currencyCode} ${parseFloat(total.amount).toFixed(2)}` : "",
          fulfillmentId: f.id, fulfillmentName: f.name, fulfilledAt: f.createdAt,
          daysSinceFulfilled: Math.floor(days),
          fulfillmentStatus: f.status, displayStatus: f.displayStatus,
          hasTracking: (f.trackingInfo || []).length > 0,
          tracking: (f.trackingInfo || []).map(t => ({ company: t.company, number: t.number, url: t.url })),
          skus, tags: (order.tags || []).join(", "),
          latestEvent: events[0] || null, allEvents: events,
        });
      }
    }

    rows.sort((a, b) => b.daysSinceFulfilled - a.daysSinceFulfilled);
    res.json({ rows, total: rows.length });
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
    const drafts = await gqlAll(b2bStore, b2bToken, DRAFT_ORDERS_QUERY,
      { first: 250, query: "status:open" },
      d => d.draftOrders.edges, d => d.draftOrders.pageInfo);

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
