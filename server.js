const express = require("express");
const path = require("path");

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

// ── Server-side cache ─────────────────────────────────────────────────────────
// Stores results in memory so users get instant responses after first load.
// Background refresh every 5 minutes keeps data current without blocking the UI.
const CACHE_TTL_MS = 5 * 60 * 1000;
const cache = {};

function cacheKey(endpoint, params) {
  return `${endpoint}:${JSON.stringify(params)}`;
}

function getCached(key) {
  const entry = cache[key];
  if (!entry) return null;
  if (Date.now() - entry.timestamp > CACHE_TTL_MS) return null;
  return entry.data;
}

function setCache(key, data) {
  cache[key] = { data, timestamp: Date.now() };
}

// Fetch fresh data in background without blocking the response
function refreshInBackground(key, fetchFn) {
  fetchFn().then(data => setCache(key, data)).catch(err => console.error("Background refresh error:", err));
}

// ── GraphQL helper ────────────────────────────────────────────────────────────
async function gql(store, token, query, variables = {}) {
  const url = `https://${store}/admin/api/2024-01/graphql.json`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15000); // 15s per request
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ query, variables }),
      signal: controller.signal,
    });
    clearTimeout(timeout);
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Shopify GQL HTTP ${res.status}: ${text.slice(0, 300)}`);
    }
    const json = await res.json();
    if (json.errors) throw new Error(json.errors.map(e => e.message).join("; "));
    // Throttle: only wait if genuinely needed, cap at 1s
    const cost = json.extensions && json.extensions.cost;
    if (cost && cost.throttleStatus) {
      const { currentlyAvailable, restoreRate } = cost.throttleStatus;
      const needed = (cost.actualQueryCost || 0) * 1.2;
      if (currentlyAvailable < needed && restoreRate > 0) {
        const waitMs = Math.min(Math.ceil((needed - currentlyAvailable) / restoreRate) * 1000, 1000);
        await new Promise(r => setTimeout(r, waitMs));
      }
    }
    return json.data;
  } catch (err) {
    clearTimeout(timeout);
    if (err.name === "AbortError") throw new Error("Shopify GQL request timed out after 15s");
    throw err;
  }
}

// Paginate through all pages of a GQL connection
async function gqlAll(store, token, query, variables, getEdges, getPageInfo) {
  let results = [];
  let cursor = null;
  let pages = 0;
  const MAX_PAGES = 20; // 20 × 250 = 5000 orders max
  const DEADLINE = Date.now() + 45000; // hard 45s total deadline

  while (pages < MAX_PAGES) {
    if (Date.now() > DEADLINE) {
      console.warn(`gqlAll hit 45s deadline after ${pages} pages, returning ${results.length} results`);
      break;
    }
    const data = await gql(store, token, query, { ...variables, after: cursor });
    const edges = getEdges(data);
    results = results.concat(edges.map(e => e.node));
    const pageInfo = getPageInfo(data);
    pages++;
    if (!pageInfo.hasNextPage) break;
    cursor = pageInfo.endCursor;
  }
  return results;
}

// ── REST helper (for inventory levels only — no GQL equivalent) ──────────────
async function restFetchAll(store, token, endpoint, key) {
  let results = [];
  let url = `https://${store}/admin/api/2024-01${endpoint}`;
  let pages = 0;
  while (url && pages < 20) {
    const res = await fetch(url, {
      headers: { "X-Shopify-Access-Token": token, "Content-Type": "application/json" },
    });
    if (!res.ok) throw new Error(`Shopify REST ${res.status}`);
    const data = await res.json();
    results = results.concat(data[key] || []);
    pages++;
    const link = res.headers.get("Link");
    url = null;
    if (link) {
      const match = link.match(/<([^>]+)>;\s*rel="next"/);
      if (match) url = match[1];
    }
    if (url) await new Promise(r => setTimeout(r, 250));
  }
  return results;
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function hoursBetween(a, b) {
  if (!a || !b) return null;
  return (new Date(b) - new Date(a)) / 36e5;
}

function avg(arr) {
  const valid = arr.filter(v => v !== null && v !== undefined && !isNaN(v));
  if (!valid.length) return null;
  return valid.reduce((a, b) => a + b, 0) / valid.length;
}

function formatDuration(hours) {
  if (hours === null || hours === undefined) return "—";
  if (hours < 1) return `${Math.round(hours * 60)}m`;
  if (hours < 24) return `${hours.toFixed(1)}h`;
  return `${(hours / 24).toFixed(1)}d`;
}

function buildDailyTimeSeries(orders, year, month) {
  const days = new Date(year, month, 0).getDate();
  const counts = Array.from({ length: days }, (_, i) => ({ day: i + 1, orders: 0, units: 0 }));
  for (const order of orders) {
    const d = new Date(order.createdAt).getDate();
    if (counts[d - 1]) {
      counts[d - 1].orders++;
      counts[d - 1].units += order._units || 0;
    }
  }
  return counts;
}

function buildFulfillmentTimeSeries(orders, year, month) {
  const days = new Date(year, month, 0).getDate();
  const counts = Array.from({ length: days }, (_, i) => ({ day: i + 1, fulfilled: 0 }));
  for (const order of orders) {
    for (const f of order._fulfillments || []) {
      const d = new Date(f.createdAt).getDate();
      if (counts[d - 1]) counts[d - 1].fulfilled++;
    }
  }
  return counts;
}

// ── GQL Queries ───────────────────────────────────────────────────────────────
const ORDERS_QUERY = `
query Orders($first: Int!, $after: String, $query: String!) {
  orders(first: $first, after: $after, query: $query, sortKey: CREATED_AT) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id
        name
        createdAt
        email
        displayFulfillmentStatus
        shippingAddress { firstName lastName }
        billingAddress { firstName lastName }
        lineItems(first: 50) {
          edges { node { quantity } }
        }
        fulfillments(first: 10) {
          createdAt
          updatedAt
          status
          displayStatus
          trackingInfo(first: 5) { company number url }
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
        id
        name
        createdAt
        completedAt
        status
        email
        tags
        totalPrice
        subtotalPrice
        lineItems(first: 50) {
          edges {
            node {
              title
              variantTitle
              sku
              quantity
              originalUnitPrice
              variant { id inventoryItem { id } }
            }
          }
        }
      }
    }
  }
}`;

// ── Process orders into metrics ───────────────────────────────────────────────
function processOrders(orders, label, year, month) {
  const THRESHOLD_HOURS = 10 * 24;
  const EXCLUDED_EMAILS = ["inquiries@lifelines.com", "care@lifelines.com"];

  const fulfillmentTimes = [];
  const deliveryTimes = [];
  let totalUnits = 0;
  const flaggedOrders = [];

  for (const order of orders) {
    const units = (order.lineItems.edges || []).reduce((s, e) => s + (e.node.quantity || 0), 0);
    order._units = units;
    totalUnits += units;

    const addr = order.shippingAddress || order.billingAddress || {};
    const customerName = [addr.firstName, addr.lastName].filter(Boolean).join(" ") || order.email || "—";

    const fulfillments = order.fulfillments || [];
    order._fulfillments = fulfillments;

    let fulfillmentHours = null;
    let deliveryHours = null;
    let trackingInfo = null;

    if (fulfillments.length > 0) {
      const sorted = [...fulfillments].sort((a, b) => new Date(a.createdAt) - new Date(b.createdAt));
      const first = sorted[0];
      const fh = hoursBetween(order.createdAt, first.createdAt);
      if (fh !== null && fh >= 0) { fulfillmentTimes.push(fh); fulfillmentHours = fh; }

      // Use latest event status OR displayStatus to detect delivery
    const delivered = fulfillments.find(f => {
      const ds = (f.displayStatus || "").toUpperCase();
      // Also check latest event
      const latestEvent = f.events && f.events.edges && f.events.edges.length > 0
        ? f.events.edges.map(e => e.node).sort((a,b) => new Date(b.happenedAt)-new Date(a.happenedAt))[0]
        : null;
      const latestStatus = (latestEvent && latestEvent.status || "").toUpperCase();
      return ds === "DELIVERED" || ds === "FULFILLED" || latestStatus === "DELIVERED";
    });
      if (delivered) {
        const dh = hoursBetween(first.createdAt, delivered.updatedAt);
        if (dh !== null && dh >= 0) { deliveryTimes.push(dh); deliveryHours = dh; }
      }

      const withTracking = [...fulfillments].reverse().find(f => f.trackingInfo && f.trackingInfo.length > 0);
      if (withTracking && withTracking.trackingInfo.length > 0) {
        const t = withTracking.trackingInfo[0];
        trackingInfo = {
          number: t.number,
          company: t.company,
          url: t.url,
          shipmentStatus: withTracking.displayStatus,
          updatedAt: withTracking.updatedAt,
        };
      }
    }

    const issues = [];
    if (fulfillmentHours !== null && fulfillmentHours > THRESHOLD_HOURS)
      issues.push({ type: "fulfillment", hours: fulfillmentHours });
    if (deliveryHours !== null && deliveryHours > THRESHOLD_HOURS)
      issues.push({ type: "delivery", hours: deliveryHours });
    if (fulfillments.length > 0 && !fulfillments.find(f => f.displayStatus === "DELIVERED" || f.displayStatus === "Delivered")) {
      const first = [...fulfillments].sort((a, b) => new Date(a.createdAt) - new Date(b.createdAt))[0];
      const stalled = hoursBetween(first.createdAt, new Date().toISOString());
      if (stalled !== null && stalled > THRESHOLD_HOURS)
        issues.push({ type: "delivery_stalled", hours: stalled });
    }

    if (issues.length > 0 && !EXCLUDED_EMAILS.includes((order.email || "").toLowerCase())) {
      flaggedOrders.push({
        store: label,
        orderNumber: order.name,
        customerName,
        email: order.email || null,
        units,
        createdAt: order.createdAt,
        issues,
        processingHours: null,
        fulfillmentHours,
        deliveryHours,
        tracking: trackingInfo,
      });
    }
  }

  return { fulfillmentTimes, deliveryTimes, totalUnits, flaggedOrders };
}

// ── Scorecard endpoint ────────────────────────────────────────────────────────
app.post("/api/scorecard", async (req, res) => {
  const { year, month } = req.body;
  const { dtcStore, dtcToken, b2bStore, b2bToken } = CREDS;
  if (!dtcStore || !dtcToken || !b2bStore || !b2bToken) {
    return res.status(400).json({ error: "Missing credentials — set DTC_STORE, DTC_TOKEN, B2B_STORE, B2B_TOKEN in Railway." });
  }

  const key = cacheKey("scorecard", { year, month });
  const cached = getCached(key);
  if (cached) {
    // Serve cached data immediately, refresh in background if >4min old
    if (Date.now() - cache[key].timestamp > 4 * 60 * 1000) {
      refreshInBackground(key, () => fetchScorecard(year, month, dtcStore, dtcToken, b2bStore, b2bToken));
    }
    return res.json({ ...cached, fromCache: true });
  }

  try {
    const data = await fetchScorecard(year, month, dtcStore, dtcToken, b2bStore, b2bToken);
    setCache(key, data);
    res.json(data);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

async function fetchScorecard(year, month, dtcStore, dtcToken, b2bStore, b2bToken) {
  const start = `${year}-${String(month).padStart(2, "0")}-01`;
  const endDate = new Date(year, month, 1);
  const end = `${endDate.getFullYear()}-${String(endDate.getMonth() + 1).padStart(2, "0")}-01`;
  const orderQuery = `created_at:>=${start} created_at:<${end}`;
  const draftQuery = `status:completed updated_at:>=${start} updated_at:<${end}`;

  try {
    const stores = [
      { label: "DTC", store: dtcStore, token: dtcToken },
      { label: "B2B", store: b2bStore, token: b2bToken },
    ];

    // Fetch both stores in parallel (different stores = different rate limit buckets)
    // but within each store fetch orders first, then drafts sequentially
    const results = await Promise.all(stores.map(async ({ label, store, token }) => {
      const orders = await gqlAll(store, token, ORDERS_QUERY, { first: 250, query: orderQuery },
        d => d.orders.edges, d => d.orders.pageInfo);

      // Drafts are optional — if they fail, just skip processing time
      let draftOrders = [];
      try {
        draftOrders = await gqlAll(store, token, DRAFT_ORDERS_QUERY, { first: 250, query: draftQuery },
          d => d.draftOrders.edges, d => d.draftOrders.pageInfo);
      } catch (err) {
        console.warn(`Draft orders fetch failed for ${label}, skipping processing time:`, err.message);
      }

      const processingTimes = draftOrders
        .filter(d => d.completedAt)
        .map(d => hoursBetween(d.createdAt, d.completedAt))
        .filter(h => h !== null && h >= 0);

      const { fulfillmentTimes, deliveryTimes, totalUnits, flaggedOrders } = processOrders(orders, label, year, month);

      return {
        label,
        totalOrders: orders.length,
        totalUnits,
        avgProcessingHours: avg(processingTimes),
        avgProcessingFormatted: formatDuration(avg(processingTimes)),
        avgFulfillmentHours: avg(fulfillmentTimes),
        avgFulfillmentFormatted: formatDuration(avg(fulfillmentTimes)),
        avgDeliveryHours: avg(deliveryTimes),
        avgDeliveryFormatted: formatDuration(avg(deliveryTimes)),
        ordersByDay: buildDailyTimeSeries(orders, year, month),
        fulfillmentsByDay: buildFulfillmentTimeSeries(orders, year, month),
        rawProcessingTimes: processingTimes,
        rawFulfillmentTimes: fulfillmentTimes,
        flaggedOrders,
      };
    }));

    const combined = {
      label: "Combined",
      totalOrders: results.reduce((s, r) => s + r.totalOrders, 0),
      totalUnits: results.reduce((s, r) => s + r.totalUnits, 0),
      avgProcessingHours: avg(results.flatMap(r => r.rawProcessingTimes)),
      avgProcessingFormatted: formatDuration(avg(results.flatMap(r => r.rawProcessingTimes))),
      avgFulfillmentHours: avg(results.flatMap(r => r.rawFulfillmentTimes)),
      avgFulfillmentFormatted: formatDuration(avg(results.flatMap(r => r.rawFulfillmentTimes))),
      avgDeliveryHours: avg(results.map(r => r.avgDeliveryHours).filter(v => v !== null)),
      avgDeliveryFormatted: formatDuration(avg(results.map(r => r.avgDeliveryHours).filter(v => v !== null))),
    };

    return {
      stores: results,
      combined,
      flaggedOrders: results.flatMap(r => r.flaggedOrders)
        .sort((a, b) => Math.max(...b.issues.map(i => i.hours)) - Math.max(...a.issues.map(i => i.hours))),
      year,
      month,
    };
  } catch (err) {
    throw err;
  }
}

// ── Care@ Scorecard endpoint ──────────────────────────────────────────────────
app.post("/api/care-scorecard", async (req, res) => {
  const { year, month } = req.body;
  const { b2bStore, b2bToken } = CREDS;
  if (!b2bStore || !b2bToken) return res.status(400).json({ error: "Missing B2B credentials." });

  const key = cacheKey("care-scorecard", { year, month });
  const cached = getCached(key);
  if (cached) {
    if (Date.now() - cache[key].timestamp > 4 * 60 * 1000) {
      refreshInBackground(key, () => fetchCareScorecard(year, month, b2bStore, b2bToken));
    }
    return res.json({ ...cached, fromCache: true });
  }

  try {
    const data = await fetchCareScorecard(year, month, b2bStore, b2bToken);
    setCache(key, data);
    res.json(data);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

async function fetchCareScorecard(year, month, b2bStore, b2bToken) {
  const start = `${year}-${String(month).padStart(2, "0")}-01`;
  const endDate = new Date(year, month, 1);
  const end = `${endDate.getFullYear()}-${String(endDate.getMonth() + 1).padStart(2, "0")}-01`;
  const orderQuery = `email:care@lifelines.com created_at:>=${start} created_at:<${end}`;

  const orders = await gqlAll(b2bStore, b2bToken, ORDERS_QUERY, { first: 250, query: orderQuery },
    d => d.orders.edges, d => d.orders.pageInfo);

  const { fulfillmentTimes, deliveryTimes, totalUnits } = processOrders(orders, "B2B", year, month);

  return {
    totalOrders: orders.length,
    totalUnits,
    avgFulfillmentHours: avg(fulfillmentTimes),
    avgFulfillmentFormatted: formatDuration(avg(fulfillmentTimes)),
    avgDeliveryHours: avg(deliveryTimes),
    avgDeliveryFormatted: formatDuration(avg(deliveryTimes)),
    ordersByDay: buildDailyTimeSeries(orders, year, month),
    fulfillmentsByDay: buildFulfillmentTimeSeries(orders, year, month),
    year,
    month,
  };
}

// ── DTC Stale Fulfillments ────────────────────────────────────────────────────
const DTC_STALE_QUERY = `
query StaleFulfillments($first: Int!, $after: String, $query: String!) {
  orders(first: $first, after: $after, query: $query, sortKey: CREATED_AT, reverse: true) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id name createdAt cancelledAt email displayFulfillmentStatus
        totalPriceSet { shopMoney { amount currencyCode } }
        customer { displayName }
        shippingAddress { name address1 address2 city province zip country }
        tags
        fulfillments(first: 10) {
          id name createdAt updatedAt status displayStatus
          trackingInfo(first: 5) { company number url }
          events(first: 50) {
            edges { node { status happenedAt message } }
          }
          fulfillmentLineItems(first: 50) {
            edges { node { quantity lineItem { sku title } } }
          }
        }
      }
    }
  }
}`;

app.post("/api/dtc-stale", async (req, res) => {
  const { dtcStore, dtcToken } = CREDS;
  if (!dtcStore || !dtcToken) return res.status(400).json({ error: "Missing DTC credentials." });

  const key = cacheKey("dtc-stale", {});
  const cached = getCached(key);
  if (cached) {
    if (Date.now() - cache[key].timestamp > 4 * 60 * 1000) {
      refreshInBackground(key, () => fetchDTCStale(dtcStore, dtcToken));
    }
    return res.json({ ...cached, fromCache: true });
  }

  try {
    const data = await fetchDTCStale(dtcStore, dtcToken);
    setCache(key, data);
    res.json(data);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

async function fetchDTCStale(dtcStore, dtcToken) {
  const MIN_DAYS = 10;
  const TARGET = new Set(["IN_TRANSIT", "CONFIRMED"]);
  function daysSince(iso) {
    if (!iso) return null;
    return (Date.now() - new Date(iso).getTime()) / 864e5;
  }

  // Only scan last 90 days — orders older than that aren't actionable
  const ninetyDaysAgo = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

  const orders = await gqlAll(
    dtcStore, dtcToken, DTC_STALE_QUERY,
    { first: 250, query: `fulfillment_status:fulfilled -status:cancelled created_at:>=${ninetyDaysAgo}` },
    d => d.orders.edges, d => d.orders.pageInfo
  );

    const rows = [];
    for (const order of orders) {
      if (order.cancelledAt) continue;
      for (const f of order.fulfillments || []) {
        const status = (f.displayStatus || "").toUpperCase().replace(/ /g, "_");
        if (!TARGET.has(status)) continue;
        const days = daysSince(f.createdAt);
        if (!days || days < MIN_DAYS) continue;

        const events = (f.events.edges || [])
          .map(e => e.node)
          .filter(e => e.happenedAt)
          .sort((a, b) => new Date(b.happenedAt) - new Date(a.happenedAt));

        const skus = (f.fulfillmentLineItems.edges || []).map(e =>
          `${e.node.lineItem.sku || e.node.lineItem.title} x${e.node.quantity}`
        );

        const addr = order.shippingAddress || {};
        const total = order.totalPriceSet?.shopMoney || {};

        rows.push({
          orderName: order.name,
          orderId: order.id,
          orderCreatedAt: order.createdAt,
          email: order.email,
          customerName: order.customer?.displayName || addr.name || "",
          shippingAddress: [addr.address1, addr.address2, addr.city, addr.province, addr.zip, addr.country].filter(Boolean).join(", "),
          orderTotal: total.amount ? `${total.currencyCode} ${parseFloat(total.amount).toFixed(2)}` : "",
          fulfillmentId: f.id,
          fulfillmentName: f.name,
          fulfilledAt: f.createdAt,
          daysSinceFulfilled: Math.floor(days),
          fulfillmentStatus: f.status,
          displayStatus: f.displayStatus,
          hasTracking: (f.trackingInfo || []).length > 0,
          tracking: (f.trackingInfo || []).map(t => ({ company: t.company, number: t.number, url: t.url })),
          skus,
          tags: (order.tags || []).join(", "),
          latestEvent: events[0] || null,
          allEvents: events,
        });
      }
    }

    rows.sort((a, b) => b.daysSinceFulfilled - a.daysSinceFulfilled);
    return { rows, total: rows.length };
}

// ── B2B Draft Orders endpoint ─────────────────────────────────────────────────
app.post("/api/b2b-drafts", async (req, res) => {
  const { b2bStore, b2bToken } = CREDS;
  if (!b2bStore || !b2bToken) return res.status(400).json({ error: "Missing B2B credentials." });

  const key = cacheKey("b2b-drafts", {});
  const cached = getCached(key);
  if (cached) {
    if (Date.now() - cache[key].timestamp > 4 * 60 * 1000) {
      refreshInBackground(key, () => fetchB2BDrafts(b2bStore, b2bToken));
    }
    return res.json({ ...cached, fromCache: true });
  }

  try {
    const data = await fetchB2BDrafts(b2bStore, b2bToken);
    setCache(key, data);
    res.json(data);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

async function fetchB2BDrafts(b2bStore, b2bToken) {
  try {
    const drafts = await gqlAll(
      b2bStore, b2bToken, DRAFT_ORDERS_QUERY,
      { first: 250, query: "status:open" },
      d => d.draftOrders.edges, d => d.draftOrders.pageInfo
    );

    const needsReview = drafts.filter(d =>
      (d.tags || []).map(t => t.toLowerCase()).includes("needs-review")
    );

    // By customer
    const customerMap = {};
    for (const d of drafts) {
      const key = d.email || "Unknown";
      if (!customerMap[key]) customerMap[key] = { customer: key, email: key, draftCount: 0, totalValue: 0 };
      customerMap[key].draftCount++;
      customerMap[key].totalValue += parseFloat(d.totalPrice || 0);
    }
    const byCustomer = Object.values(customerMap).sort((a, b) => b.draftCount - a.draftCount);

    // Collect variant IDs for OOS check
    const variantIds = [...new Set(
      drafts.flatMap(d => (d.lineItems.edges || []).map(e => e.node.variant?.id).filter(Boolean))
    )];
    const invItemIds = [...new Set(
      drafts.flatMap(d => (d.lineItems.edges || []).map(e => e.node.variant?.inventoryItem?.id).filter(Boolean))
    )];

    // Fetch inventory levels via REST (no GQL equivalent for multi-location totals)
    const inventoryMap = {};
    if (invItemIds.length > 0) {
      const numericIds = invItemIds.map(id => id.replace("gid://shopify/InventoryItem/", ""));
      for (let i = 0; i < numericIds.length; i += 50) {
        const batch = numericIds.slice(i, i + 50).join(",");
        const levels = await restFetchAll(b2bStore, b2bToken,
          `/inventory_levels.json?inventory_item_ids=${batch}&limit=250`, "inventory_levels");
        for (const lvl of levels) {
          inventoryMap[`gid://shopify/InventoryItem/${lvl.inventory_item_id}`] =
            (inventoryMap[`gid://shopify/InventoryItem/${lvl.inventory_item_id}`] || 0) + (lvl.available || 0);
        }
      }
    }

    // Build OOS table
    const oosMap = {};
    for (const draft of drafts) {
      for (const edge of draft.lineItems.edges || []) {
        const li = edge.node;
        if (!li.variant?.inventoryItem?.id) continue;
        const available = inventoryMap[li.variant.inventoryItem.id] ?? null;
        if (available === null || available > 0) continue;
        const vid = li.variant.id;
        if (!oosMap[vid]) {
          oosMap[vid] = {
            sku: li.sku || "—",
            productTitle: li.title || "Unknown",
            variantTitle: li.variantTitle || "",
            available,
            draftCount: 0,
            totalUnitsRequested: 0,
            affectedDrafts: [],
          };
        }
        oosMap[vid].draftCount++;
        oosMap[vid].totalUnitsRequested += li.quantity || 0;
        oosMap[vid].affectedDrafts.push(draft.name);
      }
    }
    const oosItems = Object.values(oosMap).sort((a, b) => b.draftCount - a.draftCount);

    // Needs-review export
    const needsReviewExport = needsReview.map(d => ({
      name: d.name,
      createdAt: d.createdAt,
      tags: (d.tags || []).join(", "),
      customerName: d.customer ? `${d.customer.firstName || ""} ${d.customer.lastName || ""}`.trim() : (d.email || "—"),
      email: d.customer?.email || d.email || "",
      subtotal: d.subtotalPrice,
      total: d.totalPrice,
      lineItems: (d.lineItems.edges || []).map(e => ({
        title: e.node.title,
        variantTitle: e.node.variantTitle || "",
        sku: e.node.sku || "",
        quantity: e.node.quantity,
        price: e.node.originalUnitPrice,
      })),
    }));

    return {
      totalDrafts: drafts.length,
      needsReviewCount: needsReview.length,
      needsReviewExport,
      byCustomer,
      oosItems,
    };
  } catch (err) {
    throw err;
  }
}

// ── Start ─────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Ops Scorecard running on :${PORT}`));
