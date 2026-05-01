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

// ── GraphQL helper ────────────────────────────────────────────────────────────
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
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Shopify GQL HTTP ${res.status}: ${text.slice(0, 300)}`);
    }
    const json = await res.json();
    if (json.errors) throw new Error(json.errors.map(e => e.message).join("; "));
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
    if (err.name === "AbortError") throw new Error("Shopify GQL timed out after 15s");
    throw err;
  }
}

async function gqlAll(store, token, query, variables, getEdges, getPageInfo) {
  let results = [];
  let cursor = null;
  let pages = 0;
  const DEADLINE = Date.now() + 45000;
  while (pages < 20) {
    if (Date.now() > DEADLINE) {
      console.warn(`gqlAll deadline after ${pages} pages, returning ${results.length} results`);
      break;
    }
    const data = await gql(store, token, query, { ...variables, after: cursor });
    results = results.concat(getEdges(data).map(e => e.node));
    const pageInfo = getPageInfo(data);
    pages++;
    if (!pageInfo.hasNextPage) break;
    cursor = pageInfo.endCursor;
  }
  return results;
}

// ── REST helper (inventory levels only) ──────────────────────────────────────
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

// ── Math helpers ──────────────────────────────────────────────────────────────
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
function buildDailyTimeSeries(orders, year, month) {
  const days = new Date(year, month, 0).getDate();
  const counts = Array.from({ length: days }, (_, i) => ({ day: i + 1, orders: 0, units: 0 }));
  for (const o of orders) {
    const d = new Date(o.createdAt).getDate();
    if (counts[d - 1]) { counts[d - 1].orders++; counts[d - 1].units += o._units || 0; }
  }
  return counts;
}
function buildFulfillmentTimeSeries(orders, year, month) {
  const days = new Date(year, month, 0).getDate();
  const counts = Array.from({ length: days }, (_, i) => ({ day: i + 1, fulfilled: 0 }));
  for (const o of orders) {
    for (const f of o._fulfillments || []) {
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
        id name createdAt email
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
        customer { firstName lastName email }
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
        customer { displayName }
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

// ── Process orders ────────────────────────────────────────────────────────────
function processOrders(orders, label, year, month) {
  const THRESHOLD = 10 * 24;
  const EXCLUDED = new Set(["inquiries@lifelines.com", "care@lifelines.com"]);
  const fulfillmentTimes = [], deliveryTimes = [], flaggedOrders = [];
  let totalUnits = 0;

  for (const order of orders) {
    const units = (order.lineItems.edges || []).reduce((s, e) => s + (e.node.quantity || 0), 0);
    order._units = units;
    totalUnits += units;

    const addr = order.shippingAddress || order.billingAddress || {};
    const customerName = [addr.firstName, addr.lastName].filter(Boolean).join(" ") || order.email || "—";
    const fulfs = order.fulfillments || [];
    order._fulfillments = fulfs;

    let fulfillmentHours = null, deliveryHours = null, trackingInfo = null;

    if (fulfs.length > 0) {
      const sorted = [...fulfs].sort((a, b) => new Date(a.createdAt) - new Date(b.createdAt));
      const first = sorted[0];
      const fh = hoursBetween(order.createdAt, first.createdAt);
      if (fh !== null && fh >= 0) { fulfillmentTimes.push(fh); fulfillmentHours = fh; }

      const delivered = fulfs.find(f => (f.displayStatus || "").toUpperCase() === "DELIVERED");
      if (delivered) {
        const dh = hoursBetween(first.createdAt, delivered.updatedAt);
        if (dh !== null && dh >= 0) { deliveryTimes.push(dh); deliveryHours = dh; }
      }

      const wt = [...fulfs].reverse().find(f => f.trackingInfo && f.trackingInfo.length > 0);
      if (wt) trackingInfo = {
        number: wt.trackingInfo[0].number,
        company: wt.trackingInfo[0].company,
        url: wt.trackingInfo[0].url,
        shipmentStatus: wt.displayStatus,
        updatedAt: wt.updatedAt,
      };
    }

    const issues = [];
    if (fulfillmentHours !== null && fulfillmentHours > THRESHOLD) issues.push({ type: "fulfillment", hours: fulfillmentHours });
    if (deliveryHours !== null && deliveryHours > THRESHOLD) issues.push({ type: "delivery", hours: deliveryHours });
    if (fulfs.length > 0 && !fulfs.find(f => (f.displayStatus || "").toUpperCase() === "DELIVERED")) {
      const first = [...fulfs].sort((a, b) => new Date(a.createdAt) - new Date(b.createdAt))[0];
      const stalled = hoursBetween(first.createdAt, new Date().toISOString());
      if (stalled !== null && stalled > THRESHOLD) issues.push({ type: "delivery_stalled", hours: stalled });
    }

    if (issues.length > 0 && !EXCLUDED.has((order.email || "").toLowerCase())) {
      flaggedOrders.push({
        store: label, orderNumber: order.name, customerName,
        email: order.email || null, units, createdAt: order.createdAt,
        issues, processingHours: null, fulfillmentHours, deliveryHours, tracking: trackingInfo,
      });
    }
  }
  return { fulfillmentTimes, deliveryTimes, totalUnits, flaggedOrders };
}

// ── Scorecard ─────────────────────────────────────────────────────────────────
app.post("/api/scorecard", async (req, res) => {
  const { year, month } = req.body;
  const { dtcStore, dtcToken, b2bStore, b2bToken } = CREDS;
  if (!dtcStore || !dtcToken || !b2bStore || !b2bToken) {
    return res.status(400).json({ error: "Missing credentials — set DTC_STORE, DTC_TOKEN, B2B_STORE, B2B_TOKEN in Railway." });
  }
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

    const results = await Promise.all(stores.map(async ({ label, store, token }) => {
      const orders = await gqlAll(store, token, ORDERS_QUERY, { first: 250, query: orderQuery },
        d => d.orders.edges, d => d.orders.pageInfo);

      let draftOrders = [];
      try {
        draftOrders = await gqlAll(store, token, DRAFT_ORDERS_QUERY, { first: 250, query: draftQuery },
          d => d.draftOrders.edges, d => d.draftOrders.pageInfo);
      } catch (err) {
        console.warn(`Draft fetch failed for ${label}:`, err.message);
      }

      const processingTimes = draftOrders
        .filter(d => d.completedAt)
        .map(d => hoursBetween(d.createdAt, d.completedAt))
        .filter(h => h !== null && h >= 0);

      const { fulfillmentTimes, deliveryTimes, totalUnits, flaggedOrders } = processOrders(orders, label, year, month);

      return {
        label, totalOrders: orders.length, totalUnits,
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
      totalOrders: results.reduce((s, r) => s + r.totalOrders, 0),
      totalUnits: results.reduce((s, r) => s + r.totalUnits, 0),
      avgProcessingFormatted: formatDuration(avg(results.flatMap(r => r.rawProcessingTimes))),
      avgFulfillmentFormatted: formatDuration(avg(results.flatMap(r => r.rawFulfillmentTimes))),
      avgDeliveryFormatted: formatDuration(avg(results.map(r => r.avgDeliveryHours).filter(v => v !== null))),
    };

    res.json({
      stores: results, combined,
      flaggedOrders: results.flatMap(r => r.flaggedOrders)
        .sort((a, b) => Math.max(...b.issues.map(i => i.hours)) - Math.max(...a.issues.map(i => i.hours))),
      year, month,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// ── Care@ ─────────────────────────────────────────────────────────────────────
app.post("/api/care-scorecard", async (req, res) => {
  const { year, month } = req.body;
  const { b2bStore, b2bToken } = CREDS;
  if (!b2bStore || !b2bToken) return res.status(400).json({ error: "Missing B2B credentials." });
  const start = `${year}-${String(month).padStart(2, "0")}-01`;
  const endDate = new Date(year, month, 1);
  const end = `${endDate.getFullYear()}-${String(endDate.getMonth() + 1).padStart(2, "0")}-01`;

  try {
    const orders = await gqlAll(b2bStore, b2bToken, ORDERS_QUERY,
      { first: 250, query: `email:care@lifelines.com created_at:>=${start} created_at:<${end}` },
      d => d.orders.edges, d => d.orders.pageInfo);

    const { fulfillmentTimes, deliveryTimes, totalUnits } = processOrders(orders, "B2B", year, month);

    res.json({
      totalOrders: orders.length, totalUnits,
      avgFulfillmentFormatted: formatDuration(avg(fulfillmentTimes)),
      avgDeliveryFormatted: formatDuration(avg(deliveryTimes)),
      ordersByDay: buildDailyTimeSeries(orders, year, month),
      fulfillmentsByDay: buildFulfillmentTimeSeries(orders, year, month),
      year, month,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// ── DTC Stale ─────────────────────────────────────────────────────────────────
app.post("/api/dtc-stale", async (req, res) => {
  const { dtcStore, dtcToken } = CREDS;
  if (!dtcStore || !dtcToken) return res.status(400).json({ error: "Missing DTC credentials." });

  const MIN_DAYS = 10;
  const TARGET = new Set(["IN_TRANSIT", "CONFIRMED"]);
  const ninetyDaysAgo = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

  try {
    const orders = await gqlAll(dtcStore, dtcToken, DTC_STALE_QUERY,
      { first: 250, query: `fulfillment_status:fulfilled -status:cancelled created_at:>=${ninetyDaysAgo}` },
      d => d.orders.edges, d => d.orders.pageInfo);

    const rows = [];
    for (const order of orders) {
      if (order.cancelledAt) continue;
      for (const f of order.fulfillments || []) {
        const status = (f.displayStatus || "").toUpperCase().replace(/ /g, "_");
        if (!TARGET.has(status)) continue;
        const days = (Date.now() - new Date(f.createdAt).getTime()) / 864e5;
        if (days < MIN_DAYS) continue;

        const events = (f.events.edges || [])
          .map(e => e.node).filter(e => e.happenedAt)
          .sort((a, b) => new Date(b.happenedAt) - new Date(a.happenedAt));

        const skus = (f.fulfillmentLineItems.edges || [])
          .map(e => `${e.node.lineItem.sku || e.node.lineItem.title} x${e.node.quantity}`);

        const addr = order.shippingAddress || {};
        const total = order.totalPriceSet?.shopMoney || {};

        rows.push({
          orderName: order.name, orderId: order.id, orderCreatedAt: order.createdAt,
          email: order.email,
          customerName: order.customer?.displayName || addr.name || "",
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

// ── B2B Drafts ────────────────────────────────────────────────────────────────
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
      const name = d.customer ? `${d.customer.firstName || ""} ${d.customer.lastName || ""}`.trim() : null;
      const email = d.customer?.email || d.email || null;
      const key = (name && name !== "") ? name : (email || "Unknown");
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
          oosMap[vid] = {
            sku: li.sku || "—", productTitle: li.title || "Unknown",
            variantTitle: li.variantTitle || "", available,
            draftCount: 0, totalUnitsRequested: 0, affectedDrafts: [],
          };
        }
        oosMap[vid].draftCount++;
        oosMap[vid].totalUnitsRequested += li.quantity || 0;
        oosMap[vid].affectedDrafts.push(draft.name);
      }
    }
    const oosItems = Object.values(oosMap).sort((a, b) => b.draftCount - a.draftCount);

    const needsReviewExport = needsReview.map(d => ({
      name: d.name, createdAt: d.createdAt,
      tags: (d.tags || []).join(", "),
      customerName: d.customer ? `${d.customer.firstName || ""} ${d.customer.lastName || ""}`.trim() : (d.email || "—"),
      email: d.customer?.email || d.email || "",
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

// ── Start ─────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Ops Scorecard running on :${PORT}`));
