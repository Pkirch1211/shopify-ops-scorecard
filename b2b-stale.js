// ── B2B Stale Fulfillments ────────────────────────────────────────────────────
// Two views for the B2B tab:
//   1. unfulfilled — draft-converted orders (sourceName = shopify_draft_order)
//      that are still unfulfilled / partially fulfilled past a threshold.
//      Clock starts at the LATER of conversion time or b2b.ship_date, so an
//      order converted early for a future ship date isn't flagged as stale.
//   2. labels — fulfillments with a UPS tracking number (label created) that
//      haven't been delivered past a threshold. Split into stages:
//      label_only (no carrier scan yet), in_transit, exception.
//
// Data is synced in the background by runSync() into a single snapshot table
// (one row per view, written atomically), same cadence as the DTC stale sync.
// Orders tagged "resolved" are hidden in the API response, same as DTC.

const DRAFT_SOURCE = "shopify_draft_order";

const UNFULFILLED_STALE_DAYS = parseFloat(process.env.B2B_UNFULFILLED_STALE_DAYS || "3");
const UNFULFILLED_LOOKBACK_DAYS = parseInt(process.env.B2B_UNFULFILLED_LOOKBACK_DAYS || "365", 10);
const LABEL_STALE_DAYS = parseFloat(process.env.B2B_LABEL_STALE_DAYS || "5");
const LABEL_LOOKBACK_DAYS = parseInt(process.env.B2B_LABEL_LOOKBACK_DAYS || "90", 10);
const LABEL_CARRIER = process.env.B2B_LABEL_CARRIER || "ups";
const LABEL_CARRIER_RE = new RegExp(LABEL_CARRIER, "i");
const CARRIER_IS_UPS = /^ups$/i.test(LABEL_CARRIER.trim());

// Internal / non-customer orders (inventory transfers, VAS, QC inspections,
// scrap) that sit unfulfilled by design. Matched case-insensitively on the
// order email OR the company name. Env vars ADD to these defaults
// (comma-separated); they don't replace them.
function csvSet(raw) {
  return String(raw || "").split(",").map(s => s.trim().toLowerCase()).filter(Boolean);
}
const EXCLUDED_EMAILS = new Set([
  "weiss@lifelines.com",
  "tsmith@lifelines.com",
  "christina@lifelines.com",
  "rebecca@lifelines.com",
  ...csvSet(process.env.B2B_STALE_EXCLUDE_EMAILS),
]);
const EXCLUDED_COMPANIES = new Set([
  "tim smith",
  "inspections - internal qc",
  ...csvSet(process.env.B2B_STALE_EXCLUDE_COMPANIES),
]);

// Fulfillment display statuses that mean "done" — never stale.
const TERMINAL_STATUSES = new Set(["DELIVERED", "CANCELED", "CANCELLED", "LABEL_VOIDED", "PICKED_UP", "READY_FOR_PICKUP"]);
// Carrier exception signals (display status or latest event status).
const EXCEPTION_STATUSES = new Set(["FAILURE", "ATTEMPTED_DELIVERY", "NOT_DELIVERED", "DELAYED"]);
// Statuses that mean a label exists but the carrier hasn't scanned it yet.
const PRE_TRANSIT_STATUSES = new Set(["LABEL_PRINTED", "LABEL_PURCHASED", "CONFIRMED", "FULFILLED", "MARKED_AS_FULFILLED", "SUBMITTED", ""]);

const UNFULFILLED_QUERY = `
query B2BUnfulfilled($first: Int!, $after: String, $query: String!) {
  orders(first: $first, after: $after, query: $query, sortKey: CREATED_AT) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id name createdAt cancelledAt email sourceName poNumber tags
        displayFulfillmentStatus
        totalPriceSet { shopMoney { amount currencyCode } }
        shippingAddress { company name city province }
        billingAddress { company name }
        shipDate: metafield(namespace: "b2b", key: "ship_date") { value }
        lineItems(first: 100) {
          edges { node { sku title variantTitle quantity unfulfilledQuantity } }
        }
      }
    }
  }
}`;

// Events are fetched in a second pass for candidates only — keeps the
// per-page query cost down (events x fulfillments x orders adds up fast).
const LABELS_QUERY = `
query B2BLabels($first: Int!, $after: String, $query: String!) {
  orders(first: $first, after: $after, query: $query, sortKey: CREATED_AT, reverse: true) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id name createdAt cancelledAt email poNumber tags
        totalPriceSet { shopMoney { amount currencyCode } }
        shippingAddress { company name address1 address2 city province zip }
        billingAddress { company name }
        fulfillments(first: 10) {
          id name createdAt status displayStatus inTransitAt estimatedDeliveryAt
          trackingInfo(first: 5) { company number url }
          fulfillmentLineItems(first: 50) { edges { node { quantity lineItem { sku title } } } }
        }
      }
    }
  }
}`;

const FULFILLMENT_EVENTS_QUERY = `
query FulfillmentEvents($ids: [ID!]!) {
  nodes(ids: $ids) {
    ... on Fulfillment {
      id
      events(last: 50) { edges { node { status happenedAt message } } }
    }
  }
}`;

function isoDaysAgo(days) {
  return new Date(Date.now() - days * 864e5).toISOString().slice(0, 10);
}

function parseShipDateMs(raw) {
  if (!raw) return null;
  const ms = Date.parse(String(raw).trim());
  return isNaN(ms) ? null : ms;
}

function companyName(order) {
  const s = order.shippingAddress || {};
  const b = order.billingAddress || {};
  return s.company || b.company || s.name || b.name || order.email || "";
}

function money(order) {
  const m = order.totalPriceSet?.shopMoney || {};
  return m.amount != null ? parseFloat(m.amount) : null;
}

function hasResolvedTag(tags) {
  const list = Array.isArray(tags) ? tags : String(tags || "").split(",");
  return list.map(t => String(t).trim().toLowerCase()).includes("resolved");
}

function upsTrackingUrl(number) {
  return number ? `https://www.ups.com/track?tracknum=${encodeURIComponent(number)}` : null;
}

module.exports = function createB2BStale({ app, db, gql, gqlAll, CREDS, HOLD_CUSTOMER_EMAIL }) {
  const holdEmail = String(HOLD_CUSTOMER_EMAIL || "").toLowerCase();

  function adminOrderUrl(gid) {
    const handle = String(CREDS.b2bStore || "").replace(/\.myshopify\.com$/i, "");
    const id = String(gid || "").split("/").pop();
    return handle && id ? `https://admin.shopify.com/store/${handle}/orders/${id}` : null;
  }

  function isExcludedOrder(o) {
    const email = (o.email || "").trim().toLowerCase();
    if (email === holdEmail || EXCLUDED_EMAILS.has(email)) return true;
    const s = o.shippingAddress || {}, b = o.billingAddress || {};
    return [s.company, b.company, s.name, b.name]
      .some(v => v && EXCLUDED_COMPANIES.has(String(v).trim().toLowerCase()));
  }

  function carrierMatches(t) {
    if (LABEL_CARRIER_RE.test(t.company || "")) return true;
    // UPS tracking numbers start with 1Z — catches labels where the 3PL
    // left the carrier name blank or non-standard.
    return CARRIER_IS_UPS && /^1Z/i.test(t.number || "");
  }

  async function initTable() {
    await db.query(`
      CREATE TABLE IF NOT EXISTS b2b_stale_snapshot (
        kind TEXT PRIMARY KEY,
        rows_json TEXT NOT NULL,
        meta_json TEXT,
        synced_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);
  }

  async function writeSnapshot(kind, rows, meta) {
    await db.query(`
      INSERT INTO b2b_stale_snapshot (kind, rows_json, meta_json, synced_at)
      VALUES ($1, $2, $3, NOW())
      ON CONFLICT (kind) DO UPDATE SET
        rows_json = EXCLUDED.rows_json, meta_json = EXCLUDED.meta_json, synced_at = NOW()
    `, [kind, JSON.stringify(rows), JSON.stringify(meta || {})]);
  }

  // ── View 1: draft-converted orders sitting unfulfilled ──────────────────────
  async function syncUnfulfilled() {
    const { b2bStore, b2bToken } = CREDS;
    const query = `status:open (fulfillment_status:unfulfilled OR fulfillment_status:partial) created_at:>=${isoDaysAgo(UNFULFILLED_LOOKBACK_DAYS)}`;
    const orders = await gqlAll(b2bStore, b2bToken, UNFULFILLED_QUERY, { first: 100, query },
      d => d.orders.edges, d => d.orders.pageInfo, 180000);

    const now = Date.now();
    const sourceCounts = {};
    const rows = [];
    let excludedCount = 0;

    for (const o of orders) {
      if (o.cancelledAt) continue;
      if (isExcludedOrder(o)) { excludedCount++; continue; } // hold@ + internal transfers/VAS/QC

      const src = o.sourceName || "(none)";
      sourceCounts[src] = (sourceCounts[src] || 0) + 1;
      if (src !== DRAFT_SOURCE) continue;

      const status = (o.displayFulfillmentStatus || "").toUpperCase();
      if (status === "FULFILLED" || status === "RESTOCKED") continue;

      const openLines = (o.lineItems?.edges || []).map(e => e.node)
        .map(li => ({
          sku: li.sku || "",
          title: li.title || "",
          variantTitle: li.variantTitle && li.variantTitle !== "Default Title" ? li.variantTitle : "",
          quantity: li.quantity || 0,
          openQty: li.unfulfilledQuantity ?? li.quantity ?? 0,
        }))
        .filter(li => li.openQty > 0);
      if (!openLines.length) continue;

      const createdMs = Date.parse(o.createdAt);
      const shipDate = o.shipDate?.value || null;
      const shipMs = parseShipDateMs(shipDate);
      const clockFromShipDate = shipMs !== null && shipMs > createdMs;
      const clockStart = clockFromShipDate ? shipMs : createdMs;
      const daysStale = (now - clockStart) / 864e5;
      if (daysStale < UNFULFILLED_STALE_DAYS) continue;

      rows.push({
        orderId: o.id,
        orderName: o.name,
        adminUrl: adminOrderUrl(o.id),
        poNumber: o.poNumber || "",
        createdAt: o.createdAt,
        email: o.email || "",
        customerName: companyName(o),
        shipTo: [o.shippingAddress?.city, o.shippingAddress?.province].filter(Boolean).join(", "),
        shipDate,
        clockFrom: clockFromShipDate ? "ship_date" : "conversion",
        daysStale: Math.floor(daysStale),
        daysSinceConversion: Math.floor((now - createdMs) / 864e5),
        fulfillmentStatus: status,
        orderTotal: money(o),
        openUnits: openLines.reduce((s, l) => s + l.openQty, 0),
        openLines,
        tags: (o.tags || []).join(", "),
      });
    }

    rows.sort((a, b) => b.daysStale - a.daysStale);
    await writeSnapshot("unfulfilled", rows, {
      scanned: orders.length,
      excludedCount,
      sourceCounts,
      staleDays: UNFULFILLED_STALE_DAYS,
      lookbackDays: UNFULFILLED_LOOKBACK_DAYS,
    });
    console.log(`[sync] B2B unfulfilled: ${rows.length} stale of ${orders.length} scanned`);
    return rows.length;
  }

  // ── View 2: UPS label created, not delivered ─────────────────────────────────
  async function fetchEvents(ids) {
    const { b2bStore, b2bToken } = CREDS;
    const map = {};
    for (let i = 0; i < ids.length; i += 10) {
      const batch = ids.slice(i, i + 10);
      try {
        const data = await gql(b2bStore, b2bToken, FULFILLMENT_EVENTS_QUERY, { ids: batch });
        for (const n of data.nodes || []) {
          if (!n?.id) continue;
          map[n.id] = (n.events?.edges || []).map(e => e.node).filter(e => e.happenedAt)
            .sort((a, b) => new Date(b.happenedAt) - new Date(a.happenedAt));
        }
      } catch (err) {
        // A failed batch just means no event history for those rows this cycle.
        console.warn(`[sync] B2B label events batch failed: ${err.message}`);
      }
    }
    return map;
  }

  async function syncLabels() {
    const { b2bStore, b2bToken } = CREDS;
    const query = `(fulfillment_status:fulfilled OR fulfillment_status:partial) -status:cancelled created_at:>=${isoDaysAgo(LABEL_LOOKBACK_DAYS)}`;
    const orders = await gqlAll(b2bStore, b2bToken, LABELS_QUERY, { first: 100, query },
      d => d.orders.edges, d => d.orders.pageInfo, 180000);

    const now = Date.now();
    const candidates = [];

    for (const o of orders) {
      if (o.cancelledAt) continue;
      if (isExcludedOrder(o)) continue;
      for (const f of o.fulfillments || []) {
        if ((f.status || "").toUpperCase() === "CANCELLED") continue;
        const disp = (f.displayStatus || "").toUpperCase().replace(/ /g, "_");
        if (TERMINAL_STATUSES.has(disp)) continue;
        const tracking = (f.trackingInfo || []).filter(t => t.number && carrierMatches(t));
        if (!tracking.length) continue;
        const daysSinceLabel = (now - Date.parse(f.createdAt)) / 864e5;
        if (daysSinceLabel < LABEL_STALE_DAYS) continue;
        candidates.push({ order: o, f, disp, tracking, daysSinceLabel });
      }
    }

    const eventsMap = await fetchEvents(candidates.map(c => c.f.id));
    const rows = [];

    for (const { order: o, f, disp, tracking, daysSinceLabel } of candidates) {
      const events = eventsMap[f.id] || [];
      const latest = events[0] || null;
      const latestStatus = (latest?.status || "").toUpperCase();
      if (latestStatus === "DELIVERED") continue; // display status can lag the event feed

      let stage;
      if (EXCEPTION_STATUSES.has(disp) || EXCEPTION_STATUSES.has(latestStatus)) stage = "exception";
      else if (!f.inTransitAt && PRE_TRANSIT_STATUSES.has(latestStatus) && PRE_TRANSIT_STATUSES.has(disp)) stage = "label_only";
      else stage = "in_transit";

      const addr = o.shippingAddress || {};
      rows.push({
        orderId: o.id,
        orderName: o.name,
        adminUrl: adminOrderUrl(o.id),
        poNumber: o.poNumber || "",
        fulfillmentId: f.id,
        fulfillmentName: f.name || "",
        email: o.email || "",
        customerName: companyName(o),
        shippingAddress: [addr.address1, addr.address2, addr.city, addr.province, addr.zip].filter(Boolean).join(", "),
        orderTotal: money(o),
        labelCreatedAt: f.createdAt,
        daysSinceLabel: Math.floor(daysSinceLabel),
        displayStatus: disp,
        stage,
        inTransitAt: f.inTransitAt || null,
        estimatedDeliveryAt: f.estimatedDeliveryAt || null,
        lastEvent: latest,
        daysSinceLastEvent: latest ? Math.floor((now - Date.parse(latest.happenedAt)) / 864e5) : null,
        allEvents: events,
        tracking: tracking.map(t => ({
          company: t.company || "",
          number: t.number,
          url: t.url || (CARRIER_IS_UPS ? upsTrackingUrl(t.number) : null),
        })),
        items: (f.fulfillmentLineItems?.edges || [])
          .map(e => `${e.node.lineItem?.sku || e.node.lineItem?.title || "?"} x${e.node.quantity}`),
        tags: (o.tags || []).join(", "),
      });
    }

    rows.sort((a, b) => b.daysSinceLabel - a.daysSinceLabel);
    await writeSnapshot("labels", rows, {
      scanned: orders.length,
      staleDays: LABEL_STALE_DAYS,
      lookbackDays: LABEL_LOOKBACK_DAYS,
      carrier: LABEL_CARRIER,
    });
    console.log(`[sync] B2B labels: ${rows.length} stale of ${candidates.length} candidates (${orders.length} orders scanned)`);
    return rows.length;
  }

  // Single in-flight promise so runSync and a first-load API call never
  // run the same sync twice concurrently.
  let inFlight = null;
  function sync() {
    if (!CREDS.b2bStore || !CREDS.b2bToken) return Promise.resolve();
    if (inFlight) return inFlight;
    inFlight = (async () => {
      try { await syncUnfulfilled(); } catch (err) { console.warn("[sync] B2B unfulfilled error:", err.message); }
      try { await syncLabels(); } catch (err) { console.warn("[sync] B2B labels error:", err.message); }
    })().finally(() => { inFlight = null; });
    return inFlight;
  }

  async function readSnapshots() {
    const r = await db.query("SELECT kind, rows_json, meta_json, synced_at FROM b2b_stale_snapshot");
    const out = {};
    for (const row of r.rows) {
      out[row.kind] = {
        rows: JSON.parse(row.rows_json || "[]"),
        meta: JSON.parse(row.meta_json || "{}"),
        syncedAt: row.synced_at,
      };
    }
    return out;
  }

  function splitResolved(snap) {
    if (!snap) return { rows: [], resolvedCount: 0, meta: {}, syncedAt: null };
    const active = snap.rows.filter(r => !hasResolvedTag(r.tags));
    return { rows: active, resolvedCount: snap.rows.length - active.length, meta: snap.meta, syncedAt: snap.syncedAt };
  }

  app.post("/api/b2b-stale", async (req, res) => {
    if (!CREDS.b2bStore || !CREDS.b2bToken) return res.status(400).json({ error: "Missing B2B credentials." });
    try {
      let snaps = await readSnapshots();
      // First load after deploy: no snapshot yet, so sync inline once.
      if (!snaps.unfulfilled || !snaps.labels) {
        await sync();
        snaps = await readSnapshots();
      }
      res.json({
        unfulfilled: splitResolved(snaps.unfulfilled),
        labels: splitResolved(snaps.labels),
      });
    } catch (err) {
      console.error(err);
      res.status(500).json({ error: err.message });
    }
  });

  return { initTable, sync };
};
