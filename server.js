const express = require("express");
const path = require("path");

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

// ── Shopify fetch helper ──────────────────────────────────────────────────────
async function shopifyFetch(store, token, endpoint) {
  const base = `https://${store}/admin/api/2024-01`;
  const url = `${base}${endpoint}`;
  const res = await fetch(url, {
    headers: {
      "X-Shopify-Access-Token": token,
      "Content-Type": "application/json",
    },
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Shopify ${res.status}: ${text}`);
  }
  return res.json();
}

// Paginate through all pages of a resource
async function shopifyFetchAll(store, token, endpoint, key) {
  let results = [];
  let url = `https://${store}/admin/api/2024-01${endpoint}`;

  while (url) {
    const res = await fetch(url, {
      headers: {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json",
      },
    });
    if (!res.ok) throw new Error(`Shopify ${res.status}`);
    const data = await res.json();
    results = results.concat(data[key] || []);

    // Parse Link header for next page
    const link = res.headers.get("Link");
    url = null;
    if (link) {
      const match = link.match(/<([^>]+)>;\s*rel="next"/);
      if (match) url = match[1];
    }
  }
  return results;
}

// ── Compute time-between helpers ─────────────────────────────────────────────
function hoursBetween(a, b) {
  if (!a || !b) return null;
  return (new Date(b) - new Date(a)) / 36e5;
}

function avg(arr) {
  const valid = arr.filter((v) => v !== null && v !== undefined && !isNaN(v));
  if (!valid.length) return null;
  return valid.reduce((a, b) => a + b, 0) / valid.length;
}

function formatDuration(hours) {
  if (hours === null || hours === undefined) return "—";
  if (hours < 1) return `${Math.round(hours * 60)}m`;
  if (hours < 24) return `${hours.toFixed(1)}h`;
  return `${(hours / 24).toFixed(1)}d`;
}

// ── Credentials from environment ─────────────────────────────────────────────
const CREDS = {
  dtcStore: process.env.DTC_STORE,
  dtcToken: process.env.DTC_TOKEN,
  b2bStore: process.env.B2B_STORE,
  b2bToken: process.env.B2B_TOKEN,
};

// ── Main data endpoint ────────────────────────────────────────────────────────
app.post("/api/scorecard", async (req, res) => {
  const { year, month } = req.body;
  const { dtcStore, dtcToken, b2bStore, b2bToken } = CREDS;

  if (!dtcStore || !dtcToken || !b2bStore || !b2bToken) {
    return res.status(400).json({ error: "Missing store credentials — set DTC_STORE, DTC_TOKEN, B2B_STORE, B2B_TOKEN in Railway environment variables." });
  }

  // Build date range for the selected month
  const start = new Date(year, month - 1, 1).toISOString();
  const end = new Date(year, month, 1).toISOString();

  try {
    const stores = [
      { label: "DTC", store: dtcStore, token: dtcToken },
      { label: "B2B", store: b2bStore, token: b2bToken },
    ];

    const results = await Promise.all(
      stores.map(async ({ label, store, token }) => {
        // Slim down fields to only what we need — faster response
        const orderQS = `/orders.json?status=any&created_at_min=${start}&created_at_max=${end}&limit=250&fields=id,order_number,created_at,fulfillments,line_items,email,shipping_address,billing_address`;
        // Query drafts completed within the month
        const draftQS = `/draft_orders.json?status=completed&updated_at_min=${start}&updated_at_max=${end}&limit=250&fields=id,created_at,completed_at,status`;

        const [orders, draftOrders] = await Promise.all([
          shopifyFetchAll(store, token, orderQS, "orders"),
          shopifyFetchAll(store, token, draftQS, "draft_orders"),
        ]);

        // Declare metric arrays
        const processingTimes = []; // draft created_at → draft completed_at
        const fulfillmentTimes = []; // order created_at → first fulfillment created_at
        const deliveryTimes = [];   // first fulfillment created_at → delivered updated_at

        // Processing time = how long from draft creation to completion
        for (const d of draftOrders) {
          if (d.status === 'completed' && d.completed_at) {
            const h = hoursBetween(d.created_at, d.completed_at);
            if (h !== null && h >= 0) processingTimes.push(h);
          }
        }

        let totalUnits = 0;
        const flaggedOrders = [];
        const THRESHOLD_HOURS = 10 * 24;
        const EXCLUDED_EMAILS = ['inquiries@lifelines.com', 'care@lifelines.com'];

        for (const order of orders) {
          // Units
          const units = (order.line_items || []).reduce(
            (s, li) => s + (li.quantity || 0), 0
          );
          totalUnits += units;

          const customerName = order.shipping_address
            ? `${order.shipping_address.first_name || ""} ${order.shipping_address.last_name || ""}`.trim()
            : (order.billing_address
              ? `${order.billing_address.first_name || ""} ${order.billing_address.last_name || ""}`.trim()
              : order.email || "—");

          let processingHours = null;

          // Fulfillment time
          let fulfillmentHours = null;
          let deliveryHours = null;
          let trackingInfo = null;

          if (order.fulfillments && order.fulfillments.length > 0) {
            const sortedFulfillments = [...order.fulfillments].sort(
              (a, b) => new Date(a.created_at) - new Date(b.created_at)
            );
            const firstFulfillment = sortedFulfillments[0];

            const fh = hoursBetween(order.created_at, firstFulfillment.created_at);
            if (fh !== null && fh >= 0) {
              fulfillmentTimes.push(fh);
              fulfillmentHours = fh;
            }

            // Delivery time
            const delivered = order.fulfillments.find(
              (f) => f.shipment_status === "delivered"
            );
            if (delivered && delivered.updated_at) {
              const dh = hoursBetween(firstFulfillment.created_at, delivered.updated_at);
              if (dh !== null && dh >= 0) {
                deliveryTimes.push(dh);
                deliveryHours = dh;
              }
            }

            // Tracking info from the most recent fulfillment with a tracking number
            const withTracking = [...order.fulfillments]
              .reverse()
              .find(f => f.tracking_number);
            if (withTracking) {
              trackingInfo = {
                number: withTracking.tracking_number,
                company: withTracking.tracking_company || null,
                url: withTracking.tracking_url || null,
                shipmentStatus: withTracking.shipment_status || null,
                updatedAt: withTracking.updated_at || null,
              };
            }
          }

          // Flag if any metric exceeds 10 days
          const issues = [];
          if (processingHours !== null && processingHours > THRESHOLD_HOURS)
            issues.push({ type: "processing", hours: processingHours });
          if (fulfillmentHours !== null && fulfillmentHours > THRESHOLD_HOURS)
            issues.push({ type: "fulfillment", hours: fulfillmentHours });
          if (deliveryHours !== null && deliveryHours > THRESHOLD_HOURS)
            issues.push({ type: "delivery", hours: deliveryHours });

          // Also flag in-transit orders with no delivery after 10 days
          if (
            order.fulfillments && order.fulfillments.length > 0 &&
            !order.fulfillments.find(f => f.shipment_status === "delivered")
          ) {
            const firstFulfillment = [...order.fulfillments].sort(
              (a, b) => new Date(a.created_at) - new Date(b.created_at)
            )[0];
            const daysSinceFulfillment = hoursBetween(firstFulfillment.created_at, new Date().toISOString());
            if (daysSinceFulfillment !== null && daysSinceFulfillment > THRESHOLD_HOURS) {
              issues.push({ type: "delivery_stalled", hours: daysSinceFulfillment });
            }
          }

          if (issues.length > 0 && !EXCLUDED_EMAILS.includes((order.email || '').toLowerCase())) {
            flaggedOrders.push({
              store: label,
              orderNumber: order.order_number,
              orderId: order.id,
              customerName,
              email: order.email || null,
              units,
              createdAt: order.created_at,
              issues,
              processingHours,
              fulfillmentHours,
              deliveryHours,
              tracking: trackingInfo,
            });
          }
        }

        return {
          label,
          totalOrders: orders.length,
          totalUnits,
          avgProcessingHours: avg(processingTimes),
          avgFulfillmentHours: avg(fulfillmentTimes),
          avgDeliveryHours: avg(deliveryTimes),
          ordersByDay: buildDailyTimeSeries(orders, year, month),
          fulfillmentsByDay: buildFulfillmentTimeSeries(orders, year, month),
          rawProcessingTimes: processingTimes,
          rawFulfillmentTimes: fulfillmentTimes,
          flaggedOrders,
        };
      })
    );

    // Aggregate
    const combined = {
      label: "Combined",
      totalOrders: results.reduce((s, r) => s + r.totalOrders, 0),
      totalUnits: results.reduce((s, r) => s + r.totalUnits, 0),
      avgProcessingHours: avg(results.flatMap((r) => r.rawProcessingTimes)),
      avgFulfillmentHours: avg(results.flatMap((r) => r.rawFulfillmentTimes)),
      avgDeliveryHours: avg(
        results
          .map((r) => r.avgDeliveryHours)
          .filter((v) => v !== null)
      ),
    };

    res.json({
      stores: results.map((r) => ({
        ...r,
        avgProcessingFormatted: formatDuration(r.avgProcessingHours),
        avgFulfillmentFormatted: formatDuration(r.avgFulfillmentHours),
        avgDeliveryFormatted: formatDuration(r.avgDeliveryHours),
      })),
      combined: {
        ...combined,
        avgProcessingFormatted: formatDuration(combined.avgProcessingHours),
        avgFulfillmentFormatted: formatDuration(combined.avgFulfillmentHours),
        avgDeliveryFormatted: formatDuration(combined.avgDeliveryHours),
      },
      flaggedOrders: results.flatMap(r => r.flaggedOrders)
        .sort((a, b) => {
          const maxHours = o => Math.max(...o.issues.map(i => i.hours));
          return maxHours(b) - maxHours(a);
        }),
      year,
      month,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// ── Time-series helpers ───────────────────────────────────────────────────────
function buildDailyTimeSeries(orders, year, month) {
  const days = new Date(year, month, 0).getDate();
  const counts = Array.from({ length: days }, (_, i) => ({
    day: i + 1,
    orders: 0,
    units: 0,
  }));
  for (const order of orders) {
    const d = new Date(order.created_at).getDate();
    if (counts[d - 1]) {
      counts[d - 1].orders++;
      counts[d - 1].units += (order.line_items || []).reduce(
        (s, li) => s + (li.quantity || 0),
        0
      );
    }
  }
  return counts;
}

function buildFulfillmentTimeSeries(orders, year, month) {
  const days = new Date(year, month, 0).getDate();
  const counts = Array.from({ length: days }, (_, i) => ({
    day: i + 1,
    fulfilled: 0,
  }));
  for (const order of orders) {
    for (const f of order.fulfillments || []) {
      const d = new Date(f.created_at).getDate();
      if (counts[d - 1]) counts[d - 1].fulfilled++;
    }
  }
  return counts;
}

// ── Care@ Scorecard endpoint (filters to care@lifelines.com orders only) ─────
app.post("/api/care-scorecard", async (req, res) => {
  const { year, month } = req.body;
  const { dtcStore, dtcToken } = CREDS;

  if (!dtcStore || !dtcToken) {
    return res.status(400).json({ error: "Missing DTC credentials." });
  }

  const start = new Date(year, month - 1, 1).toISOString();
  const end = new Date(year, month, 1).toISOString();

  try {
    // Fetch orders for care@lifelines.com only
    const orders = await shopifyFetchAll(
      dtcStore, dtcToken,
      `/orders.json?status=any&created_at_min=${start}&created_at_max=${end}&email=care@lifelines.com&limit=250&fields=id,order_number,created_at,fulfillments,line_items,email`,
      "orders"
    );

    const fulfillmentTimes = [];
    const deliveryTimes = [];
    let totalUnits = 0;

    for (const order of orders) {
      totalUnits += (order.line_items || []).reduce((s, li) => s + (li.quantity || 0), 0);
      if (order.fulfillments && order.fulfillments.length > 0) {
        const first = [...order.fulfillments].sort((a, b) => new Date(a.created_at) - new Date(b.created_at))[0];
        const fh = hoursBetween(order.created_at, first.created_at);
        if (fh !== null && fh >= 0) fulfillmentTimes.push(fh);
        const delivered = order.fulfillments.find(f => f.shipment_status === "delivered");
        if (delivered) {
          const dh = hoursBetween(first.created_at, delivered.updated_at);
          if (dh !== null && dh >= 0) deliveryTimes.push(dh);
        }
      }
    }

    res.json({
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
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// ── B2B Draft Orders endpoint ─────────────────────────────────────────────────
app.post('/api/b2b-drafts', async (req, res) => {
  const { b2bStore, b2bToken } = CREDS;
  if (!b2bStore || !b2bToken) {
    return res.status(400).json({ error: 'Missing B2B credentials — set B2B_STORE and B2B_TOKEN in Railway environment variables.' });
  }
  try {
    // 1. All open draft orders
    const drafts = await shopifyFetchAll(
      b2bStore, b2bToken,
      '/draft_orders.json?status=open&limit=250&fields=id,name,created_at,updated_at,tags,email,customer,line_items,subtotal_price,total_price,note',
      'draft_orders'
    );

    // 2. needs-review subset
    const needsReview = drafts.filter(d =>
      (d.tags || '').split(',').map(t => t.trim().toLowerCase()).includes('needs-review')
    );

    // 3. Draft count by customer
    const customerMap = {};
    for (const d of drafts) {
      const name = d.customer
        ? (d.customer.first_name + ' ' + (d.customer.last_name || '')).trim()
        : null;
      const email = d.email || null;
      const key = (name && name !== '') ? name : (email || 'Unknown');
      if (!customerMap[key]) {
        customerMap[key] = { customer: key, email: email || '—', draftCount: 0, totalValue: 0 };
      }
      customerMap[key].draftCount++;
      customerMap[key].totalValue += parseFloat(d.total_price || 0);
    }
    const byCustomer = Object.values(customerMap).sort((a, b) => b.draftCount - a.draftCount);

    // 4. Unique variant IDs across all drafts
    const variantIds = [...new Set(
      drafts.flatMap(d => (d.line_items || []).map(li => li.variant_id).filter(Boolean))
    )];

    // 5. Variant info in batches of 50
    const variantMap = {};
    for (let i = 0; i < variantIds.length; i += 50) {
      const ids = variantIds.slice(i, i + 50).join(',');
      const data = await shopifyFetchAll(
        b2bStore, b2bToken,
        '/variants.json?ids=' + ids + '&limit=250&fields=id,inventory_item_id,sku,title,product_id',
        'variants'
      );
      for (const v of data) {
        variantMap[v.id] = { inventoryItemId: v.inventory_item_id, sku: v.sku || '—', title: v.title || '', productId: v.product_id };
      }
    }

    // 6. Inventory levels in batches of 50
    const invItemIds = [...new Set(Object.values(variantMap).map(v => v.inventoryItemId).filter(Boolean))];
    const inventoryMap = {};
    for (let i = 0; i < invItemIds.length; i += 50) {
      const ids = invItemIds.slice(i, i + 50).join(',');
      const levels = await shopifyFetchAll(
        b2bStore, b2bToken,
        '/inventory_levels.json?inventory_item_ids=' + ids + '&limit=250',
        'inventory_levels'
      );
      for (const lvl of levels) {
        const id = lvl.inventory_item_id;
        inventoryMap[id] = (inventoryMap[id] || 0) + (lvl.available || 0);
      }
    }

    // 7. Product titles in batches of 50
    const productIds = [...new Set(Object.values(variantMap).map(v => v.productId).filter(Boolean))];
    const productTitleMap = {};
    for (let i = 0; i < productIds.length; i += 50) {
      const ids = productIds.slice(i, i + 50).join(',');
      const data = await shopifyFetchAll(
        b2bStore, b2bToken,
        '/products.json?ids=' + ids + '&limit=250&fields=id,title',
        'products'
      );
      for (const p of data) productTitleMap[p.id] = p.title;
    }

    // 8. OOS table: available <= 0, referenced by open drafts
    const oosMap = {};
    for (const draft of drafts) {
      for (const li of draft.line_items || []) {
        if (!li.variant_id) continue;
        const v = variantMap[li.variant_id];
        if (!v) continue;
        const available = inventoryMap[v.inventoryItemId] ?? null;
        if (available === null || available > 0) continue;
        if (!oosMap[li.variant_id]) {
          oosMap[li.variant_id] = {
            sku: v.sku,
            productTitle: productTitleMap[v.productId] || li.title || 'Unknown',
            variantTitle: v.title,
            available,
            draftCount: 0,
            totalUnitsRequested: 0,
            affectedDrafts: [],
          };
        }
        oosMap[li.variant_id].draftCount++;
        oosMap[li.variant_id].totalUnitsRequested += li.quantity || 0;
        oosMap[li.variant_id].affectedDrafts.push(draft.name);
      }
    }
    const oosItems = Object.values(oosMap).sort((a, b) => b.draftCount - a.draftCount);

    // 9. needs-review export shape
    const needsReviewExport = needsReview.map(d => ({
      name: d.name,
      createdAt: d.created_at,
      updatedAt: d.updated_at,
      tags: d.tags || '',
      customerName: d.customer ? (d.customer.first_name + ' ' + (d.customer.last_name || '')).trim() : (d.email || '—'),
      email: d.email || '',
      subtotal: d.subtotal_price,
      total: d.total_price,
      note: d.note || '',
      lineItems: (d.line_items || []).map(li => ({
        title: li.title, variantTitle: li.variant_title || '', sku: li.sku || '', quantity: li.quantity, price: li.price,
      })),
    }));

    res.json({ totalDrafts: drafts.length, needsReviewCount: needsReview.length, needsReviewExport, byCustomer, oosItems });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// ── DTC Stale Fulfillments (GraphQL) ─────────────────────────────────────────
const DTC_GQL_QUERY = `
query FulfilledOrders($first: Int!, $after: String, $query: String!) {
  orders(first: $first, after: $after, query: $query, sortKey: CREATED_AT, reverse: true) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id name createdAt cancelledAt displayFulfillmentStatus email
        totalPriceSet { shopMoney { amount currencyCode } }
        customer { displayName }
        shippingAddress { name address1 address2 city province zip country }
        fulfillments(first: 10) {
          id name createdAt updatedAt status displayStatus
          trackingInfo(first: 10) { company number url }
          events(first: 50) {
            edges { node { status happenedAt message } }
          }
          fulfillmentLineItems(first: 50) {
            edges { node { quantity lineItem { sku title } } }
          }
        }
        tags
      }
    }
  }
}`;

async function shopifyGQL(store, token, query, variables) {
  const url = `https://${store}/admin/api/2024-01/graphql.json`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' },
    body: JSON.stringify({ query, variables }),
  });
  if (!res.ok) throw new Error(`Shopify GQL HTTP ${res.status}`);
  const payload = await res.json();
  if (payload.errors) throw new Error(payload.errors.map(e => e.message).join('; '));
  return payload.data;
}

app.post('/api/dtc-stale', async (req, res) => {
  const { dtcStore, dtcToken } = CREDS;
  if (!dtcStore || !dtcToken) {
    return res.status(400).json({ error: 'Missing DTC credentials — set DTC_STORE and DTC_TOKEN in Railway.' });
  }

  const MIN_DAYS = 10;
  const TARGET_STATUSES = new Set(['IN_TRANSIT', 'CONFIRMED']);

  function daysSince(isoStr) {
    if (!isoStr) return null;
    return (Date.now() - new Date(isoStr).getTime()) / 864e5;
  }

  function latestEvent(fulfillment) {
    const events = (fulfillment.events?.edges || [])
      .map(e => e.node)
      .filter(e => e.happenedAt)
      .sort((a, b) => new Date(b.happenedAt) - new Date(a.happenedAt));
    return events[0] || null;
  }

  function allEvents(fulfillment) {
    return (fulfillment.events?.edges || [])
      .map(e => e.node)
      .filter(e => e.happenedAt)
      .sort((a, b) => new Date(b.happenedAt) - new Date(a.happenedAt));
  }

  try {
    const rows = [];
    let after = null;
    let pages = 0;
    const MAX_PAGES = 50; // safety cap

    while (pages < MAX_PAGES) {
      const data = await shopifyGQL(dtcStore, dtcToken, DTC_GQL_QUERY, {
        first: 100,
        after,
        query: 'fulfillment_status:fulfilled -status:cancelled',
      });

      const conn = data.orders;
      for (const edge of conn.edges) {
        const order = edge.node;
        if (order.cancelledAt) continue;

        for (const f of order.fulfillments || []) {
          const status = (f.displayStatus || '').toUpperCase().replace(/ /g, '_');
          if (!TARGET_STATUSES.has(status)) continue;
          const days = daysSince(f.createdAt);
          if (days === null || days < MIN_DAYS) continue;

          const latest = latestEvent(f);
          const events = allEvents(f);
          const tracking = (f.trackingInfo || []);
          const skus = (f.fulfillmentLineItems?.edges || []).map(e => {
            const li = e.node.lineItem;
            return `${li.sku || li.title} x${e.node.quantity}`;
          });

          const addr = order.shippingAddress || {};
          const total = order.totalPriceSet?.shopMoney || {};

          rows.push({
            orderName: order.name,
            orderId: order.id,
            orderCreatedAt: order.createdAt,
            email: order.email,
            customerName: order.customer?.displayName || addr.name || '',
            shippingAddress: [addr.address1, addr.address2, addr.city, addr.province, addr.zip, addr.country].filter(Boolean).join(', '),
            orderTotal: total.amount ? `${total.currencyCode} ${parseFloat(total.amount).toFixed(2)}` : '',
            fulfillmentId: f.id,
            fulfillmentName: f.name,
            fulfilledAt: f.createdAt,
            daysSinceFulfilled: Math.floor(days),
            fulfillmentStatus: f.status,
            displayStatus: f.displayStatus,
            hasTracking: tracking.length > 0,
            tracking: tracking.map(t => ({ company: t.company, number: t.number, url: t.url })),
            skus,
            tags: (order.tags || []).join(', '),
            latestEvent: latest ? { status: latest.status, happenedAt: latest.happenedAt, message: latest.message } : null,
            allEvents: events.map(e => ({ status: e.status, happenedAt: e.happenedAt, message: e.message })),
          });
        }
      }

      pages++;
      if (!conn.pageInfo.hasNextPage) break;
      after = conn.pageInfo.endCursor;
      // small delay to avoid rate limits
      await new Promise(r => setTimeout(r, 200));
    }

    // Sort worst first
    rows.sort((a, b) => b.daysSinceFulfilled - a.daysSinceFulfilled);
    res.json({ rows, scannedPages: pages, total: rows.length });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// ── Start ─────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log('Ops Scorecard running on :' + PORT));
