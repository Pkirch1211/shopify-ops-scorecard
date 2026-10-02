// ── B2B Lookup (read-only) ────────────────────────────────────────────────────
// Answers "how many units (and dollars) does <customer> have on order of <SKUs>?"
// without touching any existing endpoint, cache, or table.
//
// Wire-up in server.js (after the CREDS block and gql/gqlAll definitions):
//     require("./lookup")(app, { gql, gqlAll, CREDS });
//
// Required Shopify scopes on the B2B token: read_orders, read_draft_orders,
// read_customers (the last one is new — needed for the customer dropdown and
// for customer_id order search).

module.exports = function registerLookup(app, { gql, gqlAll, CREDS }) {
  const DIRECTORY_TTL = 60 * 60 * 1000; // customers rarely change
  const DRAFTS_TTL = 5 * 60 * 1000;
  const ID_CHUNK = 25;                  // customer ids per order search
  const SKU_RE = /^[A-Za-z0-9._\-]+$/;

  let directoryCache = null, directoryCacheTime = 0;
  let draftsCache = null, draftsCacheTime = 0;

  const gidNum = id => (id || "").split("/").pop();
  const labelOf = n =>
    n.shippingAddress?.company || n.billingAddress?.company ||
    n.customer?.displayName || n.email || "Unknown";
  const money = v => Math.round((parseFloat(v) || 0) * 100) / 100;

  // ── Queries ────────────────────────────────────────────────────────────────
  const CUSTOMERS_QUERY = `
  query LookupCustomers($first: Int!, $after: String, $query: String) {
    customers(first: $first, after: $after, query: $query, sortKey: NAME) {
      pageInfo { hasNextPage endCursor }
      edges { node { id displayName email defaultAddress { company } } }
    }
  }`;

  // discountedTotalSet = line total after line-level discounts / price overrides
  // (whole quantity), so unit value = discountedTotalSet / quantity.
  const DRAFTS_QUERY = `
  query LookupDrafts($first: Int!, $after: String, $query: String!) {
    draftOrders(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
      pageInfo { hasNextPage endCursor }
      edges {
        node {
          id name createdAt email tags
          customer { id displayName }
          shippingAddress { company }
          billingAddress { company }
          lineItems(first: 100) {
            edges { node { sku title quantity discountedTotalSet { shopMoney { amount } } } }
          }
        }
      }
    }
  }`;

  // discountedUnitPriceSet = unit price after line-level discounts (order-level
  // discounts are NOT allocated, by design — see "merchandise value" note on the page).
  const ORDERS_QUERY = `
  query LookupOrders($first: Int!, $after: String, $query: String!) {
    orders(first: $first, after: $after, query: $query, sortKey: CREATED_AT, reverse: true) {
      pageInfo { hasNextPage endCursor }
      edges {
        node {
          id name createdAt cancelledAt email
          customer { id displayName }
          shippingAddress { company }
          billingAddress { company }
          lineItems(first: 100) {
            edges {
              node {
                sku title quantity currentQuantity unfulfilledQuantity
                discountedUnitPriceSet { shopMoney { amount } }
              }
            }
          }
        }
      }
    }
  }`;

  // ── Data access ────────────────────────────────────────────────────────────
  async function getOpenDrafts() {
    if (draftsCache && Date.now() - draftsCacheTime < DRAFTS_TTL) return draftsCache;
    const { b2bStore, b2bToken } = CREDS;
    const drafts = await gqlAll(b2bStore, b2bToken, DRAFTS_QUERY,
      { first: 250, query: "status:open" },
      d => d.draftOrders.edges, d => d.draftOrders.pageInfo, 120000);
    draftsCache = drafts;
    draftsCacheTime = Date.now();
    return drafts;
  }

  // Customers grouped by company, so "HomeGoods" with 6 buyer contacts is one
  // dropdown entry that expands to all 6 customer ids when selected.
  async function buildDirectory() {
    const { b2bStore, b2bToken } = CREDS;
    const customers = await gqlAll(b2bStore, b2bToken, CUSTOMERS_QUERY,
      { first: 250, query: "orders_count:>0" },
      d => d.customers.edges, d => d.customers.pageInfo, 180000);

    const groups = new Map();
    const add = (id, company, name, email) => {
      const label = (company || name || email || "Unknown").trim();
      const key = label.toLowerCase();
      if (!groups.has(key)) groups.set(key, { key, label, ids: new Set(), emails: new Set() });
      const g = groups.get(key);
      if (id) g.ids.add(id);
      if (email) g.emails.add(email.toLowerCase());
    };

    for (const c of customers) add(gidNum(c.id), c.defaultAddress?.company, c.displayName, c.email);

    // Customers that only exist on an open draft (brand-new accounts).
    try {
      for (const d of await getOpenDrafts()) {
        if (d.customer?.id) add(gidNum(d.customer.id), d.shippingAddress?.company || d.billingAddress?.company, d.customer.displayName, d.email);
      }
    } catch (e) { console.warn("[lookup] drafts merge for directory failed:", e.message); }

    return [...groups.values()]
      .map(g => ({ key: g.key, label: g.label, ids: [...g.ids], emails: [...g.emails].slice(0, 3) }))
      .sort((a, b) => a.label.localeCompare(b.label));
  }

  // ── GET customer directory (dropdown source) ───────────────────────────────
  app.get("/api/lookup/customers", async (req, res) => {
    const { b2bStore, b2bToken } = CREDS;
    if (!b2bStore || !b2bToken) return res.status(400).json({ error: "Missing B2B credentials." });
    try {
      if (req.query.refresh !== "true" && directoryCache && Date.now() - directoryCacheTime < DIRECTORY_TTL) {
        return res.json({ customers: directoryCache, cached: true });
      }
      directoryCache = await buildDirectory();
      directoryCacheTime = Date.now();
      res.json({ customers: directoryCache, cached: false });
    } catch (err) {
      console.error("[lookup] directory error:", err);
      if (directoryCache) return res.json({ customers: directoryCache, cached: true, stale: true });
      const hint = /access denied|scope/i.test(err.message)
        ? " (B2B token needs the read_customers scope)" : "";
      res.status(500).json({ error: err.message + hint });
    }
  });

  // ── POST query ─────────────────────────────────────────────────────────────
  // body: { customerIds: [numeric], skus: [string], from: 'YYYY-MM-DD', to: 'YYYY-MM-DD' }
  // Returns flat line-level records; the page pivots them either by SKU
  // (customer mode) or by customer (SKU mode).
  //   draft record : open = draft line quantity, openValue = discounted line total
  //   order record : open = unfulfilledQuantity, fulfilled = currentQuantity - unfulfilledQuantity
  //                  (fulfilled only counted when the order was created inside from..to)
  //                  values = units × discounted unit price (line-level discounts only)
  app.post("/api/lookup/query", async (req, res) => {
    const { b2bStore, b2bToken } = CREDS;
    if (!b2bStore || !b2bToken) return res.status(400).json({ error: "Missing B2B credentials." });

    try {
      const customerIds = (req.body.customerIds || []).map(String).filter(s => /^\d+$/.test(s));
      const skus = [...new Set((req.body.skus || []).map(s => String(s).trim().toUpperCase()).filter(s => SKU_RE.test(s)))];
      if (!customerIds.length && !skus.length) {
        return res.status(400).json({ error: "Pick at least one customer or one SKU." });
      }

      const year = new Date().getUTCFullYear();
      const fromStr = /^\d{4}-\d{2}-\d{2}$/.test(req.body.from || "") ? req.body.from : `${year}-01-01`;
      const toStr = /^\d{4}-\d{2}-\d{2}$/.test(req.body.to || "") ? req.body.to : new Date().toISOString().slice(0, 10);
      const fromMs = Date.parse(fromStr + "T00:00:00Z");
      const toExclusive = new Date(Date.parse(toStr + "T00:00:00Z") + 864e5);
      const toExclusiveStr = toExclusive.toISOString().slice(0, 10);

      const idSet = new Set(customerIds);
      const skuSet = new Set(skus);
      const records = [];

      // Drafts — filtered in memory off the cached open-draft pull.
      const drafts = await getOpenDrafts();
      for (const d of drafts) {
        if (idSet.size && !idSet.has(gidNum(d.customer?.id))) continue;
        for (const e of d.lineItems?.edges || []) {
          const li = e.node;
          const sku = (li.sku || "").toUpperCase();
          if (skuSet.size && !skuSet.has(sku)) continue;
          if (!li.quantity) continue;
          records.push({
            type: "draft", name: d.name, createdAt: d.createdAt,
            customerId: gidNum(d.customer?.id), label: labelOf(d),
            sku: sku || "—", title: li.title || "",
            open: li.quantity, fulfilled: 0,
            openValue: money(li.discountedTotalSet?.shopMoney?.amount), fulfilledValue: 0,
          });
        }
      }

      // Orders — two searches per customer chunk, deduped by order id:
      //   A) anything still unfulfilled/partial, any age (this is "on order")
      //   B) anything created inside the date range (this feeds "fulfilled")
      const skuClause = skus.length ? "(" + skus.map(s => `sku:"${s}"`).join(" OR ") + ")" : "";
      const chunks = customerIds.length
        ? Array.from({ length: Math.ceil(customerIds.length / ID_CHUNK) }, (_, i) => customerIds.slice(i * ID_CHUNK, (i + 1) * ID_CHUNK))
        : [[]];

      const orderMap = new Map();
      for (const chunk of chunks) {
        const custClause = chunk.length ? "(" + chunk.map(i => `customer_id:${i}`).join(" OR ") + ")" : "";
        const base = ["-status:cancelled", custClause, skuClause].filter(Boolean).join(" ");
        const queries = [
          `${base} (fulfillment_status:unfulfilled OR fulfillment_status:partial)`,
          `${base} created_at:>=${fromStr} created_at:<${toExclusiveStr}`,
        ];
        for (const q of queries) {
          const nodes = await gqlAll(b2bStore, b2bToken, ORDERS_QUERY,
            { first: 100, query: q },
            d => d.orders.edges, d => d.orders.pageInfo, 120000);
          for (const n of nodes) orderMap.set(n.id, n);
        }
      }

      for (const o of orderMap.values()) {
        if (o.cancelledAt) continue;
        if (idSet.size && !idSet.has(gidNum(o.customer?.id))) continue;
        const inRange = new Date(o.createdAt).getTime() >= fromMs && new Date(o.createdAt) < toExclusive;
        for (const e of o.lineItems?.edges || []) {
          const li = e.node;
          const sku = (li.sku || "").toUpperCase();
          if (skuSet.size && !skuSet.has(sku)) continue;
          const open = Math.max(0, li.unfulfilledQuantity ?? 0);
          const current = li.currentQuantity ?? li.quantity ?? 0;
          const fulfilled = inRange ? Math.max(0, current - open) : 0;
          if (!open && !fulfilled) continue;
          const unit = parseFloat(li.discountedUnitPriceSet?.shopMoney?.amount) || 0;
          records.push({
            type: "order", name: o.name, createdAt: o.createdAt,
            customerId: gidNum(o.customer?.id), label: labelOf(o),
            sku: sku || "—", title: li.title || "",
            open, fulfilled,
            openValue: money(open * unit), fulfilledValue: money(fulfilled * unit),
          });
        }
      }

      res.json({
        asOf: new Date().toISOString(),
        range: { from: fromStr, to: toStr },
        records,
      });
    } catch (err) {
      console.error("[lookup] query error:", err);
      const hint = /access denied|scope/i.test(err.message)
        ? " (B2B token may be missing read_customers)" : "";
      res.status(500).json({ error: err.message + hint });
    }
  });
};
