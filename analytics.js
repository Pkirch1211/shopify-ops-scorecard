// ── B2B Analytics (read-only) ─────────────────────────────────────────────────
// Answers "what does <customer> have on order, and how much is it worth?" and
// "who has <SKU> on order?" without touching any existing endpoint, cache, or table.
//
// Wire-up in server.js (after the CREDS block and gql/gqlAll/restFetchAll definitions):
//     require("./analytics")(app, { gql, gqlAll, CREDS, db });
//
// Quick check after deploy: open /api/analytics/ping — it should return {"ok":true}.
// If it returns a web page instead, the require line is missing or analytics.js
// wasn't deployed.
//
// Required Shopify scopes on the B2B token: read_orders, read_draft_orders,
// read_customers (the last one is new — needed for the customer dropdown and
// for customer_id order search).

module.exports = function registerAnalytics(app, { gql, gqlAll, CREDS, db }) {
  const DIRECTORY_TTL = 60 * 60 * 1000; // customer list is refreshed in the background after this
  const DRAFTS_TTL = 5 * 60 * 1000;
  const ID_CHUNK = 25;                  // customer ids per order search
  const SKU_RE = /^[A-Za-z0-9._\-]+$/;

  // Page sizes mirror what the existing queries in server.js already run at
  // (50 x 100 nested = same cost class as their 250 x 20), so Shopify's
  // single-query cost cap isn't a new risk. gqlAll paginates for us.
  const PAGE = 50;

  let directoryCache = null, directoryCacheTime = 0, building = null;
  let draftsCache = null, draftsCacheTime = 0;

  const gidNum = id => (id || "").split("/").pop();
  const labelOf = n =>
    n.shippingAddress?.company || n.billingAddress?.company ||
    n.customer?.displayName || n.email || "Unknown";
  const money = v => Math.round((parseFloat(v) || 0) * 100) / 100;

  // ── Queries ────────────────────────────────────────────────────────────────
  const CUSTOMERS_QUERY = `
  query AnalyticsCustomers($first: Int!, $after: String, $query: String) {
    customers(first: $first, after: $after, query: $query, sortKey: NAME) {
      pageInfo { hasNextPage endCursor }
      edges { node { id displayName email defaultAddress { company } } }
    }
  }`;

  // Lightweight pull used only to rank the customer dropdown by this year's volume.
  const YTD_QUERY = `
  query AnalyticsYtd($first: Int!, $after: String, $query: String!) {
    orders(first: $first, after: $after, query: $query, sortKey: CREATED_AT) {
      pageInfo { hasNextPage endCursor }
      edges { node { id customer { id } currentSubtotalPriceSet { shopMoney { amount } } } }
    }
  }`;

  // discountedTotalSet = line total after line-level discounts / price overrides
  // (whole quantity), so unit value = discountedTotalSet / quantity.
  const DRAFTS_QUERY = `
  query AnalyticsDrafts($first: Int!, $after: String, $query: String!) {
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
  // discounts are NOT allocated, by design — see the "merchandise value" note on the page).
  const ORDERS_QUERY = `
  query AnalyticsOrders($first: Int!, $after: String, $query: String!) {
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
      { first: PAGE, query: "status:open" },
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
    const idToGroup = new Map();
    const add = (id, company, name, email) => {
      const label = (company || name || email || "Unknown").trim();
      const key = label.toLowerCase();
      if (!groups.has(key)) groups.set(key, { key, label, ids: new Set(), emails: new Set(), ytd: 0, ytdOrders: 0, drafts: 0 });
      const g = groups.get(key);
      if (id) { g.ids.add(id); idToGroup.set(id, g); }
      if (email) g.emails.add(email.toLowerCase());
    };

    for (const c of customers) add(gidNum(c.id), c.defaultAddress?.company, c.displayName, c.email);

    // Customers that only exist on an open draft (brand-new accounts), plus an
    // open-draft count per company so new accounts still rank as "active".
    try {
      for (const d of await getOpenDrafts()) {
        if (!d.customer?.id) continue;
        add(gidNum(d.customer.id), d.shippingAddress?.company || d.billingAddress?.company, d.customer.displayName, d.email);
        const g = idToGroup.get(gidNum(d.customer.id));
        if (g) g.drafts++;
      }
    } catch (e) { console.warn("[analytics] drafts merge for directory failed:", e.message); }

    // Year-to-date merchandise volume per company (calendar year, cancelled excluded).
    // If this fails the directory still works — it just isn't ranked.
    try {
      const yearStart = `${new Date().getUTCFullYear()}-01-01`;
      const orders = await gqlAll(b2bStore, b2bToken, YTD_QUERY,
        { first: 250, query: `created_at:>=${yearStart} -status:cancelled` },
        d => d.orders.edges, d => d.orders.pageInfo, 180000);
      for (const o of orders) {
        const g = idToGroup.get(gidNum(o.customer?.id));
        if (!g) continue;
        g.ytd += parseFloat(o.currentSubtotalPriceSet?.shopMoney?.amount) || 0;
        g.ytdOrders++;
      }
    } catch (e) { console.warn("[analytics] YTD volume for directory failed:", e.message); }

    return [...groups.values()]
      .map(g => ({
        key: g.key, label: g.label, ids: [...g.ids], emails: [...g.emails].slice(0, 3),
        ytd: Math.round(g.ytd), ytdOrders: g.ytdOrders, drafts: g.drafts,
      }))
      .sort((a, b) => a.label.localeCompare(b.label));
  }

  // ── Customer directory: Postgres-backed, refreshed in the background ───────
  // The list is saved to its own table (analytics_directory, one row) so it
  // survives deploys and restarts. Requests are always served from memory; a
  // stale list is returned immediately while a refresh runs behind it. Only the
  // very first load on a brand-new database ever makes anyone wait.
  async function initStore() {
    if (!db) return;
    try {
      await db.query(`CREATE TABLE IF NOT EXISTS analytics_directory (
        id INTEGER PRIMARY KEY, payload TEXT NOT NULL, updated_at TIMESTAMPTZ DEFAULT NOW())`);
      const r = await db.query("SELECT payload, updated_at FROM analytics_directory WHERE id = 1");
      if (r.rows[0]) {
        const saved = JSON.parse(r.rows[0].payload);
        if (saved.length && saved[0].ytd === undefined) {
          console.log("[analytics] saved directory predates volume ranking; rebuilding");
        } else {
          directoryCache = saved;
          directoryCacheTime = new Date(r.rows[0].updated_at).getTime();
          console.log(`[analytics] loaded ${directoryCache.length} customers from DB`);
        }
      }
    } catch (e) { console.warn("[analytics] directory store unavailable, using memory only:", e.message); }
  }

  async function saveDirectory(list) {
    if (!db) return;
    try {
      await db.query(`INSERT INTO analytics_directory (id, payload, updated_at) VALUES (1, $1, NOW())
        ON CONFLICT (id) DO UPDATE SET payload = $1, updated_at = NOW()`, [JSON.stringify(list)]);
    } catch (e) { console.warn("[analytics] could not save directory:", e.message); }
  }

  // One refresh at a time; callers that arrive mid-refresh share it.
  function refreshDirectory() {
    if (building) return building;
    building = (async () => {
      try {
        const list = await buildDirectory();
        directoryCache = list;
        directoryCacheTime = Date.now();
        await saveDirectory(list);
        console.log(`[analytics] directory refreshed: ${list.length} customers`);
        return list;
      } finally { building = null; }
    })();
    return building;
  }

  const bgRefresh = () => refreshDirectory().catch(e => console.warn("[analytics] background refresh failed:", e.message));
  const storeReady = initStore();
  storeReady.then(() => {
    if (!directoryCache || Date.now() - directoryCacheTime > DIRECTORY_TTL) setTimeout(bgRefresh, 10000);
  });
  setInterval(() => {
    if (!directoryCache || Date.now() - directoryCacheTime > DIRECTORY_TTL) bgRefresh();
  }, 15 * 60 * 1000);

  // ── GET customer directory (dropdown source) ───────────────────────────────
  app.get("/api/analytics/customers", async (req, res) => {
    const { b2bStore, b2bToken } = CREDS;
    if (!b2bStore || !b2bToken) return res.status(400).json({ error: "Missing B2B credentials." });
    try {
      await storeReady;
      if (req.query.refresh === "true" || !directoryCache) {
        const list = await refreshDirectory();
        return res.json({ customers: list, cached: false });
      }
      if (Date.now() - directoryCacheTime > DIRECTORY_TTL) bgRefresh(); // serve now, refresh behind
      res.json({ customers: directoryCache, cached: true, updatedAt: new Date(directoryCacheTime).toISOString() });
    } catch (err) {
      console.error("[analytics] directory error:", err);
      if (directoryCache) return res.json({ customers: directoryCache, cached: true, stale: true });
      const hint = /access denied|scope/i.test(err.message)
        ? " (B2B token needs the read_customers scope)" : "";
      res.status(500).json({ error: err.message + hint });
    }
  });

  // ── POST query ─────────────────────────────────────────────────────────────
  // body: { customerIds: [numeric], skus: [string], from: 'YYYY-MM-DD', to: 'YYYY-MM-DD' }
  // Returns flat line-level records; the page pivots them either by SKU
  // (customer view) or by customer (SKU view) and counts distinct orders itself.
  //   draft record : open = draft line quantity, openValue = discounted line total
  //   order record : open = unfulfilledQuantity, fulfilled = currentQuantity - unfulfilledQuantity
  //                  (fulfilled only counted when the order was created inside from..to)
  //                  values = units × discounted unit price (line-level discounts only)
  app.post("/api/analytics/query", async (req, res) => {
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
            { first: PAGE, query: q },
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
      console.error("[analytics] query error:", err);
      const hint = /access denied|scope/i.test(err.message)
        ? " (B2B token may be missing read_customers)" : "";
      res.status(500).json({ error: err.message + hint });
    }
  });
};
