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
            pageInfo { hasNextPage endCursor }
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
            pageInfo { hasNextPage endCursor }
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

  // Orders and drafts with more than 100 lines: Shopify returns the first 100 and
  // says there are more. Fetch the rest so big orders aren't silently undercounted.
  const DRAFT_MORE_LINES_QUERY = `
  query AnalyticsDraftMoreLines($id: ID!, $after: String) {
    draftOrder(id: $id) {
      lineItems(first: 100, after: $after) {
        pageInfo { hasNextPage endCursor }
        edges { node { sku title quantity discountedTotalSet { shopMoney { amount } } } }
      }
    }
  }`;
  async function completeLines(nodes, query, root) {
    for (const n of nodes) {
      let pi = n.lineItems?.pageInfo, guard = 0;
      while (pi?.hasNextPage && guard++ < 30) {
        const d = await gqlRetry(query, { id: n.id, after: pi.endCursor });
        const more = d[root]?.lineItems;
        if (!more) break;
        n.lineItems.edges = n.lineItems.edges.concat(more.edges);
        pi = n.lineItems.pageInfo = more.pageInfo;
      }
    }
  }

  // ── Data access ────────────────────────────────────────────────────────────
  async function getOpenDrafts() {
    if (draftsCache && Date.now() - draftsCacheTime < DRAFTS_TTL) return draftsCache;
    const { b2bStore, b2bToken } = CREDS;
    const drafts = await gqlAll(b2bStore, b2bToken, DRAFTS_QUERY,
      { first: PAGE, query: "status:open" },
      d => d.draftOrders.edges, d => d.draftOrders.pageInfo, 120000);
    await completeLines(drafts, DRAFT_MORE_LINES_QUERY, "draftOrder");
    draftsCache = drafts;
    draftsCacheTime = Date.now();
    return drafts;
  }

  // Customers grouped by company, so "HomeGoods" with 6 buyer contacts is one
  // dropdown entry that expands to all 6 customer ids when selected.
  // ── Parent grouping ────────────────────────────────────────────────────────
  // Store-level records ("Trudy's Hallmark #101", "New Seasons Market - Orenco")
  // roll up to one parent in the dropdown. Rules live in analytics-groups.json
  // next to this file; a missing or broken file just means no explicit rules.
  const normName = s => String(s || "").toLowerCase().replace(/[’‘`]/g, "'").replace(/[^a-z0-9' ]+/g, " ").replace(/\s+/g, " ").trim();
  function loadGroupRules() {
    try {
      const raw = JSON.parse(require("fs").readFileSync(require("path").join(__dirname, "analytics-groups.json"), "utf8"));
      return {
        autoStrip: raw.autoStrip !== false,
        rules: (raw.rules || []).filter(r => r && r.startsWith && r.parent).map(r => ({ p: normName(r.startsWith), parent: String(r.parent).trim() })),
        never: new Set((raw.neverGroup || []).map(normName)),
      };
    } catch (e) {
      if (e.code !== "ENOENT") console.warn("[analytics] analytics-groups.json unreadable:", e.message);
      return { autoStrip: true, rules: [], never: new Set() };
    }
  }
  // Looser key used to fold spelling variants of the same name together:
  // "GRETCHENS Hallmark" / "Gretchen's Hallmark" / "Gretchen’s Hallmark" all match.
  const looseKey = s => normName(s).replace(/'/g, "").replace(/\band\b/g, "&").replace(/ & /g, " ")
    .replace(/^the /, "").replace(/\b(inc|llc|ltd|co|corp|company)\b/g, "").replace(/\s+/g, " ").trim();
  const nicer = (a, b) => {   // which spelling to show: mixed case > ALL CAPS, has apostrophe, then longer
    const score = x => (x !== x.toUpperCase() ? 4 : 0) + (x !== x.toLowerCase() ? 1 : 0) + (/['’]/.test(x) ? 2 : 0);
    return score(b) > score(a) ? b : a;
  };
  function parentOf(label, cfg) {
    const n = normName(label);
    if (cfg.never.has(n)) return label;
    for (const r of cfg.rules) if (r.p && (n === r.p || n.startsWith(r.p + " ") || n.startsWith(r.p))) return r.parent;
    if (cfg.autoStrip) {
      const cut = label.split(/\s+#\s*\d|\s+[-–—]\s+/)[0].replace(/[\s,\-–—]+$/, "").trim();
      if (cut.length >= 3) return cut;
    }
    return label;
  }

  async function buildDirectory() {
    const groupCfg = loadGroupRules();
    const { b2bStore, b2bToken } = CREDS;
    const customers = await gqlAll(b2bStore, b2bToken, CUSTOMERS_QUERY,
      { first: 250, query: "orders_count:>0" },
      d => d.customers.edges, d => d.customers.pageInfo, 180000);

    const groups = new Map();
    const idToGroup = new Map();
    const add = (id, company, name, email) => {
      const store = (company || name || email || "Unknown").trim();
      const label = parentOf(store, groupCfg);
      const key = looseKey(label) || label.toLowerCase();
      if (!groups.has(key)) groups.set(key, { key, label, ids: new Set(), emails: new Set(), members: new Set(), ytd: 0, ytdOrders: 0, drafts: 0 });
      const g = groups.get(key);
      g.label = nicer(g.label, label);
      g.members.add(store);
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
        locations: g.members.size,
        members: g.members.size > 1 ? [...g.members].sort().slice(0, 80) : [],
      }))
      .sort((a, b) => a.label.localeCompare(b.label));
  }

  // ── Customer directory: Postgres-backed, refreshed in the background ───────
  // The list is saved to its own table (analytics_directory, one row) so it
  // survives deploys and restarts. Requests are always served from memory; a
  // stale list is returned immediately while a refresh runs behind it. Only the
  // very first load on a brand-new database ever makes anyone wait.
  // Fingerprint of everything that changes how the directory is built: the
  // grouping logic version plus the contents of analytics-groups.json. A saved
  // directory built under a different fingerprint is still served right away,
  // but is rebuilt immediately in the background.
  const GROUPING_VERSION = 3;
  function groupingSig() {
    let file = "";
    try { file = require("fs").readFileSync(require("path").join(__dirname, "analytics-groups.json"), "utf8"); } catch (_) {}
    return GROUPING_VERSION + ":" + require("crypto").createHash("md5").update(file).digest("hex").slice(0, 10);
  }
  let needsRebuildNow = false;

  async function initStore() {
    if (!db) return;
    try {
      await db.query(`CREATE TABLE IF NOT EXISTS analytics_directory (
        id INTEGER PRIMARY KEY, payload TEXT NOT NULL, updated_at TIMESTAMPTZ DEFAULT NOW())`);
      const r = await db.query("SELECT payload, updated_at FROM analytics_directory WHERE id = 1");
      if (r.rows[0]) {
        const parsed = JSON.parse(r.rows[0].payload);
        const saved = Array.isArray(parsed) ? parsed : (parsed.list || []);
        const sig = Array.isArray(parsed) ? null : parsed.sig;
        if (saved.length && (saved[0].ytd === undefined || saved[0].locations === undefined)) {
          console.log("[analytics] saved directory predates volume ranking; rebuilding");
        } else {
          directoryCache = saved;
          directoryCacheTime = new Date(r.rows[0].updated_at).getTime();
          if (sig !== groupingSig()) {
            needsRebuildNow = true;
            directoryCacheTime = 0;   // stale on purpose: serve it, refresh behind it right away
            console.log("[analytics] grouping rules changed; refreshing directory");
          }
          console.log(`[analytics] loaded ${directoryCache.length} customers from DB`);
        }
      }
    } catch (e) { console.warn("[analytics] directory store unavailable, using memory only:", e.message); }
  }

  async function saveDirectory(list) {
    if (!db) return;
    try {
      await db.query(`INSERT INTO analytics_directory (id, payload, updated_at) VALUES (1, $1, NOW())
        ON CONFLICT (id) DO UPDATE SET payload = $1, updated_at = NOW()`, [JSON.stringify({ sig: groupingSig(), list })]);
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
    if (!directoryCache || Date.now() - directoryCacheTime > DIRECTORY_TTL) setTimeout(bgRefresh, needsRebuildNow ? 1000 : 10000);
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

  // ── Order store (Postgres) ─────────────────────────────────────────────────
  // Order lines live in two tables of their own (analytics_orders / analytics_lines)
  // so a lookup is one SQL query instead of dozens of Shopify requests. Nothing
  // here touches the existing `orders` table or its sync.
  //
  // Loading happens in the background, in this order:
  //   1. every open (unfulfilled / partial) order of any age
  //   2. month by month, newest first, back ANALYTICS_BACKFILL_MONTHS (default 24)
  //   3. forever after: orders changed since the last sync, every 5 minutes
  // A lookup is answered from the store only when the store provably covers it
  // (open orders loaded, and the requested period is inside the loaded months).
  // Otherwise it quietly falls back to the live Shopify search.
  const STORE_MONTHS = Math.max(1, parseInt(process.env.ANALYTICS_BACKFILL_MONTHS || "24", 10) || 24);
  const SYNC_INTERVAL = 5 * 60 * 1000;
  const READ_SYNC_DEBOUNCE = 20 * 1000;   // a lookup tops the store up if the last sync is older than this
  const READ_SYNC_BUDGET = 8000;          // ...but never waits longer than this for it
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const iso = d => d.toISOString().replace(/\.\d{3}Z$/, "Z");

  const LINE_FIELDS = "id sku title quantity currentQuantity unfulfilledQuantity discountedUnitPriceSet { shopMoney { amount } }";
  const STORE_ORDERS_QUERY = `
  query AnalyticsStoreOrders($first: Int!, $after: String, $query: String!) {
    orders(first: $first, after: $after, query: $query, sortKey: CREATED_AT) {
      pageInfo { hasNextPage endCursor }
      edges {
        node {
          id name createdAt updatedAt cancelledAt email
          customer { id displayName }
          shippingAddress { company }
          billingAddress { company }
          lineItems(first: 100) {
            pageInfo { hasNextPage endCursor }
            edges { node { ${LINE_FIELDS} } }
          }
        }
      }
    }
  }`;
  const MORE_LINES_QUERY = `
  query AnalyticsMoreLines($id: ID!, $after: String) {
    order(id: $id) {
      lineItems(first: 100, after: $after) {
        pageInfo { hasNextPage endCursor }
        edges { node { ${LINE_FIELDS} } }
      }
    }
  }`;

  const st = { ready: false, openLoaded: false, coveredFrom: null, watermark: null, lastSyncAt: 0, phase: "starting", error: null };
  const storeEnabled = !!db && typeof db.connect === "function";

  async function gqlRetry(query, vars) {
    for (let attempt = 0; ; attempt++) {
      try { return await gql(CREDS.b2bStore, CREDS.b2bToken, query, vars); }
      catch (e) {
        if (attempt >= 3 || !/throttl|timeout|HTTP (429|5\d\d)|fetch failed|ECONN|ETIMEDOUT/i.test(e.message)) throw e;
        await sleep(1500 * (attempt + 1));
      }
    }
  }

  // Walks every page of an order search. No page cap (unlike gqlAll), so a big
  // window can't silently truncate. With a deadline it stops early and says so.
  async function pageOrders(search, onPage, { deadline = Infinity } = {}) {
    let after = null, n = 0;
    for (;;) {
      const d = await gqlRetry(STORE_ORDERS_QUERY, { first: PAGE, after, query: search });
      const conn = d.orders;
      const nodes = conn.edges.map(e => e.node);
      if (nodes.length) await onPage(nodes);
      n += nodes.length;
      if (!conn.pageInfo.hasNextPage) return { n, complete: true };
      after = conn.pageInfo.endCursor;
      if (Date.now() > deadline) return { n, complete: false };
    }
  }

  async function upsertOrders(nodesIn) {
    const byId = new Map();
    for (const n of nodesIn) byId.set(gidNum(n.id), n);   // a page can repeat an order; keep the last
    if (!byId.size) return;
    const O = { id: [], name: [], cust: [], label: [], created: [], updated: [], cancelled: [], email: [] };
    const L = { oid: [], lid: [], sku: [], title: [], qty: [], cur: [], unf: [], price: [] };
    for (const [oid, n] of byId) {
      O.id.push(oid); O.name.push(n.name || ""); O.cust.push(n.customer?.id ? gidNum(n.customer.id) : null);
      O.label.push(labelOf(n)); O.created.push(n.createdAt); O.updated.push(n.updatedAt || n.createdAt);
      O.cancelled.push(!!n.cancelledAt); O.email.push(n.email || null);
      let edges = n.lineItems?.edges || [], pi = n.lineItems?.pageInfo, guard = 0;
      while (pi?.hasNextPage && guard++ < 30) {   // orders with more than 100 lines
        const d = await gqlRetry(MORE_LINES_QUERY, { id: n.id, after: pi.endCursor });
        const more = d.order?.lineItems;
        if (!more) break;
        edges = edges.concat(more.edges); pi = more.pageInfo;
      }
      for (const e of edges) {
        const li = e.node;
        L.oid.push(oid); L.lid.push(gidNum(li.id)); L.sku.push((li.sku || "").toUpperCase()); L.title.push(li.title || "");
        L.qty.push(li.quantity || 0); L.cur.push(li.currentQuantity ?? li.quantity ?? 0);
        L.unf.push(Math.max(0, li.unfulfilledQuantity || 0));
        L.price.push(parseFloat(li.discountedUnitPriceSet?.shopMoney?.amount) || 0);
      }
    }
    const client = await db.connect();
    try {
      await client.query("BEGIN");
      await client.query("DELETE FROM analytics_lines WHERE order_id = ANY($1::text[])", [O.id]);
      await client.query(
        `INSERT INTO analytics_orders (id, name, customer_id, label, created_at, updated_at, cancelled, email)
         SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::timestamptz[], $6::timestamptz[], $7::boolean[], $8::text[])
         ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, customer_id = EXCLUDED.customer_id, label = EXCLUDED.label,
           created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at, cancelled = EXCLUDED.cancelled, email = EXCLUDED.email`,
        [O.id, O.name, O.cust, O.label, O.created, O.updated, O.cancelled, O.email]);
      if (L.oid.length) await client.query(
        `INSERT INTO analytics_lines (order_id, line_id, sku, title, quantity, current_quantity, unfulfilled_quantity, unit_price)
         SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::int[], $6::int[], $7::int[], $8::numeric[])
         ON CONFLICT (order_id, line_id) DO NOTHING`,
        [L.oid, L.lid, L.sku, L.title, L.qty, L.cur, L.unf, L.price]);
      await client.query("COMMIT");
    } catch (e) {
      try { await client.query("ROLLBACK"); } catch (_) {}
      throw e;
    } finally { client.release(); }
  }

  const setState = (k, v) => db.query(
    "INSERT INTO analytics_state (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2", [k, String(v)]);

  async function initOrderStore() {
    if (!storeEnabled) { st.phase = "disabled (no database)"; return false; }
    try {
      await db.query(`
        CREATE TABLE IF NOT EXISTS analytics_orders (
          id TEXT PRIMARY KEY, name TEXT, customer_id TEXT, label TEXT,
          created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ, cancelled BOOLEAN DEFAULT FALSE, email TEXT);
        CREATE TABLE IF NOT EXISTS analytics_lines (
          order_id TEXT NOT NULL, line_id TEXT NOT NULL, sku TEXT, title TEXT,
          quantity INTEGER, current_quantity INTEGER, unfulfilled_quantity INTEGER, unit_price NUMERIC(14,4),
          PRIMARY KEY (order_id, line_id));
        CREATE TABLE IF NOT EXISTS analytics_state (key TEXT PRIMARY KEY, value TEXT);
        CREATE INDEX IF NOT EXISTS idx_an_orders_cust ON analytics_orders (customer_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_an_lines_sku ON analytics_lines (sku);
        CREATE INDEX IF NOT EXISTS idx_an_lines_open ON analytics_lines (order_id) WHERE unfulfilled_quantity > 0;`);
      const r = await db.query("SELECT key, value FROM analytics_state");
      const m = {}; r.rows.forEach(x => { m[x.key] = x.value; });
      st.openLoaded = m.open_loaded === "1";
      st.coveredFrom = m.covered_from || null;
      st.watermark = m.sync_watermark || null;
      st.lastSyncAt = m.last_sync_at ? Date.parse(m.last_sync_at) : 0;
      st.ready = true; st.phase = "idle";
      return true;
    } catch (e) {
      console.warn("[analytics] order store unavailable, using live Shopify lookups:", e.message);
      st.phase = "unavailable"; st.error = e.message;
      return false;
    }
  }
  const orderStoreReady = initOrderStore();

  async function loadOpenOrders() {
    st.phase = "loading open orders";
    let total = 0;
    await pageOrders("-status:cancelled (fulfillment_status:unfulfilled OR fulfillment_status:partial)",
      async nodes => { await upsertOrders(nodes); total += nodes.length; });
    await setState("open_loaded", "1");
    st.openLoaded = true;
    console.log(`[analytics] store: loaded ${total} open orders`);
  }

  // One month at a time, newest first, so recent periods become usable first
  // and a restart resumes where it left off.
  async function backfillNextMonth() {
    const now = new Date();
    const upper = st.coveredFrom ? new Date(st.coveredFrom + "T00:00:00Z")
      : new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + 1, 1));
    const lower = new Date(Date.UTC(upper.getUTCFullYear(), upper.getUTCMonth() - 1, 1));
    st.phase = `backfilling ${lower.toISOString().slice(0, 7)}`;
    let total = 0;
    await pageOrders(`-status:cancelled created_at:>=${iso(lower)} created_at:<${iso(upper)}`,
      async nodes => { await upsertOrders(nodes); total += nodes.length; });
    st.coveredFrom = lower.toISOString().slice(0, 10);
    await setState("covered_from", st.coveredFrom);
    console.log(`[analytics] store: loaded ${total} orders for ${st.coveredFrom.slice(0, 7)}`);
  }

  let incPromise = null;
  function incrementalSync(budgetMs) {
    if (incPromise) return incPromise;
    incPromise = (async () => {
      const started = Date.now();
      const deadline = budgetMs ? started + budgetMs : Infinity;
      try {
        const since = new Date(Date.parse(st.watermark) - 2 * 60 * 1000);   // 2 min overlap
        let count = 0;
        const r = await pageOrders(`updated_at:>=${iso(since)}`,
          async nodes => { await upsertOrders(nodes); count += nodes.length; }, { deadline });
        if (r.complete) {
          st.watermark = new Date(started - 2 * 60 * 1000).toISOString();
          st.lastSyncAt = Date.now();
          await setState("sync_watermark", st.watermark);
          await setState("last_sync_at", new Date(st.lastSyncAt).toISOString());
          if (count > 200) console.log(`[analytics] store: synced ${count} changed orders`);
        }
        return r.complete;
      } finally { incPromise = null; }
    })();
    return incPromise;
  }

  let storeBusy = null;
  function storeCycle() {
    if (storeBusy) return storeBusy;
    storeBusy = (async () => {
      let nextMs = SYNC_INTERVAL;
      try {
        if (!(await orderStoreReady)) return;
        if (!st.watermark) {   // first ever run: everything changing from now on is caught by the incremental sync
          st.watermark = new Date().toISOString();
          await setState("sync_watermark", st.watermark);
        }
        const target = new Date(Date.UTC(new Date().getUTCFullYear(), new Date().getUTCMonth() - STORE_MONTHS, 1)).toISOString().slice(0, 10);
        let more = false;
        if (!st.openLoaded) { await loadOpenOrders(); more = true; }
        else if (!st.coveredFrom || st.coveredFrom > target) { await backfillNextMonth(); more = st.coveredFrom > target; }
        if (!more || Date.now() - st.lastSyncAt > SYNC_INTERVAL) { st.phase = "syncing"; await incrementalSync(); }
        st.phase = "idle"; st.error = null;
        if (more) nextMs = 1000;
      } catch (e) {
        st.error = e.message; st.phase = "retrying";
        console.warn("[analytics] store sync error:", e.message);
        nextMs = 30000;
      } finally {
        storeBusy = null;
        setTimeout(storeCycle, nextMs).unref?.();
      }
    })();
    return storeBusy;
  }
  if (storeEnabled) setTimeout(storeCycle, 15000).unref?.();   // after startup, once the directory has had its head start

  // Can this request be answered from the store?
  function storeUsable(fromStr, phase) {
    if (!storeEnabled || !st.ready || !st.openLoaded || !st.watermark) return false;
    if (phase === "open") return true;               // open orders are loaded for all ages
    return !!st.coveredFrom && fromStr >= st.coveredFrom;
  }

  // Top the store up with whatever changed in the last few minutes so a lookup
  // right after an order is placed still sees it. Bounded: never blocks long.
  async function freshen() {
    if (Date.now() - st.lastSyncAt < READ_SYNC_DEBOUNCE) return;
    try { await Promise.race([incrementalSync(READ_SYNC_BUDGET), sleep(READ_SYNC_BUDGET + 500)]); }
    catch (e) { console.warn("[analytics] freshen failed, serving store as-is:", e.message); }
  }

  async function queryStore({ customerIds, skus, fromStr, toExclusiveStr }) {
    const params = [], where = ["NOT o.cancelled"];
    if (customerIds.length) { params.push(customerIds); where.push(`o.customer_id = ANY($${params.length}::text[])`); }
    if (skus.length) { params.push(skus); where.push(`l.sku = ANY($${params.length}::text[])`); }
    params.push(fromStr + "T00:00:00Z", toExclusiveStr + "T00:00:00Z");
    const a = params.length - 1, b = params.length;
    const inRange = `(o.created_at >= $${a}::timestamptz AND o.created_at < $${b}::timestamptz)`;
    const r = await db.query(
      `SELECT o.name, o.created_at, o.customer_id, o.label, l.sku, l.title,
              l.current_quantity, l.unfulfilled_quantity, l.unit_price, ${inRange} AS in_range
         FROM analytics_orders o JOIN analytics_lines l ON l.order_id = o.id
        WHERE ${where.join(" AND ")}
          AND (l.unfulfilled_quantity > 0 OR (${inRange} AND l.current_quantity - l.unfulfilled_quantity > 0))`, params);
    return r.rows;
  }

  app.get("/api/analytics/sync-status", async (req, res) => {
    const out = {
      enabled: storeEnabled, ready: st.ready, phase: st.phase, error: st.error,
      openOrdersLoaded: st.openLoaded, coveredFrom: st.coveredFrom, targetMonths: STORE_MONTHS,
      lastSync: st.lastSyncAt ? new Date(st.lastSyncAt).toISOString() : null,
    };
    try {
      if (st.ready) {
        const r = await db.query("SELECT (SELECT COUNT(*) FROM analytics_orders) AS orders, (SELECT COUNT(*) FROM analytics_lines) AS lines");
        out.orders = Number(r.rows[0].orders); out.lines = Number(r.rows[0].lines);
      }
    } catch (_) {}
    res.json(out);
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

      // The page asks for the two halves in parallel so "on order" can show
      // up while the (much larger) fulfilled-history search is still running.
      //   phase "open"      : drafts + unfulfilled/partial orders  (open units only)
      //   phase "fulfilled" : orders in the date range             (fulfilled units only)
      //   anything else     : both, in one response
      const phase = req.body.phase === "open" || req.body.phase === "fulfilled" ? req.body.phase : "all";
      const wantOpen = phase !== "fulfilled";
      const wantFul = phase !== "open";

      // Drafts — filtered in memory off the cached open-draft pull.
      const drafts = wantOpen ? await getOpenDrafts() : [];
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

      // One place that turns an order line into a record, for both data sources.
      const addOrderRecord = o => {
        const open = wantOpen ? o.rawOpen : 0;
        const fulfilled = wantFul && o.inRange ? Math.max(0, o.current - o.rawOpen) : 0;
        if (!open && !fulfilled) return;
        records.push({
          type: "order", name: o.name, createdAt: o.createdAt,
          customerId: o.customerId, label: o.label,
          sku: o.sku || "—", title: o.title || "",
          open, fulfilled,
          openValue: money(open * o.unit), fulfilledValue: money(fulfilled * o.unit),
        });
      };

      let source = "live", asOf = new Date().toISOString();
      if (req.body.live !== true && storeUsable(fromStr, phase)) {
        // Fast path: Postgres.
        await freshen();
        const rows = await queryStore({ customerIds, skus, fromStr, toExclusiveStr });
        for (const r of rows) {
          addOrderRecord({
            name: r.name, createdAt: new Date(r.created_at).toISOString(), customerId: r.customer_id || "",
            label: r.label || "Unknown", sku: r.sku, title: r.title,
            rawOpen: Math.max(0, r.unfulfilled_quantity || 0), current: r.current_quantity || 0,
            unit: parseFloat(r.unit_price) || 0, inRange: !!r.in_range,
          });
        }
        source = "store";
        if (st.lastSyncAt) asOf = new Date(st.lastSyncAt).toISOString();
      } else {
        // Fallback: live Shopify search (same behavior as before the store existed).
        // Orders — two searches per customer chunk, deduped by order id:
        //   A) anything still unfulfilled/partial, any age (this is "on order")
        //   B) anything created inside the date range (this feeds "fulfilled")
        const skuClause = skus.length ? "(" + skus.map(s => `sku:"${s}"`).join(" OR ") + ")" : "";
        const chunks = customerIds.length
          ? Array.from({ length: Math.ceil(customerIds.length / ID_CHUNK) }, (_, i) => customerIds.slice(i * ID_CHUNK, (i + 1) * ID_CHUNK))
          : [[]];

        const orderMap = new Map();
        const jobs = [];
        for (const chunk of chunks) {
          const custClause = chunk.length ? "(" + chunk.map(i => `customer_id:${i}`).join(" OR ") + ")" : "";
          const base = ["-status:cancelled", custClause, skuClause].filter(Boolean).join(" ");
          if (wantOpen) jobs.push(`${base} (fulfillment_status:unfulfilled OR fulfillment_status:partial)`);
          // Only orders that can have shipped units; skips never-fulfilled orders in the range.
          if (wantFul) jobs.push(`${base} (fulfillment_status:fulfilled OR fulfillment_status:partial) created_at:>=${fromStr} created_at:<${toExclusiveStr}`);
        }
        // A few searches at a time; three is a deliberate ceiling for Shopify's cost throttle.
        let next = 0;
        const worker = async () => {
          while (next < jobs.length) {
            const q = jobs[next++];
            const nodes = await gqlAll(b2bStore, b2bToken, ORDERS_QUERY,
              { first: PAGE, query: q },
              d => d.orders.edges, d => d.orders.pageInfo, 120000);
            for (const n of nodes) orderMap.set(n.id, n);
          }
        };
        await Promise.all(Array.from({ length: Math.min(3, jobs.length) }, worker));
        await completeLines([...orderMap.values()], MORE_LINES_QUERY, "order");

        for (const o of orderMap.values()) {
          if (o.cancelledAt) continue;
          if (idSet.size && !idSet.has(gidNum(o.customer?.id))) continue;
          const inRange = new Date(o.createdAt).getTime() >= fromMs && new Date(o.createdAt) < toExclusive;
          for (const e of o.lineItems?.edges || []) {
            const li = e.node;
            const sku = (li.sku || "").toUpperCase();
            if (skuSet.size && !skuSet.has(sku)) continue;
            addOrderRecord({
              name: o.name, createdAt: o.createdAt, customerId: gidNum(o.customer?.id), label: labelOf(o),
              sku, title: li.title,
              rawOpen: Math.max(0, li.unfulfilledQuantity ?? 0), current: li.currentQuantity ?? li.quantity ?? 0,
              unit: parseFloat(li.discountedUnitPriceSet?.shopMoney?.amount) || 0, inRange,
            });
          }
        }
      }

      res.json({
        asOf,
        source,
        phase,
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
