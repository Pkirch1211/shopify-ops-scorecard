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
// for customer_id order search), read_products (catalog lookup for custom lines).

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
  const dirInfo = { lastOk: null, lastError: null, lastErrorAt: null };
  let draftsCache = null, draftsCacheTime = 0;

  const gidNum = id => (id || "").split("/").pop();
  const labelOf = n =>
    n.shippingAddress?.company || n.billingAddress?.company ||
    n.customer?.displayName || n.email || "Unknown";
  const money = v => Math.round((parseFloat(v) || 0) * 100) / 100;

  // ── Catalog index: resolves custom line items back to real SKUs ────────────
  // Port-pickup / outside-network orders are entered as custom line items (no
  // variant) so they don't decrement inventory. Shopify gives those lines no SKU
  // unless someone typed one into the SKU field, and often the SKU or UPC was
  // typed into the title instead. This matches them back to the catalog for
  // reporting only — nothing is written to Shopify.
  //
  // Resolution order (first hit wins, strict before loose):
  //   1. SKU field filled in            -> kept as-is ("sku")
  //   2. a catalog SKU typed as / in the title -> "title"
  //   3. a catalog UPC typed in the title      -> "barcode"
  //   4. product-name match with a clear winner -> "fuzzy"
  //   otherwise                                  -> "unmatched" (shown as its own row, never guessed)
  const CATALOG_TTL = 6 * 60 * 60 * 1000;
  const CATALOG_RETRY_MS = 10 * 60 * 1000;
  const CATALOG_QUERY = `
  query AnalyticsCatalog($first: Int!, $after: String) {
    productVariants(first: $first, after: $after) {
      pageInfo { hasNextPage endCursor }
      edges { node { sku barcode title product { title } } }
    }
  }`;
  let catalog = null, catalogBuilding = null, catalogFailAt = 0, catalogError = null;
  const nameTokens = s => String(s || "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim().split(" ").filter(t => t.length > 1);

  async function buildCatalog() {
    const variants = await gqlAll(CREDS.b2bStore, CREDS.b2bToken, CATALOG_QUERY, { first: 250 },
      d => d.productVariants.edges, d => d.productVariants.pageInfo, 180000);
    const bySku = new Map(), byBarcode = new Map(), names = [];
    for (const v of variants) {
      const sku = (v.sku || "").trim().toUpperCase();
      if (!sku) continue;
      const pt = v.product?.title || "";
      const name = (!v.title || v.title === "Default Title") ? pt : `${pt} - ${v.title}`;
      const entry = { sku, name: name || sku };
      bySku.set(sku, entry);
      const bc = String(v.barcode || "").replace(/\D/g, "");
      if (bc.length >= 8) byBarcode.set(bc, entry);
      const tokens = new Set(nameTokens(name));
      if (tokens.size) names.push({ entry, tokens });
    }
    return { bySku, byBarcode, names, builtAt: Date.now(), variants: variants.length };
  }

  // Never throws. Returns the catalog (possibly stale while a refresh runs
  // behind it) or null if it has never loaded. After a failure it waits
  // CATALOG_RETRY_MS before trying again so a missing scope can't hammer Shopify.
  async function getCatalog() {
    if (catalog && Date.now() - catalog.builtAt < CATALOG_TTL) return catalog;
    if (catalogFailAt && Date.now() - catalogFailAt < CATALOG_RETRY_MS) return catalog;
    if (!catalogBuilding) {
      catalogBuilding = buildCatalog()
        .then(c => {
          catalog = c; catalogError = null; catalogFailAt = 0;
          console.log(`[analytics] catalog loaded: ${c.bySku.size} SKUs`);
          return c;
        })
        .catch(e => {
          catalogFailAt = Date.now(); catalogError = e.message;
          const hint = /access denied|scope/i.test(e.message) ? " (B2B token needs read_products)" : "";
          console.warn("[analytics] catalog load failed:", e.message + hint);
          return catalog;
        })
        .finally(() => { catalogBuilding = null; });
    }
    return catalog || (await catalogBuilding) || null;
  }

  // rawSku/rawTitle as Shopify gave them. Returns { sku, title, source }.
  // source null = catalog not available yet; the store re-checks those later.
  function resolveLine(rawSku, rawTitle, cat) {
    const sku = String(rawSku || "").trim().toUpperCase();
    const title = String(rawTitle || "").trim();
    if (sku) {
      // SKU typed into the SKU field (e.g. legacy 165005): keep it, and fill in the
      // product name when the title is blank or just repeats the SKU.
      const hit = cat && cat.bySku.get(sku);
      const bare = !title || title.toUpperCase() === sku;
      return { sku, title: hit && bare ? hit.name : (rawTitle || ""), source: "sku" };
    }
    if (!cat) return { sku: "", title: rawTitle || "", source: null };

    // 2. catalog SKU typed as the title, or anywhere in it (only if exactly one distinct SKU)
    const up = title.toUpperCase();
    let hit = cat.bySku.get(up);
    if (!hit) {
      const found = new Map();
      for (const tok of up.match(/[A-Z0-9][A-Z0-9._-]*[A-Z0-9]/g) || []) {
        if (tok.length >= 4 && cat.bySku.has(tok)) found.set(tok, cat.bySku.get(tok));
      }
      if (found.size === 1) hit = [...found.values()][0];
    }
    if (hit) return { sku: hit.sku, title: hit.name, source: "title" };

    // 3. UPC / EAN typed in the title
    const codes = new Map();
    for (const num of title.match(/\b\d{8,14}\b/g) || []) {
      const e = cat.byBarcode.get(num);
      if (e) codes.set(e.sku, e);
    }
    if (codes.size === 1) { const e = [...codes.values()][0]; return { sku: e.sku, title: e.name, source: "barcode" }; }

    // 4. product-name match: needs a strong score AND a clear lead over the runner-up,
    //    so a vague title ("Custom item") stays unmatched instead of landing on the wrong SKU.
    const q = new Set(nameTokens(title));
    if (q.size >= 2) {
      let best = null, bestScore = 0, second = 0;
      for (const c of cat.names) {
        let inter = 0;
        for (const t of q) if (c.tokens.has(t)) inter++;
        if (!inter) continue;
        const score = inter / (q.size + c.tokens.size - inter);
        if (score > bestScore) { second = bestScore; bestScore = score; best = c.entry; }
        else if (score > second) second = score;
      }
      if (best && bestScore >= 0.6 && bestScore - second >= 0.15) return { sku: best.sku, title: best.name, source: "fuzzy" };
    }
    return { sku: "", title: rawTitle || "", source: "unmatched" };
  }

  // ── Queries ────────────────────────────────────────────────────────────────
  const CUSTOMERS_QUERY = `
  query AnalyticsCustomers($first: Int!, $after: String, $query: String) {
    customers(first: $first, after: $after, query: $query, sortKey: NAME) {
      pageInfo { hasNextPage endCursor }
      edges { node { id displayName email defaultAddress { company } companyContactProfiles { company { name } } } }
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
  const DRAFT_LINE_FIELDS = "id sku title quantity discountedTotalSet { shopMoney { amount } }";
  const DRAFT_MORE_LINES_QUERY = `
  query AnalyticsDraftMoreLines($id: ID!, $after: String) {
    draftOrder(id: $id) {
      lineItems(first: 100, after: $after) {
        pageInfo { hasNextPage endCursor }
        edges { node { ${DRAFT_LINE_FIELDS} } }
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
  // Built-in defaults, used when analytics-groups.json is not deployed next to this file.
  // If the file IS there it replaces these completely, so edit one or the other, not both.
  const DEFAULT_GROUPS = {
    "_readme": "Rolls individual store records up into one parent in the Analytics dropdown. Rules are checked top to bottom; first match wins. Each rule has a parent plus ONE of: 'startsWith' (name begins with), 'contains' (whole words anywhere in the name) or 'regex' (case-insensitive, matched on the raw name). 'startsWith'/'contains' ignore case, punctuation and extra spaces. After the rules, autoStrip trims store numbers and ' - Location' suffixes (e.g. \"Trudy's Hallmark #100\" -> \"Trudy's Hallmark\"). Names that match nothing stay as their own entry. Edit, commit, and the next directory refresh (or ?refresh=true) picks it up. Marketplace accounts whose address changes with every order (Faire): use 'customerName' (the Shopify customer name) or 'email' (the customer's email) instead \u2014 they match the customer record, not the address.",
    "autoStrip": true,
    "rules": [
      {
        "customerName": "Faire Marketplace",
        "parent": "Faire"
      },
      {
        "email": "faire@lifelines.com",
        "parent": "Faire"
      },
      {
        "regex": "^[a-z]{3}:\\s.*\\bdc\\s*#",
        "parent": "TJX Companies"
      },
      {
        "startsWith": "tjx",
        "parent": "TJX Companies"
      },
      {
        "startsWith": "tjmaxx",
        "parent": "TJX Companies"
      },
      {
        "startsWith": "t j maxx",
        "parent": "TJX Companies"
      },
      {
        "startsWith": "homegoods",
        "parent": "TJX Companies"
      },
      {
        "startsWith": "marshalls",
        "parent": "TJX Companies"
      },
      {
        "startsWith": "learning express",
        "parent": "Learning Express"
      },
      {
        "startsWith": "new seasons market",
        "parent": "New Seasons Market"
      },
      {
        "startsWith": "hobbytown",
        "parent": "HobbyTown"
      }
    ],
    "neverGroup": []
  };
  function loadGroupRules() {
    try {
      let raw;
      try { raw = JSON.parse(require("fs").readFileSync(require("path").join(__dirname, "analytics-groups.json"), "utf8")); }
      catch (e) { if (e.code !== "ENOENT") throw e; raw = DEFAULT_GROUPS; }
      return {
        autoStrip: raw.autoStrip !== false,
        rules: (raw.rules || []).filter(r => r && r.parent && (r.startsWith || r.contains || r.regex || r.customerName || r.email)).map(r => {
          let re = null;
          if (r.regex) { try { re = new RegExp(r.regex, "i"); } catch (e) { console.warn("[analytics] bad regex in analytics-groups.json:", r.regex); } }
          if (r.regex && !re) return null;
          return { p: r.startsWith ? normName(r.startsWith) : "", c: r.contains ? normName(r.contains) : "", re,
            cn: r.customerName ? normName(r.customerName) : "", em: r.email ? String(r.email).trim().toLowerCase() : "", parent: String(r.parent).trim() };
        }).filter(Boolean),
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
  function parentOf(label, cfg, who) {   // who = { name, email } of the Shopify customer record (optional)
    const n = normName(label);
    if (cfg.never.has(n)) return label;
    for (const r of cfg.rules) {
      // customer-record rules: for marketplace accounts (Faire) whose default address is overwritten with each retailer
      if (who && r.cn && normName(who.name) === r.cn) return r.parent;
      if (who && r.em && String(who.email || "").trim().toLowerCase() === r.em) return r.parent;
      if (r.p && n.startsWith(r.p)) return r.parent;
      if (r.c && (" " + n + " ").includes(" " + r.c + " ")) return r.parent;
      if (r.re && r.re.test(String(label))) return r.parent;   // regex runs on the raw name
    }
    if (cfg.autoStrip) {
      const cut = label.split(/\s+#\s*(?:\d|wh\b)|\s+[-–—]\s+/i)[0].replace(/[\s,\-–—]+$/, "").trim();
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
      const label = parentOf(store, groupCfg, { name, email });
      const key = looseKey(label) || label.toLowerCase();
      if (!groups.has(key)) groups.set(key, { key, label, ids: new Set(), emails: new Set(), members: new Set(), ytd: 0, ytdOrders: 0, drafts: 0 });
      const g = groups.get(key);
      g.label = nicer(g.label, label);
      g.members.add(store);
      if (id) { g.ids.add(id); idToGroup.set(id, g); }
      if (email) g.emails.add(email.toLowerCase());
    };

    // Name to show: the address company, else the B2B company the contact belongs to (so a buyer
    // like "Noreen Batdorf" lists under "Norman's Hallmark"), else the person's own name.
    for (const c of customers) add(gidNum(c.id), c.defaultAddress?.company || c.companyContactProfiles?.[0]?.company?.name, c.displayName, c.email);

    // Customers that only exist on an open draft (brand-new accounts), plus an
    // open-draft count per company so new accounts still rank as "active".
    try {
      for (const d of await getOpenDrafts()) {
        if (!d.customer?.id) continue;
        // A customer already in the list stays where it is: a draft shipped to some other
        // address ("Trudy's Hallmark #WH") must not create a second entry for the same id.
        if (!idToGroup.has(gidNum(d.customer.id)))
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
    try { file = require("fs").readFileSync(require("path").join(__dirname, "analytics-groups.json"), "utf8"); } catch (_) { file = JSON.stringify(DEFAULT_GROUPS); }
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
        dirInfo.lastOk = new Date().toISOString(); dirInfo.lastError = null;
        console.log(`[analytics] directory refreshed: ${list.length} customers`);
        return list;
      } finally { building = null; }
    })();
    return building;
  }

  const bgRefresh = () => refreshDirectory().catch(e => {
    console.warn("[analytics] background refresh failed:", e.message);
    dirInfo.lastError = e.message; dirInfo.lastErrorAt = new Date().toISOString();
    setTimeout(() => { if (!building && (!directoryCache || Date.now() - directoryCacheTime > DIRECTORY_TTL)) bgRefresh(); }, 2 * 60 * 1000).unref?.();   // retry in 2 min, not 15
  });
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
        return res.json({ customers: await withStoreVolume(list), cached: false });
      }
      if (Date.now() - directoryCacheTime > DIRECTORY_TTL) bgRefresh(); // serve now, refresh behind
      res.json({ customers: await withStoreVolume(directoryCache), cached: true, updatedAt: new Date(directoryCacheTime).toISOString() });
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
  //
  // Line SKUs are stored already resolved (see resolveLine): sku = the catalog SKU
  // when a custom line could be matched, sku_source = how it was matched.
  const STORE_MONTHS = Math.min(12, Math.max(1, parseInt(process.env.ANALYTICS_BACKFILL_MONTHS || "12", 10) || 12));   // capped at 12 months: always covers year-to-date, keeps memory/load small
  const SYNC_INTERVAL = Number(process.env.ANALYTICS_SYNC_INTERVAL_MS) || 5 * 60 * 1000;
  const READ_SYNC_DEBOUNCE = process.env.ANALYTICS_READ_SYNC_DEBOUNCE_MS !== undefined ? Number(process.env.ANALYTICS_READ_SYNC_DEBOUNCE_MS) : 10 * 1000;   // a lookup tops the store up if the last sync is older than this
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

  const STORE_DRAFTS_QUERY = `
  query AnalyticsStoreDrafts($first: Int!, $after: String, $query: String!) {
    draftOrders(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
      pageInfo { hasNextPage endCursor }
      edges {
        node {
          id name createdAt updatedAt email
          customer { id displayName }
          shippingAddress { company }
          billingAddress { company }
          lineItems(first: 100) {
            pageInfo { hasNextPage endCursor }
            edges { node { ${DRAFT_LINE_FIELDS} } }
          }
        }
      }
    }
  }`;
  const DRAFT_IDS_QUERY = `
  query AnalyticsDraftIds($first: Int!, $after: String, $query: String!) {
    draftOrders(first: $first, after: $after, query: $query) {
      pageInfo { hasNextPage endCursor }
      edges { node { id } }
    }
  }`;

  const st = { draftsLoaded: false, draftsWatermark: null, ready: false, openLoaded: false, coveredFrom: null, watermark: null, lastSyncAt: 0, phase: "starting", error: null };
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
    const cat = await getCatalog();
    const O = { id: [], name: [], cust: [], label: [], created: [], updated: [], cancelled: [], email: [] };
    const L = { oid: [], lid: [], sku: [], title: [], qty: [], cur: [], unf: [], price: [], src: [] };
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
        const rl = resolveLine(li.sku, li.title, cat);
        L.oid.push(oid); L.lid.push(gidNum(li.id)); L.sku.push(rl.sku); L.title.push(rl.title);
        L.qty.push(li.quantity || 0); L.cur.push(li.currentQuantity ?? li.quantity ?? 0);
        L.unf.push(Math.max(0, li.unfulfilledQuantity || 0));
        L.price.push(parseFloat(li.discountedUnitPriceSet?.shopMoney?.amount) || 0);
        L.src.push(rl.source);
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
        `INSERT INTO analytics_lines (order_id, line_id, sku, title, quantity, current_quantity, unfulfilled_quantity, unit_price, sku_source)
         SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::int[], $6::int[], $7::int[], $8::numeric[], $9::text[])
         ON CONFLICT (order_id, line_id) DO NOTHING`,
        [L.oid, L.lid, L.sku, L.title, L.qty, L.cur, L.unf, L.price, L.src]);
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
        CREATE TABLE IF NOT EXISTS analytics_drafts (
          id TEXT PRIMARY KEY, name TEXT, customer_id TEXT, label TEXT, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ, email TEXT);
        CREATE TABLE IF NOT EXISTS analytics_draft_lines (
          draft_id TEXT NOT NULL, line_id TEXT NOT NULL, sku TEXT, title TEXT, quantity INTEGER, total NUMERIC(14,4),
          PRIMARY KEY (draft_id, line_id));
        CREATE INDEX IF NOT EXISTS idx_an_drafts_cust ON analytics_drafts (customer_id);
        CREATE INDEX IF NOT EXISTS idx_an_dlines_sku ON analytics_draft_lines (sku);
        CREATE INDEX IF NOT EXISTS idx_an_orders_cust ON analytics_orders (customer_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_an_lines_sku ON analytics_lines (sku);
        CREATE INDEX IF NOT EXISTS idx_an_lines_open ON analytics_lines (order_id) WHERE unfulfilled_quantity > 0;`);
      // Added after these tables already existed in production. Rows written before
      // this column existed are NULL and get re-checked by relabelCustomLines().
      await db.query(`
        ALTER TABLE analytics_lines ADD COLUMN IF NOT EXISTS sku_source TEXT;
        ALTER TABLE analytics_draft_lines ADD COLUMN IF NOT EXISTS sku_source TEXT;`);
      const r = await db.query("SELECT key, value FROM analytics_state");
      const m = {}; r.rows.forEach(x => { m[x.key] = x.value; });
      st.openLoaded = m.open_loaded === "1";
      st.coveredFrom = m.covered_from || null;
      st.watermark = m.sync_watermark || null;
      st.draftsLoaded = m.drafts_loaded === "1";
      st.draftsWatermark = m.drafts_watermark || null;
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

  async function pageDrafts(search, onPage, { deadline = Infinity } = {}) {
    let after = null, n = 0;
    for (;;) {
      const d = await gqlRetry(STORE_DRAFTS_QUERY, { first: PAGE, after, query: search });
      const conn = d.draftOrders;
      const nodes = conn.edges.map(e => e.node);
      if (nodes.length) await onPage(nodes);
      n += nodes.length;
      if (!conn.pageInfo.hasNextPage) return { n, complete: true };
      after = conn.pageInfo.endCursor;
      if (Date.now() > deadline) return { n, complete: false };
    }
  }

  async function upsertDrafts(nodesIn) {
    const byId = new Map();
    for (const n of nodesIn) byId.set(gidNum(n.id), n);
    if (!byId.size) return;
    await completeLines([...byId.values()], DRAFT_MORE_LINES_QUERY, "draftOrder");
    const cat = await getCatalog();
    const D = { id: [], name: [], cust: [], label: [], created: [], updated: [], email: [] };
    const L = { did: [], lid: [], sku: [], title: [], qty: [], total: [], src: [] };
    for (const [did, n] of byId) {
      D.id.push(did); D.name.push(n.name || ""); D.cust.push(n.customer?.id ? gidNum(n.customer.id) : null);
      D.label.push(labelOf(n)); D.created.push(n.createdAt); D.updated.push(n.updatedAt || n.createdAt); D.email.push(n.email || null);
      for (const e of n.lineItems?.edges || []) {
        const li = e.node;
        const rl = resolveLine(li.sku, li.title, cat);
        L.did.push(did); L.lid.push(gidNum(li.id)); L.sku.push(rl.sku); L.title.push(rl.title);
        L.qty.push(li.quantity || 0); L.total.push(parseFloat(li.discountedTotalSet?.shopMoney?.amount) || 0);
        L.src.push(rl.source);
      }
    }
    const client = await db.connect();
    try {
      await client.query("BEGIN");
      await client.query("DELETE FROM analytics_draft_lines WHERE draft_id = ANY($1::text[])", [D.id]);
      await client.query(
        `INSERT INTO analytics_drafts (id, name, customer_id, label, created_at, updated_at, email)
         SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::timestamptz[], $6::timestamptz[], $7::text[])
         ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, customer_id = EXCLUDED.customer_id, label = EXCLUDED.label,
           created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at, email = EXCLUDED.email`,
        [D.id, D.name, D.cust, D.label, D.created, D.updated, D.email]);
      if (L.did.length) await client.query(
        `INSERT INTO analytics_draft_lines (draft_id, line_id, sku, title, quantity, total, sku_source)
         SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::int[], $6::numeric[], $7::text[])
         ON CONFLICT (draft_id, line_id) DO NOTHING`,
        [L.did, L.lid, L.sku, L.title, L.qty, L.total, L.src]);
      await client.query("COMMIT");
    } catch (e) {
      try { await client.query("ROLLBACK"); } catch (_) {}
      throw e;
    } finally { client.release(); }
  }

  // Deleted and completed drafts never show up in an "updated since" search, so
  // every so often list the ids of everything still open (cheap: ids only, no
  // lines) and drop whatever is no longer in that list.
  async function reconcileDrafts() {
    const ids = [];
    let after = null;
    for (;;) {
      const d = await gqlRetry(DRAFT_IDS_QUERY, { first: 250, after, query: "status:open" });
      d.draftOrders.edges.forEach(e => ids.push(gidNum(e.node.id)));
      if (!d.draftOrders.pageInfo.hasNextPage) break;
      after = d.draftOrders.pageInfo.endCursor;
    }
    const client = await db.connect();
    try {
      await client.query("BEGIN");
      await client.query("DELETE FROM analytics_draft_lines WHERE draft_id <> ALL($1::text[])", [ids]);
      const r = await client.query("DELETE FROM analytics_drafts WHERE id <> ALL($1::text[])", [ids]);
      await client.query("COMMIT");
      return r.rowCount;
    } catch (e) {
      try { await client.query("ROLLBACK"); } catch (_) {}
      throw e;
    } finally { client.release(); }
  }

  let draftPromise = null;
  // full load the first time, then "updated since" (+ optional reconcile)
  function syncDrafts({ budgetMs, reconcile } = {}) {
    if (draftPromise) return draftPromise;
    draftPromise = (async () => {
      const started = Date.now();
      const deadline = budgetMs ? started + budgetMs : Infinity;
      try {
        if (!st.draftsLoaded) {
          st.phase = "loading open drafts";
          const n = await pageDrafts("status:open", upsertDrafts);
          st.draftsWatermark = new Date(started - 2 * 60 * 1000).toISOString();
          st.draftsLoaded = true;
          await setState("drafts_watermark", st.draftsWatermark);
          await setState("drafts_loaded", "1");
          console.log(`[analytics] store: loaded ${n.n} open drafts`);
          return true;
        }
        const since = new Date(Date.parse(st.draftsWatermark) - 2 * 60 * 1000);
        const r = await pageDrafts(`status:open updated_at:>=${iso(since)}`, upsertDrafts, { deadline });
        if (r.complete) {
          st.draftsWatermark = new Date(started - 2 * 60 * 1000).toISOString();
          await setState("drafts_watermark", st.draftsWatermark);
          if (reconcile) {
            const gone = await reconcileDrafts();
            if (gone) console.log(`[analytics] store: dropped ${gone} drafts that are no longer open`);
          }
        }
        return r.complete;
      } finally { draftPromise = null; }
    })();
    return draftPromise;
  }

  // Re-checks custom lines already in the store against the catalog: rows written
  // before sku_source existed, rows written while the catalog wasn't loaded, and
  // previously unmatched rows (a product may have been added since). Also fills in
  // the product name on lines whose title is blank or just repeats the SKU.
  // Runs once per catalog build; works per distinct title, so it's cheap.
  let relabeledFor = 0;
  async function relabelCustomLines(cat) {
    let fixed = 0;
    for (const T of ["analytics_lines", "analytics_draft_lines"]) {
      const titles = await db.query(
        `SELECT DISTINCT title FROM ${T} WHERE sku = '' AND (sku_source IS NULL OR sku_source = 'unmatched')`);
      for (const { title } of titles.rows) {
        const r = resolveLine("", title, cat);
        if (r.sku) {
          const u = await db.query(`UPDATE ${T} SET sku = $1, title = $2, sku_source = $3 WHERE sku = '' AND title = $4`,
            [r.sku, r.title, r.source, title]);
          fixed += u.rowCount;
        } else {
          await db.query(`UPDATE ${T} SET sku_source = 'unmatched' WHERE sku = '' AND title = $1 AND sku_source IS NULL`, [title]);
        }
      }
      const bare = await db.query(`SELECT DISTINCT sku FROM ${T} WHERE sku <> '' AND (title = '' OR UPPER(title) = sku)`);
      for (const { sku } of bare.rows) {
        const e = cat.bySku.get(sku);
        if (e) await db.query(`UPDATE ${T} SET title = $1 WHERE sku = $2 AND (title = '' OR UPPER(title) = sku)`, [e.name, sku]);
      }
    }
    if (fixed) console.log(`[analytics] store: matched ${fixed} custom lines to catalog SKUs`);
  }

  let storeBusy = null;
  function storeCycle() {
    if (building) { setTimeout(storeCycle, 20000).unref?.(); return; }   // let the customer list rebuild finish first (they share Shopify's rate limit)
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
        else if (!st.draftsLoaded) { await syncDrafts(); more = true; }
        else if (!st.coveredFrom || st.coveredFrom > target) { await backfillNextMonth(); more = st.coveredFrom > target; }
        if (!more || Date.now() - st.lastSyncAt > SYNC_INTERVAL) {
          st.phase = "syncing";
          await incrementalSync();
          if (st.draftsLoaded) await syncDrafts({ reconcile: true });
        }
        const cat = await getCatalog();
        if (cat && cat.builtAt !== relabeledFor) {
          st.phase = "matching custom lines";
          try { await relabelCustomLines(cat); relabeledFor = cat.builtAt; }
          catch (e) { console.warn("[analytics] custom-line matching failed, will retry:", e.message); }
        }
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
  if (storeEnabled) setTimeout(storeCycle, Number(process.env.ANALYTICS_STORE_START_MS) || 15000).unref?.();   // after startup, once the directory has had its head start

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
    try {
      await Promise.race([
        Promise.all([incrementalSync(READ_SYNC_BUDGET), st.draftsLoaded ? syncDrafts({ budgetMs: READ_SYNC_BUDGET }) : null]),
        sleep(READ_SYNC_BUDGET + 500),
      ]);
    }
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
      `SELECT o.name, o.created_at, o.customer_id, o.label, l.sku, l.title, l.sku_source,
              l.current_quantity, l.unfulfilled_quantity, l.unit_price, ${inRange} AS in_range
         FROM analytics_orders o JOIN analytics_lines l ON l.order_id = o.id
        WHERE ${where.join(" AND ")}
          AND (l.unfulfilled_quantity > 0 OR (${inRange} AND l.current_quantity - l.unfulfilled_quantity > 0))`, params);
    return r.rows;
  }

  async function queryStoreDrafts({ customerIds, skus }) {
    const params = [], where = ["l.quantity > 0"];
    if (customerIds.length) { params.push(customerIds); where.push(`d.customer_id = ANY($${params.length}::text[])`); }
    if (skus.length) { params.push(skus); where.push(`l.sku = ANY($${params.length}::text[])`); }
    const r = await db.query(
      `SELECT d.name, d.created_at, d.customer_id, d.label, l.sku, l.title, l.sku_source, l.quantity, l.total
         FROM analytics_drafts d JOIN analytics_draft_lines l ON l.draft_id = d.id
        WHERE ${where.join(" AND ")}`, params);
    return r.rows;
  }

  app.get("/api/analytics/sync-status", async (req, res) => {
    const out = {
      enabled: storeEnabled, ready: st.ready, phase: st.phase, error: st.error,
      openOrdersLoaded: st.openLoaded, openDraftsLoaded: st.draftsLoaded, coveredFrom: st.coveredFrom, targetMonths: STORE_MONTHS,
      lastSync: st.lastSyncAt ? new Date(st.lastSyncAt).toISOString() : null,
      catalog: {
        skus: catalog ? catalog.bySku.size : null, variants: catalog ? catalog.variants : null,
        builtAt: catalog ? new Date(catalog.builtAt).toISOString() : null,
        error: catalogError, customLinesCheckedFor: relabeledFor ? new Date(relabeledFor).toISOString() : null,
      },
    };
    try {   // customer-list (grouping) diagnostics
      const cfg = loadGroupRules();
      out.directory = {
        entries: directoryCache ? directoryCache.length : null,
        builtThisBoot: !!dirInfo.lastOk, lastOk: dirInfo.lastOk, building: !!building,
        lastError: dirInfo.lastError, lastErrorAt: dirInfo.lastErrorAt,
        savedAt: directoryCacheTime ? new Date(directoryCacheTime).toISOString() : null,
        groupRules: cfg.rules.length,
        hasTjxRule: cfg.rules.some(r => r.parent === "TJX Companies"),
        hasFaireRule: cfg.rules.some(r => r.parent === "Faire"),
      };
    } catch (_) {}
    try {
      if (st.ready) {
        const r = await db.query("SELECT (SELECT COUNT(*) FROM analytics_orders) AS orders, (SELECT COUNT(*) FROM analytics_lines) AS lines");
        out.orders = Number(r.rows[0].orders); out.lines = Number(r.rows[0].lines);
      }
    } catch (_) {}
    res.json(out);
  });

  // ── Data audit: /api/analytics/audit ───────────────────────────────────────
  // Read-only sanity checks on the customer list and the order store, so
  // duplicates and gaps can be seen instead of guessed at.
  app.get("/api/analytics/audit", async (req, res) => {
    const out = { checkedAt: new Date().toISOString() };
    try {
      await storeReady;
      const list = directoryCache || [];
      // 1) customer list: one customer id must live in exactly one entry
      const seen = new Map(), multi = [];
      for (const c of list) for (const id of c.ids) {
        if (seen.has(id) && seen.get(id) !== c.label) multi.push({ id, entries: [seen.get(id), c.label] });
        else seen.set(id, c.label);
      }
      const keyCount = new Map();
      for (const c of list) keyCount.set(c.key, (keyCount.get(c.key) || 0) + 1);
      // 2) near-duplicate names: same first two words once case/punctuation/store numbers are ignored
      const stem = l => normName(l).replace(/\b(inc|llc|ltd|co|corp|company|the)\b/g, "").replace(/[0-9#]+/g, "").replace(/\s+/g, " ").trim().split(" ").slice(0, 2).join(" ").replace(/'/g, "");
      const byStem = new Map();
      for (const c of list) { const k = stem(c.label); if (k.length >= 4) { if (!byStem.has(k)) byStem.set(k, []); byStem.get(k).push(c.label); } }
      const near = [...byStem.values()].filter(a => a.length > 1).sort((a, b) => b.length - a.length);
      out.customerList = {
        entries: list.length,
        idsInMoreThanOneEntry: multi.length, idsInMoreThanOneEntrySample: multi.slice(0, 15),
        duplicateKeys: [...keyCount].filter(([, n]) => n > 1).length,
        similarNameGroups: near.length,
        similarNameGroupsSample: near.slice(0, 40),
      };
      // 3) order store
      if (st.ready) {
        const q = async sql => (await db.query(sql)).rows;
        const dupNames = await q(`SELECT name, COUNT(*) AS n FROM analytics_orders GROUP BY name HAVING COUNT(*) > 1 ORDER BY n DESC LIMIT 15`);
        const dupDraftNames = await q(`SELECT name, COUNT(*) AS n FROM analytics_drafts GROUP BY name HAVING COUNT(*) > 1 ORDER BY n DESC LIMIT 15`);
        const noLines = await q(`SELECT COUNT(*) AS n FROM analytics_orders o WHERE NOT EXISTS (SELECT 1 FROM analytics_lines l WHERE l.order_id = o.id)`);
        const orphanLines = await q(`SELECT (SELECT COUNT(*) FROM analytics_lines l WHERE NOT EXISTS (SELECT 1 FROM analytics_orders o WHERE o.id = l.order_id)) AS ol,
                                            (SELECT COUNT(*) FROM analytics_draft_lines l WHERE NOT EXISTS (SELECT 1 FROM analytics_drafts d WHERE d.id = l.draft_id)) AS odl`);
        const custs = await q(`SELECT customer_id AS cid, MAX(label) AS label, COUNT(*) AS n FROM (
                                 SELECT customer_id, label FROM analytics_orders WHERE customer_id IS NOT NULL
                                 UNION ALL SELECT customer_id, label FROM analytics_drafts WHERE customer_id IS NOT NULL) x GROUP BY customer_id`);
        const orphans = custs.filter(r => !seen.has(r.cid));
        const noCust = await q(`SELECT (SELECT COUNT(*) FROM analytics_orders WHERE customer_id IS NULL) AS o, (SELECT COUNT(*) FROM analytics_drafts WHERE customer_id IS NULL) AS d`);
        // custom-line matching: how each order line's SKU was determined, and the
        // biggest custom lines that still couldn't be matched to the catalog
        const srcs = await q(`SELECT COALESCE(sku_source, CASE WHEN sku = '' THEN 'pending' ELSE 'sku' END) AS src, COUNT(*) AS n
                                FROM analytics_lines GROUP BY 1 ORDER BY 2 DESC`);
        const unmatched = await q(`SELECT title, SUM(quantity) AS units, COUNT(*) AS n FROM analytics_lines
                                    WHERE sku = '' GROUP BY title ORDER BY SUM(quantity) DESC LIMIT 20`);
        out.store = {
          orders: (await q(`SELECT COUNT(*) AS n FROM analytics_orders`))[0].n * 1, drafts: (await q(`SELECT COUNT(*) AS n FROM analytics_drafts`))[0].n * 1,
          orderNamesUsedTwice: dupNames.map(r => ({ name: r.name, times: Number(r.n) })),
          draftNamesUsedTwice: dupDraftNames.map(r => ({ name: r.name, times: Number(r.n) })),
          ordersWithNoLines: Number(noLines[0].n),
          lineRowsWithNoParent: Number(orphanLines[0].ol) + Number(orphanLines[0].odl),
          ordersWithNoCustomer: Number(noCust[0].o), draftsWithNoCustomer: Number(noCust[0].d),
          customerIdsMissingFromList: orphans.length,
          customerIdsMissingFromListSample: orphans.sort((a, b) => b.n - a.n).slice(0, 15).map(r => ({ id: r.cid, label: r.label, records: Number(r.n) })),
          orderLineSkuSources: Object.fromEntries(srcs.map(r => [r.src, Number(r.n)])),
          unmatchedCustomLines: unmatched.map(r => ({ title: r.title, units: Number(r.units), lines: Number(r.n) })),
        };
        // 4) landing-table totals must equal the sum of its rows
        if (overviewReady()) {
          const rows = await overviewByCustomer(yearStartStr());
          const skuRows = await overviewBySku(yearStartStr());
          const sum = (a, k) => r2(a.reduce((t, x) => t + x[k], 0));
          out.totalsCrossCheck = {
            ytdValue_byCustomer: sum(rows, "ytdValue"), ytdValue_bySku: sum(skuRows, "ytdValue"),
            openValue_byCustomer: r2(sum(rows, "unfValue") + sum(rows, "draftValue")), openValue_bySku: r2(sum(skuRows, "unfValue") + sum(skuRows, "draftValue")),
          };
          out.totalsCrossCheck.ytdMatches = Math.abs(out.totalsCrossCheck.ytdValue_byCustomer - out.totalsCrossCheck.ytdValue_bySku) < 1;
          out.totalsCrossCheck.openMatches = Math.abs(out.totalsCrossCheck.openValue_byCustomer - out.totalsCrossCheck.openValue_bySku) < 1;
        } else out.totalsCrossCheck = "store not fully loaded yet";
      } else out.store = "order store not available";
      res.json(out);
    } catch (e) { console.error("[analytics] audit error:", e); res.status(500).json({ error: e.message }); }
  });

  // ── Overview aggregates (landing tables) ───────────────────────────────────
  // One SQL pass over the store gives every customer (or SKU) at once. Needs the
  // store to cover the whole year plus all open orders and drafts; otherwise the
  // endpoint says "not ready yet" with progress and the page keeps retrying.
  const yearStartStr = () => `${new Date().getUTCFullYear()}-01-01`;
  function overviewReady() {
    return storeEnabled && st.ready && st.openLoaded && st.draftsLoaded && !!st.coveredFrom && st.coveredFrom <= yearStartStr();
  }
  const r2 = v => Math.round((Number(v) || 0) * 100) / 100;

  // Filters shared by the order and draft halves: SKU list and customer-id list.
  function ovFilters(alias, lineAlias, skus, cids, startIdx) {
    const params = [], where = [];
    if (skus.length) { params.push(skus); where.push(`${lineAlias}.sku = ANY($${startIdx + params.length - 1}::text[])`); }
    if (cids.length) { params.push(cids); where.push(`${alias}.customer_id = ANY($${startIdx + params.length - 1}::text[])`); }
    return { params, sql: where.length ? " AND " + where.join(" AND ") : "" };
  }

  async function overviewByCustomer(yearStart, skus = [], cids = []) {
    const yS = yearStart + "T00:00:00Z";
    const fo = ovFilters("o", "l", skus, cids, 2);
    const orders = await db.query(
      `SELECT o.customer_id AS cid, MAX(o.label) AS label,
              COALESCE(SUM(CASE WHEN o.created_at >= $1::timestamptz THEN l.current_quantity * l.unit_price END), 0) AS ytd_value,
              COALESCE(SUM(CASE WHEN o.created_at >= $1::timestamptz THEN l.current_quantity END), 0) AS ytd_units,
              COUNT(DISTINCT CASE WHEN o.created_at >= $1::timestamptz AND l.current_quantity > 0 THEN o.id END) AS ytd_orders,
              COALESCE(SUM(l.unfulfilled_quantity), 0) AS unf_units,
              COALESCE(SUM(l.unfulfilled_quantity * l.unit_price), 0) AS unf_value,
              COUNT(DISTINCT CASE WHEN l.unfulfilled_quantity > 0 THEN o.id END) AS unf_orders
         FROM analytics_orders o JOIN analytics_lines l ON l.order_id = o.id
        WHERE NOT o.cancelled AND o.customer_id IS NOT NULL
          AND (o.created_at >= $1::timestamptz OR l.unfulfilled_quantity > 0)${fo.sql}
        GROUP BY o.customer_id`, [yS, ...fo.params]);
    const fd = ovFilters("d", "l", skus, cids, 1);
    const drafts = await db.query(
      `SELECT d.customer_id AS cid, MAX(d.label) AS label,
              COALESCE(SUM(l.quantity), 0) AS draft_units, COALESCE(SUM(l.total), 0) AS draft_value,
              COUNT(DISTINCT d.id) AS draft_count
         FROM analytics_drafts d JOIN analytics_draft_lines l ON l.draft_id = d.id
        WHERE d.customer_id IS NOT NULL AND l.quantity > 0${fd.sql}
        GROUP BY d.customer_id`, fd.params);
    const map = new Map();
    const row = (cid, label) => {
      if (!map.has(cid)) map.set(cid, { cid, label: label || "", ytdValue: 0, ytdUnits: 0, ytdOrders: 0, unfUnits: 0, unfValue: 0, unfOrders: 0, draftUnits: 0, draftValue: 0, draftCount: 0 });
      return map.get(cid);
    };
    for (const x of orders.rows) {
      const o = row(x.cid, x.label);
      o.ytdValue = r2(x.ytd_value); o.ytdUnits = Number(x.ytd_units); o.ytdOrders = Number(x.ytd_orders);
      o.unfUnits = Number(x.unf_units); o.unfValue = r2(x.unf_value); o.unfOrders = Number(x.unf_orders);
    }
    for (const x of drafts.rows) {
      const o = row(x.cid, x.label);
      o.draftUnits = Number(x.draft_units); o.draftValue = r2(x.draft_value); o.draftCount = Number(x.draft_count);
    }
    return [...map.values()];
  }

  // Lines with a SKU group by SKU. Custom lines that couldn't be matched (sku = '')
  // group by their title instead (ckey), so different unmatched items each get
  // their own row rather than piling into one blank-SKU bucket.
  const CUSTOM_SRC_SQL = "l.sku_source IN ('title','barcode','fuzzy','unmatched')";
  const CKEY_SQL = "CASE WHEN l.sku = '' THEN l.title ELSE '' END";
  async function overviewBySku(yearStart, cids = []) {
    const yS = yearStart + "T00:00:00Z";
    const fo = ovFilters("o", "l", [], cids, 2);
    const orders = await db.query(
      `SELECT l.sku, ${CKEY_SQL} AS ckey, MAX(l.title) AS title, BOOL_OR(${CUSTOM_SRC_SQL}) AS custom,
              COALESCE(SUM(CASE WHEN o.created_at >= $1::timestamptz THEN l.current_quantity * l.unit_price END), 0) AS ytd_value,
              COALESCE(SUM(CASE WHEN o.created_at >= $1::timestamptz THEN l.current_quantity END), 0) AS ytd_units,
              COALESCE(SUM(l.unfulfilled_quantity), 0) AS unf_units,
              COALESCE(SUM(l.unfulfilled_quantity * l.unit_price), 0) AS unf_value
         FROM analytics_orders o JOIN analytics_lines l ON l.order_id = o.id
        WHERE NOT o.cancelled AND (o.created_at >= $1::timestamptz OR l.unfulfilled_quantity > 0)${fo.sql}
        GROUP BY 1, 2`, [yS, ...fo.params]);
    const fd = ovFilters("d", "l", [], cids, 1);
    const drafts = await db.query(
      `SELECT l.sku, ${CKEY_SQL} AS ckey, MAX(l.title) AS title, BOOL_OR(${CUSTOM_SRC_SQL}) AS custom,
              COALESCE(SUM(l.quantity), 0) AS draft_units, COALESCE(SUM(l.total), 0) AS draft_value
         FROM analytics_drafts d JOIN analytics_draft_lines l ON l.draft_id = d.id
        WHERE l.quantity > 0${fd.sql} GROUP BY 1, 2`, fd.params);
    // distinct customers with this SKU open, drafts and orders combined
    const oc = ovFilters("o", "l", [], cids, 1), dc = ovFilters("d", "l", [], cids, 1 + oc.params.length);
    const custs = await db.query(
      `SELECT sku, ckey, COUNT(DISTINCT cid) AS n FROM (
         SELECT l.sku, ${CKEY_SQL} AS ckey, o.customer_id AS cid FROM analytics_orders o JOIN analytics_lines l ON l.order_id = o.id
          WHERE NOT o.cancelled AND l.unfulfilled_quantity > 0${oc.sql}
         UNION ALL
         SELECT l.sku, ${CKEY_SQL}, d.customer_id FROM analytics_drafts d JOIN analytics_draft_lines l ON l.draft_id = d.id
          WHERE l.quantity > 0${dc.sql}
       ) x GROUP BY sku, ckey`, [...oc.params, ...dc.params]);
    const map = new Map();
    const row = (sku, ckey, title, custom) => {
      const k = (sku || "") + "|" + (ckey || "");
      if (!map.has(k)) map.set(k, { sku: sku || "", title: title || "", custom: false, ytdValue: 0, ytdUnits: 0, unfUnits: 0, unfValue: 0, draftUnits: 0, draftValue: 0, openCustomers: 0 });
      const o = map.get(k); if (!o.title && title) o.title = title; if (custom) o.custom = true; return o;
    };
    for (const x of orders.rows) { const o = row(x.sku, x.ckey, x.title, x.custom); o.ytdValue = r2(x.ytd_value); o.ytdUnits = Number(x.ytd_units); o.unfUnits = Number(x.unf_units); o.unfValue = r2(x.unf_value); }
    for (const x of drafts.rows) { const o = row(x.sku, x.ckey, x.title, x.custom); o.draftUnits = Number(x.draft_units); o.draftValue = r2(x.draft_value); }
    for (const x of custs.rows) row(x.sku, x.ckey).openCustomers = Number(x.n);
    return [...map.values()];
  }

  app.get("/api/analytics/overview", async (req, res) => {
    try {
      const yearStart = yearStartStr();
      if (!overviewReady()) {
        return res.json({ ready: false, status: {
          enabled: storeEnabled, phase: st.phase, error: st.error, openOrdersLoaded: st.openLoaded, openDraftsLoaded: st.draftsLoaded,
          coveredFrom: st.coveredFrom, needFrom: yearStart } });
      }
      const view = req.query.view === "sku" ? "sku" : "customer";
      const skus = [...new Set(String(req.query.skus || "").split(",").map(x => x.trim().toUpperCase()).filter(x => x && SKU_RE.test(x)))];
      const cids = [...new Set(String(req.query.cids || "").split(",").map(x => x.trim()).filter(x => /^\d+$/.test(x)))];
      await freshen();
      const t = Date.now();
      const rows = view === "sku" ? await overviewBySku(yearStart, cids) : await overviewByCustomer(yearStart, skus, cids);
      res.json({ ready: true, view, yearStart, asOf: st.lastSyncAt ? new Date(st.lastSyncAt).toISOString() : new Date().toISOString(), ms: Date.now() - t, rows });
    } catch (err) {
      console.error("[analytics] overview error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  // The dropdown ranks by YTD volume; when the store is ready use its numbers so
  // the dropdown and the landing table always agree.
  async function withStoreVolume(list) {
    if (!overviewReady()) return list;
    try {
      const byId = new Map((await overviewByCustomer(yearStartStr())).map(r => [r.cid, r]));
      return list.map(c => {
        let ytd = 0, ytdOrders = 0, drafts = 0;
        for (const id of c.ids) { const r = byId.get(id); if (r) { ytd += r.ytdValue; ytdOrders += r.ytdOrders; drafts += r.draftCount; } }
        return { ...c, ytd: Math.round(ytd), ytdOrders, drafts };
      });
    } catch (e) { console.warn("[analytics] store volume overlay failed:", e.message); return list; }
  }

  // ── POST query ─────────────────────────────────────────────────────────────
  // body: { customerIds: [numeric], skus: [string], from: 'YYYY-MM-DD', to: 'YYYY-MM-DD' }
  // Returns flat line-level records; the page pivots them either by SKU
  // (customer view) or by customer (SKU view) and counts distinct orders itself.
  //   draft record : open = draft line quantity, openValue = discounted line total
  //   order record : open = unfulfilledQuantity, fulfilled = currentQuantity - unfulfilledQuantity
  //                  (fulfilled only counted when the order was created inside from..to)
  //                  values = units × discounted unit price (line-level discounts only)
  //   sku          : resolved SKU ("" = custom line with no catalog match; grouped by title)
  //   skuSource    : how the SKU was determined (see resolveLine)
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

      const T0 = Date.now(), timing = {};
      const useStore = req.body.live !== true && storeUsable(fromStr, phase);
      const useStoreDrafts = req.body.live !== true && storeEnabled && st.ready && st.draftsLoaded;
      if (useStore || useStoreDrafts) { const t = Date.now(); await freshen(); timing.freshenMs = Date.now() - t; }

      // Store rows are already resolved; the live paths resolve as they go.
      let catP = null;
      const liveCatalog = () => catP || (catP = getCatalog());

      // Drafts: from the store when it has them, otherwise the cached live pull.
      let t1 = Date.now();
      if (wantOpen && useStoreDrafts) {
        for (const r of await queryStoreDrafts({ customerIds, skus })) {
          records.push({
            type: "draft", name: r.name, createdAt: new Date(r.created_at).toISOString(),
            customerId: r.customer_id || "", label: r.label || "Unknown",
            sku: r.sku || "", title: r.title || "", skuSource: r.sku_source || null,
            open: r.quantity, fulfilled: 0,
            openValue: money(r.total), fulfilledValue: 0,
          });
        }
        timing.drafts = "store";
      } else if (wantOpen) {
        const cat = await liveCatalog();
        for (const d of await getOpenDrafts()) {
          if (idSet.size && !idSet.has(gidNum(d.customer?.id))) continue;
          for (const e of d.lineItems?.edges || []) {
            const li = e.node;
            const rl = resolveLine(li.sku, li.title, cat);
            if (skuSet.size && !skuSet.has(rl.sku)) continue;
            if (!li.quantity) continue;
            records.push({
              type: "draft", name: d.name, createdAt: d.createdAt,
              customerId: gidNum(d.customer?.id), label: labelOf(d),
              sku: rl.sku, title: rl.title, skuSource: rl.source,
              open: li.quantity, fulfilled: 0,
              openValue: money(li.discountedTotalSet?.shopMoney?.amount), fulfilledValue: 0,
            });
          }
        }
        timing.drafts = "live";
      }
      timing.draftsMs = Date.now() - t1;

      // One place that turns an order line into a record, for both data sources.
      const addOrderRecord = o => {
        const open = wantOpen ? o.rawOpen : 0;
        const fulfilled = wantFul && o.inRange ? Math.max(0, o.current - o.rawOpen) : 0;
        if (!open && !fulfilled) return;
        records.push({
          type: "order", name: o.name, createdAt: o.createdAt,
          customerId: o.customerId, label: o.label,
          sku: o.sku || "", title: o.title || "", skuSource: o.skuSource || null,
          open, fulfilled,
          openValue: money(open * o.unit), fulfilledValue: money(fulfilled * o.unit),
        });
      };

      let source = "live", asOf = new Date().toISOString();
      t1 = Date.now();
      if (useStore) {
        // Fast path: Postgres.
        const rows = await queryStore({ customerIds, skus, fromStr, toExclusiveStr });
        for (const r of rows) {
          addOrderRecord({
            name: r.name, createdAt: new Date(r.created_at).toISOString(), customerId: r.customer_id || "",
            label: r.label || "Unknown", sku: r.sku, title: r.title, skuSource: r.sku_source,
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
        // Note: Shopify's sku: search only sees the SKU field, so in this fallback a
        // SKU lookup won't find custom lines where the SKU was typed in the title.
        // The store path (normal case) does find them.
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

        const cat = await liveCatalog();
        for (const o of orderMap.values()) {
          if (o.cancelledAt) continue;
          if (idSet.size && !idSet.has(gidNum(o.customer?.id))) continue;
          const inRange = new Date(o.createdAt).getTime() >= fromMs && new Date(o.createdAt) < toExclusive;
          for (const e of o.lineItems?.edges || []) {
            const li = e.node;
            const rl = resolveLine(li.sku, li.title, cat);
            if (skuSet.size && !skuSet.has(rl.sku)) continue;
            addOrderRecord({
              name: o.name, createdAt: o.createdAt, customerId: gidNum(o.customer?.id), label: labelOf(o),
              sku: rl.sku, title: rl.title, skuSource: rl.source,
              rawOpen: Math.max(0, li.unfulfilledQuantity ?? 0), current: li.currentQuantity ?? li.quantity ?? 0,
              unit: parseFloat(li.discountedUnitPriceSet?.shopMoney?.amount) || 0, inRange,
            });
          }
        }
      }

      timing.ordersMs = Date.now() - t1; timing.orders = source; timing.totalMs = Date.now() - T0;
      console.log(`[analytics] query ${phase} ${customerIds.length} cust/${skus.length} sku: ${timing.totalMs}ms (orders ${source} ${timing.ordersMs}ms, drafts ${timing.drafts || "-"} ${timing.draftsMs}ms, freshen ${timing.freshenMs || 0}ms)`);
      res.json({
        asOf,
        source,
        timing,
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
