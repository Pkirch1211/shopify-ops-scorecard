# LifeLines Ops Scorecard

Shopify ops dashboard for DTC + B2B stores. Tracks orders, units, and processing/fulfillment/delivery time by month.

## Stack
- **Backend**: Node/Express — proxies Shopify Admin REST API (avoids CORS)
- **Frontend**: Vanilla HTML/JS, Chart.js — served as static files from Express
- **Deploy**: Railway via GitHub

## Local dev

```bash
npm install
npm run dev
# → http://localhost:3000
```

## Deploy to Railway

1. Push this repo to GitHub
2. New Railway project → Deploy from GitHub repo
3. Railway auto-detects Node and runs `node server.js`
4. No environment variables needed — credentials are entered in the UI and stored in browser localStorage

## Usage

1. Open the app
2. Click **⚙ Config** → enter your store handles and Admin API tokens for both DTC and B2B
3. Select a month/year → **Run Report**

## Shopify API tokens

Each store needs a **Custom App** with these access scopes:
- `read_orders`
- `read_draft_orders`
- `read_fulfillments`

Settings → Apps → Develop apps → Create app → Configure Admin API scopes → Install

## Metrics defined

| Metric | Definition |
|--------|-----------|
| Processing time | Draft order `created_at` → Order `created_at` |
| Fulfillment time | Order `created_at` → First fulfillment `created_at` |
| Delivery time | First fulfillment `created_at` → Fulfillment `updated_at` where `shipment_status = delivered` |
