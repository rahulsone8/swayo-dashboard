# Swayo Food Analytics Dashboard

A full-stack analytics dashboard for food-order operations.  
It combines KPI tracking, funnel analytics, customer/product analysis, campaign performance, and advanced data export in one interface.

---

## What this project does

This project helps teams answer questions like:

- How many orders, GMV, net revenue, and cancellations happened in a selected period?
- Which platforms and restaurants are performing best?
- Where are users dropping in the funnel?
- Which customers are high value, one-time, or coupon-heavy?
- Which products are trending, and who bought them?
- How can filtered datasets be exported quickly for operations or marketing?

The dashboard supports live filtering by date, platform, and restaurant, and exposes export-ready endpoints for analysis outside the app.

---

## Core functionality

### 1) Overview analytics

- Business KPIs (orders, GMV, net revenue, AOV, cancellation rate, coupon rate)
- Daily and monthly trend analysis
- Platform-level performance splits
- Day-of-week and hourly order distribution
- Delivery type breakdown

### 2) Funnel analytics

- App funnel and WhatsApp funnel views
- Stage-level conversion/drop-off analysis
- Weekly and top-restaurant funnel insights
- Drop-off user lists for follow-up workflows

### 3) Customer intelligence

- Segments (VIP, Loyal, Repeat, One-time)
- Lifetime order and GMV behavior
- Cancellation and coupon usage patterns
- Customer-level drilldowns

### 4) Product and item intelligence

- Product performance and item trends
- Item-level customer mapping (`/api/item_customers`)
- Trend exploration by day/hour/restaurant (`/api/trend_items`)

### 5) Campaign analytics

- Campaign-level recipient and delivery analysis
- Non-converter and user-history endpoints
- Funnel activity linked with campaign context

### 6) Export system (Data Explorer)

The export page is available at:

- `http://localhost:3002/export.html` (or your running server port)

Features:

- Filter by date, platform, restaurant, order status, coupon usage, segment, product name
- Preview first 200 rows instantly
- Client-side search in preview table
- Sort by clicking table headers
- Show/hide columns
- Download CSV or Excel (`.xls`) up to 50,000 rows
- Filter persistence via browser localStorage
- Validation for date and order-range filters

Export types:

- Orders
- Customers
- Items
- Coupons

---

## Backend API highlights

Main API is implemented in `server.js` (Express + MySQL).

Important endpoint groups:

- Health & metadata: `/api/health`, `/api/filters`
- Dashboard analytics: `/api/overview`, `/api/drill`, `/api/orders_by_month`, `/api/orders_by_date`, `/api/restaurant_orders`, etc.
- Funnel analytics: `/api/funnel`, `/api/funnel_wa`, `/api/funnel_weekly`, `/api/funnel_dropoffs`, `/api/funnel_abandoned`
- Customer/product analytics: `/api/customers`, `/api/products`, `/api/item_customers`, `/api/trend_items`
- Campaign analytics: `/api/campaigns`, `/api/campaign_nonconv`, `/api/campaign_user_history`, `/api/campaign_funnel_activity`
- Export endpoints:
  - `POST /api/export/preview`
  - `GET /api/export/csv`
  - `GET /api/export/excel`

---

## Project structure

```text
Swayo_Dashboard_1/
├── .env
├── package.json
├── server.js
├── export.html                 # source copy
├── public/
│   ├── dashboard.html
│   ├── dashboard.js
│   ├── styles.css
│   └── export.html             # served by express.static("public")
└── README.md
```

---

## Tech stack

- Node.js + Express
- MySQL (`mysql2`)
- Vanilla HTML/CSS/JavaScript frontend
- CSV and SpreadsheetML (`.xls`) export generation on server

---

## Getting started

1. Install dependencies:

```bash
npm install
```

2. Configure environment variables in `.env`:

- `DB_HOST`
- `DB_PORT`
- `DB_USER`
- `DB_PASS`
- `DB_NAME`
- optional: `PORT`, `ALLOWED_ORIGIN`, `DB_SSL`

3. Start server:

```bash
node server.js
```

4. Open in browser:

- Dashboard: `http://localhost:<PORT>/`
- Export page: `http://localhost:<PORT>/export.html`

---

## Notes

- Static frontend files are served from `public/`.
- If `export.html` is not loading, ensure the file exists at `public/export.html`.
- Keep `.env` private and never commit secrets.
