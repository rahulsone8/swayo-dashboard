# Food Analytics Dashboard v5.1 — Files

## Project structure (what goes where)

```
food-dashboard/
├── .env                     ← local secrets (NEVER push to GitHub)
├── .gitignore
├── package.json
├── railway.toml
├── render.yaml
├── server.js                ← Express API + all export endpoints
└── public/
    ├── index.html           ← dashboard.html renamed to index.html
    └── export.html          ← Data Explorer / Export page
```

---

## Export Feature (v5.1)

### What it does
A dedicated Data Explorer page (`/export.html`) that lets you:
- Filter by any combination of date, platform, restaurant, order status, customer segment, product name
- Preview first 200 rows instantly in the browser (with colour-coded columns)
- Download full results as **CSV** or **Excel** (.xls) — up to 50,000 rows

### Access
- From the dashboard: click the green **⬇ Export** button in the top nav (opens in new tab)
- Direct URL: `http://localhost:3001/export.html`

### Export types

| Type | What you get | Joins |
|------|-------------|-------|
| **Orders** | One row per order — order_id, date, platform, restaurant name, customer name, contact, segment, lifetime orders, all financial fields | fact_orders + dim_restaurants + dim_customers + agg_customer_behavior |
| **Customers** | One row per customer — contact, name, segment, total orders, GMV, first/last order date, active days | agg_customer_behavior + dim_customers |
| **Items** | One row per item sold — product name, restaurant, customer, contact, segment, order value | fact_order_items + fact_orders + dim_restaurants + dim_customers + agg_customer_behavior |
| **Funnel** | One row per funnel event — action, hour, restaurant | fact_funnel + dim_restaurants + dim_customers |

### Quick filter examples
- **"Who ordered from Shawarma House exactly once?"**
  → Type: Customers → Restaurant: Shawarma House → Min Orders: 1, Max Orders: 1 → Download CSV
  → You get: contact, name, segment, order dates, GMV

- **"All one-time customers with their contact numbers"**
  → Type: Customers → Segment: One-time → Download Excel

- **"All GrabFood WhatsApp orders in Jan 2026 with customer details"**
  → Type: Orders → Platform: GrabFood WhatsApp → From: 2026-01-01 → To: 2026-01-31

- **"Every Shawarma item sold, with who bought it"**
  → Type: Items → Product contains: shawarma → Download CSV

---

## New API Endpoints (server.js v5.1)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `POST /api/export/preview` | POST | Returns first 200 rows as JSON |
| `GET /api/export/csv` | GET | Streams full CSV with BOM (Excel-compatible) |
| `GET /api/export/excel` | GET | Streams SpreadsheetML .xls file |
| `GET /api/export/count` | GET | Returns row count before download |

All endpoints accept the same query params:
`from, to, platform, shop_id, order_status, has_coupon, customer_segment, min_orders, max_orders, product_name, export_type, limit`

---

## Setup (same as before)

```bash
npm install express mysql2 cors dotenv
node server.js
# open http://localhost:3001
```
