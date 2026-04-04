/**
 * Food Analytics API  v5.0
 * ─────────────────────────────────────────────────────────────────────
 * Fixes in v5:
 *  1. restaurant_name NULL  → COALESCE(fo.restaurant_name, r.restaurant_name, fo.shop_id)
 *  2. product_name NULL/bad → NULLIF / WHERE i.product_name NOT IN ('nan','none','')
 *  3. Default date range    → NO hardcoded dates; UI sends from/to based on actual data range
 *  4. /api/date_range       → returns MIN/MAX order_date so UI can auto-set pickers
 *  5. New endpoints for all 8 analytics requirements
 *
 * Exact column names from pipeline.py v3.0:
 *  fact_orders       : order_id, order_value, net_revenue, order_date,
 *                      order_year, order_month, order_month_name, order_week,
 *                      order_hour, order_dow, platform, shop_id, restaurant_name,
 *                      customer_contact, delivery_type, is_cancelled, has_coupon,
 *                      menu_discount, cart_discount, coupon_value,
 *                      packing_charge, delivery_charge, convenience_charge, tax
 *  fact_order_items  : order_id, product_name, platform
 *  fact_funnel       : action(PDP|PLP|VIEW_CART|CHECKOUT|ORDER), action_order(1-5),
 *                      shop_id, customer_contact, event_date, event_hour, event_dow
 *  dim_restaurants   : shop_id, restaurant_name, city, seller_name, pincode
 *  dim_customers     : customer_contact, customer_name, platform
 *  agg_customer_behavior: customer_contact, total_orders, total_gmv, avg_order_value,
 *                      total_discount, first_order_date, last_order_date,
 *                      platforms_used, cancelled_orders, coupon_usage,
 *                      customer_segment(VIP|Loyal|Repeat|One-time), cancellation_rate
 *  agg_restaurant_daily: restaurant_name, shop_id, platform, order_date,
 *                      gmv, net_revenue, order_count, avg_order_value,
 *                      discount_given, coupon_orders, cancelled_count, cancellation_rate
 *  agg_funnel_conversion: shop_id, event_date, pdp_views, plp_views, cart_views,
 *                      checkouts, orders_placed, plp_to_cart_rate,
 *                      cart_to_checkout_rate, checkout_to_order_rate, overall_conversion_rate
 *
 * Platform values  : "grabfood_whatsapp" | "swayo_whatsapp" | "swayo_app"
 * DB name          : funnel_pipeline
 */

require("dotenv").config();
const express = require("express");
const mysql   = require("mysql2/promise");
const cors    = require("cors");

const app  = express();
const PORT = process.env.PORT || 3001;

// ── CORS ─────────────────────────────────────────────────────────────────────
// On Railway: set ALLOWED_ORIGIN env var to your Railway frontend URL
// e.g. https://food-dashboard.up.railway.app
// Locally: leave unset → allows all origins
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN;
app.use(cors(
  ALLOWED_ORIGIN
    ? { origin: ALLOWED_ORIGIN, credentials: true }
    : {}                              // dev: allow all
));
app.use(express.json());
app.use(express.static("public"));

// ── DB POOL ───────────────────────────────────────────────────────────────────
// Railway provides a MYSQL_URL env var (full connection string) when you add
// the MySQL plugin.  We support both styles:
//   Style A (Railway): MYSQL_URL=mysql://user:pass@host:port/dbname
//   Style B (manual) : DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME
const isCloud = !!process.env.MYSQL_URL || !!process.env.DATABASE_URL;

const poolConfig = process.env.MYSQL_URL || process.env.DATABASE_URL
  ? {
      uri:              process.env.MYSQL_URL || process.env.DATABASE_URL,
      waitForConnections: true,
      connectionLimit:  10,
      // Railway MySQL requires SSL
      ssl: { rejectUnauthorized: false },
    }
  : {
      host:             process.env.DB_HOST || "localhost",
      port:             Number(process.env.DB_PORT) || 3306,
      user:             process.env.DB_USER || "root",
      password:         process.env.DB_PASS || "Rahul1975",
      database:         process.env.DB_NAME || "funnel_pipeline",
      waitForConnections: true,
      connectionLimit:  10,
      // Only add SSL when explicitly requested (cloud deployments)
      ...(process.env.DB_SSL === "true" ? { ssl: { rejectUnauthorized: false } } : {}),
    };

const pool = mysql.createPool(poolConfig);

console.log(`🗄  DB mode: ${isCloud ? "cloud (MYSQL_URL)" : "local (DB_HOST)"}`);
console.log(`🌐  CORS: ${ALLOWED_ORIGIN || "open (dev)"}`)

async function q(sql, params = []) {
  const [rows] = await pool.execute(sql, params);
  return rows;
}

// ── filter helpers ───────────────────────────────────────────────────────────
function ordersWhere(query) {
  const c = ["1=1"], v = [];
  if (query.from)     { c.push("fo.order_date >= ?"); v.push(query.from); }
  if (query.to)       { c.push("fo.order_date <= ?"); v.push(query.to);   }
  if (query.platform) { c.push("fo.platform = ?");    v.push(query.platform); }
  if (query.shop_id)  { c.push("fo.shop_id = ?");     v.push(query.shop_id); }
  return { where: c.join(" AND "), vals: v };
}

function plainWhere(query, alias) {
  const p = alias ? alias + "." : "";
  const c = ["1=1"], v = [];
  if (query.from)     { c.push(`${p}order_date >= ?`); v.push(query.from); }
  if (query.to)       { c.push(`${p}order_date <= ?`); v.push(query.to);   }
  if (query.platform) { c.push(`${p}platform = ?`);    v.push(query.platform); }
  if (query.shop_id)  { c.push(`${p}shop_id = ?`);     v.push(query.shop_id); }
  return { where: c.join(" AND "), vals: v };
}

// ════════════════════════════════════════════════════════════════════════════
//  HEALTH
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/health", async (_, res) => {
  try {
    await pool.execute("SELECT 1");
    res.json({ status: "ok", db: "connected", ts: new Date().toISOString() });
  } catch (e) { res.status(500).json({ status: "error", message: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  DATE RANGE — returns actual min/max from fact_orders so UI can init pickers
//  No hardcoded dates anywhere — UI calls this first
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/date_range", async (_, res) => {
  try {
    const [row] = await q(`
      SELECT
        DATE_FORMAT(MIN(order_date), '%Y-%m-%d') AS min_date,
        DATE_FORMAT(MAX(order_date), '%Y-%m-%d') AS max_date
      FROM fact_orders
      WHERE order_date IS NOT NULL`);
    res.json(row);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  FILTERS — dropdowns; restaurant names resolved via dim_restaurants
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/filters", async (_, res) => {
  try {
    // All restaurants that ever appear in orders — name from dim first, fallback shop_id
    const restaurants = await q(`
      SELECT
        fo.shop_id,
        COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name
      FROM fact_orders fo
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      WHERE fo.shop_id IS NOT NULL
      GROUP BY fo.shop_id, r.restaurant_name, fo.restaurant_name
      ORDER BY restaurant_name`);

    const platforms = await q(`
      SELECT DISTINCT platform FROM fact_orders
      WHERE platform IS NOT NULL AND platform NOT IN ('unknown','')
      ORDER BY platform`);

    // Date range for initialising the date pickers in UI
    const [dr] = await q(`
      SELECT
        DATE_FORMAT(MIN(order_date),'%Y-%m-%d') AS min_date,
        DATE_FORMAT(MAX(order_date),'%Y-%m-%d') AS max_date
      FROM fact_orders WHERE order_date IS NOT NULL`);

    res.json({ restaurants, platforms: platforms.map(p => p.platform), ...dr });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  OVERVIEW — KPIs + all trend charts
//  restaurant_name: COALESCE(fo.restaurant_name, r.restaurant_name, fo.shop_id)
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/overview", async (req, res) => {
  try {
    const { where, vals } = ordersWhere(req.query);

    const [kpis] = await q(`
      SELECT
        COALESCE(SUM(fo.order_value), 0)                                         AS total_gmv,
        COALESCE(SUM(fo.net_revenue), 0)                                         AS total_net_revenue,
        COUNT(*)                                                                  AS total_orders,
        ROUND(AVG(fo.order_value), 2)                                            AS avg_order_value,
        SUM(fo.is_cancelled)                                                      AS cancelled_orders,
        ROUND(SUM(fo.is_cancelled)*100.0/NULLIF(COUNT(*),0), 1)                 AS cancel_rate,
        COUNT(DISTINCT fo.customer_contact)                                       AS unique_customers,
        SUM(fo.has_coupon)                                                        AS coupon_orders,
        ROUND(SUM(fo.has_coupon)*100.0/NULLIF(COUNT(*),0), 1)                   AS coupon_rate,
        COALESCE(SUM(fo.menu_discount),0)+COALESCE(SUM(fo.cart_discount),0)     AS total_discount,
        COALESCE(SUM(fo.packing_charge),0)                                       AS total_packing,
        COALESCE(SUM(fo.delivery_charge),0)                                      AS total_delivery,
        COALESCE(SUM(fo.tax),0)                                                  AS total_tax
      FROM fact_orders fo WHERE ${where}`, vals);

    // ── 1. Orders Month-on-Month ─────────────────────────────────────────────
    const monthly = await q(`
      SELECT
        fo.order_year, fo.order_month,
        fo.order_month_name,
        CONCAT(fo.order_year,'-',LPAD(fo.order_month,2,'0')) AS month_key,
        COUNT(*)                          AS orders,
        SUM(fo.order_value)               AS gmv,
        SUM(fo.net_revenue)               AS net_revenue,
        ROUND(AVG(fo.order_value),2)      AS aov,
        COUNT(DISTINCT fo.customer_contact) AS unique_customers,
        SUM(fo.is_cancelled)               AS cancelled
      FROM fact_orders fo WHERE ${where}
      GROUP BY fo.order_year, fo.order_month, fo.order_month_name
      ORDER BY fo.order_year, fo.order_month`, vals);

    // ── 2. Orders Day-on-Day ─────────────────────────────────────────────────
    const daily = await q(`
      SELECT
        fo.order_date,
        COUNT(*)             AS orders,
        SUM(fo.order_value)  AS gmv,
        fo.order_dow         AS dow
      FROM fact_orders fo WHERE ${where}
      GROUP BY fo.order_date, fo.order_dow
      ORDER BY fo.order_date`, vals);

    // ── Day of week aggregate (which day has highest orders) ─────────────────
    const dow = await q(`
      SELECT
        fo.order_dow,
        COUNT(*) AS orders,
        SUM(fo.order_value) AS gmv,
        ROUND(AVG(fo.order_value),2) AS aov
      FROM fact_orders fo WHERE ${where}
      GROUP BY fo.order_dow
      ORDER BY FIELD(fo.order_dow,'Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday')`, vals);

    // ── Platform share ────────────────────────────────────────────────────────
    const platforms = await q(`
      SELECT
        fo.platform,
        COUNT(*)                                                      AS orders,
        SUM(fo.order_value)                                           AS gmv,
        SUM(fo.net_revenue)                                           AS net_revenue,
        ROUND(AVG(fo.order_value),2)                                  AS aov,
        SUM(fo.is_cancelled)                                           AS cancelled,
        ROUND(SUM(fo.is_cancelled)*100.0/NULLIF(COUNT(*),0),1)       AS cancel_rate,
        ROUND(SUM(fo.has_coupon)*100.0/NULLIF(COUNT(*),0),1)         AS coupon_rate
      FROM fact_orders fo WHERE ${where}
      GROUP BY fo.platform ORDER BY orders DESC`, vals);

    // ── Hour of day ───────────────────────────────────────────────────────────
    const hourly = await q(`
      SELECT fo.order_hour, COUNT(*) AS orders, SUM(fo.order_value) AS gmv
      FROM fact_orders fo WHERE ${where}
      GROUP BY fo.order_hour ORDER BY fo.order_hour`, vals);

    // ── Delivery type × platform ──────────────────────────────────────────────
    const delivery = await q(`
      SELECT fo.delivery_type, fo.platform, COUNT(*) AS cnt, SUM(fo.order_value) AS gmv
      FROM fact_orders fo WHERE ${where}
      GROUP BY fo.delivery_type, fo.platform ORDER BY cnt DESC`, vals);

    res.json({ kpis, monthly, daily, dow, platforms, hourly, delivery });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  RESTAURANTS  — name resolved via COALESCE
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/restaurants", async (req, res) => {
  try {
    const ac = ["1=1"], av = [];
    if (req.query.from)     { ac.push("a.order_date >= ?"); av.push(req.query.from); }
    if (req.query.to)       { ac.push("a.order_date <= ?"); av.push(req.query.to);   }
    if (req.query.platform) { ac.push("a.platform = ?");    av.push(req.query.platform); }
    if (req.query.shop_id)  { ac.push("a.shop_id = ?");     av.push(req.query.shop_id); }

    // Try agg_restaurant_daily (restaurant_name already there)
    const agg = await q(`
      SELECT
        a.shop_id,
        COALESCE(a.restaurant_name, r.restaurant_name, a.shop_id) AS name,
        r.city,
        SUM(a.order_count)   AS orders,
        SUM(a.gmv)           AS gmv,
        SUM(a.net_revenue)   AS net_revenue,
        ROUND(SUM(a.gmv)/NULLIF(SUM(a.order_count),0),2)  AS aov,
        SUM(a.cancelled_count)                              AS cancelled,
        ROUND(SUM(a.cancelled_count)*100.0/NULLIF(SUM(a.order_count),0),1) AS cancel_rate,
        SUM(a.coupon_orders) AS coupon_orders,
        SUM(a.discount_given) AS total_discount
      FROM agg_restaurant_daily a
      LEFT JOIN dim_restaurants r ON r.shop_id = a.shop_id
      WHERE ${ac.join(" AND ")}
      GROUP BY a.shop_id, a.restaurant_name, r.restaurant_name, r.city
      HAVING orders > 0
      ORDER BY gmv DESC LIMIT 40`, av);

    if (agg.length > 0) { res.json({ top: agg, source: "agg" }); return; }

    // Fallback to fact_orders with COALESCE for name
    const { where, vals } = ordersWhere(req.query);
    const live = await q(`
      SELECT
        fo.shop_id,
        COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS name,
        r.city,
        COUNT(*)               AS orders,
        SUM(fo.order_value)    AS gmv,
        SUM(fo.net_revenue)    AS net_revenue,
        ROUND(AVG(fo.order_value),2) AS aov,
        SUM(fo.is_cancelled)    AS cancelled,
        ROUND(SUM(fo.is_cancelled)*100.0/NULLIF(COUNT(*),0),1) AS cancel_rate,
        SUM(fo.has_coupon)     AS coupon_orders,
        COALESCE(SUM(fo.menu_discount),0)+COALESCE(SUM(fo.cart_discount),0) AS total_discount
      FROM fact_orders fo
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      WHERE ${where}
      GROUP BY fo.shop_id, r.restaurant_name, fo.restaurant_name, r.city
      HAVING orders > 0
      ORDER BY gmv DESC LIMIT 40`, vals);

    res.json({ top: live, source: "live" });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  FUNNEL
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/funnel", async (req, res) => {
  try {
    const fc = ["1=1"], fv = [];
    if (req.query.from)    { fc.push("event_date >= ?"); fv.push(req.query.from); }
    if (req.query.to)      { fc.push("event_date <= ?"); fv.push(req.query.to);   }
    if (req.query.shop_id) { fc.push("shop_id = ?");     fv.push(req.query.shop_id); }
    const fWhere = fc.join(" AND ");

    const stages = await q(`
      SELECT action_order, action AS stage_name, COUNT(*) AS total
      FROM fact_funnel WHERE ${fWhere}
      GROUP BY action_order, action ORDER BY action_order`, fv);

    const hourDrop = await q(`
      SELECT event_hour, action AS stage_name, action_order, COUNT(*) AS total
      FROM fact_funnel WHERE ${fWhere}
      GROUP BY event_hour, action, action_order ORDER BY event_hour, action_order`, fv);

    const ac2 = ["1=1"], av2 = [];
    if (req.query.from)    { ac2.push("event_date >= ?"); av2.push(req.query.from); }
    if (req.query.to)      { ac2.push("event_date <= ?"); av2.push(req.query.to);   }
    if (req.query.shop_id) { ac2.push("shop_id = ?");     av2.push(req.query.shop_id); }

    const [conv] = await q(`
      SELECT
        SUM(pdp_views) AS pdp_views, SUM(plp_views) AS plp_views,
        SUM(cart_views) AS cart_views, SUM(checkouts) AS checkouts,
        SUM(orders_placed) AS orders_placed,
        ROUND(AVG(plp_to_cart_rate),2) AS plp_to_cart_rate,
        ROUND(AVG(cart_to_checkout_rate),2) AS cart_to_checkout_rate,
        ROUND(AVG(checkout_to_order_rate),2) AS checkout_to_order_rate,
        ROUND(AVG(overall_conversion_rate),2) AS overall_conversion_rate
      FROM agg_funnel_conversion WHERE ${ac2.join(" AND ")}`, av2);

    res.json({ stages, hourDrop, conversion: conv || {} });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  CUSTOMERS — all 4 requirements:
//   • Segments from agg_customer_behavior
//   • Unique users month-on-month from fact_orders
//   • Total unique users across months
//   • Orders per user per month
//   • Orders per user across months
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/customers", async (req, res) => {
  try {
    const { where, vals } = ordersWhere(req.query);

    // Segment summary
    const segments = await q(`
      SELECT customer_segment, COUNT(*) AS cnt, SUM(total_gmv) AS gmv,
             ROUND(AVG(total_orders),1) AS avg_orders
      FROM agg_customer_behavior
      GROUP BY customer_segment
      ORDER BY FIELD(customer_segment,'VIP','Loyal','Repeat','One-time')`);

    // Top 15 customers with name resolved
    const top = await q(`
      SELECT
        cb.customer_contact,
        COALESCE(dc.customer_name,'—') AS customer_name,
        cb.customer_segment, cb.total_orders, cb.total_gmv,
        cb.first_order_date, cb.last_order_date, cb.platforms_used,
        cb.coupon_usage, cb.cancellation_rate
      FROM agg_customer_behavior cb
      LEFT JOIN dim_customers dc ON dc.customer_contact = cb.customer_contact
      ORDER BY cb.total_orders DESC LIMIT 15`);

    // Coupon with vs without
    const coupon = await q(`
      SELECT fo.has_coupon, COUNT(*) AS orders,
             ROUND(AVG(fo.order_value),2) AS aov,
             COALESCE(SUM(fo.menu_discount),0)+COALESCE(SUM(fo.cart_discount),0) AS total_discount
      FROM fact_orders fo WHERE ${where} GROUP BY fo.has_coupon`, vals);

    // Repeat vs new
    const repeatVsNew = await q(`
      SELECT CASE WHEN total_orders=1 THEN 'new' ELSE 'repeat' END AS buyer_type,
             COUNT(*) AS customers, SUM(total_gmv) AS gmv
      FROM agg_customer_behavior GROUP BY buyer_type`);

    // ── REQ: Unique users month-on-month ─────────────────────────────────────
    const uniqueByMonth = await q(`
      SELECT
        fo.order_year, fo.order_month, fo.order_month_name,
        CONCAT(fo.order_year,'-',LPAD(fo.order_month,2,'0')) AS month_key,
        COUNT(DISTINCT fo.customer_contact) AS unique_users,
        COUNT(*) AS orders
      FROM fact_orders fo WHERE ${where}
      GROUP BY fo.order_year, fo.order_month, fo.order_month_name
      ORDER BY fo.order_year, fo.order_month`, vals);

    // ── REQ: Number of times a user orders per month (distribution) ──────────
    const ordersPerUserMonth = await q(`
      SELECT
        fo.order_year, fo.order_month,
        CONCAT(fo.order_year,'-',LPAD(fo.order_month,2,'0')) AS month_key,
        fo.customer_contact,
        COUNT(*) AS order_count
      FROM fact_orders fo
      WHERE ${where} AND fo.customer_contact IS NOT NULL
      GROUP BY fo.order_year, fo.order_month, fo.customer_contact
      ORDER BY fo.order_year, fo.order_month, order_count DESC`, vals);

    // ── REQ: Number of times a user orders across months (lifetime) ──────────
    const ordersPerUserTotal = await q(`
      SELECT
        cb.customer_contact,
        COALESCE(dc.customer_name,'—') AS customer_name,
        cb.customer_segment,
        cb.total_orders,
        cb.total_gmv,
        cb.first_order_date,
        cb.last_order_date
      FROM agg_customer_behavior cb
      LEFT JOIN dim_customers dc ON dc.customer_contact = cb.customer_contact
      ORDER BY cb.total_orders DESC LIMIT 50`);

    res.json({ segments, top, coupon, repeatVsNew, uniqueByMonth, ordersPerUserMonth, ordersPerUserTotal });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  PRODUCTS — REQ: Top 10 items per month per restaurant
//  product_name nulls handled: filter out 'nan','none','',NULL
//  restaurant_name resolved via COALESCE
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/products", async (req, res) => {
  try {
    const { where, vals } = ordersWhere(req.query);

    // All items — clean product names, restaurant from fact_orders COALESCE dim
    const allItems = await q(`
      SELECT
        i.platform,
        i.product_name,
        fo.shop_id,
        COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name,
        fo.order_year, fo.order_month,
        CONCAT(fo.order_year,'-',LPAD(fo.order_month,2,'0')) AS month_key,
        COUNT(*) AS qty
      FROM fact_order_items i
      JOIN fact_orders fo ON fo.order_id = i.order_id
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      WHERE ${where}
        AND i.product_name IS NOT NULL
        AND LOWER(TRIM(i.product_name)) NOT IN ('nan','none','null','','n/a')
        AND LENGTH(TRIM(i.product_name)) > 1
      GROUP BY i.platform, i.product_name, fo.shop_id,
               r.restaurant_name, fo.restaurant_name,
               fo.order_year, fo.order_month
      ORDER BY fo.order_year, fo.order_month, qty DESC`, vals);

    const totalLineItems = allItems.reduce((s, r) => s + Number(r.qty), 0);

    // Platform splits (overall top)
    const appItems = allItems.filter(x => x.platform === "swayo_app")
      .sort((a,b)=>b.qty-a.qty).slice(0, 15);
    const gfItems  = allItems.filter(x => x.platform === "grabfood_whatsapp")
      .sort((a,b)=>b.qty-a.qty).slice(0, 10);
    const waItems  = allItems.filter(x => x.platform === "swayo_whatsapp")
      .sort((a,b)=>b.qty-a.qty).slice(0, 10);

    // ── REQ: Top 10 items PER MONTH PER RESTAURANT ───────────────────────────
    // Group by restaurant+month, rank items within group
    const topByRestaurantMonth = await q(`
      SELECT
        fo.order_year, fo.order_month,
        CONCAT(fo.order_year,'-',LPAD(fo.order_month,2,'0')) AS month_key,
        COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name,
        fo.shop_id,
        i.product_name,
        COUNT(*) AS qty
      FROM fact_order_items i
      JOIN fact_orders fo ON fo.order_id = i.order_id
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      WHERE ${where}
        AND i.product_name IS NOT NULL
        AND LOWER(TRIM(i.product_name)) NOT IN ('nan','none','null','','n/a')
        AND LENGTH(TRIM(i.product_name)) > 1
      GROUP BY fo.order_year, fo.order_month, fo.shop_id,
               r.restaurant_name, fo.restaurant_name, i.product_name
      ORDER BY fo.order_year, fo.order_month,
               COALESCE(r.restaurant_name, fo.restaurant_name), qty DESC`, vals);

    // By restaurant total
    const byRestaurant = await q(`
      SELECT
        COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name,
        fo.shop_id, COUNT(*) AS qty
      FROM fact_order_items i
      JOIN fact_orders fo ON fo.order_id = i.order_id
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      WHERE ${where}
        AND i.product_name IS NOT NULL
        AND LOWER(TRIM(i.product_name)) NOT IN ('nan','none','null','','n/a')
      GROUP BY fo.shop_id, r.restaurant_name, fo.restaurant_name
      ORDER BY qty DESC LIMIT 20`, vals);

    res.json({ appItems, gfItems, waItems, byRestaurant, topByRestaurantMonth, totalLineItems });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  P&L
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/pnl", async (req, res) => {
  try {
    const { where, vals } = ordersWhere(req.query);

    const monthly = await q(`
      SELECT
        CONCAT(fo.order_year,'-',LPAD(fo.order_month,2,'0')) AS month_key,
        fo.order_year, fo.order_month, fo.order_month_name,
        SUM(fo.order_value)                AS gmv,
        SUM(fo.net_revenue)                AS net_revenue,
        COALESCE(SUM(fo.menu_discount),0)  AS menu_discount,
        COALESCE(SUM(fo.cart_discount),0)  AS cart_discount,
        COALESCE(SUM(fo.coupon_value),0)   AS coupon_value,
        COALESCE(SUM(fo.packing_charge),0) AS packing_charge,
        COALESCE(SUM(fo.delivery_charge),0)AS delivery_charge,
        COALESCE(SUM(fo.convenience_charge),0) AS convenience_charge,
        COALESCE(SUM(fo.tax),0)            AS tax,
        COUNT(*) AS orders,
        ROUND(AVG(fo.order_value),2)       AS aov
      FROM fact_orders fo WHERE ${where}
      GROUP BY fo.order_year, fo.order_month, fo.order_month_name
      ORDER BY fo.order_year, fo.order_month`, vals);

    const [totals] = await q(`
      SELECT
        SUM(fo.order_value)                AS gmv,
        SUM(fo.net_revenue)                AS net_revenue,
        COALESCE(SUM(fo.menu_discount),0)  AS menu_discount,
        COALESCE(SUM(fo.cart_discount),0)  AS cart_discount,
        COALESCE(SUM(fo.coupon_value),0)   AS coupon_value,
        COALESCE(SUM(fo.packing_charge),0) AS packing_charge,
        COALESCE(SUM(fo.delivery_charge),0)AS delivery_charge,
        COALESCE(SUM(fo.convenience_charge),0) AS convenience_charge,
        COALESCE(SUM(fo.tax),0)            AS tax
      FROM fact_orders fo WHERE ${where}`, vals);

    res.json({ monthly, totals });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  AOV per month — food value only (order_value minus delivery/packing/tax)
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/aov_monthly", async (req, res) => {
  try {
    const { where, vals } = ordersWhere(req.query);
    const rows = await q(`
      SELECT
        fo.order_year, fo.order_month, fo.order_month_name,
        CONCAT(fo.order_year,'-',LPAD(fo.order_month,2,'0')) AS month_key,
        fo.platform,
        COUNT(*) AS orders,
        ROUND(AVG(fo.order_value),2) AS aov_gross,
        ROUND(AVG(
          fo.order_value
          - COALESCE(fo.packing_charge,0)
          - COALESCE(fo.delivery_charge,0)
          - COALESCE(fo.convenience_charge,0)
          - COALESCE(fo.tax,0)
        ),2) AS aov_food_only
      FROM fact_orders fo WHERE ${where}
      GROUP BY fo.order_year, fo.order_month, fo.order_month_name, fo.platform
      ORDER BY fo.order_year, fo.order_month, fo.platform`, vals);
    res.json({ rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  DATA EXPLORER + EXPORT  v5.1
//
//  POST /api/export/preview   → returns first 200 rows as JSON for UI preview
//  GET  /api/export/csv       → streams full result as CSV download
//  GET  /api/export/excel     → streams full result as Excel (.xlsx) download
//
//  Query params (all optional, all combinable):
//    from, to                 → order_date range
//    platform                 → grabfood_whatsapp | swayo_whatsapp | swayo_app
//    shop_id                  → specific restaurant
//    order_status             → Completed | Cancelled | Accepted | In-Progress
//    has_coupon               → 1 | 0
//    customer_segment         → VIP | Loyal | Repeat | One-time
//    min_orders               → customers with >= N lifetime orders
//    max_orders               → customers with <= N lifetime orders
//    product_name             → filter by item name (LIKE search)
//    export_type              → orders | customers | items | funnel
//    limit                    → max rows (default 5000, max 50000)
// ════════════════════════════════════════════════════════════════════════════

// ── Build the dynamic query based on export_type + all filters ───────────────
function buildExportQuery(query) {
  const type     = query.export_type || "orders";
  const limit    = Math.min(parseInt(query.limit) || 5000, 50000);
  const params   = [];

  // ── ORDERS export ──────────────────────────────────────────────────────────
  if (type === "orders") {
    const w = ["1=1"];
    if (query.from)         { w.push("fo.order_date >= ?");       params.push(query.from); }
    if (query.to)           { w.push("fo.order_date <= ?");       params.push(query.to); }
    if (query.platform)     { w.push("fo.platform = ?");          params.push(query.platform); }
    if (query.shop_id)      { w.push("fo.shop_id = ?");           params.push(query.shop_id); }
    if (query.order_status) { w.push("fo.order_status = ?");      params.push(query.order_status); }
    if (query.has_coupon !== undefined && query.has_coupon !== "") {
                              w.push("fo.has_coupon = ?");         params.push(query.has_coupon); }
    if (query.customer_segment) {
      w.push("cb.customer_segment = ?");
      params.push(query.customer_segment);
    }
    if (query.min_orders)   { w.push("cb.total_orders >= ?");     params.push(query.min_orders); }
    if (query.max_orders)   { w.push("cb.total_orders <= ?");     params.push(query.max_orders); }

    const sql = `
      SELECT
        fo.order_id,
        fo.order_date,
        fo.order_year,
        fo.order_month_name                                            AS month,
        fo.order_dow                                                   AS day_of_week,
        fo.order_hour,
        fo.platform,
        COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id)   AS restaurant_name,
        fo.shop_id,
        fo.order_status,
        dc.customer_name,
        fo.customer_contact,
        COALESCE(cb.customer_segment, 'Unknown')                       AS customer_segment,
        COALESCE(cb.total_orders, 1)                                   AS lifetime_orders,
        fo.order_value,
        fo.net_revenue,
        fo.discount,
        fo.menu_discount,
        fo.cart_discount,
        fo.coupon_value,
        fo.has_coupon,
        fo.packing_charge,
        fo.delivery_charge,
        fo.convenience_charge,
        fo.tax,
        fo.delivery_type,
        fo.delivery_pincode,
        fo.channel,
        fo.is_cancelled,
        fo.cancelled_by,
        fo.cancellation_remark
      FROM fact_orders fo
      LEFT JOIN dim_restaurants r           ON r.shop_id           = fo.shop_id
      LEFT JOIN dim_customers dc            ON dc.customer_contact = fo.customer_contact
      LEFT JOIN agg_customer_behavior cb    ON cb.customer_contact = fo.customer_contact
      WHERE ${w.join(" AND ")}
      ORDER BY fo.order_date DESC, fo.order_id
      LIMIT ${limit}`;
    return { sql, params, filename: "orders" };
  }

  // ── CUSTOMERS export ───────────────────────────────────────────────────────
  if (type === "customers") {
    const w = ["1=1"];
    if (query.customer_segment) { w.push("cb.customer_segment = ?"); params.push(query.customer_segment); }
    if (query.min_orders)       { w.push("cb.total_orders >= ?");    params.push(query.min_orders); }
    if (query.max_orders)       { w.push("cb.total_orders <= ?");    params.push(query.max_orders); }
    if (query.platform)         { w.push("dc.platform = ?");         params.push(query.platform); }
    // date filter: customers who ordered in range
    if (query.from || query.to || query.shop_id) {
      w.push(`cb.customer_contact IN (
        SELECT DISTINCT fo2.customer_contact FROM fact_orders fo2
        WHERE 1=1
        ${query.from    ? "AND fo2.order_date >= '" + query.from    + "'" : ""}
        ${query.to      ? "AND fo2.order_date <= '" + query.to      + "'" : ""}
        ${query.shop_id ? "AND fo2.shop_id = '"    + query.shop_id + "'" : ""}
      )`);
    }
    const sql = `
      SELECT
        cb.customer_contact,
        dc.customer_name,
        dc.platform                                  AS signup_platform,
        cb.customer_segment,
        cb.total_orders,
        cb.total_gmv,
        ROUND(cb.avg_order_value, 2)                 AS avg_order_value,
        cb.total_discount,
        cb.coupon_usage,
        cb.cancelled_orders,
        ROUND(cb.cancellation_rate, 2)               AS cancellation_rate,
        cb.platforms_used,
        cb.first_order_date,
        cb.last_order_date,
        DATEDIFF(cb.last_order_date, cb.first_order_date) AS active_days
      FROM agg_customer_behavior cb
      LEFT JOIN dim_customers dc ON dc.customer_contact = cb.customer_contact
      WHERE ${w.join(" AND ")}
      ORDER BY cb.total_orders DESC
      LIMIT ${limit}`;
    return { sql, params, filename: "customers" };
  }

  // ── ITEMS export ───────────────────────────────────────────────────────────
  if (type === "items") {
    const w = ["1=1"];
    if (query.from)          { w.push("fo.order_date >= ?");  params.push(query.from); }
    if (query.to)            { w.push("fo.order_date <= ?");  params.push(query.to); }
    if (query.platform)      { w.push("fo.platform = ?");     params.push(query.platform); }
    if (query.shop_id)       { w.push("fo.shop_id = ?");      params.push(query.shop_id); }
    if (query.product_name)  { w.push("i.product_name LIKE ?"); params.push(`%${query.product_name}%`); }
    const sql = `
      SELECT
        fo.order_date,
        fo.order_id,
        fo.platform,
        COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name,
        fo.shop_id,
        i.product_name,
        dc.customer_name,
        fo.customer_contact,
        COALESCE(cb.customer_segment, 'Unknown')                     AS customer_segment,
        fo.order_value,
        fo.order_status
      FROM fact_order_items i
      JOIN  fact_orders fo             ON fo.order_id          = i.order_id
      LEFT JOIN dim_restaurants r      ON r.shop_id            = fo.shop_id
      LEFT JOIN dim_customers dc       ON dc.customer_contact  = fo.customer_contact
      LEFT JOIN agg_customer_behavior cb ON cb.customer_contact= fo.customer_contact
      WHERE ${w.join(" AND ")}
        AND i.product_name IS NOT NULL
        AND LOWER(TRIM(i.product_name)) NOT IN ('nan','none','null','','n/a')
      ORDER BY fo.order_date DESC, fo.order_id
      LIMIT ${limit}`;
    return { sql, params, filename: "order_items" };
  }

  // ── FUNNEL export ──────────────────────────────────────────────────────────
  if (type === "funnel") {
    const w = ["1=1"];
    if (query.from)     { w.push("ff.event_date >= ?"); params.push(query.from); }
    if (query.to)       { w.push("ff.event_date <= ?"); params.push(query.to); }
    if (query.shop_id)  { w.push("ff.shop_id = ?");     params.push(query.shop_id); }
    const sql = `
      SELECT
        ff.event_date,
        ff.event_hour,
        ff.event_dow,
        ff.action,
        ff.action_order,
        ff.shop_id,
        COALESCE(r.restaurant_name, ff.shop_id) AS restaurant_name,
        ff.customer_contact,
        dc.customer_name
      FROM fact_funnel ff
      LEFT JOIN dim_restaurants r    ON r.shop_id           = ff.shop_id
      LEFT JOIN dim_customers dc     ON dc.customer_contact = ff.customer_contact
      WHERE ${w.join(" AND ")}
      ORDER BY ff.event_date DESC, ff.event_hour, ff.action_order
      LIMIT ${limit}`;
    return { sql, params, filename: "funnel_events" };
  }

  throw new Error(`Unknown export_type: ${type}`);
}

// ── Helper: rows → CSV string ─────────────────────────────────────────────────
function toCSV(rows) {
  if (!rows.length) return "";
  const headers = Object.keys(rows[0]);
  const escape  = v => {
    if (v === null || v === undefined) return "";
    const s = String(v);
    return s.includes(",") || s.includes('"') || s.includes("\n")
      ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const lines = [headers.join(","), ...rows.map(r => headers.map(h => escape(r[h])).join(","))];
  return lines.join("\r\n");
}

// ── Helper: rows → minimal Excel (.xlsx) without external library ─────────────
// Uses SpreadsheetML XML format — supported by Excel 2007+, Google Sheets, LibreOffice
function toExcel(rows, sheetName = "Export") {
  if (!rows.length) {
    // Return empty workbook
    rows = [{}];
  }
  const headers = Object.keys(rows[0]);
  const xmlEsc = s => String(s ?? "")
    .replace(/&/g,"&amp;").replace(/</g,"&lt;")
    .replace(/>/g,"&gt;").replace(/"/g,"&quot;");

  const headerRow = `<Row>${headers.map(h => `<Cell><Data ss:Type="String">${xmlEsc(h)}</Data></Cell>`).join("")}</Row>`;
  const dataRows  = rows.map(r =>
    `<Row>${headers.map(h => {
      const v = r[h];
      if (v === null || v === undefined) return `<Cell><Data ss:Type="String"></Data></Cell>`;
      const n = Number(v);
      const isNum = !isNaN(n) && v !== "" && v !== true && v !== false;
      return isNum
        ? `<Cell><Data ss:Type="Number">${n}</Data></Cell>`
        : `<Cell><Data ss:Type="String">${xmlEsc(v)}</Data></Cell>`;
    }).join("")}</Row>`
  ).join("");

  return `<?xml version="1.0"?>
<?mso-application progid="Excel.Sheet"?>
<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet"
          xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet">
  <Styles>
    <Style ss:ID="header">
      <Font ss:Bold="1"/>
      <Interior ss:Color="#1E3A5F" ss:Pattern="Solid"/>
      <Font ss:Color="#FFFFFF" ss:Bold="1"/>
    </Style>
  </Styles>
  <Worksheet ss:Name="${xmlEsc(sheetName)}">
    <Table>
      <Row>${headers.map(h => `<Cell ss:StyleID="header"><Data ss:Type="String">${xmlEsc(h)}</Data></Cell>`).join("")}</Row>
      ${dataRows}
    </Table>
  </Worksheet>
</Workbook>`;
}

// ── PREVIEW endpoint (POST, returns JSON, max 200 rows) ───────────────────────
app.post("/api/export/preview", async (req, res) => {
  try {
    const { sql, params } = buildExportQuery({ ...req.query, ...req.body, limit: "200" });
    const rows = await q(sql, params);
    res.json({ rows, count: rows.length });
  } catch (e) { res.status(400).json({ error: e.message }); }
});

// ── CSV download ──────────────────────────────────────────────────────────────
app.get("/api/export/csv", async (req, res) => {
  try {
    const { sql, params, filename } = buildExportQuery(req.query);
    const rows = await q(sql, params);
    const csv  = toCSV(rows);
    const date = new Date().toISOString().split("T")[0];
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="${filename}_${date}.csv"`);
    res.send("\uFEFF" + csv); // BOM for Excel UTF-8 compatibility
  } catch (e) { res.status(400).json({ error: e.message }); }
});

// ── Excel download ─────────────────────────────────────────────────────────────
app.get("/api/export/excel", async (req, res) => {
  try {
    const { sql, params, filename } = buildExportQuery(req.query);
    const rows  = await q(sql, params);
    const excel = toExcel(rows, filename);
    const date  = new Date().toISOString().split("T")[0];
    res.setHeader("Content-Type", "application/vnd.ms-excel; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="${filename}_${date}.xls"`);
    res.send(excel);
  } catch (e) { res.status(400).json({ error: e.message }); }
});

// ── Row count (for showing "X rows will be exported") ────────────────────────
app.get("/api/export/count", async (req, res) => {
  try {
    const { sql, params } = buildExportQuery({ ...req.query, limit: "50000" });
    // Wrap in COUNT
    const countSql = `SELECT COUNT(*) AS total FROM (${sql}) AS _sub`;
    const [row] = await q(countSql, [...params, ...params.map(()=>undefined).slice(params.length)]);
    // mysql2 returns params consumed by inner query — just re-run count safely
    const rows = await q(sql, params);
    res.json({ count: rows.length, capped: rows.length >= 50000 });
  } catch (e) { res.status(400).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  ANALYTICS ENDPOINTS v6  — all new requirements
// ════════════════════════════════════════════════════════════════════════════

// ── REQUIREMENT 5: Total DEDUPLICATED unique users across a date range ────────
// Previous implementation summed monthly uniques (double-counts returners).
// This does a single COUNT(DISTINCT customer_contact) over the whole period.
app.get("/api/unique_users_total", async (req, res) => {
  try {
    const { where, vals } = ordersWhere(req.query);
    const [total] = await q(`
      SELECT COUNT(DISTINCT fo.customer_contact) AS total_unique_users
      FROM fact_orders fo
      WHERE ${where} AND fo.customer_contact IS NOT NULL`, vals);

    // Also break down: how many ordered only once in period vs multiple times
    const breakdown = await q(`
      SELECT
        order_count_in_period,
        COUNT(*) AS users
      FROM (
        SELECT fo.customer_contact, COUNT(*) AS order_count_in_period
        FROM fact_orders fo
        WHERE ${where} AND fo.customer_contact IS NOT NULL
        GROUP BY fo.customer_contact
      ) sub
      GROUP BY order_count_in_period
      ORDER BY order_count_in_period`, vals);

    // One-timers vs repeaters in this period
    const one_timers  = breakdown.filter(r => Number(r.order_count_in_period) === 1).reduce((s,r)=>s+Number(r.users),0);
    const multi       = breakdown.filter(r => Number(r.order_count_in_period) >  1).reduce((s,r)=>s+Number(r.users),0);

    res.json({
      total_unique_users: total.total_unique_users,
      one_timers_in_period: one_timers,
      multi_timers_in_period: multi,
      breakdown
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── REQUIREMENT 6: Top 10 items PER MONTH (across all restaurants) ─────────
app.get("/api/top_items_monthly", async (req, res) => {
  try {
    const { where, vals } = ordersWhere(req.query);
    // Get all item counts by month, then rank within month in JS
    const rows = await q(`
      SELECT
        fo.order_year,
        fo.order_month,
        fo.order_month_name,
        CONCAT(fo.order_year,'-',LPAD(fo.order_month,2,'0')) AS month_key,
        i.product_name,
        COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name,
        fo.platform,
        COUNT(*) AS qty
      FROM fact_order_items i
      JOIN fact_orders fo ON fo.order_id = i.order_id
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      WHERE ${where}
        AND i.product_name IS NOT NULL
        AND LOWER(TRIM(i.product_name)) NOT IN ('nan','none','null','','n/a')
        AND LENGTH(TRIM(i.product_name)) > 1
      GROUP BY fo.order_year, fo.order_month, fo.order_month_name,
               i.product_name, r.restaurant_name, fo.restaurant_name, fo.platform
      ORDER BY fo.order_year, fo.order_month, qty DESC`, vals);

    // Group by month, keep top 10
    const byMonth = {};
    rows.forEach(r => {
      if (!byMonth[r.month_key]) byMonth[r.month_key] = { month_key: r.month_key, month_name: r.order_month_name, items: [] };
      if (byMonth[r.month_key].items.length < 10) byMonth[r.month_key].items.push(r);
    });

    res.json({ months: Object.values(byMonth) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── REQUIREMENT 9: AOV for a specific restaurant on a specific platform ───────
// Food + Packaging only (order_value - delivery_charge - tax - convenience_charge)
// Used for: MannFoodCourt on Swayo App, Jan–Mar
app.get("/api/restaurant_aov", async (req, res) => {
  try {
    // Accepts: shop_id OR restaurant_name_like, platform, from, to
    const c = ["1=1"], v = [];
    if (req.query.from)     { c.push("fo.order_date >= ?"); v.push(req.query.from); }
    if (req.query.to)       { c.push("fo.order_date <= ?"); v.push(req.query.to); }
    if (req.query.platform) { c.push("fo.platform = ?");    v.push(req.query.platform); }
    if (req.query.shop_id)  { c.push("fo.shop_id = ?");     v.push(req.query.shop_id); }
    if (req.query.restaurant_name_like) {
      c.push("COALESCE(r.restaurant_name, fo.restaurant_name) LIKE ?");
      v.push(`%${req.query.restaurant_name_like}%`);
    }

    const rows = await q(`
      SELECT
        COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name,
        fo.shop_id,
        fo.platform,
        fo.order_year,
        fo.order_month,
        fo.order_month_name,
        CONCAT(fo.order_year,'-',LPAD(fo.order_month,2,'0')) AS month_key,
        COUNT(*) AS order_count,

        -- Gross AOV (everything customer pays)
        ROUND(AVG(fo.order_value), 2)  AS aov_gross,

        -- Food + Packaging AOV (your requirement: Food + Packing only)
        ROUND(AVG(
          fo.order_value
          - COALESCE(fo.delivery_charge, 0)
          - COALESCE(fo.tax, 0)
          - COALESCE(fo.convenience_charge, 0)
        ), 2) AS aov_food_plus_packing,

        -- Pure food AOV (removes packing too)
        ROUND(AVG(
          fo.order_value
          - COALESCE(fo.delivery_charge, 0)
          - COALESCE(fo.tax, 0)
          - COALESCE(fo.convenience_charge, 0)
          - COALESCE(fo.packing_charge, 0)
        ), 2) AS aov_food_only,

        ROUND(AVG(fo.packing_charge), 2)  AS avg_packing,
        ROUND(AVG(fo.delivery_charge), 2) AS avg_delivery,
        ROUND(AVG(fo.tax), 2)             AS avg_tax,
        SUM(fo.order_value)               AS total_gmv,
        SUM(fo.is_cancelled)               AS cancelled
      FROM fact_orders fo
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      WHERE ${c.join(" AND ")}
        AND fo.is_cancelled = 0
      GROUP BY fo.shop_id, r.restaurant_name, fo.restaurant_name,
               fo.platform, fo.order_year, fo.order_month, fo.order_month_name
      ORDER BY fo.order_year, fo.order_month`, v);

    // Overall summary across all months
    const [summary] = await q(`
      SELECT
        COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name,
        fo.platform,
        COUNT(*) AS total_orders,
        ROUND(AVG(fo.order_value), 2) AS aov_gross,
        ROUND(AVG(
          fo.order_value
          - COALESCE(fo.delivery_charge,0)
          - COALESCE(fo.tax,0)
          - COALESCE(fo.convenience_charge,0)
        ), 2) AS aov_food_plus_packing,
        ROUND(AVG(
          fo.order_value
          - COALESCE(fo.delivery_charge,0)
          - COALESCE(fo.tax,0)
          - COALESCE(fo.convenience_charge,0)
          - COALESCE(fo.packing_charge,0)
        ), 2) AS aov_food_only,
        SUM(fo.order_value) AS total_gmv
      FROM fact_orders fo
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      WHERE ${c.join(" AND ")} AND fo.is_cancelled = 0
      GROUP BY fo.shop_id, r.restaurant_name, fo.restaurant_name, fo.platform`, v);

    res.json({ monthly: rows, summary: summary || {} });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── REQUIREMENT 10: One-time users on Swayo App (last 3 months) with WA number ─
// "one-time" = ordered exactly once across the ENTIRE history (not just in period)
// Returns customer_contact (their WhatsApp number) + order details
app.get("/api/onetime_users_swayo", async (req, res) => {
  try {
    const { where, vals } = ordersWhere(req.query);

    const rows = await q(`
      SELECT
        fo.customer_contact                              AS whatsapp_number,
        COALESCE(dc.customer_name, '—')                AS customer_name,
        fo.order_date,
        fo.order_id,
        COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name,
        fo.order_value,
        fo.order_status,
        cb.total_orders                                 AS lifetime_orders,
        cb.first_order_date,
        cb.last_order_date,
        fo.delivery_type,
        fo.delivery_pincode
      FROM fact_orders fo
      LEFT JOIN dim_restaurants r      ON r.shop_id           = fo.shop_id
      LEFT JOIN dim_customers dc       ON dc.customer_contact = fo.customer_contact
      LEFT JOIN agg_customer_behavior cb ON cb.customer_contact = fo.customer_contact
      WHERE ${where}
        AND fo.customer_contact IS NOT NULL
        AND fo.is_cancelled = 0
        -- Only customers whose LIFETIME total_orders = 1
        AND cb.total_orders = 1
      ORDER BY fo.order_date DESC`, vals);

    res.json({
      count: rows.length,
      users: rows
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── REQUIREMENT 11: JFP Funnel — VIEW_CART customers who did NOT order ─────────
// Logic:
//   A) Customers who did VIEW_CART for shop in date range
//   B) Minus those who also did ORDER for the same shop in the same date range
//   C) = "abandoned" customers who need to be called
//   D) Flag potential internal/team carts: customer_contact whose VIEW_CART count >= threshold
//      (default threshold = 9, configurable via ?team_threshold=9)
//   E) Return each customer with their VIEW_CART count, contact number, name
//
// Also returns: customers who DID order (for the "13 who placed orders" count)
// and within that, flags those with VIEW_CART count >= threshold (the 6 with multiple views)
app.get("/api/funnel_abandoned", async (req, res) => {
  try {
    const shop_id   = req.query.shop_id   || null;
    const from      = req.query.from      || null;
    const to        = req.query.to        || null;
    const threshold = parseInt(req.query.team_threshold) || 9; // VIEW_CART count >= this = likely team

    const fc = ["ff.action = 'VIEW_CART'"], fv = [];
    if (from)    { fc.push("ff.event_date >= ?"); fv.push(from); }
    if (to)      { fc.push("ff.event_date <= ?"); fv.push(to); }
    if (shop_id) { fc.push("ff.shop_id = ?");     fv.push(shop_id); }

    // All unique customers who hit VIEW_CART, with their count
    const cartCustomers = await q(`
      SELECT
        ff.customer_contact,
        COALESCE(dc.customer_name, '—')                        AS customer_name,
        COUNT(*) AS view_cart_count,
        MIN(ff.event_date) AS first_view,
        MAX(ff.event_date) AS last_view,
        MAX(ff.event_hour) AS last_hour
      FROM fact_funnel ff
      LEFT JOIN dim_customers dc ON dc.customer_contact = ff.customer_contact
      WHERE ${fc.join(" AND ")} AND ff.customer_contact IS NOT NULL
      GROUP BY ff.customer_contact, dc.customer_name
      ORDER BY view_cart_count DESC`, fv);

    // All customers who placed an ORDER for this shop in the same period
    const oc = ["fo.is_cancelled = 0"], ov = [];
    if (from)    { oc.push("fo.order_date >= ?"); ov.push(from); }
    if (to)      { oc.push("fo.order_date <= ?"); ov.push(to); }
    if (shop_id) { oc.push("fo.shop_id = ?");     ov.push(shop_id); }

    const orderedCustomers = await q(`
      SELECT DISTINCT fo.customer_contact
      FROM fact_orders fo
      WHERE ${oc.join(" AND ")} AND fo.customer_contact IS NOT NULL`, ov);

    const orderedSet = new Set(orderedCustomers.map(r => r.customer_contact));

    // Segment the VIEW_CART customers
    const abandoned   = [];  // viewed cart, did NOT order — real potential customers to call
    const ordered     = [];  // viewed cart AND ordered
    const likely_team = [];  // view_cart_count >= threshold — probably internal team

    cartCustomers.forEach(c => {
      const isTeam    = Number(c.view_cart_count) >= threshold;
      const didOrder  = orderedSet.has(c.customer_contact);

      if (isTeam) {
        likely_team.push({ ...c, flag: 'likely_team' });
      } else if (didOrder) {
        ordered.push({ ...c, flag: 'ordered' });
      } else {
        abandoned.push({ ...c, flag: 'abandoned_call_them' });
      }
    });

    // For ordered customers: also pull their order details
    let orderedDetails = [];
    if (ordered.length > 0) {
      const contacts = ordered.map(c => c.customer_contact);
      const placeholders = contacts.map(() => '?').join(',');
      const detailClauses = [...oc, `fo.customer_contact IN (${placeholders})`];
      orderedDetails = await q(`
        SELECT
          fo.customer_contact,
          COALESCE(dc.customer_name,'—') AS customer_name,
          fo.order_date,
          fo.order_id,
          fo.order_value,
          fo.order_status
        FROM fact_orders fo
        LEFT JOIN dim_customers dc ON dc.customer_contact = fo.customer_contact
        WHERE ${detailClauses.join(" AND ")}
        ORDER BY fo.order_date DESC`, [...ov, ...contacts]);
    }

    res.json({
      summary: {
        total_view_cart_customers : cartCustomers.length,
        abandoned_to_call         : abandoned.length,
        ordered_customers         : ordered.length,
        likely_team_filtered_out  : likely_team.length,
        team_threshold_used       : threshold,
        date_range: { from, to },
        shop_id
      },
      abandoned,      // call these — viewed cart, didn't order, not team
      ordered,        // placed orders (with their view_cart count too)
      likely_team,    // internal team — filtered out from analysis
      ordered_details: orderedDetails
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.listen(PORT, () =>
  console.log(`✅  Food Analytics API v6 → http://localhost:${PORT}`)
);
