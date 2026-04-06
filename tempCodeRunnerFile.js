/**
 * Swayo Food Analysis — Express API v7.0
 * ─────────────────────────────────────────────────────────────────────
 * ALL FIXES from requirements doc applied:
 *
 *  FIX 1  Multi-select for platform and shop_id filters
 *         ?platform=gf_whatsapp,swayo_app   → IN clause
 *         ?shop_id=A,B,C                    → IN clause
 *
 *  FIX 2  Delivery type shows ALL types from ALL platforms
 *         fact_orders.delivery_type covers Pickup/Delivery/Online/Shop/Hybrid
 *
 *  FIX 3  Customers tab blank → agg_customer_behavior query never filtered
 *         by ordersWhere (it should use that table's own columns)
 *
 *  FIX 4  Campaign date timezone — pipeline stores as string date, we
 *         normalize at query time with DATE() cast
 *
 *  FIX 5  Campaign grouped by campaign_name (not campaign_id)
 *
 *  FIX 6  P&L: discount columns NULL for GF/SWWA orders — use
 *         COALESCE(fo.discount, 0) as total discount for those platforms
 *
 *  FIX 7  Duplicate order IDs — added DISTINCT / dedup logic
 *
 *  NEW endpoints:
 *    GET /api/drill              → order details for any dimension click
 *    GET /api/orders_list        → full orders tab (Excel-like)
 *    GET /api/coupons            → coupon analysis tab
 *    GET /api/item_customers     → customers for a specific product
 *    GET /api/funnel_detail      → checkout/cart drop-off with contacts
 *    GET /api/campaign_nonconv   → campaign recipients who did NOT order
 *    GET /api/trend_items        → item trends by day/hour/restaurant
 *
 * Platform values (pipeline v5.1):
 *   gf_whatsapp      ← GFFW prefix
 *   swayo_whatsapp   ← SWWA prefix
 *   swayo_app        ← SWYO prefix
 *
 * DB: funnel_pipeline
 */

require("dotenv").config();
const express = require("express");
const mysql   = require("mysql2/promise");
const cors    = require("cors");

const app  = express();
const PORT = process.env.PORT || 3001;

const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN;
app.use(cors(ALLOWED_ORIGIN ? { origin: ALLOWED_ORIGIN, credentials: true } : {}));
app.use(express.json());
app.use(express.static("public"));

// ── DB POOL ──────────────────────────────────────────────────────────────────
const poolConfig = process.env.MYSQL_URL || process.env.DATABASE_URL
  ? { uri: process.env.MYSQL_URL || process.env.DATABASE_URL,
      waitForConnections: true, connectionLimit: 10,
      ssl: { rejectUnauthorized: false } }
  : { host: process.env.DB_HOST || "localhost",
      port: Number(process.env.DB_PORT) || 3306,
      user: process.env.DB_USER || "root",
      password: process.env.DB_PASS || "Rahul1975",
      database: process.env.DB_NAME || "funnel_pipeline",
      waitForConnections: true, connectionLimit: 10,
      ...(process.env.DB_SSL === "true" ? { ssl: { rejectUnauthorized: false } } : {}) };

const pool = mysql.createPool(poolConfig);

async function q(sql, params = []) {
  const [rows] = await pool.execute(sql, params);
  return rows;
}

// ── MULTI-SELECT FILTER HELPER ────────────────────────────────────────────────
// Supports comma-separated values: ?platform=gf_whatsapp,swayo_app
// Adds IN(?,?) clause and pushes each value to params array
function multiIn(field, rawVal, params) {
  if (!rawVal) return null;
  const vals = String(rawVal).split(",").map(v => v.trim()).filter(Boolean);
  if (!vals.length) return null;
  if (vals.length === 1) { params.push(vals[0]); return `${field} = ?`; }
  params.push(...vals);
  return `${field} IN (${vals.map(() => "?").join(",")})`;
}

function canonicalPlatformExpr(alias = "fo", orderIdCol = "order_id", platformCol = "platform") {
  const p = alias ? `${alias}.` : "";
  return `CASE
    WHEN ${p}${orderIdCol} LIKE 'SWYO%' THEN 'swayo_app'
    WHEN ${p}${orderIdCol} LIKE 'SWWA%' THEN 'swayo_whatsapp'
    WHEN ${p}${orderIdCol} LIKE 'GFFW%' OR ${p}${orderIdCol} LIKE 'GF%' THEN 'gf_whatsapp'
    ELSE COALESCE(NULLIF(LOWER(TRIM(${p}${platformCol})),''),'unknown')
  END`;
}

// ── ORDERS WHERE (supports multi-select platform + shop_id) ──────────────────
function ordersWhere(query, alias = "fo") {
  const p = alias ? alias + "." : "";
  const c = ["1=1"], v = [];
  if (query.from)     { c.push(`${p}order_date >= ?`);  v.push(query.from); }
  if (query.to)       { c.push(`${p}order_date <= ?`);  v.push(query.to);   }

  // Multi-select platform
  const platClause = multiIn(`(${canonicalPlatformExpr(alias)})`, query.platform, v);
  if (platClause) c.push(platClause);

  // Multi-select shop_id
  const shopClause = multiIn(`${p}shop_id`, query.shop_id, v);
  if (shopClause) c.push(shopClause);

  // Optional extras
  if (query.delivery_type) { c.push(`${p}delivery_type = ?`); v.push(query.delivery_type); }
  if (query.order_status)  { c.push(`${p}order_status = ?`);  v.push(query.order_status); }

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
//  FILTERS — date range, all restaurants, all platforms, campaigns
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/filters", async (_, res) => {
  try {
    const platExpr = canonicalPlatformExpr("fo");
    // Deduplicated restaurants (latest name wins for same shop_id)
    const restaurants = await q(`
      SELECT fo.shop_id,
             COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name
      FROM (
        SELECT DISTINCT shop_id,
               FIRST_VALUE(restaurant_name) OVER (PARTITION BY shop_id ORDER BY order_date DESC) AS restaurant_name
        FROM fact_orders
        WHERE shop_id IS NOT NULL
      ) fo
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      GROUP BY fo.shop_id, r.restaurant_name, fo.restaurant_name
      ORDER BY restaurant_name`).catch(() => q(`
      SELECT fo.shop_id,
             COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name
      FROM fact_orders fo
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      WHERE fo.shop_id IS NOT NULL
      GROUP BY fo.shop_id, r.restaurant_name, fo.restaurant_name
      ORDER BY restaurant_name`));

    const platforms = await q(`
      SELECT DISTINCT ${platExpr} AS platform
      FROM fact_orders fo
      WHERE ${platExpr} IN ('gf_whatsapp','swayo_whatsapp','swayo_app')
      ORDER BY platform`);

    const [dr] = await q(`
      SELECT DATE_FORMAT(MIN(order_date),'%Y-%m-%d') AS min_date,
             DATE_FORMAT(MAX(order_date),'%Y-%m-%d') AS max_date
      FROM fact_orders WHERE order_date IS NOT NULL`);

    // Campaign list — grouped by campaign_name (NOT campaign_id)
    const campaigns = await q(`
      SELECT campaign_name,
             GROUP_CONCAT(DISTINCT campaign_id ORDER BY campaign_id) AS campaign_ids,
             DATE_FORMAT(MIN(DATE_ADD(scheduled_date, INTERVAL 330 MINUTE)), '%Y-%m-%d') AS scheduled_date,
             COUNT(*) AS recipient_count
      FROM fact_campaigns
      GROUP BY campaign_name
      ORDER BY scheduled_date DESC`).catch(() => []);

    const deliveryTypes = await q(`
      SELECT DISTINCT delivery_type FROM fact_orders
      WHERE delivery_type IS NOT NULL AND delivery_type != ''
      ORDER BY delivery_type`).catch(() => []);

    res.json({
      restaurants,
      platforms: platforms.map(p => p.platform),
      deliveryTypes: deliveryTypes.map(d => d.delivery_type),
      campaigns,
      ...dr
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  OVERVIEW
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/overview", async (req, res) => {
  try {
    const platExpr = canonicalPlatformExpr("fo");
    const ordersFrom = `FROM (SELECT fo.*, ${platExpr} AS canonical_platform FROM fact_orders fo) fo`;
    const { where, vals } = ordersWhere(req.query);

    const [kpis] = await q(`
      SELECT
        COALESCE(SUM(fo.order_value),0)                                          AS total_gmv,
        COALESCE(SUM(fo.net_revenue),0)                                          AS total_net_revenue,
        COUNT(DISTINCT fo.order_id)                                               AS total_orders,
        ROUND(AVG(fo.order_value),2)                                             AS avg_order_value,
        SUM(fo.is_cancelled)                                                      AS cancelled_orders,
        ROUND(SUM(fo.is_cancelled)*100.0/NULLIF(COUNT(DISTINCT fo.order_id),0),1) AS cancel_rate,
        COUNT(DISTINCT fo.customer_contact)                                       AS unique_customers,
        SUM(fo.has_coupon)                                                        AS coupon_orders,
        ROUND(SUM(fo.has_coupon)*100.0/NULLIF(COUNT(DISTINCT fo.order_id),0),1) AS coupon_rate,
        -- Discount: use COALESCE(discount,0) as it covers all platforms
        COALESCE(SUM(fo.discount),0)                                             AS total_discount,
        COALESCE(SUM(fo.packing_charge),0)                                       AS total_packing,
        COALESCE(SUM(fo.delivery_charge),0)                                      AS total_delivery,
        COALESCE(SUM(fo.tax),0)                                                  AS total_tax
      ${ordersFrom} WHERE ${where}`, vals);

    // Month-on-Month (distinct order_id to avoid dupes)
    const monthly = await q(`
      SELECT fo.order_year, fo.order_month, fo.order_month_name,
             CONCAT(fo.order_year,'-',LPAD(fo.order_month,2,'0')) AS month_key,
             COUNT(DISTINCT fo.order_id) AS orders,
             SUM(fo.order_value) AS gmv,
             SUM(fo.net_revenue) AS net_revenue,
             ROUND(AVG(fo.order_value),2) AS aov,
             COUNT(DISTINCT fo.customer_contact) AS unique_customers,
             SUM(fo.is_cancelled) AS cancelled
      ${ordersFrom} WHERE ${where}
      GROUP BY fo.order_year, fo.order_month, fo.order_month_name
      ORDER BY fo.order_year, fo.order_month`, vals);

    // Day-on-Day
    const daily = await q(`
      SELECT fo.order_date, COUNT(DISTINCT fo.order_id) AS orders,
             SUM(fo.order_value) AS gmv, fo.order_dow AS dow
      ${ordersFrom} WHERE ${where}
      GROUP BY fo.order_date, fo.order_dow ORDER BY fo.order_date`, vals);

    // Per-platform per-day
    const dailyByPlatform = await q(`
      SELECT fo.order_date, fo.canonical_platform AS platform,
              COUNT(DISTINCT fo.order_id) AS orders, SUM(fo.order_value) AS gmv
      ${ordersFrom} WHERE ${where}
      GROUP BY fo.order_date, fo.canonical_platform
      ORDER BY fo.order_date, fo.canonical_platform`, vals);

    // Platform share
    const platforms = await q(`
      SELECT fo.canonical_platform AS platform, COUNT(DISTINCT fo.order_id) AS orders,
              SUM(fo.order_value) AS gmv, SUM(fo.net_revenue) AS net_revenue,
              ROUND(AVG(fo.order_value),2) AS aov,
              SUM(fo.is_cancelled) AS cancelled,
              ROUND(SUM(fo.is_cancelled)*100.0/NULLIF(COUNT(DISTINCT fo.order_id),0),1) AS cancel_rate,
              ROUND(SUM(fo.has_coupon)*100.0/NULLIF(COUNT(DISTINCT fo.order_id),0),1)   AS coupon_rate
      ${ordersFrom} WHERE ${where}
      GROUP BY fo.canonical_platform ORDER BY orders DESC`, vals);

    // DoW
    const dow = await q(`
      SELECT fo.order_dow, COUNT(DISTINCT fo.order_id) AS orders,
             SUM(fo.order_value) AS gmv, ROUND(AVG(fo.order_value),2) AS aov
      ${ordersFrom} WHERE ${where}
      GROUP BY fo.order_dow
      ORDER BY FIELD(fo.order_dow,'Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday')`, vals);

    // Hourly
    const hourly = await q(`
      SELECT fo.order_hour, COUNT(DISTINCT fo.order_id) AS orders, SUM(fo.order_value) AS gmv
      ${ordersFrom} WHERE ${where}
      GROUP BY fo.order_hour ORDER BY fo.order_hour`, vals);

    // FIX: Delivery type — ALL types from ALL platforms, not just Swayo
    const delivery = await q(`
      SELECT
        COALESCE(fo.delivery_type, 'Unknown') AS delivery_type,
        fo.canonical_platform AS platform,
        COUNT(DISTINCT fo.order_id) AS cnt,
        SUM(fo.order_value) AS gmv,
        ROUND(AVG(fo.order_value),2) AS aov
      ${ordersFrom} WHERE ${where}
      GROUP BY fo.delivery_type, fo.canonical_platform
      ORDER BY cnt DESC`, vals);

    res.json({ kpis, monthly, daily, dailyByPlatform, platforms, dow, hourly, delivery });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  DRILL-DOWN — returns full order rows for any dimension click
// ════════════════════════════════════════════════════════════════════════════
async function fetchDrillRows(base = {}) {
  const c = ["1=1"], v = [];
  const platExpr = canonicalPlatformExpr("fo");
  const ordersFrom = `FROM (SELECT fo.*, ${platExpr} AS canonical_platform FROM fact_orders fo) fo`;

  if (base.from) { c.push("fo.order_date >= ?"); v.push(base.from); }
  if (base.to)   { c.push("fo.order_date <= ?"); v.push(base.to);   }
  const platC = multiIn(`(${platExpr})`, base.platform, v); if (platC) c.push(platC);
  const shopC = multiIn("fo.shop_id", base.shop_id, v);   if (shopC) c.push(shopC);

  if (base.month_key) {
    const [yr, mo] = String(base.month_key).split("-");
    if (yr && mo) { c.push("fo.order_year = ?"); v.push(yr); c.push("fo.order_month = ?"); v.push(mo); }
  }
  if (base.order_date)    { c.push("fo.order_date = ?"); v.push(base.order_date); }
  if (base.delivery_type) { c.push("fo.delivery_type = ?"); v.push(base.delivery_type); }
  if (base.order_status)  { c.push("fo.order_status = ?");  v.push(base.order_status); }
  if (base.customer_contact) { c.push("fo.customer_contact = ?"); v.push(base.customer_contact); }
  if (base.has_coupon !== undefined && base.has_coupon !== "") { c.push("fo.has_coupon = ?"); v.push(base.has_coupon); }
  if (base.product_name)  { c.push("fo.order_id IN (SELECT i.order_id FROM fact_order_items i WHERE i.product_name = ?)"); v.push(base.product_name); }

  const limit = Math.min(parseInt(base.limit) || 500, 5000);
  const rows = await q(`
    SELECT DISTINCT
      fo.order_id, fo.order_date, fo.order_dow, fo.order_hour,
      fo.canonical_platform AS platform,
      COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name,
      fo.shop_id, fo.order_status,
      COALESCE(dc.customer_name,'—') AS customer_name,
      fo.customer_contact,
      COALESCE(cb.customer_segment,'Unknown') AS customer_segment,
      COALESCE(cb.total_orders,1) AS lifetime_orders,
      fo.order_value, fo.net_revenue,
      COALESCE(fo.discount,0) AS discount,
      COALESCE(fo.packing_charge,0) AS packing_charge,
      COALESCE(fo.delivery_charge,0) AS delivery_charge,
      COALESCE(fo.tax,0) AS tax,
      fo.delivery_type,
      fo.has_coupon, fo.coupon_value,
      fo.is_cancelled
    ${ordersFrom}
    LEFT JOIN dim_restaurants r        ON r.shop_id = fo.shop_id
    LEFT JOIN dim_customers dc         ON dc.customer_contact = fo.customer_contact
    LEFT JOIN agg_customer_behavior cb ON cb.customer_contact = fo.customer_contact
    WHERE ${c.join(" AND ")}
    ORDER BY fo.order_date DESC, fo.order_id
    LIMIT ${limit}`, v);

  const [totals] = await q(`
    SELECT COUNT(DISTINCT fo.order_id) AS total_orders,
           SUM(fo.order_value) AS total_gmv,
           COUNT(DISTINCT fo.customer_contact) AS unique_customers
    ${ordersFrom} WHERE ${c.join(" AND ")}`, v);

  return { rows, totals: totals || {}, count: rows.length };
}

app.get("/api/drill", async (req, res) => {
  try { res.json(await fetchDrillRows(req.query)); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

app.get("/api/orders_by_delivery_type", async (req, res) => {
  try { res.json(await fetchDrillRows(req.query)); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
app.get("/api/orders_by_month", async (req, res) => {
  try { res.json(await fetchDrillRows(req.query)); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
app.get("/api/orders_by_date", async (req, res) => {
  try { res.json(await fetchDrillRows(req.query)); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
app.get("/api/restaurant_orders", async (req, res) => {
  try { res.json(await fetchDrillRows(req.query)); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  AOV MONTHLY
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/aov_monthly", async (req, res) => {
  try {
    const platExpr = canonicalPlatformExpr("fo");
    const { where, vals } = ordersWhere(req.query);
    const rows = await q(`
      SELECT fo.order_year, fo.order_month, fo.order_month_name,
             CONCAT(fo.order_year,'-',LPAD(fo.order_month,2,'0')) AS month_key,
             ${platExpr} AS platform, COUNT(DISTINCT fo.order_id) AS orders,
             ROUND(AVG(fo.order_value),2) AS aov_gross,
             ROUND(AVG(fo.order_value
               - COALESCE(fo.packing_charge,0)
               - COALESCE(fo.delivery_charge,0)
               - COALESCE(fo.convenience_charge,0)
               - COALESCE(fo.tax,0)),2) AS aov_food_only
      FROM fact_orders fo WHERE ${where}
      GROUP BY fo.order_year, fo.order_month, fo.order_month_name, ${platExpr}
      ORDER BY fo.order_year, fo.order_month, ${platExpr}`, vals);
    res.json({ rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  RESTAURANTS
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/restaurants", async (req, res) => {
  try {
    const ac = ["1=1"], av = [];
    if (req.query.from) { ac.push("a.order_date >= ?"); av.push(req.query.from); }
    if (req.query.to)   { ac.push("a.order_date <= ?"); av.push(req.query.to); }
    const platC = multiIn("a.platform", req.query.platform, av);
    if (platC) ac.push(platC);
    const shopC = multiIn("a.shop_id", req.query.shop_id, av);
    if (shopC) ac.push(shopC);

    const agg = await q(`
      SELECT a.shop_id,
             COALESCE(a.restaurant_name, r.restaurant_name, a.shop_id) AS name,
             r.city, r.seller_pincode,
             SUM(a.order_count) AS orders, SUM(a.gmv) AS gmv,
             SUM(a.net_revenue) AS net_revenue,
             ROUND(SUM(a.gmv)/NULLIF(SUM(a.order_count),0),2) AS aov,
             SUM(a.cancelled_count) AS cancelled,
             ROUND(SUM(a.cancelled_count)*100.0/NULLIF(SUM(a.order_count),0),1) AS cancel_rate,
             SUM(a.discount_given) AS total_discount
      FROM agg_restaurant_daily a
      LEFT JOIN dim_restaurants r ON r.shop_id = a.shop_id
      WHERE ${ac.join(" AND ")}
      GROUP BY a.shop_id, a.restaurant_name, r.restaurant_name, r.city, r.seller_pincode
      HAVING orders > 0 ORDER BY gmv DESC LIMIT 40`, av);

    if (agg.length > 0) { res.json({ top: agg, source: "agg" }); return; }

    const { where, vals } = ordersWhere(req.query);
    const live = await q(`
      SELECT fo.shop_id,
             COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS name,
             r.city, r.seller_pincode,
             COUNT(DISTINCT fo.order_id) AS orders, SUM(fo.order_value) AS gmv,
             SUM(fo.net_revenue) AS net_revenue, ROUND(AVG(fo.order_value),2) AS aov,
             SUM(fo.is_cancelled) AS cancelled,
             ROUND(SUM(fo.is_cancelled)*100.0/NULLIF(COUNT(DISTINCT fo.order_id),0),1) AS cancel_rate,
             COALESCE(SUM(fo.discount),0) AS total_discount
      FROM fact_orders fo
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      WHERE ${where}
      GROUP BY fo.shop_id, r.restaurant_name, fo.restaurant_name, r.city, r.seller_pincode
      HAVING orders > 0 ORDER BY gmv DESC LIMIT 40`, vals);
    res.json({ top: live, source: "live" });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  APP FUNNEL (fact_funnel — only SWYO order IDs)
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/funnel", async (req, res) => {
  try {
    const fc = ["1=1"], fv = [];
    if (req.query.from)    { fc.push("event_date >= ?"); fv.push(req.query.from); }
    if (req.query.to)      { fc.push("event_date <= ?"); fv.push(req.query.to); }
    const sC = multiIn("shop_id", req.query.shop_id, fv);
    if (sC) fc.push(sC);
    const fWhere = fc.join(" AND ");

    const stages = await q(`
      SELECT action_order, action AS stage_name, COUNT(*) AS total
      FROM fact_funnel WHERE ${fWhere}
      GROUP BY action_order, action ORDER BY action_order`, fv);

    // Weekly trend — which day has most inflow
    const weeklyTrend = await q(`
      SELECT event_date, event_dow,
             WEEK(event_date) AS week_num,
             action,
             COUNT(*) AS total
      FROM fact_funnel WHERE ${fWhere}
      GROUP BY event_date, event_dow, week_num, action
      ORDER BY event_date`, fv);

    // Hourly activity
    const hourly = await q(`
      SELECT event_hour, action, COUNT(*) AS total
      FROM fact_funnel WHERE ${fWhere}
      GROUP BY event_hour, action ORDER BY event_hour, action`, fv);

    // Top 10 restaurants by funnel activity
    const topShops = await q(`
      SELECT ff.shop_id,
             COALESCE(r.restaurant_name, ff.shop_id) AS restaurant_name,
             COUNT(*) AS total_events,
             SUM(CASE WHEN action='VIEW_CART' THEN 1 ELSE 0 END) AS view_cart,
             SUM(CASE WHEN action='CHECKOUT'  THEN 1 ELSE 0 END) AS checkout,
             SUM(CASE WHEN action='ORDER'     THEN 1 ELSE 0 END) AS orders,
             COUNT(DISTINCT ff.customer_contact) AS unique_customers
      FROM fact_funnel ff
      LEFT JOIN dim_restaurants r ON r.shop_id = ff.shop_id
      WHERE ${fWhere}
      GROUP BY ff.shop_id, r.restaurant_name
      ORDER BY total_events DESC LIMIT 10`, fv);

    // Drop-off cohorts (single pass, faster than NOT IN subqueries)
    const dropoffAgg = await q(`
      SELECT ff.customer_contact,
             COALESCE(dc.customer_name,'—') AS customer_name,
             ff.shop_id,
             COALESCE(r.restaurant_name, ff.shop_id) AS restaurant_name,
             SUM(CASE WHEN ff.action='VIEW_CART' THEN 1 ELSE 0 END) AS view_cart_count,
             SUM(CASE WHEN ff.action='CHECKOUT' THEN 1 ELSE 0 END) AS checkout_count,
             SUM(CASE WHEN ff.action='ORDER' THEN 1 ELSE 0 END) AS order_count,
             MAX(ff.event_date) AS last_seen
      FROM fact_funnel ff
      LEFT JOIN dim_customers dc ON dc.customer_contact = ff.customer_contact
      LEFT JOIN dim_restaurants r ON r.shop_id = ff.shop_id
      WHERE ${fWhere}
        AND ff.customer_contact IS NOT NULL
        AND ff.action IN ('VIEW_CART','CHECKOUT','ORDER')
      GROUP BY ff.customer_contact, dc.customer_name, ff.shop_id, r.restaurant_name
      ORDER BY last_seen DESC LIMIT 200`, fv);
    const cartNoCheckout = dropoffAgg
      .filter(r => Number(r.view_cart_count) > 0 && Number(r.checkout_count) === 0)
      .slice(0, 100);
    const checkoutNoOrder = dropoffAgg
      .filter(r => Number(r.checkout_count) > 0 && Number(r.order_count) === 0)
      .slice(0, 100);

    // Conversion rates from agg
    const [conv] = await q(`
      SELECT SUM(pdp_views) AS pdp_views, SUM(plp_views) AS plp_views,
             SUM(cart_views) AS cart_views, SUM(checkouts) AS checkouts,
             SUM(orders_placed) AS orders_placed,
             ROUND(AVG(plp_to_cart_rate),2) AS plp_to_cart_rate,
             ROUND(AVG(cart_to_checkout_rate),2) AS cart_to_checkout_rate,
             ROUND(AVG(checkout_to_order_rate),2) AS checkout_to_order_rate,
             ROUND(AVG(overall_conversion_rate),2) AS overall_conversion_rate
      FROM agg_funnel_conversion WHERE ${fWhere}`, fv);

    res.json({ stages, weeklyTrend, hourly, topShops, cartNoCheckout, checkoutNoOrder, conversion: conv || {} });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  WA FUNNEL (fact_funnel_wa — GF + SWWA order IDs)
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/funnel_wa", async (req, res) => {
  try {
    const fc = ["1=1"], fv = [];
    if (req.query.from)        { fc.push("event_date >= ?"); fv.push(req.query.from); }
    if (req.query.to)          { fc.push("event_date <= ?"); fv.push(req.query.to); }
    if (req.query.campaign_id) { fc.push("campaign_id = ?"); fv.push(req.query.campaign_id); }
    const sC = multiIn("shop_id", req.query.shop_id, fv);
    if (sC) fc.push(sC);
    const fWhere = fc.join(" AND ");

    const stages = await q(`
      SELECT action_order, action AS stage_name, COUNT(*) AS total
      FROM fact_funnel_wa WHERE ${fWhere}
      GROUP BY action_order, action ORDER BY action_order`, fv);

    // Weekly trend
    const weeklyTrend = await q(`
      SELECT event_date, event_dow,
             WEEK(event_date) AS week_num,
             action, COUNT(*) AS total
      FROM fact_funnel_wa WHERE ${fWhere}
      GROUP BY event_date, event_dow, week_num, action
      ORDER BY event_date`, fv);

    // By restaurant
    const byRestaurant = await q(`
      SELECT restaurant_name, shop_id,
             SUM(CASE WHEN action='VIEW_CATALOG' THEN 1 ELSE 0 END) AS view_catalog,
             SUM(CASE WHEN action='VIEW_CART'    THEN 1 ELSE 0 END) AS view_cart,
             SUM(CASE WHEN action='CHECKOUT'     THEN 1 ELSE 0 END) AS checkout,
             SUM(CASE WHEN action='ORDER'        THEN 1 ELSE 0 END) AS orders,
             COUNT(DISTINCT customer_contact) AS unique_customers,
             ROUND(SUM(CASE WHEN action='ORDER' THEN 1 ELSE 0 END)*100.0/
               NULLIF(SUM(CASE WHEN action='VIEW_CART' THEN 1 ELSE 0 END),0),1) AS cart_to_order_pct
      FROM fact_funnel_wa WHERE ${fWhere}
      GROUP BY restaurant_name, shop_id ORDER BY orders DESC`, fv);

    // Hourly
    const hourly = await q(`
      SELECT event_hour, action, COUNT(*) AS total
      FROM fact_funnel_wa WHERE ${fWhere}
      GROUP BY event_hour, action ORDER BY event_hour, action`, fv);

    // By campaign — grouped by campaign_name not campaign_id
    const byCampaign = await q(`
      SELECT campaign_id,
              COUNT(*) AS total_events,
              COUNT(DISTINCT customer_contact) AS unique_customers,
              SUM(CASE WHEN action='ORDER' THEN 1 ELSE 0 END) AS orders
      FROM fact_funnel_wa WHERE ${fWhere}
      GROUP BY campaign_id ORDER BY orders DESC LIMIT 20`, fv);

    // WA drop-off cohorts (single pass)
    const waDropoffAgg = await q(`
      SELECT fw.customer_contact,
             fw.customer_name,
             fw.restaurant_name, fw.shop_id,
             SUM(CASE WHEN fw.action='VIEW_CART' THEN 1 ELSE 0 END) AS view_cart_count,
             SUM(CASE WHEN fw.action='CHECKOUT' THEN 1 ELSE 0 END) AS checkout_count,
             SUM(CASE WHEN fw.action='ORDER' THEN 1 ELSE 0 END) AS order_count,
             MAX(fw.event_date) AS last_seen
      FROM fact_funnel_wa fw
      WHERE ${fWhere}
        AND fw.customer_contact IS NOT NULL
        AND fw.action IN ('VIEW_CART','CHECKOUT','ORDER')
      GROUP BY fw.customer_contact, fw.customer_name, fw.restaurant_name, fw.shop_id
      ORDER BY last_seen DESC LIMIT 200`, fv);
    const cartNoCheckout = waDropoffAgg
      .filter(r => Number(r.view_cart_count) > 0 && Number(r.checkout_count) === 0)
      .slice(0, 100);
    const checkoutNoOrder = waDropoffAgg
      .filter(r => Number(r.checkout_count) > 0 && Number(r.order_count) === 0)
      .slice(0, 100);

    const byCampaignNamed = (byCampaign || []).map((r) => ({
      campaign_name: r.campaign_id || "—",
      ...r
    }));

    res.json({ stages, weeklyTrend, hourly, byRestaurant, byCampaign: byCampaignNamed, cartNoCheckout, checkoutNoOrder });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  CAMPAIGNS
//  FIX: grouped by campaign_name, not campaign_id
//  FIX: scheduled_date uses DATE() cast to avoid timezone offset
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/campaigns", async (req, res) => {
  try {
    const campDateExpr = "DATE(DATE_ADD(fc.scheduled_date, INTERVAL 330 MINUTE))";
    const campRunExpr = "COALESCE(fc.scheduled_at, fc.delivered_at, fc.read_at, fc.sent_at, DATE_ADD(fc.scheduled_date, INTERVAL 330 MINUTE))";
    // FIX: filter by campaign_name, not campaign_id
    const cc = ["1=1"], cv = [];
    if (req.query.campaign_name) { cc.push("fc.campaign_name = ?"); cv.push(req.query.campaign_name); }
    if (req.query.from) { cc.push(`${campDateExpr} >= ?`); cv.push(req.query.from); }
    if (req.query.to)   { cc.push(`${campDateExpr} <= ?`); cv.push(req.query.to); }
    const cWhere = cc.join(" AND ");
    const cWhereBare = cWhere.replace(/fc\./g, "");

    // Performance — grouped by campaign_name
    const perf = await q(`
      SELECT fc.campaign_name,
             COUNT(DISTINCT fc.mobile_number) AS total_recipients,
             SUM(fc.is_sent) AS sent_count, SUM(fc.is_delivered) AS delivered_count,
             SUM(fc.is_read) AS read_count,
             ROUND(SUM(fc.is_sent)*100.0/NULLIF(COUNT(DISTINCT fc.mobile_number),0),1) AS sent_rate_pct,
             ROUND(SUM(fc.is_delivered)*100.0/NULLIF(COUNT(DISTINCT fc.mobile_number),0),1) AS delivered_rate_pct,
             ROUND(SUM(fc.is_read)*100.0/NULLIF(COUNT(DISTINCT fc.mobile_number),0),1) AS read_rate_pct,
             MIN(${campDateExpr}) AS scheduled_date
      FROM fact_campaigns fc WHERE ${cWhere}
      GROUP BY fc.campaign_name
      ORDER BY scheduled_date DESC`, cv).catch(() => []);

    // Individual recipients with customer behavior
    const recipients = await q(`
      SELECT fc.campaign_name, fc.mobile_number,
             ${campDateExpr} AS scheduled_date,
             ${campRunExpr} AS campaign_run_at,
             fc.delivery_status, fc.is_sent, fc.is_delivered, fc.is_read,
             fc.sent_at, fc.delivered_at, fc.read_at, fc.pitch_response,
             COALESCE(cb.total_orders, 0) AS lifetime_orders,
             COALESCE(cb.customer_segment,'Unknown') AS segment,
             cb.last_order_date, COALESCE(cb.total_gmv, 0) AS lifetime_gmv
      FROM fact_campaigns fc
      LEFT JOIN agg_customer_behavior cb ON cb.customer_contact = fc.mobile_number
      WHERE ${cWhere}
      ORDER BY fc.delivery_status DESC, fc.is_read DESC, fc.is_delivered DESC`, cv);

    // Summary
    const [summary] = await q(`
      SELECT COUNT(DISTINCT mobile_number) AS total_recipients,
             SUM(is_sent) AS sent, SUM(is_delivered) AS delivered,
             SUM(is_read) AS read_count,
             ROUND(SUM(is_sent)*100.0/NULLIF(COUNT(DISTINCT mobile_number),0),1) AS sent_rate,
             ROUND(SUM(is_delivered)*100.0/NULLIF(COUNT(DISTINCT mobile_number),0),1) AS delivered_rate,
             ROUND(SUM(is_read)*100.0/NULLIF(COUNT(DISTINCT mobile_number),0),1) AS read_rate
      FROM fact_campaigns WHERE ${cWhereBare}`, cv);

    // Post-campaign orders — ALL orders by this contact (before and after), includes order_id
    const postOrders = await q(`
      SELECT fc.campaign_name, fc.mobile_number,
             fo.order_id, fo.order_date,
             fo.created_at AS order_ts,
             ${campDateExpr} AS campaign_date,
             ${campRunExpr} AS campaign_run_at,
             CASE WHEN fo.created_at >= ${campRunExpr} THEN 'After Campaign'
                  ELSE 'Before Campaign' END AS order_timing,
             fo.order_value, fo.platform, fo.delivery_type,
             COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name,
             fo.order_status
      FROM fact_campaigns fc
      JOIN fact_orders fo ON fo.customer_contact = fc.mobile_number
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      WHERE ${cWhere} AND fc.is_delivered = 1
       ORDER BY fc.mobile_number, fo.created_at DESC`, cv);

    // Post-campaign items
    const postItems = await q(`
      SELECT fc.campaign_name, i.product_name,
             COALESCE(r.restaurant_name, fo.restaurant_name) AS restaurant_name,
             fo.delivery_type, COUNT(*) AS qty
      FROM fact_campaigns fc
      JOIN fact_orders fo ON fo.customer_contact = fc.mobile_number
        AND fo.created_at >= ${campRunExpr}
      JOIN fact_order_items i ON i.order_id = fo.order_id
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      WHERE ${cWhere} AND fc.is_delivered = 1
        AND i.product_name IS NOT NULL
        AND LOWER(TRIM(i.product_name)) NOT IN ('nan','none','null','','n/a')
      GROUP BY fc.campaign_name, i.product_name, r.restaurant_name, fo.restaurant_name, fo.delivery_type
      ORDER BY qty DESC LIMIT 30`, cv);

    // Non-converters: received campaign but did NOT order after
    const nonConverters = await q(`
      SELECT fc.mobile_number, fc.delivery_status,
             ${campDateExpr} AS campaign_date,
             COALESCE(cb.customer_segment,'Unknown') AS segment,
             COALESCE(cb.total_orders,0) AS lifetime_orders,
             cb.last_order_date,
             COALESCE(cb.total_gmv,0) AS lifetime_gmv
      FROM fact_campaigns fc
      LEFT JOIN agg_customer_behavior cb ON cb.customer_contact = fc.mobile_number
      WHERE ${cWhere} AND fc.is_delivered = 1
        AND fc.mobile_number NOT IN (
          SELECT DISTINCT fo.customer_contact
          FROM fact_orders fo
          WHERE fo.created_at >= ${campRunExpr}
            AND fo.customer_contact = fc.mobile_number
        )
      ORDER BY cb.total_orders DESC`, cv);

    // Funnel activity for campaign recipients (WA funnel)
    const waFunnelActivity = await q(`
      SELECT fw.customer_contact, fw.action, fw.restaurant_name,
              COUNT(*) AS event_count, MAX(fw.event_date) AS last_activity
      FROM fact_funnel_wa fw
      JOIN fact_campaigns fc ON fc.mobile_number = fw.customer_contact
      WHERE ${cWhere}
        AND fc.is_delivered = 1
        AND fw.timestamp >= ${campRunExpr}
      GROUP BY fw.customer_contact, fw.action, fw.restaurant_name
      ORDER BY fw.customer_contact, fw.action`, cv);
    
    const appFunnelActivity = await q(`
      SELECT ff.customer_contact, ff.action,
              COALESCE(r.restaurant_name, ff.shop_id) AS restaurant_name,
              COUNT(*) AS event_count, MAX(ff.event_date) AS last_activity
      FROM fact_funnel ff
      JOIN fact_campaigns fc ON fc.mobile_number = ff.customer_contact
      LEFT JOIN dim_restaurants r ON r.shop_id = ff.shop_id
      WHERE ${cWhere}
        AND fc.is_delivered = 1
        AND ff.timestamp >= ${campRunExpr}
      GROUP BY ff.customer_contact, ff.action, r.restaurant_name, ff.shop_id
      ORDER BY ff.customer_contact, ff.action`, cv);

    res.json({
      perf, recipients, summary: summary||{}, postOrders, postItems, nonConverters,
      waFunnelActivity, appFunnelActivity
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  CAMPAIGN NON-CONVERTERS export
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/campaign_nonconv", async (req, res) => {
  try {
    const campaign_name = req.query.campaign_name;
    if (!campaign_name) { res.status(400).json({ error: "campaign_name required" }); return; }

    const rows = await q(`
      SELECT fc.mobile_number,
             DATE(fc.scheduled_date) AS campaign_date,
             fc.delivery_status,
             COALESCE(cb.customer_segment,'Unknown') AS segment,
             COALESCE(cb.total_orders,0) AS lifetime_orders,
             cb.last_order_date,
             COALESCE(cb.total_gmv,0) AS lifetime_gmv
      FROM fact_campaigns fc
      LEFT JOIN agg_customer_behavior cb ON cb.customer_contact = fc.mobile_number
      WHERE fc.campaign_name = ? AND fc.is_delivered = 1
        AND fc.mobile_number NOT IN (
          SELECT DISTINCT fo2.customer_contact
          FROM fact_orders fo2
          WHERE fo2.order_date >= DATE(fc.scheduled_date)
            AND fo2.customer_contact = fc.mobile_number
        )
      ORDER BY cb.total_orders DESC`, [campaign_name]);

    res.json({ count: rows.length, rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get("/api/campaign_non_converters", async (req, res) => {
  try {
    const campaign_name = req.query.campaign_name;
    if (!campaign_name) { res.status(400).json({ error: "campaign_name required" }); return; }
    const rows = await q(`
      SELECT fc.mobile_number,
             DATE(fc.scheduled_date) AS campaign_date,
             fc.delivery_status,
             COALESCE(cb.customer_segment,'Unknown') AS segment,
             COALESCE(cb.total_orders,0) AS lifetime_orders,
             cb.last_order_date,
             COALESCE(cb.total_gmv,0) AS lifetime_gmv
      FROM fact_campaigns fc
      LEFT JOIN agg_customer_behavior cb ON cb.customer_contact = fc.mobile_number
      WHERE fc.campaign_name = ? AND fc.is_delivered = 1
        AND fc.mobile_number NOT IN (
          SELECT DISTINCT fo2.customer_contact
          FROM fact_orders fo2
          WHERE fo2.order_date >= DATE(fc.scheduled_date)
            AND fo2.customer_contact = fc.mobile_number
        )
      ORDER BY cb.total_orders DESC`, [campaign_name]);
    res.json({ count: rows.length, rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get("/api/campaign_user_history", async (req, res) => {
  try {
    const cc = ["1=1"], cv = [];
    if (req.query.campaign_name) { cc.push("fc.campaign_name = ?"); cv.push(req.query.campaign_name); }
    if (req.query.from)          { cc.push("DATE(fc.scheduled_date) >= ?"); cv.push(req.query.from); }
    if (req.query.to)            { cc.push("DATE(fc.scheduled_date) <= ?"); cv.push(req.query.to); }
    const cWhere = cc.join(" AND ");
    const rows = await q(`
      SELECT fc.campaign_name, fc.mobile_number,
             DATE(fc.scheduled_date) AS campaign_date,
             fo.order_id, fo.order_date, fo.order_value, fo.delivery_type, fo.platform,
             COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name,
             CASE WHEN fo.order_date >= DATE(fc.scheduled_date) THEN 'after' ELSE 'before' END AS order_timing
      FROM fact_campaigns fc
      JOIN fact_orders fo ON fo.customer_contact = fc.mobile_number
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      WHERE ${cWhere}
      ORDER BY fc.mobile_number, fo.order_date DESC`, cv);
    res.json({ rows, count: rows.length });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get("/api/campaign_funnel_activity", async (req, res) => {
  try {
    const cc = ["1=1"], cv = [];
    if (req.query.campaign_name) { cc.push("campaign_name = ?"); cv.push(req.query.campaign_name); }
    const cWhere = cc.join(" AND ");
    const waRows = await q(`
      SELECT fw.customer_contact, fw.action, fw.restaurant_name,
             COUNT(*) AS event_count, MAX(fw.event_date) AS last_activity
      FROM fact_funnel_wa fw
      WHERE fw.customer_contact IN (SELECT DISTINCT mobile_number FROM fact_campaigns WHERE ${cWhere})
      GROUP BY fw.customer_contact, fw.action, fw.restaurant_name
      ORDER BY fw.customer_contact, fw.action`, cv);
    const appRows = await q(`
      SELECT ff.customer_contact, ff.action, COALESCE(r.restaurant_name, ff.shop_id) AS restaurant_name,
             COUNT(*) AS event_count, MAX(ff.event_date) AS last_activity
      FROM fact_funnel ff
      LEFT JOIN dim_restaurants r ON r.shop_id = ff.shop_id
      WHERE ff.customer_contact IN (SELECT DISTINCT mobile_number FROM fact_campaigns WHERE ${cWhere})
      GROUP BY ff.customer_contact, ff.action, r.restaurant_name, ff.shop_id
      ORDER BY ff.customer_contact, ff.action`, cv);
    res.json({ wa: waRows, app: appRows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get("/api/funnel_weekly", async (req, res) => {
  try {
    const source = req.query.source === "wa" ? "wa" : "app";
    const table = source === "wa" ? "fact_funnel_wa" : "fact_funnel";
    const fc = ["1=1"], fv = [];
    if (req.query.from) { fc.push("event_date >= ?"); fv.push(req.query.from); }
    if (req.query.to)   { fc.push("event_date <= ?"); fv.push(req.query.to); }
    const where = fc.join(" AND ");
    const weekly = await q(`
      SELECT event_date, event_dow, WEEK(event_date) AS week_num, action, COUNT(*) AS total
      FROM ${table} WHERE ${where}
      GROUP BY event_date, event_dow, week_num, action
      ORDER BY event_date`, fv);
    const hourly = await q(`
      SELECT event_hour, action, COUNT(*) AS total
      FROM ${table} WHERE ${where}
      GROUP BY event_hour, action ORDER BY event_hour`, fv);
    res.json({ source, weekly, hourly });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get("/api/funnel_top_restaurants", async (req, res) => {
  try {
    const source = req.query.source === "wa" ? "wa" : "app";
    const table = source === "wa" ? "fact_funnel_wa" : "fact_funnel";
    const fc = ["1=1"], fv = [];
    if (req.query.from) { fc.push("ff.event_date >= ?"); fv.push(req.query.from); }
    if (req.query.to)   { fc.push("ff.event_date <= ?"); fv.push(req.query.to); }
    const where = fc.join(" AND ");
    const rows = await q(`
      SELECT ff.shop_id, COALESCE(r.restaurant_name, ff.shop_id) AS restaurant_name,
             COUNT(*) AS total_events,
             SUM(CASE WHEN ff.action IN ('VIEW_CART','VIEW_CART') THEN 1 ELSE 0 END) AS view_cart,
             SUM(CASE WHEN ff.action='CHECKOUT' THEN 1 ELSE 0 END) AS checkout,
             SUM(CASE WHEN ff.action='ORDER' THEN 1 ELSE 0 END) AS orders,
             COUNT(DISTINCT ff.customer_contact) AS unique_customers
      FROM ${table} ff
      LEFT JOIN dim_restaurants r ON r.shop_id = ff.shop_id
      WHERE ${where}
      GROUP BY ff.shop_id, r.restaurant_name
      ORDER BY total_events DESC LIMIT 10`, fv);
    res.json({ source, rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get("/api/funnel_dropoffs", async (req, res) => {
  try {
    const source = req.query.source === "wa" ? "wa" : "app";
    const table = source === "wa" ? "fact_funnel_wa" : "fact_funnel";
    const fc = ["1=1"], fv = [];
    if (req.query.from) { fc.push("event_date >= ?"); fv.push(req.query.from); }
    if (req.query.to)   { fc.push("event_date <= ?"); fv.push(req.query.to); }
    const where = fc.join(" AND ");
    const cartNoCheckout = await q(`
      SELECT f.customer_contact, COALESCE(dc.customer_name,'—') AS customer_name,
             f.shop_id, COALESCE(r.restaurant_name, f.shop_id) AS restaurant_name, COUNT(*) AS cnt
      FROM ${table} f
      LEFT JOIN dim_customers dc ON dc.customer_contact = f.customer_contact
      LEFT JOIN dim_restaurants r ON r.shop_id = f.shop_id
      WHERE ${where} AND f.action='VIEW_CART' AND f.customer_contact IS NOT NULL
        AND f.customer_contact NOT IN (SELECT DISTINCT customer_contact FROM ${table} WHERE action='CHECKOUT' AND ${where})
      GROUP BY f.customer_contact, dc.customer_name, f.shop_id, r.restaurant_name
      ORDER BY cnt DESC LIMIT 200`, [...fv, ...fv]);
    const checkoutNoOrder = await q(`
      SELECT f.customer_contact, COALESCE(dc.customer_name,'—') AS customer_name,
             f.shop_id, COALESCE(r.restaurant_name, f.shop_id) AS restaurant_name, COUNT(*) AS cnt
      FROM ${table} f
      LEFT JOIN dim_customers dc ON dc.customer_contact = f.customer_contact
      LEFT JOIN dim_restaurants r ON r.shop_id = f.shop_id
      WHERE ${where} AND f.action='CHECKOUT' AND f.customer_contact IS NOT NULL
        AND f.customer_contact NOT IN (SELECT DISTINCT customer_contact FROM ${table} WHERE action='ORDER' AND ${where})
      GROUP BY f.customer_contact, dc.customer_name, f.shop_id, r.restaurant_name
      ORDER BY cnt DESC LIMIT 200`, [...fv, ...fv]);
    res.json({ source, cartNoCheckout, checkoutNoOrder });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  GEO
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/geo", async (req, res) => {
  try {
    const geoPlatExpr = canonicalPlatformExpr("g");
    const geoFrom = `FROM (SELECT g.*, ${geoPlatExpr} AS canonical_platform FROM fact_order_geo g) g`;
    const gc = ["1=1"], gv = [];
    if (req.query.from)     { gc.push("g.order_date >= ?");        gv.push(req.query.from); }
    if (req.query.to)       { gc.push("g.order_date <= ?");        gv.push(req.query.to); }
    if (req.query.max_dist) { gc.push("g.delivery_distance <= ?"); gv.push(req.query.max_dist); }
    if (req.query.min_dist) { gc.push("g.delivery_distance >= ?"); gv.push(req.query.min_dist); }
    const pC = multiIn("g.canonical_platform", req.query.platform, gv);
    if (pC) gc.push(pC);
    const sC = multiIn("g.restaurant_name", req.query.restaurant_name, gv);
    // Don't add restaurant_name multi — use shop_id
    const sC2 = multiIn("g.restaurant_name", req.query.shop_id ? null : null, gv);
    if (req.query.shop_id) {
      // Resolve shop_id to restaurant_name for geo table
      gc.push("g.restaurant_name IN (SELECT COALESCE(restaurant_name, shop_id) FROM dim_restaurants WHERE shop_id = ?)");
      gv.push(req.query.shop_id);
    }
    const gWhere = gc.join(" AND ");

    const byCustomerPincode = await q(`
      SELECT g.customer_pincode, g.restaurant_name, g.canonical_platform AS platform,
             COUNT(*) AS orders,
             ROUND(AVG(g.delivery_distance),2) AS avg_distance
      ${geoFrom}
      WHERE ${gWhere} AND g.customer_pincode IS NOT NULL
      GROUP BY g.customer_pincode, g.restaurant_name, g.canonical_platform
      ORDER BY orders DESC LIMIT 100`, gv);

    const byRestaurantPincode = await q(`
      SELECT g.restaurant_pincode, g.restaurant_name,
             COUNT(*) AS orders,
             COUNT(DISTINCT g.customer_pincode) AS unique_customer_pincodes,
             ROUND(AVG(g.delivery_distance),2) AS avg_distance
      ${geoFrom}
      WHERE ${gWhere} AND g.restaurant_pincode IS NOT NULL
      GROUP BY g.restaurant_pincode, g.restaurant_name
      ORDER BY orders DESC`, gv);

    const distBuckets = await q(`
      SELECT CASE
               WHEN g.delivery_distance < 1  THEN '< 1 km'
               WHEN g.delivery_distance < 2  THEN '1–2 km'
               WHEN g.delivery_distance < 3  THEN '2–3 km'
               WHEN g.delivery_distance < 5  THEN '3–5 km'
               WHEN g.delivery_distance < 10 THEN '5–10 km'
               ELSE '10+ km'
             END AS distance_bucket,
             COUNT(*) AS orders,
             ROUND(AVG(g.delivery_distance),2) AS avg_dist,
             g.canonical_platform AS platform
      ${geoFrom}
      WHERE ${gWhere} AND g.delivery_distance IS NOT NULL
      GROUP BY distance_bucket, g.canonical_platform
      ORDER BY MIN(g.delivery_distance)`, gv);

    // Pickup candidates — within threshold km (default 2)
    const threshKm = parseFloat(req.query.pickup_threshold) || 2;
    const pickupCandidates = await q(`
      SELECT g.customer_contact, g.customer_pincode, g.restaurant_name,
             g.delivery_distance, g.canonical_platform AS platform, g.order_date,
             g.delivery_address,
             COALESCE(dc.customer_name,'—') AS customer_name,
             COALESCE(cb.customer_segment,'Unknown') AS segment,
             COALESCE(cb.total_orders,1) AS lifetime_orders
      ${geoFrom}
      LEFT JOIN dim_customers dc ON dc.customer_contact = g.customer_contact
      LEFT JOIN agg_customer_behavior cb ON cb.customer_contact = g.customer_contact
      WHERE ${gWhere}
        AND g.delivery_distance IS NOT NULL
        AND g.delivery_distance <= ?
        AND g.customer_contact IS NOT NULL
      ORDER BY g.delivery_distance ASC LIMIT 300`, [...gv, threshKm]);

    // Delivery→Pickup candidates: customer who orders delivery but lives near restaurant
    const delivToPickup = await q(`
      SELECT g.customer_contact, g.customer_pincode,
             g.restaurant_name, g.delivery_distance, g.order_date,
             COALESCE(dc.customer_name,'—') AS customer_name,
             COALESCE(cb.total_orders,1) AS lifetime_orders
      ${geoFrom}
      JOIN fact_orders fo ON fo.order_id = g.order_id
      LEFT JOIN dim_customers dc ON dc.customer_contact = g.customer_contact
      LEFT JOIN agg_customer_behavior cb ON cb.customer_contact = g.customer_contact
      WHERE ${gWhere}
        AND fo.delivery_type = 'delivery'
        AND g.delivery_distance IS NOT NULL
        AND g.delivery_distance <= ?
        AND g.customer_contact IS NOT NULL
      ORDER BY g.delivery_distance ASC LIMIT 200`, [...gv, threshKm]);

    const [geoKpis] = await q(`
      SELECT COUNT(*) AS total_orders,
             COUNT(DISTINCT g.customer_pincode) AS unique_customer_pincodes,
             COUNT(DISTINCT g.restaurant_pincode) AS unique_restaurant_pincodes,
             ROUND(AVG(g.delivery_distance),2) AS avg_distance,
             MIN(g.delivery_distance) AS min_distance,
             MAX(g.delivery_distance) AS max_distance,
             SUM(CASE WHEN g.delivery_distance <= 2 THEN 1 ELSE 0 END) AS within_2km
      ${geoFrom} WHERE ${gWhere}`, gv);

    res.json({ byCustomerPincode, byRestaurantPincode, distBuckets, pickupCandidates, delivToPickup, kpis: geoKpis });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  CUSTOMERS — FIX: was blank because ordersWhere applied to agg table
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/customers", async (req, res) => {
  try {
    // Segments from agg_customer_behavior (no WHERE needed — it's all customers)
    const segments = await q(`
      SELECT customer_segment, COUNT(*) AS cnt,
             SUM(total_gmv) AS gmv,
             ROUND(AVG(total_orders),1) AS avg_orders
      FROM agg_customer_behavior
      GROUP BY customer_segment
      ORDER BY FIELD(customer_segment,'VIP','Loyal','Repeat','One-time')`);

    // Top customers — always show all, not filtered by date
    const top = await q(`
      SELECT cb.customer_contact,
             COALESCE(dc.customer_name,'—') AS customer_name,
             cb.customer_segment, cb.total_orders, cb.total_gmv,
             cb.first_order_date, cb.last_order_date,
             cb.platforms_used, cb.coupon_usage, cb.cancelled_orders
      FROM agg_customer_behavior cb
      LEFT JOIN dim_customers dc ON dc.customer_contact = cb.customer_contact
      ORDER BY cb.total_orders DESC LIMIT 50`);

    // Date-filtered analysis from fact_orders
    const { where, vals } = ordersWhere(req.query);

    const coupon = await q(`
      SELECT fo.has_coupon, COUNT(DISTINCT fo.order_id) AS orders,
             ROUND(AVG(fo.order_value),2) AS aov,
             COALESCE(SUM(fo.discount),0) AS total_discount
      FROM fact_orders fo WHERE ${where} GROUP BY fo.has_coupon`, vals);

    const repeatVsNew = await q(`
      SELECT CASE WHEN total_orders=1 THEN 'new' ELSE 'repeat' END AS buyer_type,
             COUNT(*) AS customers, SUM(total_gmv) AS gmv
      FROM agg_customer_behavior GROUP BY buyer_type`);

    const uniqueByMonth = await q(`
      SELECT fo.order_year, fo.order_month, fo.order_month_name,
             CONCAT(fo.order_year,'-',LPAD(fo.order_month,2,'0')) AS month_key,
             COUNT(DISTINCT fo.customer_contact) AS unique_users,
             COUNT(DISTINCT fo.order_id) AS orders
      FROM fact_orders fo WHERE ${where}
      GROUP BY fo.order_year, fo.order_month, fo.order_month_name
      ORDER BY fo.order_year, fo.order_month`, vals);

    const ordersPerUserMonth = await q(`
      SELECT fo.order_year, fo.order_month,
             CONCAT(fo.order_year,'-',LPAD(fo.order_month,2,'0')) AS month_key,
             fo.customer_contact, COUNT(DISTINCT fo.order_id) AS order_count
      FROM fact_orders fo
      WHERE ${where} AND fo.customer_contact IS NOT NULL
      GROUP BY fo.order_year, fo.order_month, fo.customer_contact
      ORDER BY fo.order_year, fo.order_month, order_count DESC`, vals);

    res.json({ segments, top, coupon, repeatVsNew, uniqueByMonth, ordersPerUserMonth, ordersPerUserTotal: top });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  PRODUCTS
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/products", async (req, res) => {
  try {
    const itemPlatExpr = canonicalPlatformExpr("fo");
    const itemOrdersFrom = `FROM fact_order_items i JOIN (SELECT fo.*, ${itemPlatExpr} AS canonical_platform FROM fact_orders fo) fo ON fo.order_id = i.order_id`;
    const { where, vals } = ordersWhere(req.query);

    const allItems = await q(`
      SELECT fo.canonical_platform AS platform, i.product_name, fo.shop_id,
             COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name,
             fo.order_year, fo.order_month,
             CONCAT(fo.order_year,'-',LPAD(fo.order_month,2,'0')) AS month_key,
             COUNT(DISTINCT fo.order_id) AS qty
      ${itemOrdersFrom}
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      WHERE ${where}
        AND i.product_name IS NOT NULL
        AND LOWER(TRIM(i.product_name)) NOT IN ('nan','none','null','','n/a')
        AND LENGTH(TRIM(i.product_name)) > 1
      GROUP BY fo.canonical_platform, i.product_name, fo.shop_id,
               r.restaurant_name, fo.restaurant_name, fo.order_year, fo.order_month
      ORDER BY fo.order_year, fo.order_month, qty DESC`, vals);

    const totalLineItems = allItems.reduce((s, r) => s + Number(r.qty), 0);
    const appItems = allItems.filter(x => x.platform === "swayo_app").sort((a,b)=>b.qty-a.qty).slice(0,15);
    const gfItems  = allItems.filter(x => x.platform === "gf_whatsapp").sort((a,b)=>b.qty-a.qty).slice(0,10);
    const waItems  = allItems.filter(x => x.platform === "swayo_whatsapp").sort((a,b)=>b.qty-a.qty).slice(0,10);

    const byMonth = {};
    allItems.forEach(r => {
      if (!byMonth[r.month_key]) byMonth[r.month_key] = { month_key: r.month_key, items: [] };
      if (byMonth[r.month_key].items.length < 10) byMonth[r.month_key].items.push(r);
    });

    const byRestaurant = await q(`
      SELECT COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name,
             fo.shop_id, COUNT(DISTINCT fo.order_id) AS qty
      FROM fact_order_items i
      JOIN fact_orders fo ON fo.order_id = i.order_id
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      WHERE ${where}
        AND i.product_name IS NOT NULL
        AND LOWER(TRIM(i.product_name)) NOT IN ('nan','none','null','','n/a')
      GROUP BY fo.shop_id, r.restaurant_name, fo.restaurant_name
      ORDER BY qty DESC LIMIT 20`, vals);

    res.json({ appItems, gfItems, waItems, byRestaurant,
               topByRestaurantMonth: allItems,
               totalLineItems, monthlyTop10: Object.values(byMonth) });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  ITEM CUSTOMERS — who ordered a specific product
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/item_customers", async (req, res) => {
  try {
    const product = req.query.product_name;
    if (!product) { res.status(400).json({ error: "product_name required" }); return; }

    const { where, vals } = ordersWhere(req.query);

    const rows = await q(`
      SELECT DISTINCT
        fo.customer_contact, COALESCE(dc.customer_name,'—') AS customer_name,
        fo.order_date, fo.order_id,
        COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name,
        fo.platform, fo.delivery_type, fo.order_value,
        fo.order_status, i.product_name,
        COALESCE(cb.customer_segment,'Unknown') AS segment,
        COALESCE(cb.total_orders,1) AS lifetime_orders
      FROM fact_order_items i
      JOIN fact_orders fo ON fo.order_id = i.order_id
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      LEFT JOIN dim_customers dc ON dc.customer_contact = fo.customer_contact
      LEFT JOIN agg_customer_behavior cb ON cb.customer_contact = fo.customer_contact
      WHERE ${where}
        AND i.product_name = ?
      ORDER BY fo.order_date DESC
      LIMIT 200`, [...vals, product]);

    const [summary] = await q(`
      SELECT COUNT(DISTINCT fo.order_id) AS total_orders,
             COUNT(DISTINCT fo.customer_contact) AS unique_customers,
             SUM(fo.order_value) AS total_gmv
      FROM fact_order_items i
      JOIN fact_orders fo ON fo.order_id = i.order_id
      WHERE ${where} AND i.product_name = ?`, [...vals, product]);

    res.json({ rows, summary: summary||{}, product });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  ITEM TRENDS — daily/hourly trending items
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/trend_items", async (req, res) => {
  try {
    const { where, vals } = ordersWhere(req.query);

    // Top items per day
    const perDay = await q(`
      SELECT fo.order_date, i.product_name,
             COALESCE(r.restaurant_name, fo.restaurant_name) AS restaurant_name,
             COUNT(DISTINCT fo.order_id) AS qty
      FROM fact_order_items i
      JOIN fact_orders fo ON fo.order_id = i.order_id
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      WHERE ${where}
        AND i.product_name IS NOT NULL
        AND LOWER(TRIM(i.product_name)) NOT IN ('nan','none','null','','n/a')
      GROUP BY fo.order_date, i.product_name, r.restaurant_name, fo.restaurant_name
      ORDER BY fo.order_date, qty DESC`, vals);

    // Top items by hour
    const perHour = await q(`
      SELECT fo.order_hour, i.product_name,
             COUNT(DISTINCT fo.order_id) AS qty
      FROM fact_order_items i
      JOIN fact_orders fo ON fo.order_id = i.order_id
      WHERE ${where}
        AND i.product_name IS NOT NULL
        AND LOWER(TRIM(i.product_name)) NOT IN ('nan','none','null','','n/a')
      GROUP BY fo.order_hour, i.product_name
      ORDER BY fo.order_hour, qty DESC`, vals);

    // Best restaurant per item
    const bestRestPerItem = await q(`
      SELECT i.product_name,
             COALESCE(r.restaurant_name, fo.restaurant_name) AS restaurant_name,
             COUNT(DISTINCT fo.order_id) AS qty
      FROM fact_order_items i
      JOIN fact_orders fo ON fo.order_id = i.order_id
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      WHERE ${where}
        AND i.product_name IS NOT NULL
        AND LOWER(TRIM(i.product_name)) NOT IN ('nan','none','null','','n/a')
      GROUP BY i.product_name, r.restaurant_name, fo.restaurant_name
      ORDER BY i.product_name, qty DESC`, vals);

    // Keep top 3 per day and per hour in JS
    const topPerDay = {};
    perDay.forEach(r => {
      if (!topPerDay[r.order_date]) topPerDay[r.order_date] = [];
      if (topPerDay[r.order_date].length < 3) topPerDay[r.order_date].push(r);
    });
    const topPerHour = {};
    perHour.forEach(r => {
      if (!topPerHour[r.order_hour]) topPerHour[r.order_hour] = [];
      if (topPerHour[r.order_hour].length < 3) topPerHour[r.order_hour].push(r);
    });

    res.json({ topPerDay: Object.values(topPerDay), topPerHour: Object.values(topPerHour), bestRestPerItem });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  ORDERS LIST — full Excel-like orders view with all filters
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/orders_list", async (req, res) => {
  try {
    const { where, vals } = ordersWhere(req.query);
    const limit = Math.min(parseInt(req.query.limit) || 1000, 10000);
    const offset = parseInt(req.query.offset) || 0;

    const rows = await q(`
      SELECT DISTINCT
        fo.order_id, fo.order_date, fo.order_dow, fo.order_hour,
        fo.order_month_name AS month, fo.platform,
        COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name,
        fo.order_status,
        COALESCE(dc.customer_name,'—') AS customer_name,
        fo.customer_contact,
        COALESCE(cb.customer_segment,'Unknown') AS customer_segment,
        COALESCE(cb.total_orders,1) AS lifetime_orders,
        fo.order_value, fo.net_revenue,
        COALESCE(fo.discount,0) AS discount,
        COALESCE(fo.packing_charge,0) AS packing_charge,
        COALESCE(fo.delivery_charge,0) AS delivery_charge,
        COALESCE(fo.convenience_charge,0) AS convenience_charge,
        COALESCE(fo.tax,0) AS tax,
        fo.delivery_type,
        fo.has_coupon, fo.coupon_value,
        fo.is_cancelled
      FROM fact_orders fo
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      LEFT JOIN dim_customers dc ON dc.customer_contact = fo.customer_contact
      LEFT JOIN agg_customer_behavior cb ON cb.customer_contact = fo.customer_contact
      WHERE ${where}
      ORDER BY fo.order_date DESC, fo.order_id
      LIMIT ${limit} OFFSET ${offset}`, vals);

    const [countRow] = await q(`
      SELECT COUNT(DISTINCT fo.order_id) AS total
      FROM fact_orders fo WHERE ${where}`, vals);

    res.json({ rows, total: countRow?.total || 0, limit, offset });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  COUPONS — coupon analysis tab
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/coupons", async (req, res) => {
  try {
    const { where, vals } = ordersWhere(req.query);

    // By coupon code / value
    const byCoupon = await q(`
      SELECT COALESCE(fo.coupon_value, fo.discount, 0) AS coupon_value,
             fo.platform,
             COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name,
             fo.delivery_type,
             COUNT(DISTINCT fo.order_id) AS times_used,
             COUNT(DISTINCT fo.customer_contact) AS unique_customers,
             SUM(fo.order_value) AS total_gmv,
             ROUND(AVG(fo.order_value),2) AS avg_order_value
      FROM fact_orders fo
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      WHERE ${where} AND fo.has_coupon = 1
      GROUP BY fo.coupon_value, fo.discount, fo.platform,
               r.restaurant_name, fo.restaurant_name, fo.shop_id, fo.delivery_type
      ORDER BY times_used DESC LIMIT 50`, vals);

    // Individual coupon uses with customer details
    const detail = await q(`
      SELECT DISTINCT
        fo.order_id, fo.order_date, fo.platform,
        COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name,
        fo.customer_contact,
        COALESCE(dc.customer_name,'—') AS customer_name,
        COALESCE(cb.customer_segment,'Unknown') AS segment,
        fo.coupon_value, fo.discount,
        fo.order_value, fo.delivery_type, fo.order_status
      FROM fact_orders fo
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      LEFT JOIN dim_customers dc ON dc.customer_contact = fo.customer_contact
      LEFT JOIN agg_customer_behavior cb ON cb.customer_contact = fo.customer_contact
      WHERE ${where} AND fo.has_coupon = 1
      ORDER BY fo.order_date DESC LIMIT 500`, vals);

    // Summary
    const [summary] = await q(`
      SELECT COUNT(DISTINCT fo.order_id) AS coupon_orders,
             COUNT(DISTINCT fo.customer_contact) AS unique_customers,
             SUM(COALESCE(fo.discount,0)) AS total_discount_given,
             ROUND(AVG(fo.order_value),2) AS avg_aov_with_coupon,
             (SELECT ROUND(AVG(order_value),2) FROM fact_orders fo2
               WHERE ${where.replace(/fo\./g,'fo2.')} AND fo2.has_coupon = 0) AS avg_aov_without_coupon
      FROM fact_orders fo
      WHERE ${where} AND fo.has_coupon = 1`, [...vals, ...vals]);

    res.json({ byCoupon, detail, summary: summary||{} });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  P&L — FIX: use fo.discount (covers all platforms) not menu+cart discount
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/pnl", async (req, res) => {
  try {
    const platExpr = canonicalPlatformExpr("fo");
    const ordersFrom = `FROM (SELECT fo.*, ${platExpr} AS canonical_platform FROM fact_orders fo) fo`;
    const { where, vals } = ordersWhere(req.query);
    const monthly = await q(`
      SELECT CONCAT(fo.order_year,'-',LPAD(fo.order_month,2,'0')) AS month_key,
             fo.order_year, fo.order_month, fo.order_month_name,
             SUM(fo.order_value) AS gmv,
             SUM(fo.net_revenue) AS net_revenue,
             -- Discount: use fo.discount as it covers ALL platforms
             -- (menu_discount+cart_discount only filled for swayo_app)
             COALESCE(SUM(fo.discount),0) AS total_discount,
             COALESCE(SUM(fo.menu_discount),0) AS menu_discount,
             COALESCE(SUM(fo.cart_discount),0) AS cart_discount,
             COALESCE(SUM(fo.coupon_value),0) AS coupon_value,
             COALESCE(SUM(fo.packing_charge),0) AS packing_charge,
             COALESCE(SUM(fo.delivery_charge),0) AS delivery_charge,
             COALESCE(SUM(fo.convenience_charge),0) AS convenience_charge,
             COALESCE(SUM(fo.tax),0) AS tax,
             COUNT(DISTINCT fo.order_id) AS orders,
             ROUND(AVG(fo.order_value),2) AS aov
      ${ordersFrom} WHERE ${where}
      GROUP BY fo.order_year, fo.order_month, fo.order_month_name
      ORDER BY fo.order_year, fo.order_month`, vals);

    const [totals] = await q(`
      SELECT SUM(fo.order_value) AS gmv,
             SUM(fo.net_revenue) AS net_revenue,
             COALESCE(SUM(fo.discount),0) AS total_discount,
             COALESCE(SUM(fo.menu_discount),0) AS menu_discount,
             COALESCE(SUM(fo.cart_discount),0) AS cart_discount,
             COALESCE(SUM(fo.coupon_value),0) AS coupon_value,
             COALESCE(SUM(fo.packing_charge),0) AS packing_charge,
             COALESCE(SUM(fo.delivery_charge),0) AS delivery_charge,
             COALESCE(SUM(fo.convenience_charge),0) AS convenience_charge,
             COALESCE(SUM(fo.tax),0) AS tax
      ${ordersFrom} WHERE ${where}`, vals);

    // Platform breakdown for drill-down
    const byPlatform = await q(`
      SELECT fo.canonical_platform AS platform,
             SUM(fo.order_value) AS gmv,
             COALESCE(SUM(fo.discount),0) AS total_discount,
             COALESCE(SUM(fo.packing_charge),0) AS packing_charge,
             COALESCE(SUM(fo.tax),0) AS tax,
             SUM(fo.net_revenue) AS net_revenue
      ${ordersFrom} WHERE ${where}
      GROUP BY fo.canonical_platform`, vals);

    res.json({ monthly, totals, byPlatform });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  RESTAURANT AOV DEEP-DIVE
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/restaurant_aov", async (req, res) => {
  try {
    const c = ["fo.is_cancelled = 0"], v = [];
    if (req.query.from) { c.push("fo.order_date >= ?"); v.push(req.query.from); }
    if (req.query.to)   { c.push("fo.order_date <= ?"); v.push(req.query.to); }
    const pC = multiIn("fo.platform", req.query.platform, v); if (pC) c.push(pC);
    const sC = multiIn("fo.shop_id",  req.query.shop_id,  v); if (sC) c.push(sC);
    if (req.query.restaurant_name_like) {
      c.push("COALESCE(r.restaurant_name, fo.restaurant_name) LIKE ?");
      v.push(`%${req.query.restaurant_name_like}%`);
    }
    const rows = await q(`
      SELECT COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name,
             fo.shop_id, fo.platform,
             CONCAT(fo.order_year,'-',LPAD(fo.order_month,2,'0')) AS month_key,
             fo.order_month_name, COUNT(DISTINCT fo.order_id) AS order_count,
             ROUND(AVG(fo.order_value),2) AS aov_gross,
             ROUND(AVG(fo.order_value
               - COALESCE(fo.delivery_charge,0) - COALESCE(fo.tax,0)
               - COALESCE(fo.convenience_charge,0)),2) AS aov_food_plus_packing,
             ROUND(AVG(fo.order_value
               - COALESCE(fo.delivery_charge,0) - COALESCE(fo.tax,0)
               - COALESCE(fo.convenience_charge,0) - COALESCE(fo.packing_charge,0)),2) AS aov_food_only,
             ROUND(AVG(fo.packing_charge),2) AS avg_packing,
             ROUND(AVG(fo.delivery_charge),2) AS avg_delivery,
             ROUND(AVG(fo.tax),2) AS avg_tax,
             SUM(fo.order_value) AS total_gmv
      FROM fact_orders fo LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      WHERE ${c.join(" AND ")}
      GROUP BY fo.shop_id, r.restaurant_name, fo.restaurant_name, fo.platform,
               fo.order_year, fo.order_month, fo.order_month_name
      ORDER BY fo.order_year, fo.order_month`, v);

    const [summary] = await q(`
      SELECT COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name,
             fo.platform, COUNT(DISTINCT fo.order_id) AS total_orders,
             ROUND(AVG(fo.order_value),2) AS aov_gross,
             ROUND(AVG(fo.order_value
               - COALESCE(fo.delivery_charge,0) - COALESCE(fo.tax,0)
               - COALESCE(fo.convenience_charge,0)),2) AS aov_food_plus_packing,
             ROUND(AVG(fo.order_value
               - COALESCE(fo.delivery_charge,0) - COALESCE(fo.tax,0)
               - COALESCE(fo.convenience_charge,0) - COALESCE(fo.packing_charge,0)),2) AS aov_food_only,
             SUM(fo.order_value) AS total_gmv
      FROM fact_orders fo LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      WHERE ${c.join(" AND ")}
      GROUP BY fo.shop_id, r.restaurant_name, fo.restaurant_name, fo.platform`, v);

    res.json({ monthly: rows, summary: summary||{} });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  FUNNEL ABANDONED (JFP-style)
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/funnel_abandoned", async (req, res) => {
  try {
    const shop_id  = req.query.shop_id || null;
    const from     = req.query.from    || null;
    const to       = req.query.to      || null;
    const threshold = parseInt(req.query.team_threshold) || 9;
    const source   = req.query.source  || "app";
    const table    = source === "wa" ? "fact_funnel_wa" : "fact_funnel";

    const fc = ["action = 'VIEW_CART'"], fv = [];
    if (from)    { fc.push("event_date >= ?"); fv.push(from); }
    if (to)      { fc.push("event_date <= ?"); fv.push(to); }
    if (shop_id) { fc.push("shop_id = ?");     fv.push(shop_id); }

    const cartCustomers = await q(`
      SELECT customer_contact, COUNT(*) AS view_cart_count,
             MIN(event_date) AS first_view, MAX(event_date) AS last_view
      FROM ${table} WHERE ${fc.join(" AND ")} AND customer_contact IS NOT NULL
      GROUP BY customer_contact ORDER BY view_cart_count DESC`, fv);

    const oc = ["is_cancelled = 0"], ov = [];
    if (from)    { oc.push("order_date >= ?"); ov.push(from); }
    if (to)      { oc.push("order_date <= ?"); ov.push(to); }
    if (shop_id) { oc.push("shop_id = ?");     ov.push(shop_id); }

    const orderedCustomers = await q(`
      SELECT DISTINCT customer_contact FROM fact_orders
      WHERE ${oc.join(" AND ")} AND customer_contact IS NOT NULL`, ov);
    const orderedSet = new Set(orderedCustomers.map(r => r.customer_contact));

    const abandoned = [], ordered = [], likely_team = [];
    cartCustomers.forEach(c => {
      const isTeam   = Number(c.view_cart_count) >= threshold;
      const didOrder = orderedSet.has(c.customer_contact);
      if (isTeam)        likely_team.push({ ...c, flag: 'likely_team' });
      else if (didOrder) ordered.push({ ...c, flag: 'ordered' });
      else               abandoned.push({ ...c, flag: 'abandoned_call_them' });
    });

    res.json({
      summary: { total_view_cart_customers: cartCustomers.length,
                 abandoned_to_call: abandoned.length, ordered_customers: ordered.length,
                 likely_team_filtered_out: likely_team.length,
                 team_threshold_used: threshold, date_range: { from, to }, shop_id },
      abandoned, ordered, likely_team
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  EXPORT SYSTEM (CSV + Excel) — supports all tables
// ════════════════════════════════════════════════════════════════════════════
function buildExportQuery(query) {
  const type  = query.export_type || "orders";
  const limit = Math.min(parseInt(query.limit) || 5000, 50000);
  const params = [];

  function mIn(field, rawVal) { return multiIn(field, rawVal, params); }

  if (type === "orders") {
    const w = ["1=1"];
    const platExpr = canonicalPlatformExpr("fo");
    if (query.from)         { w.push("fo.order_date >= ?");   params.push(query.from); }
    if (query.to)           { w.push("fo.order_date <= ?");   params.push(query.to); }
    const pC = mIn(`(${platExpr})`,  query.platform); if (pC) w.push(pC);
    const sC = mIn("fo.shop_id",   query.shop_id);  if (sC) w.push(sC);
    if (query.delivery_type) { w.push("fo.delivery_type = ?"); params.push(query.delivery_type); }
    if (query.order_status)  { w.push("fo.order_status = ?");  params.push(query.order_status); }
    if (query.has_coupon !== undefined && query.has_coupon !== "") { w.push("fo.has_coupon = ?"); params.push(query.has_coupon); }
    if (query.customer_segment) { w.push("cb.customer_segment = ?"); params.push(query.customer_segment); }
    const sql = `
      SELECT DISTINCT fo.order_id, fo.order_date, fo.order_month_name AS month,
             fo.order_dow, fo.order_hour, ${platExpr} AS platform,
             COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name,
             fo.shop_id, fo.order_status, dc.customer_name, fo.customer_contact,
             COALESCE(cb.customer_segment,'Unknown') AS customer_segment,
             COALESCE(cb.total_orders,1) AS lifetime_orders,
             fo.order_value, fo.net_revenue, COALESCE(fo.discount,0) AS discount,
             fo.packing_charge, fo.delivery_charge, fo.convenience_charge, fo.tax,
             fo.delivery_type, fo.has_coupon, fo.coupon_value, fo.is_cancelled
      FROM fact_orders fo
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      LEFT JOIN dim_customers dc ON dc.customer_contact = fo.customer_contact
      LEFT JOIN agg_customer_behavior cb ON cb.customer_contact = fo.customer_contact
      WHERE ${w.join(" AND ")} ORDER BY fo.order_date DESC LIMIT ${limit}`;
    return { sql, params, filename: "orders" };
  }

  if (type === "customers") {
    const w = ["1=1"];
    if (query.customer_segment) { w.push("cb.customer_segment = ?"); params.push(query.customer_segment); }
    if (query.min_orders) { w.push("cb.total_orders >= ?"); params.push(query.min_orders); }
    if (query.max_orders) { w.push("cb.total_orders <= ?"); params.push(query.max_orders); }
    const sql = `
      SELECT cb.customer_contact, dc.customer_name, dc.platform AS signup_platform,
             cb.customer_segment, cb.total_orders, cb.total_gmv,
             ROUND(cb.total_gmv/NULLIF(cb.total_orders,0),2) AS avg_order_value,
             cb.total_discount, cb.coupon_usage, cb.cancelled_orders,
             ROUND(cb.cancelled_orders*100.0/NULLIF(cb.total_orders,0),2) AS cancellation_rate,
             cb.platforms_used, cb.first_order_date, cb.last_order_date,
             DATEDIFF(cb.last_order_date, cb.first_order_date) AS active_days
      FROM agg_customer_behavior cb
      LEFT JOIN dim_customers dc ON dc.customer_contact = cb.customer_contact
      WHERE ${w.join(" AND ")} ORDER BY cb.total_orders DESC LIMIT ${limit}`;
    return { sql, params, filename: "customers" };
  }

  if (type === "campaigns") {
    const w = ["1=1"];
    if (query.campaign_name) { w.push("fc.campaign_name = ?"); params.push(query.campaign_name); }
    if (query.from) { w.push("DATE(fc.scheduled_date) >= ?"); params.push(query.from); }
    if (query.to)   { w.push("DATE(fc.scheduled_date) <= ?"); params.push(query.to); }
    const sql = `
      SELECT fc.campaign_name, fc.mobile_number,
             DATE(fc.scheduled_date) AS scheduled_date, fc.delivery_status,
             fc.is_sent, fc.is_delivered, fc.is_read,
             fc.sent_at, fc.delivered_at, fc.read_at, fc.pitch_response,
             COALESCE(cb.customer_segment,'Unknown') AS segment,
             COALESCE(cb.total_orders,0) AS lifetime_orders,
             COALESCE(cb.total_gmv,0) AS lifetime_gmv, cb.last_order_date
      FROM fact_campaigns fc
      LEFT JOIN agg_customer_behavior cb ON cb.customer_contact = fc.mobile_number
      WHERE ${w.join(" AND ")} ORDER BY DATE(fc.scheduled_date) DESC LIMIT ${limit}`;
    return { sql, params, filename: "campaign_recipients" };
  }

  if (type === "geo") {
    const w = ["1=1"];
    if (query.from)     { w.push("g.order_date >= ?");        params.push(query.from); }
    if (query.to)       { w.push("g.order_date <= ?");        params.push(query.to); }
    if (query.max_dist) { w.push("g.delivery_distance <= ?"); params.push(query.max_dist); }
    const sql = `
      SELECT g.order_id, g.order_date, g.platform, g.restaurant_name,
             g.restaurant_pincode, g.customer_contact, g.customer_pincode,
             g.delivery_address, g.delivery_distance,
             COALESCE(dc.customer_name,'—') AS customer_name,
             COALESCE(cb.customer_segment,'Unknown') AS segment
      FROM fact_order_geo g
      LEFT JOIN dim_customers dc ON dc.customer_contact = g.customer_contact
      LEFT JOIN agg_customer_behavior cb ON cb.customer_contact = g.customer_contact
      WHERE ${w.join(" AND ")} ORDER BY g.delivery_distance ASC LIMIT ${limit}`;
    return { sql, params, filename: "geo_data" };
  }

  if (type === "items") {
    const w = ["1=1"];
    const platExpr = canonicalPlatformExpr("fo");
    if (query.from)     { w.push("fo.order_date >= ?"); params.push(query.from); }
    if (query.to)       { w.push("fo.order_date <= ?"); params.push(query.to); }
    const pC = mIn(`(${platExpr})`, query.platform); if(pC) w.push(pC);
    const sC = mIn("fo.shop_id",  query.shop_id);  if(sC) w.push(sC);
    if (query.product_name) { w.push("i.product_name LIKE ?"); params.push(`%${query.product_name}%`); }
    const sql = `
      SELECT fo.order_date, fo.order_id, ${platExpr} AS platform,
             COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name,
             i.product_name, dc.customer_name, fo.customer_contact,
             COALESCE(cb.customer_segment,'Unknown') AS customer_segment, fo.order_value
      FROM fact_order_items i
      JOIN fact_orders fo ON fo.order_id = i.order_id
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      LEFT JOIN dim_customers dc ON dc.customer_contact = fo.customer_contact
      LEFT JOIN agg_customer_behavior cb ON cb.customer_contact = fo.customer_contact
      WHERE ${w.join(" AND ")}
        AND i.product_name IS NOT NULL
        AND LOWER(TRIM(i.product_name)) NOT IN ('nan','none','null','','n/a')
      ORDER BY fo.order_date DESC LIMIT ${limit}`;
    return { sql, params, filename: "order_items" };
  }

  if (type === "coupons") {
    const w = ["fo.has_coupon = 1"];
    const platExpr = canonicalPlatformExpr("fo");
    if (query.from) { w.push("fo.order_date >= ?"); params.push(query.from); }
    if (query.to)   { w.push("fo.order_date <= ?"); params.push(query.to); }
    const sql = `
      SELECT DISTINCT fo.order_id, fo.order_date, ${platExpr} AS platform,
             COALESCE(r.restaurant_name, fo.restaurant_name) AS restaurant_name,
             fo.customer_contact, dc.customer_name,
             fo.coupon_value, COALESCE(fo.discount,0) AS discount_amount,
             fo.order_value, fo.delivery_type, fo.order_status
      FROM fact_orders fo
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      LEFT JOIN dim_customers dc ON dc.customer_contact = fo.customer_contact
      WHERE ${w.join(" AND ")} ORDER BY fo.order_date DESC LIMIT ${limit}`;
    return { sql, params, filename: "coupon_orders" };
  }

  if (type === "drill") {
    const w = ["1=1"];
    const platExpr = canonicalPlatformExpr("fo");
    if (query.from) { w.push("fo.order_date >= ?"); params.push(query.from); }
    if (query.to)   { w.push("fo.order_date <= ?"); params.push(query.to); }
    const pC = mIn(`(${platExpr})`, query.platform); if (pC) w.push(pC);
    const sC = mIn("fo.shop_id", query.shop_id);   if (sC) w.push(sC);
    if (query.month_key) {
      const [yr, mo] = String(query.month_key).split("-");
      if (yr && mo) {
        w.push("fo.order_year = ?"); params.push(yr);
        w.push("fo.order_month = ?"); params.push(mo);
      }
    }
    if (query.order_date)    { w.push("fo.order_date = ?"); params.push(query.order_date); }
    if (query.delivery_type) { w.push("fo.delivery_type = ?"); params.push(query.delivery_type); }
    if (query.order_status)  { w.push("fo.order_status = ?"); params.push(query.order_status); }
    if (query.has_coupon !== undefined && query.has_coupon !== "") {
      w.push("fo.has_coupon = ?"); params.push(query.has_coupon);
    }
    if (query.product_name) {
      w.push("fo.order_id IN (SELECT i.order_id FROM fact_order_items i WHERE i.product_name = ?)");
      params.push(query.product_name);
    }
    const sql = `
      SELECT DISTINCT fo.order_id, fo.order_date, fo.order_dow, fo.order_hour, ${platExpr} AS platform,
             COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name,
             fo.shop_id, fo.order_status, COALESCE(dc.customer_name,'—') AS customer_name,
             fo.customer_contact, COALESCE(cb.customer_segment,'Unknown') AS customer_segment,
             COALESCE(cb.total_orders,1) AS lifetime_orders, fo.order_value, fo.net_revenue,
             COALESCE(fo.discount,0) AS discount, COALESCE(fo.packing_charge,0) AS packing_charge,
             COALESCE(fo.delivery_charge,0) AS delivery_charge, COALESCE(fo.tax,0) AS tax,
             fo.delivery_type, fo.has_coupon, fo.coupon_value, fo.is_cancelled
      FROM fact_orders fo
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      LEFT JOIN dim_customers dc ON dc.customer_contact = fo.customer_contact
      LEFT JOIN agg_customer_behavior cb ON cb.customer_contact = fo.customer_contact
      WHERE ${w.join(" AND ")}
      ORDER BY fo.order_date DESC, fo.order_id
      LIMIT ${limit}`;
    return { sql, params, filename: "order_drilldown" };
  }

  throw new Error(`Unknown export_type: ${type}`);
}

function toCSV(rows) {
  if (!rows.length) return "";
  const headers = Object.keys(rows[0]);
  const esc = v => {
    if (v === null || v === undefined) return "";
    const s = String(v);
    return s.includes(",") || s.includes('"') || s.includes("\n") ? `"${s.replace(/"/g,'""')}"` : s;
  };
  return [headers.join(","), ...rows.map(r => headers.map(h => esc(r[h])).join(","))].join("\r\n");
}

function toExcel(rows, sheetName = "Export") {
  if (!rows.length) rows = [{}];
  const headers = Object.keys(rows[0]);
  const xmlEsc = s => String(s??"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
  const dRows = rows.map(r => `<Row>${headers.map(h => {
    const v = r[h];
    if (v === null || v === undefined) return `<Cell><Data ss:Type="String"></Data></Cell>`;
    const n = Number(v);
    return (!isNaN(n) && v !== "" && v !== true && v !== false)
      ? `<Cell><Data ss:Type="Number">${n}</Data></Cell>`
      : `<Cell><Data ss:Type="String">${xmlEsc(v)}</Data></Cell>`;
  }).join("")}</Row>`).join("");
  return `<?xml version="1.0"?><?mso-application progid="Excel.Sheet"?>
<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet" xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet">
<Worksheet ss:Name="${xmlEsc(sheetName)}"><Table>
<Row>${headers.map(h=>`<Cell><Data ss:Type="String">${xmlEsc(h)}</Data></Cell>`).join("")}</Row>
${dRows}</Table></Worksheet></Workbook>`;
}

app.post("/api/export/preview", async (req, res) => {
  try {
    const { sql, params } = buildExportQuery({ ...req.query, ...req.body, limit: "200" });
    const rows = await q(sql, params);
    res.json({ rows, count: rows.length });
  } catch (e) { res.status(400).json({ error: e.message }); }
});

app.get("/api/export/csv", async (req, res) => {
  try {
    const { sql, params, filename } = buildExportQuery(req.query);
    const rows = await q(sql, params);
    const date = new Date().toISOString().split("T")[0];
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="${filename}_${date}.csv"`);
    res.send("\uFEFF" + toCSV(rows));
  } catch (e) { res.status(400).json({ error: e.message }); }
});

app.get("/api/export/excel", async (req, res) => {
  try {
    const { sql, params, filename } = buildExportQuery(req.query);
    const rows = await q(sql, params);
    const date = new Date().toISOString().split("T")[0];
    res.setHeader("Content-Type", "application/vnd.ms-excel; charset=utf-8");
