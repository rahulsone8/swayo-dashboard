/**
 * Swayo Food Analysis — Express API  v6.0
 * ─────────────────────────────────────────────────────────────────────
 * Pipeline v5.1 schema — 13 tables:
 *
 *  dim_restaurants        shop_id, restaurant_name, city, seller_pincode
 *  dim_customers          customer_contact, customer_name, platform
 *  fact_orders            order_id, order_value, net_revenue, order_date,
 *                         order_year, order_month, order_month_name,
 *                         order_week, order_hour, order_dow, platform,
 *                         shop_id, restaurant_name, customer_contact,
 *                         delivery_type, is_cancelled, has_coupon,
 *                         menu_discount, cart_discount, coupon_value,
 *                         packing_charge, delivery_charge, convenience_charge, tax
 *  fact_order_items       order_id, product_name, platform
 *  fact_order_geo         order_id, platform, restaurant_name, restaurant_pincode,
 *                         customer_contact, customer_pincode, delivery_address,
 *                         delivery_distance, order_date
 *  fact_funnel            (Swayo App) action, action_order, shop_id,
 *                         customer_contact, event_date, event_hour, event_dow
 *  fact_funnel_wa         (WhatsApp) timestamp, action, action_order,
 *                         campaign_id, shop_id, shop_code, restaurant_name,
 *                         customer_contact, customer_name, wa_message,
 *                         event_date, event_hour, event_dow, status
 *  fact_campaigns         campaign_name, campaign_id, mobile_number,
 *                         scheduled_date, scheduled_time, scheduled_at,
 *                         sent_at, delivered_at, read_at,
 *                         is_sent, is_delivered, is_read,
 *                         delivery_status, pitch_response, loaded_at
 *  agg_platform_daily     platform, order_date, gmv, net_revenue, order_count,
 *                         avg_order_value, discount_given, coupon_orders,
 *                         cancelled_count, cancellation_rate
 *  agg_restaurant_daily   restaurant_name, shop_id, platform, order_date,
 *                         gmv, net_revenue, order_count, avg_order_value,
 *                         discount_given, cancelled_count, cancellation_rate
 *  agg_funnel_conversion  shop_id, event_date, pdp_views, plp_views,
 *                         cart_views, checkouts, orders_placed,
 *                         plp_to_cart_rate, cart_to_checkout_rate,
 *                         checkout_to_order_rate, overall_conversion_rate
 *  agg_customer_behavior  customer_contact, total_orders, total_gmv,
 *                         avg_order_value, total_discount, first_order_date,
 *                         last_order_date, platforms_used, cancelled_orders,
 *                         coupon_usage, customer_segment, cancellation_rate
 *  agg_campaign_performance campaign_name, campaign_id, scheduled_date,
 *                         total_recipients, sent_count, delivered_count,
 *                         read_count, orders_after_24h,
 *                         sent_rate_pct, delivered_rate_pct,
 *                         read_rate_pct, conversion_rate_pct
 *
 * Platform values (v5.1 pipeline — IMPORTANT, changed from v5.0):
 *   "gf_whatsapp"      ← GFFW prefix (GrabFood WhatsApp ONDC)
 *   "swayo_whatsapp"   ← SWWA prefix
 *   "swayo_app"        ← SWYO prefix
 *   (old: "grabfood_whatsapp" is now "gf_whatsapp")
 *
 * PLATFORM FILTER FIX: The filter now correctly uses fact_orders.platform
 * which stores the exact string from detect_platform() in pipeline.py.
 * The UI dropdowns must match these exact strings.
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

// ── FILTER HELPERS ────────────────────────────────────────────────────────────
function ordersWhere(query) {
  const c = ["1=1"], v = [];
  if (query.from)     { c.push("fo.order_date >= ?"); v.push(query.from); }
  if (query.to)       { c.push("fo.order_date <= ?"); v.push(query.to);   }
  if (query.platform) { c.push("fo.platform = ?");    v.push(query.platform); }
  if (query.shop_id)  { c.push("fo.shop_id = ?");     v.push(query.shop_id);  }
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
//  FILTERS — dropdowns + date range
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/filters", async (_, res) => {
  try {
    const restaurants = await q(`
      SELECT fo.shop_id,
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

    const [dr] = await q(`
      SELECT DATE_FORMAT(MIN(order_date),'%Y-%m-%d') AS min_date,
             DATE_FORMAT(MAX(order_date),'%Y-%m-%d') AS max_date
      FROM fact_orders WHERE order_date IS NOT NULL`);

    // Campaign list
    const campaigns = await q(`
      SELECT DISTINCT campaign_name, campaign_id, scheduled_date
      FROM fact_campaigns ORDER BY scheduled_date DESC`).catch(() => []);

    res.json({ restaurants, platforms: platforms.map(p => p.platform), ...dr, campaigns });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  OVERVIEW — KPIs, trends, platform, DoW, hourly, delivery
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/overview", async (req, res) => {
  try {
    const { where, vals } = ordersWhere(req.query);

    const [kpis] = await q(`
      SELECT
        COALESCE(SUM(fo.order_value),0)                                        AS total_gmv,
        COALESCE(SUM(fo.net_revenue),0)                                        AS total_net_revenue,
        COUNT(*)                                                                AS total_orders,
        ROUND(AVG(fo.order_value),2)                                           AS avg_order_value,
        SUM(fo.is_cancelled)                                                    AS cancelled_orders,
        ROUND(SUM(fo.is_cancelled)*100.0/NULLIF(COUNT(*),0),1)                AS cancel_rate,
        COUNT(DISTINCT fo.customer_contact)                                     AS unique_customers,
        SUM(fo.has_coupon)                                                      AS coupon_orders,
        ROUND(SUM(fo.has_coupon)*100.0/NULLIF(COUNT(*),0),1)                  AS coupon_rate,
        COALESCE(SUM(fo.menu_discount),0)+COALESCE(SUM(fo.cart_discount),0)   AS total_discount,
        COALESCE(SUM(fo.packing_charge),0)                                     AS total_packing,
        COALESCE(SUM(fo.delivery_charge),0)                                    AS total_delivery,
        COALESCE(SUM(fo.tax),0)                                                AS total_tax
      FROM fact_orders fo WHERE ${where}`, vals);

    // Month-on-Month
    const monthly = await q(`
      SELECT fo.order_year, fo.order_month, fo.order_month_name,
             CONCAT(fo.order_year,'-',LPAD(fo.order_month,2,'0')) AS month_key,
             COUNT(*) AS orders, SUM(fo.order_value) AS gmv,
             SUM(fo.net_revenue) AS net_revenue, ROUND(AVG(fo.order_value),2) AS aov,
             COUNT(DISTINCT fo.customer_contact) AS unique_customers,
             SUM(fo.is_cancelled) AS cancelled
      FROM fact_orders fo WHERE ${where}
      GROUP BY fo.order_year, fo.order_month, fo.order_month_name
      ORDER BY fo.order_year, fo.order_month`, vals);

    // Day-on-Day
    const daily = await q(`
      SELECT fo.order_date, COUNT(*) AS orders, SUM(fo.order_value) AS gmv,
             fo.order_dow AS dow
      FROM fact_orders fo WHERE ${where}
      GROUP BY fo.order_date, fo.order_dow ORDER BY fo.order_date`, vals);

    // Per-platform per-day (for stacked/grouped daily chart)
    const dailyByPlatform = await q(`
      SELECT fo.order_date, fo.platform, COUNT(*) AS orders, SUM(fo.order_value) AS gmv
      FROM fact_orders fo WHERE ${where}
      GROUP BY fo.order_date, fo.platform ORDER BY fo.order_date, fo.platform`, vals);

    // Platform share
    const platforms = await q(`
      SELECT fo.platform, COUNT(*) AS orders, SUM(fo.order_value) AS gmv,
             SUM(fo.net_revenue) AS net_revenue, ROUND(AVG(fo.order_value),2) AS aov,
             SUM(fo.is_cancelled) AS cancelled,
             ROUND(SUM(fo.is_cancelled)*100.0/NULLIF(COUNT(*),0),1) AS cancel_rate,
             ROUND(SUM(fo.has_coupon)*100.0/NULLIF(COUNT(*),0),1)   AS coupon_rate
      FROM fact_orders fo WHERE ${where}
      GROUP BY fo.platform ORDER BY orders DESC`, vals);

    // DoW
    const dow = await q(`
      SELECT fo.order_dow, COUNT(*) AS orders, SUM(fo.order_value) AS gmv,
             ROUND(AVG(fo.order_value),2) AS aov
      FROM fact_orders fo WHERE ${where}
      GROUP BY fo.order_dow
      ORDER BY FIELD(fo.order_dow,'Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday')`, vals);

    // Hourly
    const hourly = await q(`
      SELECT fo.order_hour, COUNT(*) AS orders, SUM(fo.order_value) AS gmv
      FROM fact_orders fo WHERE ${where}
      GROUP BY fo.order_hour ORDER BY fo.order_hour`, vals);

    // Delivery mix
    const delivery = await q(`
      SELECT fo.delivery_type, fo.platform, COUNT(*) AS cnt, SUM(fo.order_value) AS gmv
      FROM fact_orders fo WHERE ${where}
      GROUP BY fo.delivery_type, fo.platform ORDER BY cnt DESC`, vals);

    // GMV detail by platform per month (for drill-down)
    const gmvDrilldown = await q(`
      SELECT CONCAT(fo.order_year,'-',LPAD(fo.order_month,2,'0')) AS month_key,
             fo.platform, SUM(fo.order_value) AS gmv, COUNT(*) AS orders
      FROM fact_orders fo WHERE ${where}
      GROUP BY month_key, fo.platform ORDER BY month_key, fo.platform`, vals);

    res.json({ kpis, monthly, daily, dailyByPlatform, platforms, dow, hourly, delivery, gmvDrilldown });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  AOV MONTHLY (food value only)
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/aov_monthly", async (req, res) => {
  try {
    const { where, vals } = ordersWhere(req.query);
    const rows = await q(`
      SELECT fo.order_year, fo.order_month, fo.order_month_name,
             CONCAT(fo.order_year,'-',LPAD(fo.order_month,2,'0')) AS month_key,
             fo.platform, COUNT(*) AS orders,
             ROUND(AVG(fo.order_value),2) AS aov_gross,
             ROUND(AVG(fo.order_value
               - COALESCE(fo.packing_charge,0)
               - COALESCE(fo.delivery_charge,0)
               - COALESCE(fo.convenience_charge,0)
               - COALESCE(fo.tax,0)),2) AS aov_food_only
      FROM fact_orders fo WHERE ${where}
      GROUP BY fo.order_year, fo.order_month, fo.order_month_name, fo.platform
      ORDER BY fo.order_year, fo.order_month, fo.platform`, vals);
    res.json({ rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  RESTAURANTS
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/restaurants", async (req, res) => {
  try {
    const ac = ["1=1"], av = [];
    if (req.query.from)     { ac.push("a.order_date >= ?"); av.push(req.query.from); }
    if (req.query.to)       { ac.push("a.order_date <= ?"); av.push(req.query.to);   }
    if (req.query.platform) { ac.push("a.platform = ?");    av.push(req.query.platform); }
    if (req.query.shop_id)  { ac.push("a.shop_id = ?");     av.push(req.query.shop_id);  }

    const agg = await q(`
      SELECT a.shop_id,
             COALESCE(a.restaurant_name, r.restaurant_name, a.shop_id) AS name,
             r.city, SUM(a.order_count) AS orders, SUM(a.gmv) AS gmv,
             SUM(a.net_revenue) AS net_revenue,
             ROUND(SUM(a.gmv)/NULLIF(SUM(a.order_count),0),2) AS aov,
             SUM(a.cancelled_count) AS cancelled,
             ROUND(SUM(a.cancelled_count)*100.0/NULLIF(SUM(a.order_count),0),1) AS cancel_rate,
             SUM(a.discount_given) AS total_discount
      FROM agg_restaurant_daily a
      LEFT JOIN dim_restaurants r ON r.shop_id = a.shop_id
      WHERE ${ac.join(" AND ")}
      GROUP BY a.shop_id, a.restaurant_name, r.restaurant_name, r.city
      HAVING orders > 0 ORDER BY gmv DESC LIMIT 40`, av);

    if (agg.length > 0) { res.json({ top: agg, source: "agg" }); return; }

    const { where, vals } = ordersWhere(req.query);
    const live = await q(`
      SELECT fo.shop_id,
             COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS name,
             r.city, COUNT(*) AS orders, SUM(fo.order_value) AS gmv,
             SUM(fo.net_revenue) AS net_revenue, ROUND(AVG(fo.order_value),2) AS aov,
             SUM(fo.is_cancelled) AS cancelled,
             ROUND(SUM(fo.is_cancelled)*100.0/NULLIF(COUNT(*),0),1) AS cancel_rate,
             COALESCE(SUM(fo.menu_discount),0)+COALESCE(SUM(fo.cart_discount),0) AS total_discount
      FROM fact_orders fo
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      WHERE ${where} GROUP BY fo.shop_id, r.restaurant_name, fo.restaurant_name, r.city
      HAVING orders > 0 ORDER BY gmv DESC LIMIT 40`, vals);
    res.json({ top: live, source: "live" });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  FUNNEL (Swayo App)
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
      GROUP BY event_hour, action, action_order
      ORDER BY event_hour, action_order`, fv);

    const ac2 = ["1=1"], av2 = [];
    if (req.query.from)    { ac2.push("event_date >= ?"); av2.push(req.query.from); }
    if (req.query.to)      { ac2.push("event_date <= ?"); av2.push(req.query.to);   }
    if (req.query.shop_id) { ac2.push("shop_id = ?");     av2.push(req.query.shop_id); }
    const [conv] = await q(`
      SELECT SUM(pdp_views) AS pdp_views, SUM(plp_views) AS plp_views,
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
//  WA FUNNEL — fact_funnel_wa
//  Actions: VIEW_CATALOG | TOFU | VIEW_CART | CHECKOUT | ORDER | QUERY
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/funnel_wa", async (req, res) => {
  try {
    const fc = ["1=1"], fv = [];
    if (req.query.from)        { fc.push("event_date >= ?");     fv.push(req.query.from); }
    if (req.query.to)          { fc.push("event_date <= ?");     fv.push(req.query.to);   }
    if (req.query.shop_id)     { fc.push("shop_id = ?");         fv.push(req.query.shop_id); }
    if (req.query.campaign_id) { fc.push("campaign_id = ?");     fv.push(req.query.campaign_id); }
    const fWhere = fc.join(" AND ");

    // Stage totals
    const stages = await q(`
      SELECT action_order, action AS stage_name, COUNT(*) AS total
      FROM fact_funnel_wa WHERE ${fWhere}
      GROUP BY action_order, action ORDER BY action_order`, fv);

    // By restaurant
    const byRestaurant = await q(`
      SELECT restaurant_name, shop_id,
             SUM(CASE WHEN action='VIEW_CATALOG' THEN 1 ELSE 0 END) AS view_catalog,
             SUM(CASE WHEN action='VIEW_CART'    THEN 1 ELSE 0 END) AS view_cart,
             SUM(CASE WHEN action='CHECKOUT'     THEN 1 ELSE 0 END) AS checkout,
             SUM(CASE WHEN action='ORDER'        THEN 1 ELSE 0 END) AS orders,
             COUNT(DISTINCT customer_contact)                         AS unique_customers,
             ROUND(SUM(CASE WHEN action='ORDER' THEN 1 ELSE 0 END)*100.0/
               NULLIF(SUM(CASE WHEN action='VIEW_CART' THEN 1 ELSE 0 END),0),1) AS cart_to_order_pct
      FROM fact_funnel_wa WHERE ${fWhere}
      GROUP BY restaurant_name, shop_id ORDER BY orders DESC`, fv);

    // Hourly pattern
    const hourly = await q(`
      SELECT event_hour, action AS stage_name, COUNT(*) AS total
      FROM fact_funnel_wa WHERE ${fWhere}
      GROUP BY event_hour, action ORDER BY event_hour, action_order`, fv);

    // By campaign
    const byCampaign = await q(`
      SELECT campaign_id,
             COUNT(*) AS total_events,
             COUNT(DISTINCT customer_contact) AS unique_customers,
             SUM(CASE WHEN action='ORDER' THEN 1 ELSE 0 END) AS orders
      FROM fact_funnel_wa WHERE ${fWhere}
      GROUP BY campaign_id ORDER BY orders DESC LIMIT 20`, fv);

    // DoW pattern
    const dow = await q(`
      SELECT event_dow, action AS stage_name, COUNT(*) AS total
      FROM fact_funnel_wa WHERE ${fWhere}
      GROUP BY event_dow, action ORDER BY action_order`, fv);

    res.json({ stages, byRestaurant, hourly, byCampaign, dow });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  CAMPAIGNS — fact_campaigns + agg_campaign_performance
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/campaigns", async (req, res) => {
  try {
    const cc = ["1=1"], cv = [];
    if (req.query.campaign_name) { cc.push("campaign_name = ?"); cv.push(req.query.campaign_name); }
    if (req.query.campaign_id)   { cc.push("campaign_id = ?");   cv.push(req.query.campaign_id);   }
    if (req.query.from)          { cc.push("scheduled_date >= ?"); cv.push(req.query.from); }
    if (req.query.to)            { cc.push("scheduled_date <= ?"); cv.push(req.query.to);   }
    const cWhere = cc.join(" AND ");

    // Performance summary from agg
    const perf = await q(`
      SELECT * FROM agg_campaign_performance
      WHERE ${cWhere} ORDER BY scheduled_date DESC`
      .replace("scheduled_date >= ?", "scheduled_date >= ?")
      , cv).catch(() => []);

    // Individual recipient list from fact_campaigns
    const recipients = await q(`
      SELECT fc.campaign_name, fc.campaign_id, fc.mobile_number,
             fc.scheduled_date, fc.delivery_status, fc.is_sent, fc.is_delivered,
             fc.is_read, fc.sent_at, fc.delivered_at, fc.read_at, fc.pitch_response,
             -- post-campaign order info
             COALESCE(cb.total_orders, 0)     AS lifetime_orders,
             COALESCE(cb.customer_segment,'Unknown') AS segment,
             COALESCE(cb.last_order_date, NULL) AS last_order_date,
             COALESCE(cb.total_gmv, 0)        AS lifetime_gmv
      FROM fact_campaigns fc
      LEFT JOIN agg_customer_behavior cb ON cb.customer_contact = fc.mobile_number
      WHERE ${cWhere}
      ORDER BY fc.scheduled_date DESC, fc.delivery_status`, cv);

    // Summary KPIs
    const [summary] = await q(`
      SELECT COUNT(*) AS total_recipients,
             SUM(is_sent) AS sent, SUM(is_delivered) AS delivered,
             SUM(is_read) AS read_count,
             ROUND(SUM(is_sent)*100.0/NULLIF(COUNT(*),0),1)      AS sent_rate,
             ROUND(SUM(is_delivered)*100.0/NULLIF(COUNT(*),0),1) AS delivered_rate,
             ROUND(SUM(is_read)*100.0/NULLIF(COUNT(*),0),1)      AS read_rate
      FROM fact_campaigns WHERE ${cWhere}`, cv);

    // Post-campaign orders: recipients who placed orders after being sent campaign
    // Join campaign mobile_number → fact_orders customer_contact
    const postOrders = await q(`
      SELECT fc.campaign_name, fc.campaign_id, fc.mobile_number,
             fo.order_id, fo.order_date, fo.order_value, fo.platform,
             COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name,
             fo.order_status
      FROM fact_campaigns fc
      JOIN fact_orders fo ON fo.customer_contact = fc.mobile_number
        AND fo.order_date >= fc.scheduled_date
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      WHERE ${cWhere} AND fc.is_delivered = 1
      ORDER BY fc.campaign_id, fo.order_date`, cv);

    // What did post-campaign customers order?
    const postItems = await q(`
      SELECT fc.campaign_name, i.product_name,
             COALESCE(r.restaurant_name, fo.restaurant_name) AS restaurant_name,
             COUNT(*) AS qty
      FROM fact_campaigns fc
      JOIN fact_orders fo ON fo.customer_contact = fc.mobile_number
        AND fo.order_date >= fc.scheduled_date
      JOIN fact_order_items i ON i.order_id = fo.order_id
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      WHERE ${cWhere} AND fc.is_delivered = 1
        AND i.product_name IS NOT NULL
        AND LOWER(TRIM(i.product_name)) NOT IN ('nan','none','null','','n/a')
      GROUP BY fc.campaign_name, i.product_name, r.restaurant_name, fo.restaurant_name
      ORDER BY qty DESC LIMIT 30`, cv);

    res.json({ perf, recipients, summary: summary || {}, postOrders, postItems });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  GEO — fact_order_geo for map & distance analysis
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/geo", async (req, res) => {
  try {
    const gc = ["1=1"], gv = [];
    if (req.query.from)      { gc.push("g.order_date >= ?");       gv.push(req.query.from); }
    if (req.query.to)        { gc.push("g.order_date <= ?");       gv.push(req.query.to);   }
    if (req.query.platform)  { gc.push("g.platform = ?");          gv.push(req.query.platform); }
    if (req.query.shop_id)   { gc.push("g.restaurant_name = (SELECT COALESCE(restaurant_name, ?) FROM dim_restaurants WHERE shop_id = ? LIMIT 1)"); gv.push(req.query.shop_id, req.query.shop_id); }
    if (req.query.max_dist)  { gc.push("g.delivery_distance <= ?"); gv.push(req.query.max_dist); }
    if (req.query.min_dist)  { gc.push("g.delivery_distance >= ?"); gv.push(req.query.min_dist); }
    const gWhere = gc.join(" AND ");

    // Summary by pincode area
    const byCustomerPincode = await q(`
      SELECT g.customer_pincode,
             g.restaurant_name,
             g.platform,
             COUNT(*) AS orders,
             ROUND(AVG(g.delivery_distance),2) AS avg_distance,
             MIN(g.delivery_distance) AS min_distance,
             MAX(g.delivery_distance) AS max_distance
      FROM fact_order_geo g
      WHERE ${gWhere} AND g.customer_pincode IS NOT NULL
      GROUP BY g.customer_pincode, g.restaurant_name, g.platform
      ORDER BY orders DESC LIMIT 100`, gv);

    // By restaurant pincode
    const byRestaurantPincode = await q(`
      SELECT g.restaurant_pincode, g.restaurant_name,
             COUNT(*) AS orders,
             COUNT(DISTINCT g.customer_pincode) AS unique_customer_pincodes,
             ROUND(AVG(g.delivery_distance),2) AS avg_distance
      FROM fact_order_geo g
      WHERE ${gWhere} AND g.restaurant_pincode IS NOT NULL
      GROUP BY g.restaurant_pincode, g.restaurant_name
      ORDER BY orders DESC`, gv);

    // Distance distribution buckets
    const distBuckets = await q(`
      SELECT
        CASE
          WHEN g.delivery_distance < 1    THEN '< 1 km'
          WHEN g.delivery_distance < 2    THEN '1–2 km'
          WHEN g.delivery_distance < 3    THEN '2–3 km'
          WHEN g.delivery_distance < 5    THEN '3–5 km'
          WHEN g.delivery_distance < 10   THEN '5–10 km'
          ELSE '10+ km'
        END AS distance_bucket,
        COUNT(*) AS orders,
        ROUND(AVG(g.delivery_distance),2) AS avg_dist,
        g.platform
      FROM fact_order_geo g
      WHERE ${gWhere} AND g.delivery_distance IS NOT NULL
      GROUP BY distance_bucket, g.platform
      ORDER BY MIN(g.delivery_distance)`, gv);

    // Pickup candidates: customers within 2 km
    const pickupCandidates = await q(`
      SELECT g.customer_contact, g.customer_pincode, g.restaurant_name,
             g.delivery_distance, g.platform, g.order_date,
             COALESCE(dc.customer_name,'—') AS customer_name,
             COALESCE(cb.customer_segment,'Unknown') AS segment,
             COALESCE(cb.total_orders,1) AS lifetime_orders
      FROM fact_order_geo g
      LEFT JOIN dim_customers dc     ON dc.customer_contact = g.customer_contact
      LEFT JOIN agg_customer_behavior cb ON cb.customer_contact = g.customer_contact
      WHERE ${gWhere}
        AND g.delivery_distance IS NOT NULL
        AND g.delivery_distance <= 2
        AND g.customer_contact IS NOT NULL
      ORDER BY g.delivery_distance ASC LIMIT 200`, gv);

    // [NEW] KPIs
    const [geoKpis] = await q(`
      SELECT COUNT(*) AS total_orders,
             COUNT(DISTINCT g.customer_pincode) AS unique_customer_pincodes,
             COUNT(DISTINCT g.restaurant_pincode) AS unique_restaurant_pincodes,
             ROUND(AVG(g.delivery_distance),2) AS avg_distance,
             MIN(g.delivery_distance) AS min_distance,
             MAX(g.delivery_distance) AS max_distance,
             SUM(CASE WHEN g.delivery_distance <= 2 THEN 1 ELSE 0 END) AS within_2km
      FROM fact_order_geo g WHERE ${gWhere}`, gv);

    res.json({ byCustomerPincode, byRestaurantPincode, distBuckets, pickupCandidates, kpis: geoKpis });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  CUSTOMERS
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/customers", async (req, res) => {
  try {
    const segments = await q(`
      SELECT customer_segment, COUNT(*) AS cnt, SUM(total_gmv) AS gmv,
             ROUND(AVG(total_orders),1) AS avg_orders
      FROM agg_customer_behavior
      GROUP BY customer_segment
      ORDER BY FIELD(customer_segment,'VIP','Loyal','Repeat','One-time')`);

    const top = await q(`
      SELECT cb.customer_contact, COALESCE(dc.customer_name,'—') AS customer_name,
             cb.customer_segment, cb.total_orders, cb.total_gmv,
             cb.first_order_date, cb.last_order_date, cb.platforms_used,
             cb.coupon_usage, cb.cancellation_rate
      FROM agg_customer_behavior cb
      LEFT JOIN dim_customers dc ON dc.customer_contact = cb.customer_contact
      ORDER BY cb.total_orders DESC LIMIT 15`);

    const { where, vals } = ordersWhere(req.query);
    const coupon = await q(`
      SELECT fo.has_coupon, COUNT(*) AS orders, ROUND(AVG(fo.order_value),2) AS aov,
             COALESCE(SUM(fo.menu_discount),0)+COALESCE(SUM(fo.cart_discount),0) AS total_discount
      FROM fact_orders fo WHERE ${where} GROUP BY fo.has_coupon`, vals);

    const repeatVsNew = await q(`
      SELECT CASE WHEN total_orders=1 THEN 'new' ELSE 'repeat' END AS buyer_type,
             COUNT(*) AS customers, SUM(total_gmv) AS gmv
      FROM agg_customer_behavior GROUP BY buyer_type`);

    const uniqueByMonth = await q(`
      SELECT fo.order_year, fo.order_month, fo.order_month_name,
             CONCAT(fo.order_year,'-',LPAD(fo.order_month,2,'0')) AS month_key,
             COUNT(DISTINCT fo.customer_contact) AS unique_users,
             COUNT(*) AS orders
      FROM fact_orders fo WHERE ${where}
      GROUP BY fo.order_year, fo.order_month, fo.order_month_name
      ORDER BY fo.order_year, fo.order_month`, vals);

    const ordersPerUserMonth = await q(`
      SELECT fo.order_year, fo.order_month,
             CONCAT(fo.order_year,'-',LPAD(fo.order_month,2,'0')) AS month_key,
             fo.customer_contact, COUNT(*) AS order_count
      FROM fact_orders fo
      WHERE ${where} AND fo.customer_contact IS NOT NULL
      GROUP BY fo.order_year, fo.order_month, fo.customer_contact
      ORDER BY fo.order_year, fo.order_month, order_count DESC`, vals);

    const ordersPerUserTotal = await q(`
      SELECT cb.customer_contact, COALESCE(dc.customer_name,'—') AS customer_name,
             cb.customer_segment, cb.total_orders, cb.total_gmv,
             cb.first_order_date, cb.last_order_date
      FROM agg_customer_behavior cb
      LEFT JOIN dim_customers dc ON dc.customer_contact = cb.customer_contact
      ORDER BY cb.total_orders DESC LIMIT 50`);

    res.json({ segments, top, coupon, repeatVsNew, uniqueByMonth, ordersPerUserMonth, ordersPerUserTotal });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  PRODUCTS
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/products", async (req, res) => {
  try {
    const { where, vals } = ordersWhere(req.query);
    const allItems = await q(`
      SELECT i.platform, i.product_name, fo.shop_id,
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
             fo.shop_id, COUNT(*) AS qty
      FROM fact_order_items i
      JOIN fact_orders fo ON fo.order_id = i.order_id
      LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      WHERE ${where}
        AND i.product_name IS NOT NULL
        AND LOWER(TRIM(i.product_name)) NOT IN ('nan','none','null','','n/a')
      GROUP BY fo.shop_id, r.restaurant_name, fo.restaurant_name
      ORDER BY qty DESC LIMIT 20`, vals);

    const topByRestaurantMonth = allItems; // full data for cross-filter

    res.json({ appItems, gfItems, waItems, byRestaurant, topByRestaurantMonth, totalLineItems,
               monthlyTop10: Object.values(byMonth) });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  P&L
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/pnl", async (req, res) => {
  try {
    const { where, vals } = ordersWhere(req.query);
    const monthly = await q(`
      SELECT CONCAT(fo.order_year,'-',LPAD(fo.order_month,2,'0')) AS month_key,
             fo.order_year, fo.order_month, fo.order_month_name,
             SUM(fo.order_value) AS gmv, SUM(fo.net_revenue) AS net_revenue,
             COALESCE(SUM(fo.menu_discount),0) AS menu_discount,
             COALESCE(SUM(fo.cart_discount),0) AS cart_discount,
             COALESCE(SUM(fo.coupon_value),0) AS coupon_value,
             COALESCE(SUM(fo.packing_charge),0) AS packing_charge,
             COALESCE(SUM(fo.delivery_charge),0) AS delivery_charge,
             COALESCE(SUM(fo.convenience_charge),0) AS convenience_charge,
             COALESCE(SUM(fo.tax),0) AS tax, COUNT(*) AS orders,
             ROUND(AVG(fo.order_value),2) AS aov
      FROM fact_orders fo WHERE ${where}
      GROUP BY fo.order_year, fo.order_month, fo.order_month_name
      ORDER BY fo.order_year, fo.order_month`, vals);

    const [totals] = await q(`
      SELECT SUM(fo.order_value) AS gmv, SUM(fo.net_revenue) AS net_revenue,
             COALESCE(SUM(fo.menu_discount),0) AS menu_discount,
             COALESCE(SUM(fo.cart_discount),0) AS cart_discount,
             COALESCE(SUM(fo.coupon_value),0) AS coupon_value,
             COALESCE(SUM(fo.packing_charge),0) AS packing_charge,
             COALESCE(SUM(fo.delivery_charge),0) AS delivery_charge,
             COALESCE(SUM(fo.convenience_charge),0) AS convenience_charge,
             COALESCE(SUM(fo.tax),0) AS tax
      FROM fact_orders fo WHERE ${where}`, vals);

    res.json({ monthly, totals });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  RESTAURANT AOV DEEP-DIVE
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/restaurant_aov", async (req, res) => {
  try {
    const c = ["fo.is_cancelled = 0"], v = [];
    if (req.query.from)                 { c.push("fo.order_date >= ?"); v.push(req.query.from); }
    if (req.query.to)                   { c.push("fo.order_date <= ?"); v.push(req.query.to);   }
    if (req.query.platform)             { c.push("fo.platform = ?");    v.push(req.query.platform); }
    if (req.query.shop_id)              { c.push("fo.shop_id = ?");     v.push(req.query.shop_id); }
    if (req.query.restaurant_name_like) {
      c.push("COALESCE(r.restaurant_name, fo.restaurant_name) LIKE ?");
      v.push(`%${req.query.restaurant_name_like}%`);
    }
    const rows = await q(`
      SELECT COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name,
             fo.shop_id, fo.platform,
             CONCAT(fo.order_year,'-',LPAD(fo.order_month,2,'0')) AS month_key,
             fo.order_month_name, COUNT(*) AS order_count,
             ROUND(AVG(fo.order_value),2) AS aov_gross,
             ROUND(AVG(fo.order_value - COALESCE(fo.delivery_charge,0)
               - COALESCE(fo.tax,0) - COALESCE(fo.convenience_charge,0)),2) AS aov_food_plus_packing,
             ROUND(AVG(fo.order_value - COALESCE(fo.delivery_charge,0)
               - COALESCE(fo.tax,0) - COALESCE(fo.convenience_charge,0)
               - COALESCE(fo.packing_charge,0)),2) AS aov_food_only,
             ROUND(AVG(fo.packing_charge),2) AS avg_packing,
             ROUND(AVG(fo.delivery_charge),2) AS avg_delivery,
             ROUND(AVG(fo.tax),2) AS avg_tax, SUM(fo.order_value) AS total_gmv
      FROM fact_orders fo LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      WHERE ${c.join(" AND ")}
      GROUP BY fo.shop_id, r.restaurant_name, fo.restaurant_name, fo.platform,
               fo.order_year, fo.order_month, fo.order_month_name
      ORDER BY fo.order_year, fo.order_month`, v);

    const [summary] = await q(`
      SELECT COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name,
             fo.platform, COUNT(*) AS total_orders,
             ROUND(AVG(fo.order_value),2) AS aov_gross,
             ROUND(AVG(fo.order_value - COALESCE(fo.delivery_charge,0)
               - COALESCE(fo.tax,0) - COALESCE(fo.convenience_charge,0)),2) AS aov_food_plus_packing,
             ROUND(AVG(fo.order_value - COALESCE(fo.delivery_charge,0)
               - COALESCE(fo.tax,0) - COALESCE(fo.convenience_charge,0)
               - COALESCE(fo.packing_charge,0)),2) AS aov_food_only,
             SUM(fo.order_value) AS total_gmv
      FROM fact_orders fo LEFT JOIN dim_restaurants r ON r.shop_id = fo.shop_id
      WHERE ${c.join(" AND ")}
      GROUP BY fo.shop_id, r.restaurant_name, fo.restaurant_name, fo.platform`, v);

    res.json({ monthly: rows, summary: summary || {} });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  ONE-TIME USERS
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/onetime_users_swayo", async (req, res) => {
  try {
    const { where, vals } = ordersWhere(req.query);
    const rows = await q(`
      SELECT fo.customer_contact AS whatsapp_number,
             COALESCE(dc.customer_name,'—') AS customer_name,
             fo.order_date, fo.order_id,
             COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name,
             fo.order_value, fo.order_status, cb.total_orders AS lifetime_orders,
             cb.first_order_date, cb.last_order_date, fo.delivery_type
      FROM fact_orders fo
      LEFT JOIN dim_restaurants r      ON r.shop_id           = fo.shop_id
      LEFT JOIN dim_customers dc       ON dc.customer_contact = fo.customer_contact
      LEFT JOIN agg_customer_behavior cb ON cb.customer_contact = fo.customer_contact
      WHERE ${where} AND fo.customer_contact IS NOT NULL
        AND fo.is_cancelled = 0 AND cb.total_orders = 1
      ORDER BY fo.order_date DESC`, vals);
    res.json({ count: rows.length, users: rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  FUNNEL ABANDONED (JFP-style analysis)
// ════════════════════════════════════════════════════════════════════════════
app.get("/api/funnel_abandoned", async (req, res) => {
  try {
    const shop_id   = req.query.shop_id   || null;
    const from      = req.query.from      || null;
    const to        = req.query.to        || null;
    const threshold = parseInt(req.query.team_threshold) || 9;
    const source    = req.query.source    || "app"; // "app" | "wa"
    const table     = source === "wa" ? "fact_funnel_wa" : "fact_funnel";
    const cartAction = source === "wa" ? "VIEW_CART" : "VIEW_CART";

    const fc = [`action = '${cartAction}'`], fv = [];
    if (from)    { fc.push("event_date >= ?"); fv.push(from); }
    if (to)      { fc.push("event_date <= ?"); fv.push(to);   }
    if (shop_id) { fc.push("shop_id = ?");     fv.push(shop_id); }

    const cartCustomers = await q(`
      SELECT customer_contact, COUNT(*) AS view_cart_count,
             MIN(event_date) AS first_view, MAX(event_date) AS last_view
      FROM ${table} WHERE ${fc.join(" AND ")} AND customer_contact IS NOT NULL
      GROUP BY customer_contact ORDER BY view_cart_count DESC`, fv);

    const oc = ["is_cancelled = 0"], ov = [];
    if (from)    { oc.push("order_date >= ?"); ov.push(from); }
    if (to)      { oc.push("order_date <= ?"); ov.push(to);   }
    if (shop_id) { oc.push("shop_id = ?");     ov.push(shop_id); }

    const orderedCustomers = await q(`
      SELECT DISTINCT customer_contact FROM fact_orders
      WHERE ${oc.join(" AND ")} AND customer_contact IS NOT NULL`, ov);
    const orderedSet = new Set(orderedCustomers.map(r => r.customer_contact));

    const abandoned = [], ordered = [], likely_team = [];
    cartCustomers.forEach(c => {
      const isTeam = Number(c.view_cart_count) >= threshold;
      const didOrder = orderedSet.has(c.customer_contact);
      if (isTeam)       likely_team.push({ ...c, flag: 'likely_team' });
      else if (didOrder) ordered.push({ ...c, flag: 'ordered' });
      else               abandoned.push({ ...c, flag: 'abandoned_call_them' });
    });

    res.json({
      summary: { total_view_cart_customers: cartCustomers.length,
                 abandoned_to_call: abandoned.length, ordered_customers: ordered.length,
                 likely_team_filtered_out: likely_team.length, team_threshold_used: threshold,
                 date_range: { from, to }, shop_id },
      abandoned, ordered, likely_team
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ════════════════════════════════════════════════════════════════════════════
//  EXPORT SYSTEM (CSV + Excel)
// ════════════════════════════════════════════════════════════════════════════
function buildExportQuery(query) {
  const type  = query.export_type || "orders";
  const limit = Math.min(parseInt(query.limit) || 5000, 50000);
  const params = [];

  if (type === "orders") {
    const w = ["1=1"];
    if (query.from)         { w.push("fo.order_date >= ?");    params.push(query.from); }
    if (query.to)           { w.push("fo.order_date <= ?");    params.push(query.to);   }
    if (query.platform)     { w.push("fo.platform = ?");       params.push(query.platform); }
    if (query.shop_id)      { w.push("fo.shop_id = ?");        params.push(query.shop_id); }
    if (query.order_status) { w.push("fo.order_status = ?");   params.push(query.order_status); }
    if (query.has_coupon !== undefined && query.has_coupon !== "")
                            { w.push("fo.has_coupon = ?");     params.push(query.has_coupon); }
    if (query.customer_segment) { w.push("cb.customer_segment = ?"); params.push(query.customer_segment); }
    if (query.min_orders)   { w.push("cb.total_orders >= ?");  params.push(query.min_orders); }
    if (query.max_orders)   { w.push("cb.total_orders <= ?");  params.push(query.max_orders); }
    const sql = `
      SELECT fo.order_id, fo.order_date, fo.order_month_name AS month,
             fo.order_dow AS day_of_week, fo.order_hour, fo.platform,
             COALESCE(r.restaurant_name, fo.restaurant_name, fo.shop_id) AS restaurant_name,
             fo.shop_id, fo.order_status, dc.customer_name, fo.customer_contact,
             COALESCE(cb.customer_segment,'Unknown') AS customer_segment,
             COALESCE(cb.total_orders,1) AS lifetime_orders,
             fo.order_value, fo.net_revenue, fo.discount,
             fo.menu_discount, fo.cart_discount, fo.coupon_value,
             fo.has_coupon, fo.packing_charge, fo.delivery_charge,
             fo.convenience_charge, fo.tax, fo.delivery_type, fo.is_cancelled
      FROM fact_orders fo
      LEFT JOIN dim_restaurants r      ON r.shop_id           = fo.shop_id
      LEFT JOIN dim_customers dc       ON dc.customer_contact = fo.customer_contact
      LEFT JOIN agg_customer_behavior cb ON cb.customer_contact = fo.customer_contact
      WHERE ${w.join(" AND ")} ORDER BY fo.order_date DESC LIMIT ${limit}`;
    return { sql, params, filename: "orders" };
  }

  if (type === "customers") {
    const w = ["1=1"];
    if (query.customer_segment) { w.push("cb.customer_segment = ?"); params.push(query.customer_segment); }
    if (query.min_orders)       { w.push("cb.total_orders >= ?");    params.push(query.min_orders); }
    if (query.max_orders)       { w.push("cb.total_orders <= ?");    params.push(query.max_orders); }
    if (query.platform)         { w.push("dc.platform = ?");         params.push(query.platform); }
    const sql = `
      SELECT cb.customer_contact, dc.customer_name, dc.platform AS signup_platform,
             cb.customer_segment, cb.total_orders, cb.total_gmv,
             ROUND(cb.avg_order_value,2) AS avg_order_value,
             cb.total_discount, cb.coupon_usage, cb.cancelled_orders,
             ROUND(cb.cancellation_rate,2) AS cancellation_rate,
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
    if (query.from)          { w.push("fc.scheduled_date >= ?"); params.push(query.from); }
    if (query.to)            { w.push("fc.scheduled_date <= ?"); params.push(query.to);   }
    const sql = `
      SELECT fc.campaign_name, fc.campaign_id, fc.mobile_number,
             fc.scheduled_date, fc.delivery_status,
             fc.is_sent, fc.is_delivered, fc.is_read,
             fc.sent_at, fc.delivered_at, fc.read_at, fc.pitch_response,
             COALESCE(cb.customer_segment,'Unknown') AS segment,
             COALESCE(cb.total_orders,0) AS lifetime_orders,
             COALESCE(cb.total_gmv,0) AS lifetime_gmv,
             cb.last_order_date
      FROM fact_campaigns fc
      LEFT JOIN agg_customer_behavior cb ON cb.customer_contact = fc.mobile_number
      WHERE ${w.join(" AND ")} ORDER BY fc.scheduled_date DESC, fc.delivery_status
      LIMIT ${limit}`;
    return { sql, params, filename: "campaign_recipients" };
  }

  if (type === "geo") {
    const w = ["1=1"];
    if (query.from)     { w.push("g.order_date >= ?");        params.push(query.from); }
    if (query.to)       { w.push("g.order_date <= ?");        params.push(query.to);   }
    if (query.platform) { w.push("g.platform = ?");           params.push(query.platform); }
    if (query.max_dist) { w.push("g.delivery_distance <= ?"); params.push(query.max_dist); }
    const sql = `
      SELECT g.order_id, g.order_date, g.platform, g.restaurant_name,
             g.restaurant_pincode, g.customer_contact, g.customer_pincode,
             g.delivery_address, g.delivery_distance,
             COALESCE(dc.customer_name,'—') AS customer_name,
             COALESCE(cb.customer_segment,'Unknown') AS segment
      FROM fact_order_geo g
      LEFT JOIN dim_customers dc     ON dc.customer_contact = g.customer_contact
      LEFT JOIN agg_customer_behavior cb ON cb.customer_contact = g.customer_contact
      WHERE ${w.join(" AND ")} ORDER BY g.delivery_distance ASC LIMIT ${limit}`;
    return { sql, params, filename: "geo_data" };
  }

  if (type === "items") {
    const w = ["1=1"];
    if (query.from)         { w.push("fo.order_date >= ?"); params.push(query.from); }
    if (query.to)           { w.push("fo.order_date <= ?"); params.push(query.to);   }
    if (query.platform)     { w.push("fo.platform = ?");    params.push(query.platform); }
    if (query.shop_id)      { w.push("fo.shop_id = ?");     params.push(query.shop_id); }
    if (query.product_name) { w.push("i.product_name LIKE ?"); params.push(`%${query.product_name}%`); }
    const sql = `
      SELECT fo.order_date, fo.order_id, fo.platform,
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

  throw new Error(`Unknown export_type: ${type}`);
}

function toCSV(rows) {
  if (!rows.length) return "";
  const headers = Object.keys(rows[0]);
  const escape  = v => {
    if (v === null || v === undefined) return "";
    const s = String(v);
    return s.includes(",") || s.includes('"') || s.includes("\n")
      ? `"${s.replace(/"/g,'""')}"` : s;
  };
  return [headers.join(","), ...rows.map(r => headers.map(h => escape(r[h])).join(","))].join("\r\n");
}

function toExcel(rows, sheetName = "Export") {
  if (!rows.length) rows = [{}];
  const headers = Object.keys(rows[0]);
  const xmlEsc = s => String(s??"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
  const dataRows = rows.map(r =>
    `<Row>${headers.map(h => {
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
${dataRows}</Table></Worksheet></Workbook>`;
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
    const rows  = await q(sql, params);
    const date  = new Date().toISOString().split("T")[0];
    res.setHeader("Content-Type", "application/vnd.ms-excel; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="${filename}_${date}.xls"`);
    res.send(toExcel(rows, filename));
  } catch (e) { res.status(400).json({ error: e.message }); }
});

app.listen(PORT, () => console.log(`✅  Swayo Food Analysis API v6 → http://localhost:${PORT}`));