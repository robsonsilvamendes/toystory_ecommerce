WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

order_items_refunds AS (
    SELECT * FROM {{ ref('int_order_items_with_refunds') }}
),

product_metrics AS (
    SELECT
        product_id,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(price_usd) AS total_revenue_usd,
        SUM(CASE WHEN is_refunded THEN 1 ELSE 0 END) AS total_refunds,
        ROUND(
            SUM(CASE WHEN is_refunded THEN 1.0 ELSE 0.0 END)
            / NULLIF(COUNT(*), 0) * 100,
            2
        ) AS refund_rate_pct
    FROM order_items_refunds
    GROUP BY product_id
),

final AS (
    SELECT
        p.product_id,
        p.product_name,
        p.created_at,
        COALESCE(pm.total_orders, 0) AS total_orders,
        COALESCE(pm.total_revenue_usd, 0) AS total_revenue_usd,
        COALESCE(pm.total_refunds, 0) AS total_refunds,
        COALESCE(pm.refund_rate_pct, 0) AS refund_rate_pct
    FROM products p
    LEFT JOIN product_metrics pm
        ON p.product_id = pm.product_id
)

SELECT * FROM final
