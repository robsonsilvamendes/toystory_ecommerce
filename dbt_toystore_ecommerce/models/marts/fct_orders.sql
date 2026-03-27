WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

order_items_refunds AS (
    SELECT * FROM {{ ref('int_order_items_with_refunds') }}
),

sessions AS (
    SELECT * FROM {{ ref('stg_website_sessions') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

order_refunds_agg AS (
    SELECT
        order_id,
        SUM(refund_amount_usd) AS total_refund_usd,
        SUM(CASE WHEN is_refunded THEN 1 ELSE 0 END) AS total_items_refunded
    FROM order_items_refunds
    GROUP BY order_id
),

final AS (
    SELECT
        o.order_id,
        o.created_at,
        o.website_session_id,
        o.user_id,
        o.primary_product_id,
        p.product_name AS primary_product_name,
        o.items_purchased,

        -- Financeiro
        o.price_usd,
        o.cogs_usd,
        o.price_usd - o.cogs_usd AS profit_usd,
        COALESCE(ora.total_refund_usd, 0) AS total_refund_usd,
        o.price_usd - COALESCE(ora.total_refund_usd, 0) AS net_revenue_usd,

        -- Atribuição de tráfego
        s.utm_source,
        s.utm_campaign,
        s.utm_content,
        s.device_type,
        s.http_referer,
        s.is_repeat_session,

        -- Flags
        CASE
            WHEN COALESCE(ora.total_items_refunded, 0) > 0 THEN TRUE
            ELSE FALSE
        END AS is_refunded,
        COALESCE(ora.total_items_refunded, 0) AS total_items_refunded

    FROM orders o
    LEFT JOIN order_refunds_agg ora
        ON o.order_id = ora.order_id
    LEFT JOIN sessions s
        ON o.website_session_id = s.website_session_id
    LEFT JOIN products p
        ON o.primary_product_id = p.product_id
)

SELECT * FROM final
