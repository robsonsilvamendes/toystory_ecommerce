WITH order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

refunds AS (
    SELECT * FROM {{ ref('stg_order_item_refunds') }}
),

order_items_with_refunds AS (
    SELECT
        oi.order_item_id,
        oi.created_at,
        oi.order_id,
        oi.product_id,
        oi.is_primary_item,
        oi.price_usd,
        oi.cogs_usd,
        r.order_item_refund_id,
        r.created_at AS refund_created_at,
        r.refund_amount_usd,
        CASE
            WHEN r.order_item_refund_id IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS is_refunded
    FROM order_items oi
    LEFT JOIN refunds r
        ON oi.order_item_id = r.order_item_id
)

SELECT * FROM order_items_with_refunds
