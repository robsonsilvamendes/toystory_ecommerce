WITH sessions AS (
    SELECT * FROM {{ ref('stg_website_sessions') }}
),

session_pageviews AS (
    SELECT * FROM {{ ref('int_session_pageviews') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

final AS (
    SELECT
        -- Sessão
        s.website_session_id,
        s.created_at,
        s.user_id,
        s.is_repeat_session,

        -- Tráfego
        s.utm_source,
        s.utm_campaign,
        s.utm_content,
        s.device_type,
        s.http_referer,

        -- Engajamento
        COALESCE(sp.total_pageviews, 0) AS total_pageviews,
        sp.landing_page,
        sp.exit_page,

        -- Conversão
        o.order_id,
        CASE
            WHEN o.order_id IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS is_converted,
        o.price_usd AS order_price_usd,
        o.items_purchased AS order_items_purchased

    FROM sessions s
    LEFT JOIN session_pageviews sp
        ON s.website_session_id = sp.website_session_id
    LEFT JOIN orders o
        ON s.website_session_id = o.website_session_id
)

SELECT * FROM final
