WITH sessions AS (
    SELECT * FROM {{ ref('stg_website_sessions') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

first_session AS (
    SELECT
        user_id,
        MIN(created_at) AS first_session_at,
        MIN(website_session_id) AS first_session_id
    FROM sessions
    GROUP BY user_id
),

first_session_details AS (
    SELECT
        fs.user_id,
        fs.first_session_at,
        s.utm_source AS first_utm_source,
        s.utm_campaign AS first_utm_campaign,
        s.utm_content AS first_utm_content,
        s.device_type AS first_device_type,
        s.http_referer AS first_http_referer
    FROM first_session fs
    INNER JOIN sessions s
        ON fs.first_session_id = s.website_session_id
),

user_sessions_agg AS (
    SELECT
        user_id,
        COUNT(*) AS total_sessions
    FROM sessions
    GROUP BY user_id
),

user_orders_agg AS (
    SELECT
        user_id,
        COUNT(*) AS total_orders,
        SUM(price_usd) AS total_revenue_usd,
        MIN(created_at) AS first_order_at
    FROM orders
    GROUP BY user_id
),

final AS (
    SELECT
        fsd.user_id,
        fsd.first_session_at,
        uoa.first_order_at,

        -- Métricas de sessão
        usa.total_sessions,

        -- Métricas de pedidos
        COALESCE(uoa.total_orders, 0) AS total_orders,
        COALESCE(uoa.total_revenue_usd, 0) AS total_revenue_usd,

        -- Primeira atribuição
        fsd.first_utm_source,
        fsd.first_utm_campaign,
        fsd.first_utm_content,
        fsd.first_device_type,
        fsd.first_http_referer,

        -- Flags
        CASE
            WHEN COALESCE(uoa.total_orders, 0) > 1 THEN TRUE
            ELSE FALSE
        END AS is_repeat_customer

    FROM first_session_details fsd
    INNER JOIN user_sessions_agg usa
        ON fsd.user_id = usa.user_id
    LEFT JOIN user_orders_agg uoa
        ON fsd.user_id = uoa.user_id
)

SELECT * FROM final
