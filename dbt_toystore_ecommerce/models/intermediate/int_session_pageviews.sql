WITH pageviews AS (
    SELECT * FROM {{ ref('stg_website_pageviews') }}
),

first_last_pageview AS (
    SELECT
        website_session_id,
        MIN(website_pageview_id) AS first_pageview_id,
        MAX(website_pageview_id) AS last_pageview_id,
        COUNT(*) AS total_pageviews
    FROM pageviews
    GROUP BY website_session_id
),

session_pageviews AS (
    SELECT
        flp.website_session_id,
        flp.total_pageviews,
        landing.pageview_url AS landing_page,
        exit_pv.pageview_url AS exit_page
    FROM first_last_pageview flp
    LEFT JOIN pageviews landing
        ON flp.first_pageview_id = landing.website_pageview_id
    LEFT JOIN pageviews exit_pv
        ON flp.last_pageview_id = exit_pv.website_pageview_id
)

SELECT * FROM session_pageviews
