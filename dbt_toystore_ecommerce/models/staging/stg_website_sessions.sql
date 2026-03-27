SELECT
    *
FROM {{ source('toystore_ecommerce', 'website_sessions') }}