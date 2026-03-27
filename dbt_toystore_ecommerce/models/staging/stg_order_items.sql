SELECT
    *
FROM {{ source('toystore_ecommerce', 'order_items') }}