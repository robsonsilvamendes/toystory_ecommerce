SELECT
    *
FROM {{ source('toystore_ecommerce', 'order_item_refunds') }}