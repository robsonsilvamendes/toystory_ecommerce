SELECT
    *
FROM {{ source('toystore_ecommerce', 'orders') }}