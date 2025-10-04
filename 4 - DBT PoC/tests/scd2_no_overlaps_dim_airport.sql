SELECT
    a.airport_code,
    a.valid_from,
    a.valid_to,
    b.valid_from AS overlapping_from,
    b.valid_to AS overlapping_to
FROM {{ ref('dim_airport') }} a
JOIN {{ ref('dim_airport') }} b
    ON a.airport_code = b.airport_code
    AND a.airport_id != b.airport_id
    AND a.valid_from < b.valid_to
    AND b.valid_from < a.valid_to