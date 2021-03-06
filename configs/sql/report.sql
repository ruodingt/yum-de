WITH t1 AS (
    SELECT line_items.name as item, SUM(line_items.quantity) as quantity, orders.venue_id, DATE_TRUNC('HOUR', line_items.updated_at) as dth
    FROM line_items
    LEFT JOIN orders
    ON line_items.order_id = orders.id
    WHERE line_items.updated_at >= TIMESTAMP ("2021-05-01T00:00:00.000000Z")
    GROUP BY item, venue_id, dth
    ),
t2 AS (
    SELECT item, quantity, venue_id,
           venues.name as venue_name,
           venues.country as venue_country,
           venues.timezone as venue_timezone,
           dth
    FROM t1
    LEFT JOIN venues
    ON t1.venue_id = venues.id
    )
SELECT * FROM t2
