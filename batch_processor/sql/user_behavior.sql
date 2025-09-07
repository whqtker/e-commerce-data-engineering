-- 행동 가중치: purchase(4), add_to_cart(3), click(2), view(1)
WITH behavior_with_weight AS (
    SELECT
        user_id,
        product_id,
        CASE
            WHEN action_type = 'purchase' THEN 4.0
            WHEN action_type = 'add_to_cart' THEN 3.0
            WHEN action_type = 'click' THEN 2.0
            ELSE 1.0
        END AS weight
    FROM
        user_behavior_events
)

SELECT
    user_id,
    product_id,
    SUM(weight) AS interaction_strength
FROM
    behavior_with_weight
GROUP BY
    user_id,
    product_id
HAVING
    SUM(weight) > 0;
