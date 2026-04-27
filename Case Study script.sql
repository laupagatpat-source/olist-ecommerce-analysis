SELECT 
    HOUR(order_purchase_timestamp) AS hour_of_day,
    COUNT(*) AS total_orders
FROM olist_orders_dataset
GROUP BY hour_of_day
ORDER BY hour_of_day;


SELECT 
    DAYNAME(order_purchase_timestamp) AS day_of_week,
    DAYOFWEEK(order_purchase_timestamp) AS day_num,
    COUNT(*) AS total_orders
FROM olist_orders_dataset
GROUP BY day_of_week, day_num
ORDER BY day_num;


SELECT 
    c.customer_state,
    COUNT(DISTINCT c.customer_unique_id) AS unique_customers,
    COUNT(o.order_id) AS total_orders,
    ROUND(AVG(p.payment_value), 2) AS avg_order_value
FROM olist_customers_dataset c
JOIN olist_orders_dataset o 
    ON c.customer_id = o.customer_id
JOIN olist_order_payments_dataset p 
    ON o.order_id = p.order_id
GROUP BY c.customer_state
ORDER BY total_orders DESC
LIMIT 10;


SELECT 
    r.review_score,
    ROUND(AVG(DATEDIFF(
        o.order_delivered_customer_date,
        o.order_purchase_timestamp
    )), 2) AS avg_delivery_days,
    COUNT(*) AS total_orders
FROM olist_order_reviews_dataset r
JOIN olist_orders_dataset o 
    ON r.order_id = o.order_id
WHERE o.order_delivered_customer_date IS NOT NULL
GROUP BY r.review_score
ORDER BY r.review_score DESC;


SELECT 
    purchase_frequency,
    COUNT(*) AS num_customers,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM (
    SELECT 
        c.customer_unique_id,
        CASE 
            WHEN COUNT(DISTINCT o.order_id) = 1 THEN 'One-Time Buyer'
            WHEN COUNT(DISTINCT o.order_id) BETWEEN 2 AND 3 THEN 'Occasional Buyer'
            ELSE 'Frequent Buyer'
        END AS purchase_frequency
    FROM olist_customers_dataset c
    JOIN olist_orders_dataset o 
        ON c.customer_id = o.customer_id
    GROUP BY c.customer_unique_id
) buyer_segments
GROUP BY purchase_frequency;


SELECT 
    YEAR(o.order_purchase_timestamp) AS year,
    MONTH(o.order_purchase_timestamp) AS month_num,
    MONTHNAME(o.order_purchase_timestamp) AS month_name,
    ROUND(SUM(oi.price), 2) AS total_sales,
    COUNT(DISTINCT o.order_id) AS total_orders
FROM olist_orders_dataset o
JOIN olist_order_items_dataset oi 
    ON o.order_id = oi.order_id
GROUP BY year, month_num, month_name
ORDER BY year, month_num;


SELECT 
    t.product_category_name_english AS category,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(oi.price), 2) AS total_revenue,
    ROUND(AVG(oi.price), 2) AS avg_price
FROM olist_order_items_dataset oi
JOIN olist_orders_dataset o 
    ON oi.order_id = o.order_id
JOIN olist_products_dataset p 
    ON oi.product_id = p.product_id
JOIN product_category_name_translation t 
    ON p.product_category_name = t.product_category_name
GROUP BY category
ORDER BY total_revenue DESC
LIMIT 10;


SELECT 
    t.product_category_name_english AS category,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(oi.price), 2) AS total_revenue,
    ROUND(AVG(oi.price), 2) AS avg_price
FROM olist_order_items_dataset oi
JOIN olist_orders_dataset o 
    ON oi.order_id = o.order_id
JOIN olist_products_dataset p 
    ON oi.product_id = p.product_id
JOIN product_category_name_translation t 
    ON p.product_category_name = t.product_category_name
GROUP BY category
ORDER BY total_revenue ASC
LIMIT 10;


SELECT 
    payment_type,
    COUNT(*) AS total_transactions,
    ROUND(SUM(payment_value), 2) AS total_revenue,
    ROUND(AVG(payment_value), 2) AS avg_transaction_value
FROM olist_order_payments_dataset
GROUP BY payment_type
ORDER BY total_revenue DESC;


SELECT 
    YEAR(o.order_purchase_timestamp) AS year,
    ROUND(SUM(oi.price), 2) AS total_sales,
    ROUND(SUM(oi.price) - LAG(SUM(oi.price)) 
        OVER (ORDER BY YEAR(o.order_purchase_timestamp)), 2) AS yoy_growth,
    ROUND((SUM(oi.price) - LAG(SUM(oi.price)) 
        OVER (ORDER BY YEAR(o.order_purchase_timestamp))) 
        / LAG(SUM(oi.price)) 
        OVER (ORDER BY YEAR(o.order_purchase_timestamp)) * 100, 2) AS growth_percentage
FROM olist_orders_dataset o
JOIN olist_order_items_dataset oi 
    ON o.order_id = oi.order_id
GROUP BY year
ORDER BY year;


SELECT 
    c.customer_state,
    ROUND(AVG(DATEDIFF(
        o.order_delivered_customer_date,
        o.order_purchase_timestamp
    )), 2) AS avg_delivery_days,
    ROUND(AVG(DATEDIFF(
        o.order_estimated_delivery_date,
        o.order_delivered_customer_date
    )), 2) AS avg_days_early_late,
    COUNT(*) AS total_orders
FROM olist_orders_dataset o
JOIN olist_customers_dataset c 
    ON o.customer_id = c.customer_id
WHERE o.order_delivered_customer_date IS NOT NULL
GROUP BY c.customer_state
ORDER BY avg_delivery_days DESC;


SELECT 
    delivery_status,
    COUNT(*) AS total_orders,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM (
    SELECT 
        CASE 
            WHEN order_delivered_customer_date <= order_estimated_delivery_date 
            THEN 'On Time'
            WHEN order_delivered_customer_date > order_estimated_delivery_date 
            THEN 'Late'
            ELSE 'Not Delivered'
        END AS delivery_status
    FROM olist_orders_dataset
    WHERE order_delivered_customer_date IS NOT NULL
) delivery_data
GROUP BY delivery_status;


SELECT 
    CASE 
        WHEN o.order_delivered_customer_date <= o.order_estimated_delivery_date 
        THEN 'On Time'
        WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date 
        THEN 'Late'
    END AS delivery_status,
    ROUND(AVG(r.review_score), 2) AS avg_review_score,
    COUNT(*) AS total_orders
FROM olist_orders_dataset o
JOIN olist_order_reviews_dataset r 
    ON o.order_id = r.order_id
WHERE o.order_delivered_customer_date IS NOT NULL
GROUP BY delivery_status;


SELECT 
    c.customer_state,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(oi.freight_value), 2) AS total_freight_cost,
    ROUND(AVG(oi.freight_value), 2) AS avg_freight_cost,
    ROUND(AVG(DATEDIFF(
        o.order_delivered_customer_date,
        o.order_purchase_timestamp
    )), 2) AS avg_delivery_days
FROM olist_orders_dataset o
JOIN olist_customers_dataset c 
    ON o.customer_id = c.customer_id
JOIN olist_order_items_dataset oi 
    ON o.order_id = oi.order_id
WHERE o.order_delivered_customer_date IS NOT NULL
GROUP BY c.customer_state
ORDER BY total_orders DESC;