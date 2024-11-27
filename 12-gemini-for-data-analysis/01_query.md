## Data Exploration with BigQuery

1. How much are we selling monthly? Is it high or low compared to last month?
```
SELECT
  EXTRACT(MONTH FROM oi.Created_at) AS months,
  ROUND(SUM(oi.sale_price * o.num_of_item), 2) AS revenue,
  COUNT(DISTINCT oi.order_id) AS order_count,
  COUNT(DISTINCT oi.user_id) AS customers_purchased
FROM bigquery-public-data.thelook_ecommerce.order_items oi
INNER JOIN `bigquery-public-data.thelook_ecommerce.orders` AS o 
ON oi.order_id = o.order_id
WHERE oi.status NOT IN ('Cancelled', 'Returned')
GROUP BY months
ORDER BY revenue DESC;
```

2. Who are our customers? Which country do we have major customers coming from? 
Which Gender and Age group brought in the most profit?
2.1 Customers by Country
```
WITH customers AS (
  SELECT
    DISTINCT oi.user_id,
    SUM(CASE WHEN u.gender = 'M' THEN 1 ELSE null END) AS male,
    SUM(CASE WHEN u.gender = 'F' THEN 1 ELSE null END) AS female,
    u.country AS country
    FROM bigquery-public-data.thelook_ecommerce.order_items AS oi
    INNER JOIN bigquery-public-data.thelook_ecommerce.users AS u
    ON oi.user_id = u.id
    WHERE oi.status not in ('Cancelled', 'Returned')
    GROUP BY user_id, country
    )

SELECT
  country,
  COUNT(DISTINCT user_id) AS customers_count,
  COUNT(female) AS female,
  COUNT(male) AS male
FROM customers
GROUP BY country
ORDER BY customers_count DESC;
```

2.2. Customers by Gender
```
SELECT 
  o.gender,
  ROUND(SUM(oi.sale_price * o.num_of_item), 2) AS revenue,
  SUM(o.num_of_item) AS quantity
FROM bigquery-public-data.thelook_ecommerce.order_items AS oi
INNER JOIN bigquery-public-data.thelook_ecommerce.orders AS o
ON oi.user_id = o.user_id
WHERE oi.status NOT IN ('Cancelled', 'Returned')
GROUP BY gender
ORDER BY revenue DESC;
```

2.3. Customers by Age Group
```
SELECT
  CASE
    WHEN u.age < 12 THEN 'Kids'
    WHEN u.age BETWEEN 12 AND 20 THEN 'Teenagers'
    WHEN u.age BETWEEN 20 AND 30 THEN 'Young Adults'
    WHEN u.age BETWEEN 30 AND 50 THEN 'Adults'
    WHEN u.age > 50 THEN 'Elderly'
    END AS age_group,
  COUNT(DISTINCT oi.user_id) AS total_customer
FROM bigquery-public-data.thelook_ecommerce.order_items AS oi
INNER JOIN bigquery-public-data.thelook_ecommerce.users AS u
ON oi.user_id = u.id
GROUP BY age_group
ORDER BY total_customer DESC;
```

3. What brands and product categories are we selling more and the least? What are we making money on?

3.1 Brand sales
```
SELECT 
  p.brand AS brand,
  ROUND(SUM(sale_price * num_of_item), 2) AS revenue,
  SUM(num_of_item) AS quantity
FROM bigquery-public-data.thelook_ecommerce.order_items oi
INNER JOIN bigquery-public-data.thelook_ecommerce.orders o
ON oi.order_id = o.order_id
INNER JOIN bigquery-public-data.thelook_ecommerce.products p
ON oi.product_id = p.id
WHERE oi.status NOT IN ('Cancelled', 'Returned')
GROUP BY brand
ORDER BY revenue DESC;
```
3.2 Product category sales
```
SELECT
  category AS product_category,
  ROUND(SUM(sale_price * num_of_item), 2) AS revenue,
  SUM(num_of_item) AS quantity
FROM bigquery-public-data.thelook_ecommerce.order_items oi
INNER JOIN bigquery-public-data.thelook_ecommerce.orders o
ON oi.order_id = o.order_id
INNER JOIN bigquery-public-data.thelook_ecommerce.products p
ON oi.product_id = p.id
WHERE oi.status NOT IN ('cancelled', 'Returned')
GROUP BY category
ORDER BY revenue DESC;
```


4. What are the most canceled and returned brands and product categories?

4.1 Brand cancellation and return
```
SELECT
  p.brand AS brand,
  SUM(CASE WHEN oi.status = 'Cancelled' THEN 1 ELSE null END) AS Cancelled,
  SUM(CASE WHEN oi.status = 'Returned' THEN 1 ELSE null END) AS Returned
FROM bigquery-public-data.thelook_ecommerce.order_items oi
INNER JOIN bigquery-public-data.thelook_ecommerce.products p
ON oi.product_id = p.id
GROUP BY brand
ORDER BY Cancelled DESC;
--ORDER BY Returned DESC;
```

4.2 Product category cancellation and return
```
SELECT p.category AS Category,
  SUM(CASE WHEN oi.status = 'Cancelled' THEN 1 ELSE null END) AS Cancelled,
  SUM(CASE WHEN oi.status = 'Returned' THEN 1 ELSE null END) AS Returned
FROM bigquery-public-data.thelook_ecommerce.order_items oi
INNER JOIN bigquery-public-data.thelook_ecommerce.products p
ON oi.product_id = p.id
GROUP BY Category
ORDER by Cancelled DESC
--ORDER by Returned DESC;
```

5. What marketing channel are we doing well on?

```
SELECT 
  u.traffic_source AS traffic_source,
  COUNT(DISTINCT oi.user_id) AS total_customers
FROM bigquery-public-data.thelook_ecommerce.order_items oi
INNER JOIN bigquery-public-data.thelook_ecommerce.users u
ON oi.user_id = u.id
WHERE oi.status NOT IN ('Cancelled', 'Returned')
GROUP BY traffic_source
ORDER BY total_customers DESC;

```

6. We will provide promotions during Chinese New Year celebrations for female customers in China via email.

```

SELECT 
  id,
  email
FROM `bigquery-public-data.thelook_ecommerce.users` 
WHERE gender = 'F' AND country = 'China'
ORDER BY id;
```

7. Provide a list of 10 customer IDs and emails with the largest total overall purchase. We will give a discount for Campaign 12.12!


```
SELECT
  u.id AS customer_id,
  u.email AS  email,
  ROUND(SUM(oi.sale_price * o.num_of_item), 2) AS total_purchase
FROM bigquery-public-data.thelook_ecommerce.order_items AS oi
INNER JOIN bigquery-public-data.thelook_ecommerce.orders AS o
ON oi.order_id = o.order_id
INNER JOIN bigquery-public-data.thelook_ecommerce.users AS u
ON o.user_id = u.id
GROUP BY customer_id, email
ORDER BY total_purchase DESC
LIMIT 10;
```

8. Create a query to get frequencies, average order value, and the total number of unique users where status is completed grouped by month

```

SELECT 
  FORMAT_DATE("%Y-%m", created_at) AS month_year,
  ROUND((COUNT(DISTINCT order_id)/COUNT(DISTINCT user_id)),2) AS frequencies,
  ROUND((SUM(sale_price)/COUNT(DISTINCT order_id)),2) AS Average_order_value,
  COUNT(DISTINCT user_id) AS total_unique_users
FROM `bigquery-public-data.thelook_ecommerce.order_items`
WHERE status = 'Complete'
GROUP BY month_year
ORDER BY month_year DESC;
```