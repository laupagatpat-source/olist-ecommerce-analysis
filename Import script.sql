olist_closed_deals_datasetCREATE DATABASE olist;
USE olist;

SET GLOBAL local_infile = 1;

CREATE TABLE olist_customers_dataset (
    customer_id VARCHAR(50),
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100),
    customer_state VARCHAR(10)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_customers_dataset.csv'
INTO TABLE olist_customers_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE olist_sellers_dataset (
    seller_id VARCHAR(50),
    seller_zip_code_prefix VARCHAR(10),
    seller_city VARCHAR(100),
    seller_state VARCHAR(10)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_sellers_dataset.csv'
INTO TABLE olist_sellers_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


CREATE TABLE olist_orders_dataset (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_status VARCHAR(30),
    order_purchase_timestamp DATETIME NULL,
    order_approved_at DATETIME NULL,
    order_delivered_carrier_date DATETIME NULL,
    order_delivered_customer_date DATETIME NULL,
    order_estimated_delivery_date DATETIME NULL
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_orders_dataset.csv'
INTO TABLE olist_orders_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    order_id,
    customer_id,
    order_status,
    @order_purchase_timestamp,
    @order_approved_at,
    @order_delivered_carrier_date,
    @order_delivered_customer_date,
    @order_estimated_delivery_date
)
SET
    order_purchase_timestamp = NULLIF(@order_purchase_timestamp, ''),
    order_approved_at = NULLIF(@order_approved_at, ''),
    order_delivered_carrier_date = NULLIF(@order_delivered_carrier_date, ''),
    order_delivered_customer_date = NULLIF(@order_delivered_customer_date, ''),
    order_estimated_delivery_date = NULLIF(@order_estimated_delivery_date, '');
    
    CREATE TABLE olist_order_items_dataset (
    order_id VARCHAR(50),
    order_item_id INT,
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    shipping_limit_date DATETIME,
    price DECIMAL(10,2),
    freight_value DECIMAL(10,2)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_order_items_dataset.csv'
INTO TABLE olist_order_items_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE olist_order_payments_dataset (
    order_id VARCHAR(50),
    payment_sequential INT,
    payment_type VARCHAR(30),
    payment_installments INT,
    payment_value DECIMAL(10,2)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_order_payments_dataset.csv'
INTO TABLE olist_order_payments_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


CREATE TABLE olist_order_reviews_dataset (
    review_id VARCHAR(50),
    order_id VARCHAR(50),
    review_score INT,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date DATETIME,
    review_answer_timestamp DATETIME
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_order_reviews_dataset.csv'
IGNORE
INTO TABLE olist_order_reviews_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    @review_creation_date,
    @review_answer_timestamp
)
SET
    review_creation_date = NULLIF(TRIM(BOTH '\r' FROM @review_creation_date), ''),
    review_answer_timestamp = NULLIF(TRIM(BOTH '\r' FROM @review_answer_timestamp), '');

CREATE TABLE olist_products_dataset (
    product_id VARCHAR(50),
    product_category_name VARCHAR(100),
    product_name_lenght INT NULL,
    product_description_lenght INT NULL,
    product_photos_qty INT NULL,
    product_weight_g INT NULL,
    product_length_cm INT NULL,
    product_height_cm INT NULL,
    product_width_cm INT NULL
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_products_dataset.csv'
IGNORE
INTO TABLE olist_products_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(
    product_id,
    product_category_name,
    @product_name_lenght,
    @product_description_lenght,
    @product_photos_qty,
    @product_weight_g,
    @product_length_cm,
    @product_height_cm,
    @product_width_cm
)
SET
    product_name_lenght        = ROUND(NULLIF(TRIM(BOTH '\r' FROM @product_name_lenght), '')),
    product_description_lenght = ROUND(NULLIF(TRIM(BOTH '\r' FROM @product_description_lenght), '')),
    product_photos_qty         = ROUND(NULLIF(TRIM(BOTH '\r' FROM @product_photos_qty), '')),
    product_weight_g           = ROUND(NULLIF(TRIM(BOTH '\r' FROM @product_weight_g), '')),
    product_length_cm          = ROUND(NULLIF(TRIM(BOTH '\r' FROM @product_length_cm), '')),
    product_height_cm          = ROUND(NULLIF(TRIM(BOTH '\r' FROM @product_height_cm), '')),
    product_width_cm           = ROUND(NULLIF(TRIM(BOTH '\r' FROM @product_width_cm), ''));


CREATE TABLE product_category_name_translation (
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/product_category_name_translation.csv'
IGNORE
INTO TABLE product_category_name_translation
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(
    product_category_name,
    product_category_name_english
)
SET
    product_category_name         = TRIM(BOTH '\r' FROM product_category_name),
    product_category_name_english = TRIM(BOTH '\r' FROM product_category_name_english);


CREATE TABLE olist_geolocation_dataset (
    geolocation_zip_code_prefix INT NULL,
    geolocation_lat DECIMAL(20,15) NULL,
    geolocation_lng DECIMAL(20,15) NULL,
    geolocation_city VARCHAR(100),
    geolocation_state VARCHAR(10)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_geolocation_dataset.csv'
IGNORE
INTO TABLE olist_geolocation_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(
    @geolocation_zip_code_prefix,
    @geolocation_lat,
    @geolocation_lng,
    @geolocation_city,
    @geolocation_state
)
SET
    geolocation_zip_code_prefix = NULLIF(TRIM(BOTH '\r' FROM @geolocation_zip_code_prefix), ''),
    geolocation_lat             = NULLIF(TRIM(BOTH '\r' FROM @geolocation_lat), ''),
    geolocation_lng             = NULLIF(TRIM(BOTH '\r' FROM @geolocation_lng), ''),
    geolocation_city            = TRIM(BOTH '\r' FROM @geolocation_city),
    geolocation_state           = TRIM(BOTH '\r' FROM @geolocation_state);


CREATE TABLE olist_marketing_qualified_leads_dataset (
    mql_id VARCHAR(50),
    first_contact_date DATE NULL,
    landing_page_id VARCHAR(50),
    origin VARCHAR(50)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_marketing_qualified_leads_dataset.csv'
IGNORE
INTO TABLE olist_marketing_qualified_leads_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(
    mql_id,
    @first_contact_date,
    landing_page_id,
    @origin
)
SET
    first_contact_date = CASE 
		WHEN TRIM(BOTH '\r' FROM @first_contact_date) = '' THEN NULL
		WHEN TRIM(BOTH '\r' FROM @first_contact_date) IS NULL THEN NULL
		ELSE STR_TO_DATE(TRIM(BOTH '\r' FROM @first_contact_date), '%m/%d/%Y')
		END,
    origin = TRIM(BOTH '\r' FROM @origin);


CREATE TABLE olist_closed_deals_dataset (
    mql_id VARCHAR(50),
    seller_id VARCHAR(50),
    sdr_id VARCHAR(50),
    sr_id VARCHAR(50),
    won_date DATETIME NULL,
    business_segment VARCHAR(100),
    lead_type VARCHAR(100),
    lead_behaviour_profile VARCHAR(100),
    has_company VARCHAR(10),
    has_gtin VARCHAR(10),
    average_stock VARCHAR(50),
    business_type VARCHAR(100),
    declared_product_catalog_size DECIMAL(10,2) NULL,
    declared_monthly_revenue DECIMAL(15,2) NULL
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_closed_deals_dataset.csv'
IGNORE
INTO TABLE olist_closed_deals_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(
    mql_id,
    seller_id,
    sdr_id,
    sr_id,
    @won_date,
    business_segment,
    lead_type,
    lead_behaviour_profile,
    has_company,
    has_gtin,
    average_stock,
    business_type,
    @declared_product_catalog_size,
    @declared_monthly_revenue
)
SET
    won_date = CASE
                  WHEN TRIM(BOTH '\r' FROM @won_date) = '' THEN NULL
                  WHEN TRIM(BOTH '\r' FROM @won_date) IS NULL THEN NULL
                  ELSE STR_TO_DATE(TRIM(BOTH '\r' FROM @won_date), '%m/%d/%Y %H:%i')
               END,
    declared_product_catalog_size = NULLIF(TRIM(BOTH '\r' FROM @declared_product_catalog_size), ''),
    declared_monthly_revenue      = NULLIF(TRIM(BOTH '\r' FROM @declared_monthly_revenue), '');