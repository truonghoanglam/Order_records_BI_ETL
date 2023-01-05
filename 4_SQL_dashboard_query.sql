-- Create new_added table:
CREATE TABLE IF NOT EXISTS project_1_data (
                    id                     VARCHAR(250),
                    status_name            VARCHAR(250),
                    status                 INTEGER,
                    status_history         VARCHAR(500),
                    inserted_at            TIMESTAMP,
                    tags                   VARCHAR(250),
                    warehouse_info         VARCHAR(250),
					partner                VARCHAR(250),
					shop_id                VARCHAR(250),
					assigning_seller       VARCHAR(250),
					updated_at             TIMESTAMP,
					revenue                NUMERIC(15,3),          
					id_column              VARCHAR(250) PRIMARY KEY;

CREATE TABLE IF NOT EXISTS d_client (
                    shop_id                VARCHAR(20) PRIMARY KEY,
	                api_key                VARCHAR(200),
                    shop_name              VARCHAR(50),
					country                VARCHAR(50));

--------------------------------------------------------------------------------------------------------------------------------------------
-- Remove bad data:
CREATE VIEW clean_data AS 
	SELECT * 
	FROM project_1_data
	WHERE 	LOWER(tags) NOT LIKE '%tag: (double data)%'
		AND LOWER(tags) NOT LIKE '%tag: (wrong data)%';

--------------------------------------------------------------------------------------------------------------------------------------------
-- Create main_metric table:
CREATE VIEW main_metric AS
	SELECT
		-- Date, shop_id, warehouse
		DATE(inserted_at), shop_id , warehouse_info AS warehouse,
		-- new_added orders
		COUNT(*) AS new_added_orders,
		-- Confirmed orders
		SUM(CASE WHEN status_name NOT IN ('new_added', 'deny')
			THEN 1 ELSE 0 END) AS confirmed_orders,
		-- deny orders
		SUM(CASE WHEN status_name = 'deny' 
			THEN 1 ELSE 0 END) AS deny_orders,
		-- Can't connect orders
		SUM(CASE 
				WHEN status_name = 'new_added' THEN 1
				WHEN status_name = 'deny' 
					AND LOWER(tags) LIKE '%tag: (*Can_not_connect*)%' THEN 1
				ELSE 0 END) AS can_not_connect_orders,
				
		---------------------------------------
		-- received order
		SUM(CASE WHEN status_name = 'received' THEN 1 
			ELSE 0 END) AS received_orders,
		-- arrived to warehouse order
		SUM(CASE WHEN status_name IN ('on way to warehouse', 'arrived to warehouse') THEN 1 
			ELSE 0 END) AS arrived_to_warehouse_orders,
		-- Delivering order
		SUM(CASE WHEN status_name = 'shipped' THEN 1 
			ELSE 0 END) AS delivering_orders
		
		---------------------------------------
	FROM 
		clean_data
	GROUP BY 
		DATE(inserted_at), 
		shop_id, 
		warehouse_info;

--------------------------------------------------------------------------------------------------------------------------------------------
-- Create view for waiting for pick up orders:

CREATE VIEW waiting_orders AS
SELECT
	date, 
	shop_id , 
	warehouse_info AS warehouse,
	COUNT(*) AS waiting_orders
FROM
	(SELECT
		(CASE 
			WHEN status_history LIKE '%, S: 8 - %' 
				THEN TO_DATE(
					SUBSTRING (status_history ,POSITION(', S: 8 - ' IN status_history) + 9, 10 )
					,'YYYY-MM-DD')
			WHEN status_history LIKE '%, S: 9 - %' 
				THEN TO_DATE(
					SUBSTRING (status_history , POSITION(', S: 9 - ' IN status_history) + 9, 10 )
					,'YYYY-MM-DD')
			ELSE NULL END) AS date,
		shop_id,
		warehouse_info
	FROM clean_data
	) AS a
WHERE 
	date IS NOT NULL
GROUP BY
	date,
	shop_id,
	warehouse_info;


-- join 3 tables to make main query-------------------------------------------------------

CREATE VIEW main_dash_query AS		
SELECT
	a.date,
	a.warehouse,
	a.new_added_orders,
	a.confirmed_orders,
	a.deny_orders,
	a.can_not_connect_orders,
	a.received_orders,
	a.arrived_to_warehouse_orders,
	a.delivering_orders,
	a.waiting_orders,
	a.shipped_orders,

	d.shop_name,
	d.country,
	d.client_type
FROM
	(
		SELECT
			COALESCE (f.date, w.date, s.date) AS date,
			COALESCE (f.shop_id, w.shop_id, s.shop_id) AS shop_id,
			COALESCE (f.warehouse, w.warehouse, s.warehouse) AS warehouse,
			f.new_added_orders,
			f.confirmed_orders,
			f.deny_orders,
			f.can_not_connect_orders,
			f.received_orders,
			f.arrived_to_warehouse_orders,
			f.delivering_orders,

			w.waiting_orders,
			s.shipped_orders
		FROM
			main_metric AS f 
		FULL JOIN 
			waiting_orders AS w 
			ON f.date = w.date
			AND f.shop_id = w.shop_id
			AND f.warehouse = w.warehouse 
		FULL JOIN 
			shipped_order AS s 
			ON f.date = s.date
			AND f.shop_id = s.shop_id
			AND f.warehouse = s.warehouse
	) AS a
LEFT JOIN d_client AS d 
	ON a.shop_id = d.shop_id;