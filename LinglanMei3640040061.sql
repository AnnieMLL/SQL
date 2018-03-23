/*
1. 
Create one database and load data into it
*/
create database if not exists supermarket;
use supermarket;

-- aisle table
CREATE TABLE IF NOT EXISTS aisle (
	id			INT(11) NOT NULL,
	aisle			VARCHAR(100),
	PRIMARY KEY	(id)
);

LOAD DATA LOCAL INFILE '/Users/meilinglan/Desktop/capstone project/Archive/aisles.csv' 
INTO TABLE aisle
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;

-- department table
CREATE TABLE IF NOT EXISTS department (
	id			INT(11) NOT NULL,
    department	VARCHAR(30),
    PRIMARY KEY	(id)
);

LOAD DATA LOCAL INFILE '/Users/meilinglan/Desktop/capstone project/Archive/departments.csv' 
INTO TABLE department
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;

-- orders table
CREATE TABLE IF NOT EXISTS orders (
	id						INT(11) NOT NULL,
    user_id					INT(11),
    eval_set				VARCHAR(10),
    order_number			INT(11),
    order_dow				INT(11),
    order_hour_of_day		INT(11),
    days_since_prior_order	INT(11),
    PRIMARY KEY (id)
);

LOAD DATA LOCAL INFILE '/Users/meilinglan/Desktop/capstone project/Archive/orders.csv' 
INTO TABLE orders
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n' 
IGNORE 1 LINES
SET days_since_prior_order = nullif(@vdays_since_prior_order,'');

-- product table
CREATE TABLE IF NOT EXISTS product (
	id				INT(11) NOT NULL,
    name			VARCHAR(200),
    aisle_id		INT(11),
    department_id	INT(11),
    PRIMARY KEY (id),
    FOREIGN KEY (aisle_id) 
		REFERENCES  aisle (id) 
			ON DELETE NO ACTION
            ON UPDATE CASCADE,
	FOREIGN KEY (department_id)
		REFERENCES department (id)
			ON DELETE NO ACTION
			ON UPDATE CASCADE
);

LOAD DATA LOCAL INFILE '/Users/meilinglan/Desktop/capstone project/Archive/products.csv' 
INTO TABLE product
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- order_product table
CREATE TABLE IF NOT EXISTS order_product (
	order_id			INT(11),
    product_id			INT(11),
    add_to_cart_order	INT(11),
    reordered			INT(11),
    PRIMARY KEY (order_id, product_id)
);

LOAD DATA LOCAL INFILE '/Users/meilinglan/Desktop/capstone project/Archive/order_products.csv' 
INTO TABLE order_product
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

show databases;

select * from aisle;
select * from department;
select * from orders;
select * from product;
select * from order_product;


/*
2.
Selecting top 10 product sales for each day in the week
Including product_id, product_name, total order amount, and day
*/

SELECT product_id, product_name, total_order_amount, day
FROM
(
    SELECT *,
        @curr_day := IF(@prev_day = day, @curr_day + 1, 1) AS curr_day,
        @prev_day := day
    FROM (
		SELECT prod.id AS product_id, prod.name AS product_name,
		COUNT(ordprod.add_to_cart_order) AS total_order_amount,
		o.order_dow AS day
		FROM orders AS o
		JOIN product AS prod
		JOIN order_product AS ordprod
		WHERE o.id = ordprod.order_id
        AND ordprod.product_id = prod.id
		GROUP BY prod.id
		ORDER BY day ASC, total_order_amount DESC
		) AS tmptable
    JOIN (SELECT @prev_day := NULL, @curr_day := 0) AS vars
    ORDER BY day, total_order_amount DESC
) AS summary_table
WHERE curr_day <= 10
AND day NOT IN (0,6);



/* in HIVE
USE jason_supermarket;

SELECT product_id, p.name as product_name, total_order_amount, day FROM
(
SELECT *, RANK() OVER(PARTITION BY day ORDER BY total_order_amount DESC) num
FROM (SELECT product_id, SUM(ordprod.add_to_cart_order) AS total_order_amount,
	  orders.order_dow AS day
	  FROM order_product
	  JOIN orders
	  JOIN product
	  WHERE order_product.order_id = orders.id
	  AND order_product.product_id = product.id
	  GROUP BY product_id, order_dow) Y
) X
LEFT OUTER JOIN product p 
ON (product_id = p.id)
WHERE num <= 10
AND day NOT IN (0,6);
*/


/*
3.
Write a query to display the 5 most popular products in each aisle
from Monday to Friday. Listing product_id, aisle, and day in the week.
*/

SELECT product_id, aisle, day, total_order_amount
FROM
(
    SELECT *,
        @aisle_count := IF(@prev_aisle = aisle_id, @aisle_count + 1, 1) AS aisle_count,
        @prev_aisle := aisle_id,
        @curr_day := IF(@curr_day = day, @curr_day + 1, 1) as curr_day,
        @prev_day := day
    FROM (
		SELECT prod.id AS product_id, prod.name AS product_name,
		COUNT(ordprod.add_to_cart_order) AS total_order_amount,
		o.order_dow AS day, aisle.aisle as aisle, aisle.id as aisle_id
		FROM orders AS o
		JOIN product AS prod
		JOIN order_product AS ordprod
        JOIN aisle
		WHERE o.id = ordprod.order_id
        AND ordprod.product_id = prod.id
        AND prod.aisle_id = aisle.id
		GROUP BY prod.id
		ORDER BY day ASC, total_order_amount DESC
		) AS tmptable
    JOIN (SELECT @prev_aisle := NULL, @aisle_count := 0, 
			@prev_day := NULL, @curr_day := 0) AS vars
    ORDER BY day ASC, aisle_id, total_order_amount DESC
) AS summary_table
WHERE aisle_count <= 5
AND day not in (0,6);


/* in HIVE
USE jason_supermarket;

SELECT product_id, total_order_amount, aisle, day FROM
(
SELECT *, RANK() OVER(PARTITION BY day, aisle ORDER BY total_order_amount DESC) num
FROM (SELECT product_id, SUM(ordprod.add_to_cart_order) AS total_order_amount, aisle,
	  orders.order_dow AS day
	  FROM order_product
	  JOIN orders
	  JOIN product
	  JOIN aisle
	  WHERE order_product.order_id = orders.id
	  AND order_product.product_id = product.id
	  AND product.aisle_id = aisle.id
	  GROUP BY product_id, order_dow, aisle) Y
) X
LEFT OUTER JOIN product p 
ON (product_id = p.id)
WHERE num <= 5
AND day NOT IN (0,6);
*/


/*
4.
Query to select the top 10 products that the users have the most frequent
reorder rate. Only need to give the results with product id.
*/

create table q4 (select SUM(reordered)/SUM(add_to_cart_order) AS reorder_rate,product_id 
from order_product group by product_id order by reorder_rate desc limit 10);

select product_id from q4;

/*
5. 
*/
-- (1). Create a report listing order id and all unique aisle id
select * from order_product
join product
where order_product.product_id = product.id
order by order_id, aisle_id;

-- (2). Find the most popular shopping paths

select path, count(*) as path_occurence
FROM
(
	SELECT order_id, GROUP_CONCAT(aisle_id SEPARATOR ' ') as path
	FROM
	(
		select distinct order_id, aisle_id from order_product
		join product
		where order_product.product_id = product.id
		order by order_id, aisle_id) as tmp
	GROUP BY order_id) as tmp2
GROUP BY path
ORDER BY path_occurence DESC;


/* in HIVE 
USE jason_supermarket;

select path, count(*) as path_occurence
FROM
(
SELECT order_id, collect_set(aisle_id) as path
FROM
(
select distinct order_id, aisle_id from order_product
join product
where order_product.product_id = product.id
order by order_id, aisle_id) as tmp
GROUP BY order_id) as tmp2
GROUP BY path
ORDER BY path_occurence DESC;
*/

/* 
6. Find the pair of items that is most frequently bought together.	
*/

select prod1, prod2, count(*) as counts
from
	(select distinct a.order_id, a.product_id as prod1,
	b.product_id as prod2
	from
		(select * from order_product
		order by order_id, product_id) as a
		join
		(select * from order_product
		order by order_id, product_id desc) as b
	where a.order_id = b.order_id
	and a.product_id <> b.product_id) as pairs
group by prod1, prod2
order by counts desc
limit 200;

/* in HIVE
use jason_supermarket;

select prod1, prod2, count(*) as counts
from
(select distinct a.order_id, a.product_id as prod1,
b.product_id as prod2
from
(select * from order_product
order by order_id, product_id) as a
join
(select * from order_product
order by order_id, product_id desc) as b
where a.order_id = b.order_id
and a.product_id <> b.product_id) as pairs
group by prod1, prod2
order by counts desc
limit 200;
*/


