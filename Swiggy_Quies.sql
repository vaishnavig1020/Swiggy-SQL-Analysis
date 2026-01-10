SELECT * FROM swiggy_data

-- Data Cleaning & Validation

--Null Check
SELECT 
SUM(CASE WHEN State IS NULL THEN 1 ELSE 0 END) AS null_state,
SUM(CASE WHEN City IS NULL THEN 1 ELSE 0 END) AS null_city,
SUM(CASE WHEN Order_Date IS NULL THEN 1 ELSE 0 END) AS null_order_date,
SUM(CASE WHEN Restaurant_Name IS NULL THEN 1 ELSE 0 END) AS null_restaruant_name,
SUM(CASE WHEN Location IS NULL THEN 1 ELSE 0 END) AS null_location,
SUM(CASE WHEN Category IS NULL THEN 1 ELSE 0 END) AS null_category,
SUM(CASE WHEN Dish_Name IS NULL THEN 1 ELSE 0 END) AS null_dish_name,
SUM(CASE WHEN Price_INR IS NULL THEN 1 ELSE 0 END) AS null_price,
SUM(CASE WHEN Rating IS NULL THEN 1 ELSE 0 END) AS null_rating,
SUM(CASE WHEN Rating_Count IS NULL THEN 1 ELSE 0 END) AS null_rating_count
FROM swiggy_data;

-- Blank/Empty String Check
SELECT * 
FROM swiggy_data
WHERE
(State = '' OR City = '' OR Restaurant_Name = '' OR Location = '' OR Category = '' OR Dish_Name = '');

-- Duplicate Detection
SELECT 
State,city,order_date,restaurant_name,location,category,
dish_name,price_inr,rating,rating_count,COUNT(*) AS CNT
FROM swiggy_data
GROUP BY State,city,order_date,restaurant_name,location,category,
dish_name,price_inr,rating,Rating_Count
HAVING COUNT(*) > 1;

-- Delect Duplicate
WITH CTE AS(
SELECT *, ROW_NUMBER() OVER(
PARTITION BY State,city,order_date,restaurant_name,location,category,
dish_name,price_inr,rating,rating_count
ORDER BY (SELECT NULL)
) AS rn
FROM swiggy_data
)
DELETE FROM CTE WHERE rn > 1

-- Create Schema
-- Dimension Table 

-- Date Table
CREATE TABLE dim_date (
date_id INT IDENTITY(1,1) PRIMARY KEY,
full_date DATE,
Year INT ,
Month INT,
Month_name VARCHAR(20),
Quarter INT,
Day INT,
Week INT
);

-- Location Table
CREATE TABLE dim_location(
location_id INT IDENTITY(1,1) PRIMARY KEY,
City VARCHAR(50),
State VARCHAR(100),
Location VARCHAR(150)
);

-- Restaurant Table 
CREATE TABLE dim_restaurant(
restaurant_id INT IDENTITY(1,1) PRIMARY KEY,
Restaurant_Name VARCHAR(150)
);

-- Category Table 
CREATE TABLE dim_category(
category_id INT IDENTITY(1,1) PRIMARY KEY,
Category VARCHAR(150)
);

-- Dish_Name Table
CREATE TABLE dim_dish(
dish_id INT IDENTITY(1,1) PRIMARY KEY,
Dish_Name VARCHAR(200)
);


-- Fact Table
CREATE TABLE fact_swiggy_order(
order_id INT IDENTITY(1,1) PRIMARY KEY ,

date_id INT,
Price_INR DECIMAL(10,2),
Rating DECIMAL(4,2),
Rating_Count INT,

location_id INT,
restaurant_id INT,
category_id INT,
dish_id INT,

FOREIGN KEY(date_id) REFERENCES dim_date(date_id),
FOREIGN KEY(location_id) REFERENCES dim_location(location_id),
FOREIGN KEY(restaurant_id) REFERENCES dim_restaurant(restaurant_id),
FOREIGN KEY(category_id) REFERENCES dim_category(category_id),
FOREIGN KEY(dish_id) REFERENCES dim_dish(dish_id)
);

-- Insert the data into the table
-- dim_date table
INSERT INTO dim_date(full_date,Year,Month,Month_name,Quarter,Day,Week)
SELECT DISTINCT
Order_Date,
YEAR(Order_Date),
Month(Order_Date),
DATENAME(Month,Order_Date),
DATEPART(Quarter,Order_Date),
DAY(Order_Date),
DATEPART(Week,Order_Date)
FROM swiggy_data
WHERE Order_Date IS NOT NULL;

-- dim_location tablE
INSERT INTO dim_location(City,State,Location)
SELECT DISTINCT
	City,
	State,
	Location
FROM swiggy_data;

-- dim_category table
INSERT INTO dim_category(Category)
SELECT DISTINCT
Category
FROM swiggy_data;

-- dim_restaurant table
INSERT INTO dim_restaurant(Restaurant_Name)
SELECT DISTINCT
	Restaurant_Name
FROM swiggy_data

-- dim-dish table
INSERT INTO dim_dish(Dish_Name)
SELECT DISTINCT
	Dish_Name
FROM swiggy_data;

DELETE FROM fact_swiggy_order;


-- fact table
INSERT INTO fact_swiggy_order(
	date_id,
	Price_INR,
	Rating,
	Rating_Count ,
	location_id ,
	restaurant_id ,
	category_id,
	dish_id 
)
SELECT
dd.date_id,
s.Price_INR,
s.Rating,
s.Rating_Count,

dl.location_id,
dr.restaurant_id,
dc.category_id,
dsh.dish_id
FROM swiggy_data s

JOIN dim_date dd
	ON dd.full_date = s.Order_Date

JOIN dim_location dl
	ON dl.State = s.State
	AND dl.City = s.City
	AND dl.Location = s.Location

JOIN dim_restaurant dr
	ON dr.Restaurant_Name = s.Restaurant_Name

JOIN dim_category dc
	ON dc.Category = s.Category

JOIN dim_dish dsh
	ON dsh.Dish_Name = s.Dish_Name;


SELECT * FROM fact_swiggy_order f
JOIN dim_date d ON d.date_id = f.date_id
JOIN dim_location l ON l.location_id = f.location_id
JOIN dim_category c ON c.category_id = f.category_id
JOIN dim_dish di ON di.dish_id = f.dish_id


-- KPI's

-- Total Orders
SELECT COUNT(*) AS Total_Order
FROM fact_swiggy_order

-- Total Revenue
SELECT
FORMAT(SUM(CONVERT(FLOAT,Price_INR))/1000000 ,'N2') + 'INR MILLION'
AS Total_Revenue
FROM fact_swiggy_order

-- Avg Dish Price
SELECT 
FORMAT(AVG(CONVERT(FLOAT,Price_INR)), 'N2')+ 'INR MILLION'
AS Avg_Revenue
FROM fact_swiggy_order;

-- Avg Rating
SELECT 
AVG(Rating)
AS Avg_Rating
FROM fact_swiggy_order;


-- DEEP - DRIVE Business Analysis
--Monthly Trend 
SELECT 
d.year,
d.month,
d.month_name,
count(*) AS Total_Order
FROM fact_swiggy_order f
JOIN dim_date d ON d.date_id = f.date_id
GROUP BY d.year,d.month,d.month_name ;

-- Quartely Trend
SELECT 
d.year,
d.quarter,
count(*) AS Total_Order
FROM fact_swiggy_order f
JOIN dim_date d ON d.date_id = f.date_id
GROUP BY d.year,d.quarter
ORDER BY count(*);

-- Yearly Trend
SELECT 
d.Year,
count(*) AS Total_Order
FROM fact_swiggy_order f
JOIN dim_date d ON d.date_id = f.date_id
GROUP BY d.Year
ORDER BY d.Year desc;

-- Order by Day of Week(Mon-Sun)
SELECT
	DATENAME(WEEKDAY,full_date),
	count(*) AS Total_Order
	FROM fact_swiggy_order f
JOIN dim_date d ON d.date_id = f.date_id
GROUP BY DATENAME(WEEKDAY,full_date), DATEPART(WEEKDAY,full_date)
ORDER BY DATEPART(WEEKDAY,full_date) desc;

-- Location - Based Analysis
-- Top 10 Cities by Order Volume
SELECT TOP 10
	l.city,
	SUM(f.Price_INR) as Total_Revenue
	FROM fact_swiggy_order f
JOIN dim_location l ON l.location_id = f.location_id
GROUP BY l.city
ORDER BY SUM(f.Price_INR)  DESC;

-- Revenue distributed by State
SELECT
	l.state,
	SUM(f.Price_INR) AS Total_Revenue
	FROM fact_swiggy_order f
JOIN dim_location l ON l.location_id = f.location_id
GROUP BY l.state
ORDER BY SUM(f.Price_INR) DESC;


-- Food - Performance
-- Top 10 Restaurant by Orders
SELECT TOP 10
	r.Restaurant_Name,
	SUM(f.Price_INR) AS Total_Revenue
	FROM fact_swiggy_order f
JOIN dim_restaurant r ON r.restaurant_id = f.restaurant_id
GROUP BY r.Restaurant_Name
ORDER BY SUM(f.Price_INR) DESC;

-- Top Category by order volume
SELECT
	c.Category,
	SUM(f.Price_INR) AS Total_Revenue
	FROM fact_swiggy_order f
JOIN dim_category c ON c.category_id = f.category_id
GROUP BY c.Category  
ORDER BY SUM(f.Price_INR) DESC;

-- Most Ordered Dish 
SELECT 
	dsh.Dish_Name,
	COUNT(*) AS order_count
	FROM fact_swiggy_order f
JOIN dim_dish dsh ON dsh.dish_id = f.dish_id
GROUP BY dsh.Dish_Name
ORDER BY COUNT(*) DESC;


-- Customer Spending Price
-- Cuisine Perfromance(Order + Avg Rating)
SELECT 
	c.Category,
	COUNT(*) AS Total_Order,
	AVG(f.Rating) AS Avg_Rating
	FROM fact_swiggy_order f
JOIN dim_category c ON c.category_id = f.category_id
GROUP BY c.Category
ORDER BY Total_Order Desc;

-- Total Order By Price Range
SELECT
		CASE
			WHEN CONVERT(FLOAT,Price_INR) < 100 THEN 'Under 100'
			WHEN CONVERT(FLOAT,Price_INR) BETWEEN 100 AND 199 THEN '100 - 199'
			WHEN CONVERT(FLOAT,Price_INR) BETWEEN 200 AND 299 THEN '200 - 299'
			WHEN CONVERT(FLOAT,Price_INR) BETWEEN 300 AND 399 THEN '300 - 399'
		ELSE '500+'
	END as Price_Range,
	count(*) as total_order
	FROM fact_swiggy_order 
GROUP BY 
		CASE
			WHEN CONVERT(FLOAT,Price_INR) < 100 THEN 'Under 100'
			WHEN CONVERT(FLOAT,Price_INR) BETWEEN 100 AND 199 THEN '100 - 199'
			WHEN CONVERT(FLOAT,Price_INR) BETWEEN 200 AND 299 THEN '200 - 299'
			WHEN CONVERT(FLOAT,Price_INR) BETWEEN 300 AND 399 THEN '300 - 399'
		ELSE '500+'
	END
ORDER BY total_order desc;

-- Rating Analysis
-- Rating Count Distribution(1-5)
SELECT
	Rating,
	COUNT(*) AS Rating_Count
	FROM fact_swiggy_order
GROUP BY Rating
ORDER BY Rating_Count Desc;

