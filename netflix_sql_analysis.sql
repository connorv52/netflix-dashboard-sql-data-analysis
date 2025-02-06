/* Before proceeding with the analysis, we should clean the data. We shoud look for duplicates (if 
there are any), standardize the data, handle null values (if there are any), and remove any 
redundant or unncessary columns. This may ultimately be an optional or entirely unneeded step,
but we should always clean the data to the best of our ability before proceeding with our analysis. */ 

-- Remove Duplicates
CREATE TABLE netflix_users_staging AS 
SELECT * FROM netflix_users;

SELECT *, 
ROW_NUMBER() OVER(PARTITION BY user_id, subscription_type, monthly_revenue,
			   join_date, last_payment, country, age, gender) AS row_num
			   FROM netflix_users_staging;

WITH duplicate_cte AS (
SELECT * ,
ROW_NUMBER() OVER(PARTITION BY user_id, subscription_type, monthly_revenue,
			   join_date, last_payment, country, age, gender) AS row_num
			   FROM netflix_users_staging
)
SELECT * FROM duplicate_cte
WHERE row_num > 1;

-- No duplicates were found, so we proceed with standardization

SELECT * FROM netflix_users_staging;

UPDATE netflix_users_staging
SET subscription_type = TRIM(subscription_type), 
country = TRIM(country), gender = TRIM(gender), device = TRIM(device);

SELECT DISTINCT country 
FROM netflix_users_staging;

-- The dates look fine, so no need to adjust those. However, we should change its typing from 
-- text to date. 

ALTER TABLE netflix_users_staging
ALTER COLUMN join_date TYPE DATE USING TO_DATE(join_date, 'DD-MM-YY');

SELECT join_date, last_payment FROM netflix_users_staging;

ALTER TABLE netflix_users_staging
ALTER COLUMN last_payment TYPE DATE USING TO_DATE(last_payment, 'DD-MM-YY');

SELECT * FROM netflix_users_staging
LIMIT 10;

-- Now we check for any null values

SELECT *
FROM netflix_users_staging
WHERE user_id IS NULL
   OR subscription_type IS NULL
   OR monthly_revenue IS NULL
   OR join_date IS NULL
   OR last_payment IS NULL
   OR country IS NULL
   OR age IS NULL
   OR gender IS NULL
   OR device IS NULL
   OR plan_duration IS NULL;

-- Fortunately, there doesn't seem to be any NULLs in our table

-- The plan_duration column only contains 1 value (1 month), but we may find a use for this 
-- later in our analysis, so we will maintain the column for now. 

/* Question Set 1 */

/* Q1: Which country has the most subscriptions? Which country has the least? */

SELECT country, COUNT(*) AS total_subscriptions
FROM netflix_users_staging
group by country;

SELECT country, COUNT(*) AS total_subscriptions
FROM netflix_users_staging
group by country
ORDER BY country DESC
LIMIT 1;

SELECT country, COUNT(*) AS total_subscriptions
FROM netflix_users_staging
group by country
ORDER BY country ASC
LIMIT 1;

-- Before moving on, further exploration of these values reveals that there are ties in place
-- for both countries with the highest and lowest number of subscribers, so 
-- the queries above are not entirely appropriate. It should also be noted that this dataset 
-- contains only a small sample of netflix users, and that the number of countries is restricted to 
-- only these 10 (presumably the 10 largest real-world countries with respect to Netflix subscribers. 

SELECT country, COUNT(*) AS total_subscriptions
FROM netflix_users_staging
GROUP BY country
HAVING COUNT(*) = (SELECT MAX(subscription_count) 
				   FROM (SELECT COUNT(*) AS subscription_count FROM netflix_users GROUP BY country) 
				   AS counts);
				   
SELECT country, COUNT(*) AS total_subscriptions
FROM netflix_users_staging
GROUP BY country
HAVING COUNT(*) = (SELECT MIN(subscription_count) 
				   FROM (SELECT COUNT(*) AS subscription_count FROM netflix_users GROUP BY country) 
				   AS counts);

-- Through subqueries, we can assess how many countries are tied for the highest or lowest
-- number of Netflix subscribers. 

/* Q2: What is the distribution of users by age and gender? 
Does this vary across the different subscription types? */

SELECT age, COUNT(*) AS user_count
FROM netflix_users_staging
GROUP BY age
ORDER BY age ASC;

SELECT gender, COUNT(*) AS user_count
FROM netflix_users_staging
GROUP BY gender;

SELECT age, gender, COUNT(*) AS user_count
FROM netflix_users_staging
GROUP BY age, gender
ORDER BY age, gender;

SELECT 
    subscription_type,
    age,
    gender,
    COUNT(*) AS user_count
FROM netflix_users
GROUP BY subscription_type, age, gender
ORDER BY subscription_type, age, gender;

/* Q3: What is the average subscription duration? Does this vary across ages and devices? */

SELECT 
    AVG(DATE_PART('month', AGE(last_payment, join_date))) AS average_subscription_duration_months
FROM netflix_users_staging;

CREATE VIEW subs_length AS
SELECT 
    age,
    AVG(DATE_PART('month', AGE(last_payment, join_date))) AS average_subscription_duration_month
FROM netflix_users_staging
GROUP BY age
ORDER BY age;

SELECT 
    device,
    AVG(DATE_PART('month', AGE(last_payment, join_date))) AS average_subscription_duration_month
FROM netflix_users_staging
GROUP BY device
ORDER BY device;

/* Q4: What are the most popular devices used to access Netflix? 
What are the least popular devices? */

SELECT device, COUNT(*) AS number_of_devices
FROM netflix_users_staging
GROUP BY device;

SELECT device, number_of_devices
FROM (SELECT 
        Device,
        COUNT(*) AS number_of_devices,
        RANK() OVER (ORDER BY COUNT(*) DESC) AS device_rank
    FROM netflix_users
    GROUP BY device
) AS ranked_devices
WHERE device_rank = 1;

SELECT Device, device_count
FROM (
    SELECT 
        Device,
        COUNT(*) AS device_count,
        RANK() OVER (ORDER BY COUNT(*) ASC) AS device_rank
    FROM netflix_users
    GROUP BY Device
) AS ranked_devices
WHERE device_rank = 1;

/* Q5: What does the distribution of subscriptions (Basic, Standard, Premium) look like? 
How does it vary across countries? */

SELECT * FROM netflix_users_staging
LIMIT 10;

SELECT 
    subscription_type,
    COUNT(*) AS Count
FROM netflix_users_staging
GROUP BY subscription_type;

SELECT
	country,
    subscription_type,
    COUNT(*) AS Count,
	SUM(COUNT(*)) OVER(PARTITION BY country ORDER BY subscription_type) AS running_total
FROM netflix_users_staging
GROUP BY country, subscription_type
ORDER BY country, subscription_type;

-- This window function will allow us to more easily view the total number of subscribers for 
-- each country

/* Q6: Are there seasonal trends visible regarding when people subscribe to Netflix? */

SELECT
    EXTRACT(MONTH FROM join_date) AS join_month,
    COUNT(*) AS new_subscriptions
FROM
    netflix_users_staging
GROUP BY
    EXTRACT(MONTH FROM join_date)
ORDER BY
    EXTRACT(MONTH FROM join_date);

/* Q7: What is the total monthly revenue? How does revenue vary by subscription type and country? 
How many users are present in this table? */
SELECT SUM(monthly_revenue) AS total_monthly_revenue 
FROM netflix_users_staging;

SELECT 
    subscription_type,
    SUM(monthly_revenue) AS total_monthly_revenue
FROM netflix_users_staging
GROUP BY subscription_type;

SELECT 
    country,
    SUM(monthly_revenue) AS total_monthly_revenue
FROM netflix_users_staging
GROUP BY country;

SELECT subscription_type, country, 
SUM(monthly_revenue) AS total_monthly_revenue,
SUM(SUM(monthly_revenue)) OVER (PARTITION BY country 
							   ORDER BY subscription_type)
							   AS running_total_monthly_revenue
FROM netflix_users_staging
GROUP BY country, subscription_type
ORDER BY country, subscription_type;

SELECT COUNT(DISTINCT user_id) FROM netflix_users_staging;

/* Q8: Is there a correlation between the age distribution of Netflix users and the devices
they use to access the platform? Do certain age groups prefer a specific device? */

SELECT
    CASE
        WHEN age BETWEEN 18 AND 24 THEN '18-24'
        WHEN age BETWEEN 25 AND 34 THEN '25-34'
        WHEN age BETWEEN 35 AND 44 THEN '35-44'
        WHEN age BETWEEN 45 AND 54 THEN '45-54'
        WHEN age BETWEEN 55 AND 64 THEN '55-64'
        ELSE '65+'
    END AS age_group,
    device,
    COUNT(*) AS user_count
FROM
    netflix_users
GROUP BY
    age_group,
    device
ORDER BY
    age_group,
    user_count DESC;
	
--
