------ Cohort Analysis & User Segmentation ------


/* 1.1.	Basic Retention Curve: 
Task: 
A.	As you know that 'Telco Card' is the most product in the Telco group (accounting for more than 99% of the total).
You want to evaluate the quality of user acquisition in Jan 2019 by the retention metric.
First, you need to know how many users are retained in each subsequent month from the first month (Jan 2019)
they pay the successful transaction (only get data of 2019). 
*/



WITH subsequent_month AS(
SELECT DISTINCT customer_id
    ,transaction_time
    ,MIN(month(transaction_time)) OVER(PARTITION BY customer_id) as first_trans_cus
    ,DATEDIFF(MONTH,(MIN((transaction_time)) OVER(PARTITION BY customer_id )),transaction_time) as subsequent_month
FROM  fact_transaction_2019 as tr_19
JOIN dim_scenario as sce 
ON tr_19.scenario_id = sce.scenario_id 
WHERE status_id = 1 AND sub_category = 'Telco Card' 
)
SELECT subsequent_month
    ,COUNT(DISTINCT customer_id) as retained_users
FROM subsequent_month 
WHERE first_trans_cus = 1
GROUP BY  subsequent_month


/* 
B. You realize that the number of retained customers has decreased over time.
Let’s calculate retention =  number of retained customers / total users of the first month. 
*/
n

WITH subsequent_month AS(
SELECT distinct customer_id
    ,transaction_time
    ,MIN(month(transaction_time)) OVER(PARTITION BY customer_id) as first_trans_cus
    ,DATEDIFF(MONTH,(MIN((transaction_time)) OVER(PARTITION BY customer_id )),transaction_time) as subsequent_month
FROM  fact_transaction_2019 as tr_19
JOIN dim_scenario as sce 
ON tr_19.scenario_id = sce.scenario_id 
WHERE status_id = 1 AND sub_category = 'Telco Card' 
)
, retained_user AS(
SELECT subsequent_month
    ,COUNT(distinct customer_id) as retained_users
FROM subsequent_month 
WHERE first_trans_cus = 1
GROUP BY subsequent_month
)
SELECT *
    , FIRST_VALUE(retained_users) OVER( ORDER BY subsequent_month) AS original_users
    , FORMAT(1.0*retained_users/FIRST_VALUE(retained_users) OVER( ORDER BY subsequent_month ASC), 'p') AS pct_retained_users
FROM retained_user

/*
1.2.	Cohorts Derived from the Time Series Itself
Task: Expend your previous query to calculate retention for multi attributes from the acquisition month (from Jan to December). 
*/



WITH subsequent_month AS(
SELECT distinct customer_id
    ,transaction_time
    ,MIN(month(transaction_time)) OVER(PARTITION BY customer_id) as first_trans_cus
    ,DATEDIFF(MONTH,(MIN((transaction_time)) OVER(PARTITION BY customer_id )),transaction_time) as subsequent_month
FROM  fact_transaction_2019 as tr_19
JOIN dim_scenario as sce 
ON tr_19.scenario_id = sce.scenario_id 
WHERE status_id = 1 AND sub_category = 'Telco Card' 
)
, retained_user AS(
SELECT first_trans_cus as acquisition_month
    ,subsequent_month
    ,COUNT(distinct customer_id) as retained_users
FROM subsequent_month 
GROUP BY subsequent_month,first_trans_cus
)
SELECT *
    ,FIRST_VALUE(retained_users) OVER( PARTITION BY acquisition_month ORDER BY subsequent_month) AS original_users
    ,FORMAT(1.0*retained_users/FIRST_VALUE(retained_users) OVER( PARTITION BY acquisition_month ORDER BY subsequent_month ASC), 'p') AS pct_retained_users
INTO #retention_months -- lưu vào bảng local 
FROM retained_user


SELECT acquisition_month
    , original_users
    , "0", "1", "2", "3","4", "5", "6", "7","8", "9", "10", "11"
FROM (
    SELECT acquisition_month, subsequent_month, original_users,  pct_retained_users
    FROM #retention_months
) AS source_table 
PIVOT (
    MIN(pct_retained_users)
    FOR subsequent_month IN ("0", "1", "2", "3","4", "5", "6", "7","8", "9", "10", "11")
) pivot_table
ORDER BY acquisition_month

/* 2.	USER SEGMENTATION:
----------RFM Segmentation: Recency, Frequency,Monetary
*/ 

/* 2.1.	The first step in building an RFM model is to assign Recency, Frequency and Monetary values to each customer.
Let’s calculate these metrics for all successful paying customer of ‘Telco Card’ in 2019 and 2020: 
•	Recency: Difference between each customer's last payment date and '2020-12-31'
•	Frequency: Number of successful payment days of each customer
•	Monetary: Total charged amount of each customer 
*/


(SELECT  * 
INTO #tr_19_20
FROM  fact_transaction_2019 as tr_19 WHERE status_id = 1
UNION
SELECT * FROM fact_transaction_2020 as tr_20  WHERE status_id = 1)

WITH rfm_metric as (
SELECT  customer_id
    ,DATEDIFF(day,(MAX(transaction_time)),'2020 -12-31') as Recency
    , COUNT (DISTINCT CONVERT(varchar(10), transaction_time, 102)) AS frequency -- đếm số ngày thanh toán, CONVERT về DATE 
    , SUM(1.0*charged_amount) AS monetary
FROM #tr_19_20 
JOIN dim_scenario as sce 
ON #tr_19_20.scenario_id  = sce.scenario_id
WHERE sub_category = 'Telco Card' 
group BY customer_id
-- ORDER by customer_id
)
, rfm_rank AS (
SELECT *
    , PERCENT_RANK() OVER ( ORDER BY recency ASC ) AS r_percent_rank
    , PERCENT_RANK() OVER ( ORDER BY frequency DESC ) AS f_percent_rank
    , PERCENT_RANK() OVER ( ORDER BY monetary DESC ) AS m_percent_rank
FROM rfm_metric
)
, rfm_tier AS ( 
SELECT *
    , CASE WHEN r_percent_rank > 0.75 THEN 4
        WHEN r_percent_rank > 0.5 THEN 3
        WHEN r_percent_rank > 0.25 THEN 2
        ELSE 1 END AS r_tier
    , CASE WHEN f_percent_rank > 0.75 THEN 4
        WHEN f_percent_rank > 0.5 THEN 3
        WHEN f_percent_rank > 0.25 THEN 2
        ELSE 1 END AS f_tier
    , CASE WHEN m_percent_rank > 0.75 THEN 4
        WHEN m_percent_rank > 0.5 THEN 3
        WHEN m_percent_rank > 0.25 THEN 2
        ELSE 1 END AS m_tier
FROM rfm_rank
)
, rfm_group AS ( 
SELECT * 
    , CONCAT(r_tier, f_tier, m_tier) AS rfm_score -- tạo 1 cái score
FROM rfm_tier
) -- Step 3: Grouping these customers based on segmentation rules
, segment_table AS (
SELECT *
    , CASE 
        WHEN rfm_score  =  111 THEN 'Best Customers'
        WHEN rfm_score LIKE '[3-4][3-4][1-4]' THEN 'Lost Bad Customer'
        WHEN rfm_score LIKE '[3-4]2[1-4]' THEN 'Lost Customers'
        WHEN rfm_score LIKE  '21[1-4]' THEN 'Almost Lost' -- sắp lost 
        WHEN rfm_score LIKE  '11[2-4]' THEN 'Loyal Customers'
        WHEN rfm_score LIKE  '[1-2][1-3]1' THEN 'Big Spenders'
        WHEN rfm_score LIKE  '[1-2]4[1-4]' THEN 'New Customers' 
        WHEN rfm_score LIKE  '[3-4]1[1-4]' THEN 'Hibernating'
        WHEN rfm_score LIKE  '[1-2][2-3][2-4]' THEN 'Potential Loyalists'
    ELSE 'unknown'
    END AS segment -- cố gắng ưu tiên tìm những segment muốn đầu tiên trước.
FROM rfm_group
)
SELECT
    segment
    , COUNT( customer_id) AS number_users 
    , SUM( COUNT( customer_id)) OVER() AS total_users
    , FORMAT( 1.0*COUNT( customer_id) / SUM( COUNT( customer_id)) OVER(), 'p') AS pct
FROM segment_table
GROUP BY segment
ORDER BY number_users DESC






