-- Time Series Analysis --

/* 1.1.	Simple trend
Task: You need to analyze the trend of payment transactions of Billing category from 2019 to 2020.
First, let’s show the trend of the number of successful transactions by month. 
*/


-- Bảng tạm   #trans_19_20 --
select * 
    into #trans_19_20 
from fact_transaction_2019 as trans_19
UNION 
select * from fact_transaction_2020 as trans_20

---
select YEAR(transaction_time) as [year]
    , month(transaction_time) as [month]
    ,case when  month(transaction_time) <=9 then CONCAT(YEAR(transaction_time),'0',month(transaction_time)) 
    else CONCAT(YEAR(transaction_time),month(transaction_time))  end as time_calendar
    ,count(transaction_id) as number_trans
from #trans_19_20 
LEFT JOIN dim_scenario as sce 
on #trans_19_20.scenario_id = sce.scenario_id
where status_id = 1
and category = 'Billing'
GROUP BY  Year(transaction_time),Month(transaction_time)  



/* 1.2.	Comparing Component
Task: You know that there are many sub-categories of Billing group.
After reviewing the above result, you should break down the trend into each sub-categories.
*/


select YEAR(transaction_time) as [year]
    , month(transaction_time) as [month]
    ,sub_category
    ,count(transaction_id) as number_trans
from #trans_19_20 
LEFT JOIN dim_scenario as sce 
on #trans_19_20.scenario_id = sce.scenario_id
where status_id = 1
and category = 'Billing'
GROUP BY  Year(transaction_time),Month(transaction_time),sub_category

/* Then modify the result as the following table:
Only select the sub-categories belong to list (Electricity, Internet and Water) 
*/

select YEAR(transaction_time) as [year]
    , month(transaction_time) as [month]
    ,Count(case when sub_category = 'Electricity' then (transaction_id) end) as electricity
    ,count(case when sub_category = 'Internet' then (transaction_id) end) as Internet
    ,count(case when sub_category = 'Water' then (transaction_id) end) as Water
from #trans_19_20 
LEFT JOIN dim_scenario as sce 
on #trans_19_20.scenario_id = sce.scenario_id
where status_id = 1
and category = 'Billing'
GROUP BY  Year(transaction_time),Month(transaction_time)
ORDER BY [year],[month]

/* 1.3.	Percent of Total Calculations:
When working with time series data that has multiple parts or attributes that constitute a whole,
it’s often useful to analyze each part’s contribution to the whole and whether that has changed over time.
Unless the data already contains a time series of the total values,
we’ll need to calculate the overall total in order to calculate the percent of total for each row. 
Task: Based on the previous query,
you need to calculate the proportion of each sub-category (Electricity, Internet and Water) in the total for each month.
*/


with sub_month as(
select YEAR(transaction_time) as [year]
    , month(transaction_time) as [month]
    ,Count(case when sub_category = 'Electricity' then (transaction_id) end) as electricity_trans
    ,count(case when sub_category = 'Internet' then (transaction_id) end) as internet_trans
    ,count(case when sub_category = 'Water' then (transaction_id) end) as water_trans
from #trans_19_20 
LEFT JOIN dim_scenario as sce 
on #trans_19_20.scenario_id = sce.scenario_id
where status_id = 1
and category = 'Billing'
GROUP BY  Year(transaction_time),Month(transaction_time)
-- ORDER BY [year],[month]
)
, total_month AS ( 
    SELECT * 
    , ISNULL(electricity_trans,0) + ISNULL(internet_trans,0) + ISNULL(water_trans,0) AS total_trans_month
FROM sub_month
)
SELECT *
    , FORMAT(1.0*electricity_trans/total_trans_month, 'p') AS elec_pct
    , FORMAT(1.0*internet_trans/total_trans_month, 'p') AS iternet_pct
    , FORMAT(1.0*water_trans/total_trans_month, 'p') AS water_pct
FROM total_month


/* 1.4.	Indexing to See Percent Change over Time:
Indexing data is a way to understand the changes in a time series relative to a base period (starting point).
Indices are widely used in economics as well as business settings.
Task: Select only these sub-categories in the list (Electricity, Internet and Water),
you need to calculate the number of successful paying customers for each month (from 2019 to 2020).
Then find the percentage change from the first month (Jan 2019) for each subsequent month.
*/ 



select YEAR(transaction_time) as [year]
    , month(transaction_time) as [month]
    ,COUNT (DISTINCT customer_id) as number_cus
    ,FIRST_VALUE(COUNT (DISTINCT customer_id)) OVER( ORDER BY YEAR(transaction_time),month(transaction_time)) as starting_point 
    ,FORMAT (1.0*COUNT (DISTINCT customer_id)/FIRST_VALUE(COUNT (DISTINCT customer_id)) OVER( ORDER BY YEAR(transaction_time),month(transaction_time)) -1 , 'p') AS diff_pct 
from #trans_19_20 
LEFT JOIN dim_scenario as sce 
on #trans_19_20.scenario_id = sce.scenario_id
where status_id = 1
and sub_category in ('Electricity','Internet','Water')
GROUP BY  Year(transaction_time),Month(transaction_time)
ORDER BY [year],[month]


/* 2.1.	Calculating Rolling Time Windows
Task: Select only these sub-categories in the list (Electricity, Internet and Water),
you need to calculate the number of successful paying customers for each week number from 2019 to 2020).
Then get rolling annual paying users of this group. 
*/
with week_user AS (
select YEAR(transaction_time) as [year]
    ,DATEPART(WEEK,transaction_time) as [week_number]
    ,COUNT( DISTINCT customer_id ) AS number_customer
from #trans_19_20 
LEFT JOIN dim_scenario as sce 
on #trans_19_20.scenario_id = sce.scenario_id
where status_id = 1
and sub_category in ('Electricity','Internet','Water')
GROUP BY  Year(transaction_time),DATEPART(WEEK,transaction_time)
-- ORDER BY [year],[week] 
)
SELECT *
    , SUM(number_customer) OVER ( PARTITION BY year ORDER BY week_number ASC ) AS rolling_customer_year
FROM week_user


/* 2.2
Task: Based on the previous query, calculate the average number of customers for the last 4 weeks in each observation week. 
Then compare the difference between the current value and the average value of the last 4 weeks.
*/ 

WITH week_user AS (
SELECT YEAR(transaction_time) year, DATEPART(week, transaction_time) AS week_number
    , COUNT( DISTINCT customer_id ) AS number_customer
FROM  #trans_19_20 
JOIN dim_scenario AS scena ON #trans_19_20.scenario_id = scena.scenario_id
WHERE category = 'Billing' AND status_id = 1 AND sub_category IN ('Electricity', 'Internet',  'Water')
GROUP BY YEAR(transaction_time), DATEPART(week, transaction_time)
)
-- Cần tính trung bình 4 tuần gần nhất --> trả kết quả về dòng hiện tại 
SELECT *
    , AVG(number_customer) OVER ( PARTITION BY year ORDER BY week_number ASC 
                                    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW ) AS avg_last_4_weeks
FROM week_user
