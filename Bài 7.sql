/* 1.1A:A.	As you know that 'Telco Card' is the most product in the Telco group (accounting for more than 99% of the total).
You want to evaluate the quality of user acquisition in Jan 2019 by the retention metric.
First, you need to know how many users are retained in each subsequent month from the first month (Jan 2019)
they pay the successful transaction (only get data of 2019). 
*/ 


select customer_id
    ,MONTH(transaction_time) as month
into #users_month_1
from fact_transaction_2019 as fact_2019 
LEFT JOIN dim_scenario as scena 
on fact_2019.scenario_id = scena.scenario_id
where sub_category = 'Telco Card'
and MONTH(transaction_time) = 1
and status_id = 1

with retained_users as(
SELECT MONTH(transaction_time) as month
    ,count(distinct fact_2019.customer_id) as retained_users
from #users_month_1 
JOIN  fact_transaction_2019 as fact_2019 
on fact_2019.customer_id = #users_month_1.customer_id
GROUP BY MONTH(transaction_time)
)
select ISNULL((LAG(month,1) OVER(order by month)),0) as subsequent_month
    ,retained_users
into   #retained_users
from retained_users


/* 1.1B:You realize that the number of retained customers has decreased over time
Let’s calculate retention =  number of retained customers / total users of the first month. 
*/

with original_users as(
select  subsequent_month
    ,retained_users
    ,(FIRST_VALUE(retained_users) OVER(order by retained_users desc)) as original_users 
    -- ,pct_retained = 1.0*retained_users/(FIRST_VALUE(retained_users) OVER(order by retained_users))  
from #retained_users
-- ORDER BY subsequent_month asc
)
select * 
    ,FORMAT((1.0* retained_users / original_users) ,'p') as pct_retained
into #pct_retained
from original_users
---------
select case when subsequent_month BETWEEN 0 AND 11 then '1'  end as acquisition_month
    ,subsequent_month
    ,retained_users
    ,original_users
    ,pct_retained
into #acquisition_month_19
from #pct_retained
ORDER BY subsequent_month


/* 1.2 Cohorts Derived from the Time Series Itself
Task: Expend your previous query to calculate retention for multi attributes from the acquisition month (from Jan to December). (Hard)
Let’s see the desired outcome:
*/ 


-- Em chưa hiểu đề luôn ạ :v -----




/*2.1	The first step in building an RFM model is to assign Recency, Frequency and Monetary values to each customer.
Let’s calculate these metrics for all successful paying customer of ‘Telco Card’ in 2019 and 2020: 
•	Recency: Difference between each customer's last payment date and '2020-12-31'
•	Frequency: Number of successful payment days of each customer
•	Monetary: Total charged amount of each customer 
*/

select *
into #fact_table
from fact_transaction_2019
UNION
SELECT *
from fact_transaction_2020

select DISTINCT customer_id  
    -- ,CONVERT(varchar,transaction_time,102) as day
    -- ,MAX(CONVERT(varchar,transaction_time,102)) OVER(partition by customer_id) as last_date
    ,DATEDIFF(DAY,MAX(CONVERT(varchar,transaction_time,102)) OVER(partition by customer_id),'2020.12.31') as diff_date
    ,count(transaction_time) OVER(PARTITION BY customer_id ) as number_day_transaction 
    ,Sum(charged_amount) OVER(PARTITION BY customer_id)  as total_amount
from #fact_table 
LEFT JOIN dim_scenario as scen
on #fact_table .scenario_id = scen.scenario_id
where sub_category= 'Telco Card'
and status_id = 1


