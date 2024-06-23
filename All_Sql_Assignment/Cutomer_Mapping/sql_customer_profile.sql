use extenso_config;

select * from most_used_prod;
-- Clearing all the table
drop table if exists customer_mapping;
drop table if exists months;
drop table if exists joined_rw_pcm;
drop table if exists most_used_prod;
drop table if exists filtered;
drop table if exists mapping;
drop table if exists customer_mapping;
drop table if exists final_customer_mapping;

-- Creating join of rw_transaction_data and product_category_map 
create table joined_rw_pcm as
select * from rw_transaction_data join product_category_map
using (product_id, product_type_id, module_id);

-- Slecting top 10 most used product 
create table most_used_prod as
select product_name,count(product_name)as cn from joined_rw_pcm group by(product_name) 
order by cn desc limit 10;

-- Creating table which contains only row from the 
-- joined_rw_pcm where product_name is in top 10 product 
create table filtered as
SELECT joined_rw_pcm.*
FROM joined_rw_pcm
JOIN most_used_prod
ON joined_rw_pcm.product_name = most_used_prod.product_name;

-- Converting last_modified_date to the date where all month starts from the 1st day 
alter table filtered add column months date;
SET SQL_SAFE_UPDATES = 0;
update filtered 
set months = date_format(last_modified_date,'%Y-%m-01');

-- Creating table mapping in which count of product is grouped by 
-- payer_acc_id,product_name and months
create table mapping as
select payer_account_id,product_name,months,count(amount) as cnt from filtered group by payer_account_id,product_name,months;

-- Creating table named months which contains the months between 
-- max and min from the table mapping  
create table months as
WITH RECURSIVE DateCTE AS (
    SELECT MIN(months) AS months
    FROM mapping
    UNION ALL
    SELECT DATE_ADD(months, INTERVAL 1 MONTH)
    FROM DateCTE
    WHERE DATE_ADD(months, INTERVAL 1 MONTH) <= (SELECT MAX(months) FROM mapping)
)
SELECT *
FROM DateCTE;

-- Implementation of Customer profile_mapping starts here 

-- Below codes dynamically creates pivot table  
SET @sql = NULL;

SELECT 
    GROUP_CONCAT(DISTINCT CONCAT(
        'COUNT(CASE WHEN months = "', months, '" THEN cnt ELSE null END) AS `', months, '`'
    )) 
INTO @sql
FROM months;

SET @sql = CONCAT('Create table customer_mapping as SELECT payer_account_id, product_name, ', @sql,
 ' FROM mapping GROUP BY payer_account_id, product_name ORDER BY payer_account_id');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Below code dynamically used to create mapping 
SELECT GROUP_CONCAT(DISTINCT CONCAT('`', months,'`') order by months)
INTO @distinct_month
FROM months;

SET @sql_query = CONCAT('create table final_customer_mapping as SELECT customer_mapping.*, CONCAT(', @distinct_month, ') AS sequence FROM customer_mapping');

PREPARE stmt FROM @sql_query;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

select * from final_customer_mapping;
