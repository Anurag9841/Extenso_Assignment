create database client_rw;

Use client_rw;

select * from fc_account_master;

select * from fc_transaction_base;

 ALTER TABLE fc_transaction_base
 RENAME COLUMN ï»¿tran_date to tran_date;
select * from fc_transaction_base;

SELECT
    fc_transaction_base.account_number as acc_num,
    fc_account_master.customer_code as customer_code,
    month(tran_date),
    AVG(CASE WHEN fc_transaction_base.dc_indicator = "withdraw" THEN fc_transaction_base.lcy_amount ELSE NULL END) AS avg_monthly_withdraw,
    STD(CASE WHEN fc_transaction_base.dc_indicator = "withdraw" THEN fc_transaction_base.lcy_amount ELSE NULL END) AS std_monthly_withdraw,
    VARIANCE(CASE WHEN fc_transaction_base.dc_indicator = "withdraw" THEN fc_transaction_base.lcy_amount ELSE NULL END) AS var_monthly_withdraw,
    AVG(CASE WHEN fc_transaction_base.dc_indicator = "deposit" THEN fc_transaction_base.lcy_amount ELSE NULL END) AS avg_monthly_deposit,
    STD(CASE WHEN fc_transaction_base.dc_indicator = "deposit" THEN fc_transaction_base.lcy_amount ELSE NULL END) AS std_monthly_deposit,
    VARIANCE(CASE WHEN fc_transaction_base.dc_indicator = "deposit" THEN fc_transaction_base.lcy_amount ELSE NULL END) AS var_monthly_deposit
FROM fc_transaction_base
join fc_account_master on fc_account_master.account_number = fc_transaction_base.account_number
GROUP BY fc_transaction_base.account_number,fc_account_master.customer_code,month(fc_transaction_base.tran_date) ;

select * from fc_account_master;

create database fc_facts;

use fc_facts;
CREATE TABLE fc_deposi_facts AS
SELECT * FROM (SELECT
    fc_transaction_base.account_number as acc_num,
    fc_account_master.customer_code as customer_code,
    month(tran_date),
    AVG(CASE WHEN fc_transaction_base.dc_indicator = "withdraw" THEN fc_transaction_base.lcy_amount ELSE NULL END) AS avg_monthly_withdraw,
    STD(CASE WHEN fc_transaction_base.dc_indicator = "withdraw" THEN fc_transaction_base.lcy_amount ELSE NULL END) AS std_monthly_withdraw,
    VARIANCE(CASE WHEN fc_transaction_base.dc_indicator = "withdraw" THEN fc_transaction_base.lcy_amount ELSE NULL END) AS var_monthly_withdraw,
    AVG(CASE WHEN fc_transaction_base.dc_indicator = "deposit" THEN fc_transaction_base.lcy_amount ELSE NULL END) AS avg_monthly_deposit,
    STD(CASE WHEN fc_transaction_base.dc_indicator = "deposit" THEN fc_transaction_base.lcy_amount ELSE NULL END) AS std_monthly_deposit,
    VARIANCE(CASE WHEN fc_transaction_base.dc_indicator = "deposit" THEN fc_transaction_base.lcy_amount ELSE NULL END) AS var_monthly_deposit
FROM fc_transaction_base
join fc_account_master on fc_account_master.account_number = fc_transaction_base.account_number
GROUP BY fc_transaction_base.account_number,fc_account_master.customer_code,month(fc_transaction_base.tran_date))as alias;

select * from fc_transaction_base 

