use client_rw;

create table transaction_summary(
	Account_No varchar(50),
    DATE datetime,
    TRANSACTION_DETAILS varchar(100),
    Cheq_No varchar(10),
    VALUE_DATE datetime,
    WITHDRAWL_AMOUNT decimal(20,2),
    DEPOSIT_AMT decimal(20,2),
    BALANCE_AMT decimal(20,2));
show tables;
SHOW VARIABLES LIKE "secure_file_priv";

LOAD DATA INFILE "F:/transaction_summary_1.csv" 
INTO TABLE transaction_summary
FIELDS terminated by','
enclosed by '"'
lines terminated by '\r\n'
Ignore 1 rows
(Account_No,@DATE,TRANSACTION_DETAILS,Cheq_No,@VALUE_DATE,@WITHDRAWL_AMOUNT,@DEPOSIT_AMT,@BALANCE_AMT)
set
DATE = str_to_date(@DATE,"%d-%b-%y"),
VALUE_DATE = str_to_date(@VALUE_DATE,"%d-%b-%y"),
WITHDRAWL_AMOUNT = 	if(@WITHDRAWL_AMOUNT = '',0,@WITHDRAWL_AMOUNT),
DEPOSIT_AMT = if(@DEPOSIT_AMT = '',0,@DEPOSIT_AMT),
BALANCE_AMT = IF(@BALANCE_AMT='',0,@BALANCE_AMT);

SELECT * FROM transaction_summary;

create table trans_summ2 as
select * from(
select *,
CASE 
when WITHDRAWL_AMOUNT = 0 AND DEPOSIT_AMT != 0 THEN DEPOSIT_AMT
WHEN WITHDRAWL_AMOUNT != 0 AND DEPOSIT_AMT  = 0 THEN -WITHDRAWL_AMOUNT
ELSE DEPOSIT_AMT - WITHDRAWL_AMOUNT
END AS BALANCE FROM transaction_summary) as anurag;

select * from trans_summ2;

ALTER TABLE trans_summ2
ADD transaction_id INT AUTO_INCREMENT,
ADD PRIMARY KEY (transaction_id);
select * from trans_summ2;
select *, sum(BALANCE) over (order by id asc) balance_amt
from trans_summ
order by id;

select *,
CASE 
when WITHDRAWL_AMOUNT = 0 AND DEPOSIT_AMT != 0 THEN DEPOSIT_AMT
WHEN WITHDRAWL_AMOUNT != 0 AND DEPOSIT_AMT  = 0 THEN -WITHDRAWL_AMOUNT
ELSE DEPOSIT_AMT - WITHDRAWL_AMOUNT
END AS BALANCE FROM transaction_summary;
    

