use client_rw;
select * from fc_transaction_base;

create table transaction_salary as
	select * from fc_transaction_base 
where description1 like '%salary%';

select * from transaction_salary;
