select * from sales;

select *,lead(sale)over(partition by sales_employee order by fiscal_year) from sales;