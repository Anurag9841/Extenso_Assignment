use windows_function;
select * from sales;

select *,lag(sale,1,0) 
over(partition  by sales_employee order by fiscal_year) as prev_year_sale,sale - lag(sale,1,0) 
over(partition  by sales_employee order by fiscal_year)as difference from sales;
