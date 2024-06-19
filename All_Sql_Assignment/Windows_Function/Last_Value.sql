select * from overtime;

select *,last_value(employee_name)
over(partition by deprtment order by 
hours range between unbounded preceding and unbounded following) as most_hours_worked
from overtime;