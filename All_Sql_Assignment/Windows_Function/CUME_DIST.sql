use windows_function;
drop table scores;
CREATE TABLE scores (
    name VARCHAR(20) PRIMARY KEY,
    score INT NOT NULL
);
Insert into scores(name,score)
Values
	('Smith',81),
	('Jones',55),
	('Williams',55),
	('Taylor',62),
	('Brown',62),
	('Davies',84),
	('Evans',87),
	('Wilson',72),
	('Thomas',72),
	('Johnson',100);

select * from scores;

select *,row_number()over(order by score)as row_num,
cume_dist() over(order by score) as cum_dist_val
from scores;