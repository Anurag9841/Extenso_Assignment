-- use store_procedure;
-- CREATE TABLE calendars (
--     date DATE PRIMARY KEY,
--     month INT NOT NULL,
--     quarter INT NOT NULL,
--     year INT NOT NULL
-- );

-- delimiter //
-- create procedure filldates(in startdate Date,in enddate Date) 
-- begin
-- 	declare currentdate date default startdate;
--     insert_date :loop
-- 		set currentdate = date_add(currentdate,interval 1 Day);	
--     if currentdate > enddate then
-- 		leave insert_date;
-- 	end if;    
--     insert into calendars(date,month,quarter,year)
--     values(currentdate,month(currentdate),quarter(currentdate),year(currentdate));
-- end loop;        
-- end //
-- delimiter ;

-- drop table multi;
-- CREATE TABLE multi (
--     ones int PRIMARY KEY,
--     two int ,
-- 	three int,
-- 	four int
-- );

delimiter //
-- drop procedure multiple //
create procedure multiple(in lowerbound int,in upperbound int)
begin
	declare val1 int default 0;
    declare val2 int default 0;
    declare val3 int default val1 + val2;
    insert_into : loop
		set val1 =val1 + 1;
        set val2 = val2 +1;
        set val3 = val1 + val2;
	if val1 > upperbound and val2 > upperbound then 
		leave insert_into;
	end if;
    insert into multi(ones,two,three,four)
    values (val1,val2,val3,val1+val2+val3);
    end loop;
end //
delimiter ;

CALL fillDates('2024-01-01','2024-12-31');
select * from calendars;

call multiple(1,10);
select * from multi;