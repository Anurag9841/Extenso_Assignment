-- use store_procedure;
-- CREATE TABLE calendar (
--     date DATE PRIMARY KEY,
--     month INT NOT NULL,
--     quarter INT NOT NULL,
--     year INT NOT NULL
-- );

-- delimiter //
-- create procedure insertCalender(in currentdate date)
-- begin 
-- 	insert into calendar(date,month,quarter,year)
--     values (currentdate,month(currentdate),quarter(currentdate),year(currentdate));
-- end //
-- delimiter ;

delimiter //
-- drop procedure inserts//
create procedure inserts(in start_date date,in days int)
begin
declare counter int default 0;
declare startdate date default start_date;
while counter < days do
	call insertCalender(startdate);
    set counter = counter + 1;
    set startdate = date_add(startdate,interval 1 day);
end while;
end //
delimiter ;

call inserts('2021-01-01',365);
select * from calendar;multi