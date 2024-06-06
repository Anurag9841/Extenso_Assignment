use store_procedure;

-- DELIMITER //
-- CREATE PROCEDURE GET_ELEMENTS()
-- BEGIN
-- 	declare totalcount int default 0;
--     select count(*) into totalcount from cars;
--     SELECT totalcount;
-- END //
-- DELIMITER ;

-- delimiter //
-- create procedure get_sum(in years int)
-- begin
-- 	declare sum_val decimal(10,2) default 0;
--     select sum(value) into sum_val from cars where year = years;
--     select sum_val;
-- end//
-- delimiter ;


-- delimiter //
-- drop procedure if exists sp //
-- create procedure sp(in years int,out levels varchar(20))
-- begin
-- 	declare price decimal(10,2) default 0;
--     select value into price from cars where year = years;
--     if price >= 50000 then
-- 		set levels = "Diamond";
-- 	else
-- 		set levels = "Gold";
-- 	end if;
-- end //
-- delimiter ;

-- delimiter //
-- drop procedure if exists levels //
-- create procedure levels(in years int,out levels varchar(20))
-- begin
-- 	declare price decimal(10,2) default 0;
-- 	select value into price from cars where year = years;
--     if price <= 20000 then
-- 		set levels = "animal_level";
-- 	elseif price > 50000 then
-- 		set levels = "human_level";
-- 	else 
-- 		set levels = "god_level";
-- 	end if;
-- end //
-- delimiter ;

-- SHOW PROCEDURE STATUS where db = 'store_procedure';

-- drop procedure if exists casewhen ;

delimiter $$
create procedure casewhen (in ids int, out timely varchar(10))
begin
	declare nam varchar(20);
    select make into nam from cars where id = ids limit 1;
    case nam
    when 'Porsche' then 
		set timely = "no";
	when 'Ferrari' then
		set timely = "yes";
	else
		set timely = "none";
	end case;
end $$
delimiter ;

show databases;