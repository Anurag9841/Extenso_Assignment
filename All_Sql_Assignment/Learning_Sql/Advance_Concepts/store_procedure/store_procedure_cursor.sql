-- use store_procedure;
use store_procedure;
delimiter //
create procedure cursors(inout num text)
begin
	declare done bool default false;
    declare numlist int default 0;
    declare cur cursor for select ones from multi;
	DECLARE CONTINUE HANDLER 
	FOR NOT FOUND SET done = true;
    open cur;
    set num = 0;
    process_num : loop
    fetch cur into numlist;
    if done = true then
		leave process_num;
	end if;
	SET num= CONCAT(num,";",numlist);
	END LOOP;
    close cur;   
end //
delimiter ;

call cursors(@num);
select @num;
