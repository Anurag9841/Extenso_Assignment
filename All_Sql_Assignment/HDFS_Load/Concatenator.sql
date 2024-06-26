drop procedure if exists executor_mappings_without_cursor;
DELIMITER //
CREATE PROCEDURE main_database.executor_mappings_without_cursor(in _id int)
BEGIN 
	select group_concat(source_name,' AS ',destination_name) as field_mapping_query from  main_database.field_mapping where id = _id into @querys;
    
	select @querys;
END //
DELIMITER ;


call main_database.executor_mappings_without_cursor(1);