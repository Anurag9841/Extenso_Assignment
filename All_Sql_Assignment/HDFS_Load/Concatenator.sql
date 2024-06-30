
drop procedure if exists main_database.executor_mappings_without_cursor;

DELIMITER //
CREATE PROCEDURE main_database.executor_mappings_without_cursor(in _id int)
BEGIN 
	select group_concat(source_name,' AS ',destination_name) as field_mapping_query from  main_database.field_mapping where id = _id into @querys;
	select @querys;
END //
DELIMITER ;

call main_database.executor_mappings_without_cursor(1);


-- Code from mentor 

create table main_database.field_mapping_query
select a.id, CONCAT('SELECT ',group_concat(concat(source_name,' AS ',destination_name) separator ','),' from ',schema_names,'.',table_names) as field_mapping_query 
from   main_database.cf_etl_table a
JOIN main_database.field_mapping b on a.id=b.id
group by a.id;

select * from main_database.field_mapping_query;
