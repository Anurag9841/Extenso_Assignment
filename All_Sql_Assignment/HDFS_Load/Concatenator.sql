
drop procedure if exists main_database.executor_mappings_without_cursor;

DELIMITER //
CREATE PROCEDURE main_database.executor_mappings_without_cursor(in _id int)
BEGIN 
	select group_concat(source_name,' AS ',destination_name) as field_mapping_query from  main_database.field_mapping where id = _id into @querys;
	select @querys;
END //
DELIMITER ;

call main_database.executor_mappings_without_cursor(1);


-- tnxid AS Transaction_id,Tnx_date AS Transaction_date,acc_id AS Account_id,product AS Products,Date(Tnx_date) AS Dates
-- tnxid AS Transaction_id_ni,Tnx_date AS Transaction_date_ni,acc_id AS Account_id_ni,product AS Products_ni,Date(Tnx_date) AS Dates_ni