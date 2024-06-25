use main_database;
drop procedure if exists main_database.executor_mappings;
DELIMITER //

CREATE PROCEDURE main_database.executor_mappings(
    IN _id INT,
    IN schema_names VARCHAR(50),
    IN table_names VARCHAR(50),
    IN is_inc INT,
    IN inc_field VARCHAR(30),
    IN start_Date DATETIME,
    IN end_Date DATETIME
)
BEGIN 
    DECLARE done BOOL DEFAULT FALSE;
    DECLARE source_col VARCHAR(255);
    DECLARE dest_col VARCHAR(255);
    DECLARE select_col_change TEXT DEFAULT '';
    
    DECLARE col_cursor CURSOR FOR
        SELECT source_name, destination_name 
        FROM main_database.field_mapping 
        WHERE id = _id;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN col_cursor;
    
    col_loop: LOOP
        FETCH col_cursor INTO source_col, dest_col;
        IF done THEN 
            LEAVE col_loop;
        END IF;

        SET select_col_change = CONCAT(select_col_change, IF(LENGTH(select_col_change) > 0, ', ', ''), source_col, ' AS ', dest_col);
    END LOOP;
    CLOSE col_cursor;

    IF is_inc = 1 THEN
        SET @querys = CONCAT('SELECT ', select_col_change, ' FROM ', schema_names, '.', table_names, ' WHERE ', inc_field, ' BETWEEN ''', start_Date, ''' AND ''', end_Date, '''');
    ELSE
        SET @querys = CONCAT('SELECT ', select_col_change, ' FROM ', schema_names, '.', table_names);
    END IF;

    PREPARE stmt FROM @querys;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //

DELIMITER ;


