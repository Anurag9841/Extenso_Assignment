drop table ids;
create table ids (
pid int auto_increment primary key,
id int 
);

delimiter //


CREATE PROCEDURE adder(IN a INT)
BEGIN
    DECLARE idee INT DEFAULT 1;  

    WHILE idee <= a DO
        INSERT INTO ids (id)
        VALUES (idee), (idee + 1), (idee + 2), (idee + 3), (idee + 4),
               (idee + 5), (idee + 6), (idee + 7), (idee + 8), (idee + 9);
                SET idee = idee + 10;
    END WHILE;
END //

delimiter ;

call adder(10000);
select * from ids;
use windows_function;
select count(*) from ids;

select * from ids where id = 1;

create index ides on ids(id);

select * from ids where id = 100000;

