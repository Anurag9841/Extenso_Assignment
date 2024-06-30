USE moviesdb;

select * from movies;
DELIMITER $$
create procedure movies_name()
Begin
	select * from movies;
End $$
DELIMITER ;
call movies_name();

DELIMITER $$
create procedure find_indu(in industries char(25))
Begin
	select * from movies where industry = industries;
END $$
DELIMITER ;

call find_indu("Anurag");

select * from movies;
Delimiter $$
create procedure year(in id int)
Begin
	select * from movies where release_year = id;
End $$
Delimiter ;

call year(2022);