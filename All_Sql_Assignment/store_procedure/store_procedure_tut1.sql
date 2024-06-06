create database store_procedure;

use store_procedure;
drop table cars;

CREATE TABLE cars (
	id int auto_increment primary key,
    make varchar(100),
    model varchar(100),
    year int,
    value decimal(10, 2)
);

INSERT INTO cars(make,model,year,value)
VALUES
('Porsche', '911 GT3', 2020, 169700),
('Porsche1', 'Cayman GT4', 2018, 118000),
('Porsche2', 'Panamera', 2022, 113200),
('Porsche3', 'Macan', 2019, 27400),
('Porsche4', '718 Boxster', 2017, 48880),
('Ferrari1', '488 GTB', 2015, 254750),
('Ferrari2', 'F8 Tributo', 2019, 375000),
('Ferrari3', 'SF90 Stradale', 2020, 627000),
('Ferrari4', '812 Superfast', 2017, 335300),
('Ferrari5', 'GTC4Lusso', 2016, 268000);
select * from cars;

DELIMITER //
CREATE procedure GET_VALS()
begin
	SELECT * FROM cars order by make,value desc;
END//

DELIMITER ;

DELIMITER //

CREATE PROCEDURE GET_CARS(IN IN_DATES INT,IN OUT_DATES INT)

BEGIN 
	SELECT * FROM cars where year >= IN_DATES AND year <= OUT_DATES;
END //
DELIMITER ;

call GET_CARS(2016,2018);

DELIMITER //
CREATE PROCEDURE FILTER_BY_YEARS(IN YEARS INT)
BEGIN
	SELECT * FROM cars where year = YEARS;
END //

DELIMITER ;

drop procedure IF EXISTS FILTER_BY_YEARS;


DELIMITER //

CREATE PROCEDURE CAR_STATS(IN YEARS INT,
OUT CAR_NUM INT,
OUT MINS DECIMAL(10,2),
OUT MAXS DECIMAL(10,2),
OUT AVERAGE DECIMAL(10,2))
BEGIN 
SELECT COUNT(*),MIN(value),MAX(value),AVG(value) INTO
CAR_NUM,MINS,MAXS,AVERAGE FROM CARS WHERE year = YEARS order by make,value desc;
END //
DELIMITER ;



