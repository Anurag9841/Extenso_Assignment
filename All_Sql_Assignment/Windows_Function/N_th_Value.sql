CREATE TABLE basic_pays(
    employee_name VARCHAR(50) NOT NULL,
    department VARCHAR(50) NOT NULL,
    salary INT NOT NULL,
    PRIMARY KEY (employee_name , department)
);

INSERT INTO 
	basic_pays(employee_name, 
			   department, 
			   salary)
VALUES
	('Diane Murphy','Accounting',8435),
	('Mary Patterson','Accounting',9998),
	('Jeff Firrelli','Accounting',8992),
	('William Patterson','Accounting',8870),
	('Gerard Bondur','Accounting',11472),
	('Anthony Bow','Accounting',6627),
	('Leslie Jennings','IT',8113),
	('Leslie Thompson','IT',5186),
	('Julie Firrelli','Sales',9181),
	('Steve Patterson','Sales',9441),
	('Foon Yue Tseng','Sales',6660),
	('George Vanauf','Sales',10563),
	('Loui Bondur','SCM',10449),
	('Gerard Hernandez','SCM',6949),
	('Pamela Castillo','SCM',11303),
	('Larry Bott','SCM',11798),
	('Barry Jones','SCM',10586);
    
    SELECT * FROM basic_pays;
    
select *,nth_value(employee_name,2)over
(partition by department order by salary desc range
between unbounded preceding and unbounded following) 
as Second_highest from basic_pays;
