create table overtime(
id  int auto_increment primary key,
employee_name varchar(100),
deprtment varchar(50),
hours int not null);
INSERT INTO overtime(employee_name, deprtment, hours)
VALUES('Diane Murphy','Accounting',37),
('Mary Patterson','Accounting',74),
('Jeff Firrelli','Accounting',40),
('William Patterson','Finance',58),
('Gerard Bondur','Finance',47),
('Anthony Bow','Finance',66),
('Leslie Jennings','IT',90),
('Leslie Thompson','IT',88),
('Julie Firrelli','Sales',81),
('Steve Patterson','Sales',29),
('Foon Yue Tseng','Sales',65),
('George Vanauf','Marketing',89),
('Loui Bondur','Marketing',49),
('Gerard Hernandez','Marketing',66),
('Pamela Castillo','SCM',96),
('Larry Bott','SCM',100),
('Barry Jones','SCM',65);

select * from overtime;

select *,first_value(employee_name) over(partition by deprtment order by hours) as least_val from overtime;