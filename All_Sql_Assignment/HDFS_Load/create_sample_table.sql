use  test_database1;
create table sample_data(
	tnxid int auto_increment primary key,
    Tnx_date datetime,
    acc_id varchar(10),
    product varchar(20)
);

INSERT INTO sample_data (Tnx_date, acc_id, product) VALUES
('2023-06-23 10:30:00', 'ACC001', 'ProductA'),
('2023-06-24 11:00:00', 'ACC002', 'ProductB'),
('2023-06-25 09:45:00', 'ACC003', 'ProductC'),
('2023-06-26 12:15:00', 'ACC004', 'ProductD'),
('2023-06-27 14:30:00', 'ACC005', 'ProductE'),
('2023-06-28 08:20:00', 'ACC006', 'ProductF'),
('2023-06-29 16:50:00', 'ACC007', 'ProductG'),
('2023-06-30 13:10:00', 'ACC008', 'ProductH');

select * from sample_data;