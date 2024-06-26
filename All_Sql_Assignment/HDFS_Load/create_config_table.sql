use main_database;

drop table if exists cf_etl_table;

create table cf_etl_table(
id int auto_increment primary key,
Schema_names varchar(20),
Table_names varchar(20),
location varchar(100),
hdfs_file_name varchar(20),
start_date_time datetime,
end_date_time datetime,
is_incremental int,
execution_date date default null,
inc_field varchar(20),
partition_by varchar(20),
interval_days int
);

insert into cf_etl_table(Schema_names,Table_names,location,hdfs_file_name,start_date_time,
end_date_time,is_incremental,inc_field,partition_by,interval_days)
values ('test_database1','sample_data','hdfs://localhost:19000//airflow/','sample_data','2023-06-23 00:00:00','2023-06-25 23:59:59',1,'Tnx_date','Dates',1),
('test_database2','sample_data1','hdfs://localhost:19000//airflow/','sample_data1',Null,Null,0,Null,Null,Null);

-- DELETE FROM cf_etl_table WHERE id = 1;
select * from cf_etl_table;

drop table if exists  main_database.field_mapping;

create table field_mapping(
id int,
source_name varchar(50),
destination_name varchar(50)
);
insert into field_mapping(id,source_name,destination_name)
values (1,'tnxid','Transaction_id'),
(1,'Tnx_date','Transaction_date'),
(1,'acc_id','Account_id'),
(1,'product','Products'),
(1,'Date(Tnx_date)','Dates'),
(2,'tnxid','Transaction_id_ni'),
(2,'Tnx_date','Transaction_date_ni'),
(2,'acc_id','Account_id_ni'),
(2,'product','Products_ni'),
(2,'Date(Tnx_date)','Dates_ni');

select * from main_database.field_mapping;

-- select tnxid as Transaction_id,Tnx_date as Transaction_date,acc_id as Account_id,product as Products from test_database1.sample_data;