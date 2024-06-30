create table items(
id int auto_increment primary key,
items varchar(50) not null,
price float not null
);

INSERT INTO items(id, items, price) 
VALUES (1, 'Item', 50.00);

select * from items;

CREATE TABLE item_changes (
    change_id INT PRIMARY KEY AUTO_INCREMENT,
    item_id INT,
    change_type VARCHAR(10),
    change_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (item_id) REFERENCES items(id)
);


delimiter //
create trigger on_update after update
on items
for each row
begin
	insert into item_changes(item_id,change_type)
    values(new.id,'update');
end //
delimiter ;


delimiter //
create trigger on_insert after insert
on items 
for each row
begin
	insert into item_changes(item_id,change_type)
    values(new.id,'insert');
end //
delimiter ;

insert into items(items,price)
values ('Item2',500);

update items 
set price = 500
where id = 5;
select * from items;
select * from item_changes;

show triggers;