drop table messages;
CREATE TABLE IF NOT EXISTS messages (
    id INT AUTO_INCREMENT PRIMARY KEY,
    message VARCHAR(255) NOT NULL,
    created_at DATETIME DEFAULT NOW()
);

create event inserts 
on Schedule at current_timestamp
do 
	INSERT INTO	 messages(messages)
	values("onetime event");

CREATE EVENT IF NOT EXISTS one_time_log
ON SCHEDULE AT CURRENT_TIMESTAMP
DO
  INSERT INTO messages(message)
  VALUES('One-time event');

SELECT * FROM messages;

create event recurring_log
on schedule every 10 second
starts current_timestamp 
ends current_timestamp + Interval 1 minute
on completion preserve
do 	
	insert into messages(message)
    values(concat('Message started at ',now()));
    
select * from messages;    
show events from windows_function;drop event one_time_log;
