Use moviesdb;
# Different Types of SQL Joins
# Inner Join --> returns records(row) that have matching values in both
# Full(Outer) Join --> returns records when there is match in either left or right table
# Left(Outer) Join --> returns records from the left table and matched records from right table
# Right(Outer) Join --> returns records from the right table, and the matched records from the left table
 select * from movies;
 select * from financials;
 #Inner Join
 select movies.movie_id, title,budget,revenue,
 currency,unit From movies
 inner Join financials on movies.movie_id = 
 financials.movie_id;
#Left Join
select m.movie_id,title,budget,revenue,currency,unit from movies m
left join financials f on m.movie_id = f.movie_id;
#Right Join
select m.movie_id,title,budget,revenue,currency,unit from movies m
right join financials f on m.movie_id = f.movie_id;
#Outer Join
select m.movie_id,title,budget,revenue,currency,unit from movies m 
right JOIN financials f on m.movie_id = f.movie_id
Union
select m.movie_id,title,budget,revenue,currency,unit from movies m 
Left JOIN financials f on m.movie_id = f.movie_id;
 