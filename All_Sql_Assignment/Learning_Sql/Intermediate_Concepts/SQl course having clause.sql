use moviesdb;
select * from movies;
#print all the years where more than 2 movies were released.
#The below code generates error. 
select release_year,count(release_year) 
as cs 
from movies 
where cs >2 
group by release_year order by cs desc;
# Beacuse.
# Execution order.
# From --> Where ---> Group by ---> Having ---> Order By.
# We can use same as Where which is Having clause to do above thing without error.
# Column used in having should be present in select clause as well
select release_year, count(release_year) 
as movie_count 
from movies group by(release_year) 
having movie_count>2 
order by movie_count desc;
