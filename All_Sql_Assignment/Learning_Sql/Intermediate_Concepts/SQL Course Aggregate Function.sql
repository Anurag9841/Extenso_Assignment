Use moviesdb;
Select * from movies;
Select max(imdb_rating) as max,min(imdb_rating),round(avg(imdb_rating),2) from movies where industry = "bollywood";
Select studio, max(imdb_rating) as max from movies group by studio;
Select studio, max(imdb_rating),min(imdb_rating),avg(imdb_rating) from movies group by studio;
Select industry, max(release_year),min(release_year) from movies group by industry;
Select studio,count(*) as cs from movies where studio !="" group by studio order by cs desc;
Select studio,count(studio) as cs from movies group by studio order by cs desc;