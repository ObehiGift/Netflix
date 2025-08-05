CREATE DATABASE netflix;

DROP TABLE `netflix dataset`;

DROP TABLE netflixdata;
-- importing the dataset

CREATE TABLE netflixdata2
like netflixdata;

SELECT@@secure_file_priv;

LOAD DATA INFILE 
'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\NetflixData.csv'
INTO TABLE netflixdata2
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES; 

SELECT *
FROM netflixdata;

-- Duplicating the dataset
CREATE TABLE netflixdata1
LIKE netflixdata;

INSERT INTO netflixdata1
SELECT *
FROM netflixdata;

-- Checking for blanks/nulls and filling them or deleting them
SELECT *
FROM netflixdata1
WHERE Director = '';
-- There are alot of empty columns for director, i dont think there's a movie without directors.. so i'll be deleting all the movies without directors

DELETE FROM netflixdata1
WHERE Director = '';

SELECT *
FROM netflixdata1
WHERE Cast = '';
-- there are no movies without casts, i'll be deleting this too

DELETE FROM netflixdata1
WHERE Cast = '';

SELECT *
FROM netflixdata1
WHERE Country = '';
-- all movies must have a country, i'll be deleting this too

DELETE FROM netflixdata1
WHERE Country = '';

SELECT *
FROM netflixdata1
WHERE Rating = '';
-- i'll fill this blanks with unrated

UPDATE netflixdata1
SET Rating = 'Unrated'
WHERE Rating = '';

SELECT *
FROM netflixdata2
WHERE `Description` = '';

-- all blanks have been deleted/filled

-- checking for duplicates
SELECT *, 
	(row_number() OVER(
    PARTITION BY Show_Id, 
    Category, 
    Title, 
    Cast, 
    Country,
    Release_Date, 
    Rating, 
    Duration, 
    `Type`, 
    `Description`)) AS row_num
FROM netflixdata1;

SELECT *
FROM (SELECT *, 
	(row_number() OVER(
    PARTITION BY Show_Id, 
    Category, 
    Title, 
    Cast, 
    Country,
    Release_Date, 
    Rating, 
    Duration, 
    `Type`, 
    `Description`)) AS row_num
    FROM netflixdata1) AS row_num1
WHERE row_num > 1;

SELECT *
FROM netflixdata1
WHERE Title = 'The lost okoroshi';

SELECT *
FROM netflixdata1
WHERE Title = 'Backfire';

-- we found 2 duplicates now we have to delete them
CREATE TABLE netflixdata2
LIKE netflixdata1; 

ALTER TABLE netflixdata2
ADD row_num INT AFTER `Description`;  

INSERT INTO netflixdata2
SELECT *
FROM (SELECT *, 
	(row_number() OVER(
    PARTITION BY Show_Id, 
    Category, 
    Title, 
    Cast, 
    Country,
    Release_Date, 
    Rating, 
    Duration, 
    `Type`, 
    `Description`)) AS row_num
    FROM netflixdata1) AS row_num1;
    
    SELECT *
    FROM netflixdata2;
    
    DELETE FROM netflixdata2
    WHERE row_num > 1;
    
    -- Standardizing the data
    
SELECT *
FROM netflixdata2;

SELECT DISTINCT Country
FROM netflixdata2
ORDER BY 1;

-- Changing my date and time to time series
SELECT Release_date, STR_TO_DATE(Release_date, '%M %d, %Y') AS Date_of_release
FROM netflixdata2;

UPDATE netflixdata2
SET Release_date = STR_TO_DATE(Release_date, '%M %d, %Y');

SELECT Duration, TRIM(Duration)
FROM netflixdata2;

UPDATE netflixdata2
SET Duration = TRIM(Duration);

SELECT *
FROM netflixdata2;

SELECT DISTINCT Rating
FROM netflixdata2;

-- some of my columns have a comma so i want to create new tables for those columns and separate the commas 
CREATE TABLE director
(Show_id TEXT,
Category TEXT,
Title TEXT,
Director_name TEXT);

INSERT INTO director
SELECT Show_id, Category, Title, Director
FROM netflixdata2;

SELECT *
FROM director;
    
CREATE TABLE rdirector
LIKE director;
    
SELECT *
FROM director
WHERE (LENGTH(Director_name) - LENGTH(REPLACE(Director_name,',',''))+1) = 1;
 
INSERT INTO rdirector
SELECT *
 FROM (SELECT *
 FROM director
 WHERE (LENGTH(Director_name) - LENGTH(REPLACE(Director_name,',',''))+1) = 1) AS N;
    
SELECT *
FROM (SELECT Director_name, LENGTH(Director_name) - LENGTH(REPLACE(Director_name,',',''))+1
FROM director) AS G
WHERE (LENGTH(Director_name) - LENGTH(REPLACE(Director_name,',',''))+1) = 2;

SELECT Director_name, SUBSTRING_INDEX(Director_name,',',1)
FROM director
WHERE (LENGTH(Director_name) - LENGTH(REPLACE(Director_name,',',''))+1) = 2;

INSERT INTO rdirector
SELECT *
FROM((SELECT Show_id, category, title, SUBSTRING_INDEX(Director_name,',',1)
FROM director
WHERE (LENGTH(Director_name) - LENGTH(REPLACE(Director_name,',',''))+1) = 2)
UNION ALL
(SELECT Show_id, category, title, TRIM(SUBSTRING_INDEX(Director_name,',',-1))
FROM director
WHERE (LENGTH(Director_name) - LENGTH(REPLACE(Director_name,',',''))+1) = 2)) AS N;

SELECT *
FROM rdirector;

SELECT Director_name, LENGTH(Director_name) - LENGTH(REPLACE(Director_name,',',''))+1
FROM director
WHERE (LENGTH(Director_name) - LENGTH(REPLACE(Director_name,',',''))+1) = 3;

INSERT INTO rdirector
SELECT *
FROM(SELECT Show_id, Category, Title, TRIM(SUBSTRING_INDEX(Director_name,',',1))
FROM director
WHERE (LENGTH(Director_name) - LENGTH(REPLACE(Director_name,',',''))+1) = 3
UNION ALL
SELECT Show_id, Category, Title, TRIM(SUBSTRING_INDEX(Director_name,',',-1))
FROM director
WHERE (LENGTH(Director_name) - LENGTH(REPLACE(Director_name,',',''))+1) = 3
UNION ALL
SELECT Show_id, Category, Title, TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(Director_name,',',2),',',-1))
FROM director
WHERE (LENGTH(Director_name) - LENGTH(REPLACE(Director_name,',',''))+1) = 3) AS M;

SELECT Director_name, LENGTH(Director_name) - LENGTH(REPLACE(Director_name,',',''))+1
FROM director
WHERE (LENGTH(Director_name) - LENGTH(REPLACE(Director_name,',',''))+1) = 4;

INSERT INTO rdirector
SELECT *
FROM(SELECT Show_id, Category, Title, TRIM(SUBSTRING_INDEX(Director_name,',',1))
FROM director
WHERE (LENGTH(Director_name) - LENGTH(REPLACE(Director_name,',',''))+1) = 4
UNION ALL
SELECT Show_id, Category, Title, TRIM(SUBSTRING_INDEX(Director_name,',',-1))
FROM director
WHERE (LENGTH(Director_name) - LENGTH(REPLACE(Director_name,',',''))+1) = 4
UNION ALL
SELECT Show_id, Category, Title, TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(Director_name,',',2),',',-1))
FROM director
WHERE (LENGTH(Director_name) - LENGTH(REPLACE(Director_name,',',''))+1) = 4
UNION ALL
SELECT Show_id, Category, Title, TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(Director_name,',',3),',',-1))
FROM director
WHERE (LENGTH(Director_name) - LENGTH(REPLACE(Director_name,',',''))+1) = 4) AS M;

INSERT INTO rdirector
SELECT *
FROM(SELECT Show_id, Category, Title, TRIM(SUBSTRING_INDEX(Director_name,',',1))
FROM director
WHERE (LENGTH(Director_name) - LENGTH(REPLACE(Director_name,',',''))+1) = 5
UNION ALL
SELECT Show_id, Category, Title, TRIM(SUBSTRING_INDEX(Director_name,',',-1))
FROM director
WHERE (LENGTH(Director_name) - LENGTH(REPLACE(Director_name,',',''))+1) = 5
UNION ALL
SELECT Show_id, Category, Title, TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(Director_name,',',2),',',-1))
FROM director
WHERE (LENGTH(Director_name) - LENGTH(REPLACE(Director_name,',',''))+1) = 5
UNION ALL
SELECT Show_id, Category, Title, TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(Director_name,',',3),',',-1))
FROM director
WHERE (LENGTH(Director_name) - LENGTH(REPLACE(Director_name,',',''))+1) = 5
UNION ALL
SELECT Show_id, Category, Title, TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(Director_name,',',4),',',-1))
FROM director
WHERE (LENGTH(Director_name) - LENGTH(REPLACE(Director_name,',',''))+1) = 5) AS K;

SELECT *
FROM rdirector;
-- Done with directors

-- now to cast
CREATE TABLE cast
(Show_id TEXT,
Title TEXT,
Cast TEXT);

INSERT INTO cast
SELECT Show_Id, Title, Cast
FROM netflixdata2;

SELECT Title, Cast, MAX(LENGTH(CAST) - LENGTH(REPLACE(CAST,',',''))+1)
FROM netflixdata2
GROUP BY 1,2
ORDER BY 3 DESC;

WITH RECURSIVE split_cast AS
(SELECT Show_id, Title,`cast`, SUBSTRING_INDEX(`cast`,',',1), SUBSTRING(`cast`,LENGTH(SUBSTRING_INDEX(`cast`,',',1))+2) AS rest
FROM cast
UNION ALL
SELECT Show_id, Title, `cast`, SUBSTRING_INDEX(rest,',',1), SUBSTRING(rest,LENGTH(SUBSTRING_INDEX(rest,',',1))+2)
FROM split_cast
WHERE rest != '')
SELECT *
FROM split_cast;

CREATE TABLE rcast
LIKE cast;

INSERT INTO rcast
WITH RECURSIVE split_cast AS
(SELECT Show_id, Title,`cast`, SUBSTRING_INDEX(`cast`,',',1) AS cast_name, SUBSTRING(`cast`,LENGTH(SUBSTRING_INDEX(`cast`,',',1))+2) AS rest
FROM cast
UNION ALL
SELECT Show_id, Title, `cast`, SUBSTRING_INDEX(rest,',',1), SUBSTRING(rest,LENGTH(SUBSTRING_INDEX(rest,',',1))+2)
FROM split_cast
WHERE rest != '')
SELECT Show_id, Title, cast_name
FROM split_cast;

SELECT *
FROM rcast;

UPDATE rcast
SET Cast = TRIM(Cast);
-- done with cast

-- now unto country
SELECT *
FROM netflixdata2;

CREATE TABLE COUNTRY
(Show_id TEXT,
Title TEXT,
Country TEXT);

INSERT INTO country
SELECT Show_id, Title, `Country`
FROM netflixdata2;

CREATE TABLE rcountry
LIKE country;

INSERT INTO rcountry 
WITH RECURSIVE Split_country AS
(SELECT Show_id, Title, `Country`, SUBSTRING_INDEX(`Country`,',',1) AS new_country, 
SUBSTRING(`Country`, LENGTH(SUBSTRING_INDEX(`Country`,',',1))+2) AS rest
FROM country
UNION ALL
SELECT Show_id, Title, `Country`, SUBSTRING_INDEX(rest,',',1) AS rest, 
SUBSTRING(rest, LENGTH(SUBSTRING_INDEX(rest,',',1))+2) AS rest
FROM Split_country
WHERE rest != '')
SELECT Show_id, Title, new_country
FROM Split_country;

SELECT *
FROM rcountry
WHERE `country` = 'nigeria';

UPDATE rcountry
SET `COUNTRY` = TRIM(`country`);
-- Done with country

-- now genre
CREATE TABLE Genre
(Show_id TEXT,
Title TEXT,
Genre TEXT);

INSERT INTO genre
SELECT Show_id, Title, `Type`
FROM netflixdata2;

CREATE TABLE rgenre
LIKE genre;

INSERT INTO rgenre 
WITH RECURSIVE Split_genre AS
(SELECT Show_id, Title, `Genre`, SUBSTRING_INDEX(`genre`,',',1) AS new_country, 
SUBSTRING(`genre`, LENGTH(SUBSTRING_INDEX(`genre`,',',1))+2) AS rest
FROM genre
UNION ALL
SELECT Show_id, Title, `Genre`, SUBSTRING_INDEX(rest,',',1) AS rest, 
SUBSTRING(rest, LENGTH(SUBSTRING_INDEX(rest,',',1))+2) AS rest
FROM Split_genre
WHERE rest != '')
SELECT Show_id, Title, new_country
FROM Split_genre;

UPDATE rgenre
SET `Genre` = TRIM(`Genre`);
-- done with genre

-- now movie info
CREATE TABLE Movie_info
(Show_id TEXT,
Title TEXT,
Release_date DATE,
Rating TEXT,
Duration TEXT,
`Description` TEXT);

INSERT INTO movie_info
SELECT Show_id, Title, Release_date, Rating, Duration, `Description`
FROM netflixdata2;

SELECT *
FROM movie_info;
-- now we're done

ALTER TABLE netflixdata2
DROP COLUMN row_num;

-- How many movies and Tv shows are available?
SELECT Category, COUNT(*)
FROM netflixdata2
GROUP BY 1;

-- How many movies are added each year?
SELECT YEAR(Release_date) AS `Year`, COUNT(*)
FROM netflixdata2
GROUP BY 1
ORDER BY 1 DESC;

-- Which country have the most title on netflix?
SELECT `Country`,count(*)
FROM rcountry
GROUP BY 1
ORDER BY 2 DESC;

-- Which genre appeared MOst frequently?

SELECT genre, most_frequent
FROM
(SELECT `genre`, Count(*) AS most_frequent
FROM rgenre
GROUP BY 1
ORDER BY 2 DESC) AS M
LIMIT 1;

-- TOP DIRECTORS ON NETFLIX
SELECT `Director_name`, COUNT(*)
FROM rdirector
GROUP BY 1
ORDER BY 2 DESC
LIMIT 5;

-- Oldest and Newest titles available
SELECT Title, MAX(release_date)
FROM movie_info
GROUP BY 1
ORDER BY 2 DESC
LIMIT 3;

SELECT Title, MIN(release_date)
FROM movie_info
GROUP BY 1
ORDER BY 2 ASC
LIMIT 1;

-- How many titles fall into each rating?
SELECT Rating, COUNT(*)
FROM movie_info
GROUP BY 1
ORDER BY 2 DESC;

-- Which actor appear in the most titles?
SELECT`cast`, COUNT(*)
FROM rcast
GROUP BY 1
ORDER BY 2 DESC;

-- Most frequent actors from nigeria 
SELECT rcast.`cast`, rcountry.`country`, COUNT(*)
FROM rcast
JOIN rcountry
ON rcast.Show_id = rcountry.Show_id
GROUP BY 1,2
HAVING Country = 'Nigeria'
ORDER BY 3 DESC;

-- top contributing countries per year
SELECT YEAR(netflixdata2.Release_date) AS `Year`, rcountry.`country` AS county, 
COUNT(rcountry.`country`) AS cnt
FROM netflixdata2
JOIN rcountry
ON netflixdata2.Show_id = rcountry.Show_id
GROUP BY 1,2
ORDER BY 3 DESC;

WITH cte AS
(SELECT YEAR(netflixdata2.Release_date) AS `Year`, rcountry.`country` AS `country`, 
COUNT(rcountry.`country`) AS cnt, 
DENSE_RANK() OVER (PARTITION BY YEAR(netflixdata2.Release_date) ORDER BY COUNT(rcountry.`country`)DESC) AS rnk
FROM netflixdata2
JOIN rcountry
ON netflixdata2.Show_id = rcountry.Show_id
GROUP BY 1,2) 
SELECT `Year`,`country`,cnt
FROM cte
WHERE rnk = 1;

-- I want to select the movie with the maximum duration, i noticed my duration is in text format. so i created a table for the category and converted
-- the duration to integer
SELECT Title, MIN(Duration) AS `duration`, RANK() OVER(ORDER BY MIN(Duration)DESC)
FROM movie_info
GROUP BY 1
HAVING `duration` LIKE '%Season%';

SELECT Title, Duration, CAST(TRIM(SUBSTRING_INDEX(Duration,' ',1)) AS UNSIGNED) AS M
FROM movie_info
WHERE Duration LIKE '%min%';

SELECT Title, Duration, CAST(TRIM(SUBSTRING_INDEX(Duration,' ',1)) AS UNSIGNED) AS N
FROM movie_info
WHERE Duration LIKE '%season%';

SELECT Category
FROM netflixdata2
WHERE Category LIKE '%SHOW';

CREATE TABLE Category
(Show_id TEXT,
Title TEXT,
Category TEXT,
Duration INT,
Season_min TEXT);

INSERT INTO category (Show_id, Title, `Category`)
SELECT Show_id, Title, `Category`
FROM netflixdata2;

SELECT *
FROM category;

WITH CTE AS
 (SELECT Title, Duration, CAST(TRIM(SUBSTRING_INDEX(Duration,' ',1)) AS UNSIGNED) AS M
FROM movie_info
WHERE Duration LIKE '%min'
LIMIT 500 OFFSET 0)
UPDATE `category`
JOIN CTE ON category.Title = CTE.Title
SET category.`Duration` = CTE.M;

WITH CTE AS
 (SELECT Title, Duration, CAST(TRIM(SUBSTRING_INDEX(Duration,' ',1)) AS UNSIGNED) AS M
FROM movie_info
WHERE Duration LIKE '%min'
LIMIT 500 OFFSET 500)
UPDATE `category`
JOIN CTE ON category.Title = CTE.Title
SET category.`Duration` = CTE.M;

WITH CTE AS
 (SELECT Title, Duration, CAST(TRIM(SUBSTRING_INDEX(Duration,' ',1)) AS UNSIGNED) AS M
FROM movie_info
WHERE Duration LIKE '%min'
LIMIT 1000 OFFSET 1000)
UPDATE `category`
JOIN CTE ON category.Title = CTE.Title
SET category.`Duration` = CTE.M;

WITH CTE AS
 (SELECT Title, Duration, CAST(TRIM(SUBSTRING_INDEX(Duration,' ',1)) AS UNSIGNED) AS M
FROM movie_info
WHERE Duration LIKE '%min'
LIMIT 1000 OFFSET 2000)
UPDATE `category`
JOIN CTE ON category.Title = CTE.Title
SET category.`Duration` = CTE.M;

WITH CTE AS
 (SELECT Title, Duration, CAST(TRIM(SUBSTRING_INDEX(Duration,' ',1)) AS UNSIGNED) AS M
FROM movie_info
WHERE Duration LIKE '%min'
LIMIT 1000 OFFSET 3000)
UPDATE `category`
JOIN CTE ON category.Title = CTE.Title
SET category.`Duration` = CTE.M;

WITH CTE AS
 (SELECT Title, Duration, CAST(TRIM(SUBSTRING_INDEX(Duration,' ',1)) AS UNSIGNED) AS M
FROM movie_info
WHERE Duration LIKE '%min'
LIMIT 1000 OFFSET 4000)
UPDATE `category`
JOIN CTE ON category.Title = CTE.Title
SET category.`Duration` = CTE.M;

SELECT *
FROM category;

WITH CTE AS
 (SELECT Title, Duration, CAST(TRIM(SUBSTRING_INDEX(Duration,' ',1)) AS UNSIGNED) AS M
FROM movie_info
WHERE Duration LIKE '%seasons')
UPDATE `category`
JOIN CTE ON category.Title = CTE.Title
SET category.`Duration` = CTE.M;

SELECT Title, Duration, TRIM(SUBSTRING_INDEX(Duration,' ',-1)) AS M
FROM movie_info
WHERE Duration LIKE '%min';

WITH CTE AS
 (SELECT Title, Duration, TRIM(SUBSTRING_INDEX(Duration,' ',-1)) AS M
FROM movie_info
WHERE Duration LIKE '%min%'
LIMIT 1500 OFFSET 0)
UPDATE category
JOIN CTE ON category.Title = CTE.Title
SET category.`Season_min` = CTE.M;

WITH CTE AS
 (SELECT Title, Duration, TRIM(SUBSTRING_INDEX(Duration,' ',-1)) AS M
FROM movie_info
WHERE Duration LIKE '%min%'
LIMIT 1500 OFFSET 1500)
UPDATE category
JOIN CTE ON category.Title = CTE.Title
SET category.`Season_min` = CTE.M;

WITH CTE AS
 (SELECT Title, Duration, TRIM(SUBSTRING_INDEX(Duration,' ',-1)) AS M
FROM movie_info
WHERE Duration LIKE '%min%'
LIMIT 1500 OFFSET 3000)
UPDATE category
JOIN CTE ON category.Title = CTE.Title
SET category.`Season_min` = CTE.M;

WITH CTE AS
 (SELECT Title, Duration, TRIM(SUBSTRING_INDEX(Duration,' ',-1)) AS M
FROM movie_info
WHERE Duration LIKE '%min%'
LIMIT 1500 OFFSET 4500)
UPDATE category
JOIN CTE ON category.Title = CTE.Title
SET category.`Season_min` = CTE.M;

WITH CTE AS
 (SELECT Title, Duration, TRIM(SUBSTRING_INDEX(Duration,' ',-1)) AS M
FROM movie_info
WHERE Duration LIKE '%season%')
UPDATE category
JOIN CTE ON category.Title = CTE.Title
SET category.`Season_min` = CTE.M;

SELECT *
FROM category;

-- Shortest Movie on NETFLIX
SELECT Title, `MIN`, Season_min
FROM(WITH cte AS
(SELECT Title, MIN(Duration) AS `MIN`, `Season_min`, RANK() OVER(ORDER BY MIN(Duration)) AS rnk
FROM category
GROUP BY 1,3
HAVING `Season_min` LIKE '%min%')
SELECT Title, `MIN`, `Season_min`, rnk
FROM cte
HAVING rnk = 1) AS M;

-- longest movie on NETFLIX
SELECT Title, `MAX`, Season_min
FROM(WITH cte AS
(SELECT Title, MAX(Duration) AS `MAX`, `Season_min`, RANK() OVER(ORDER BY MAX(Duration)DESC) AS rnk
FROM category
GROUP BY 1,3
HAVING `Season_min` LIKE '%min%')
SELECT Title, `MAX`, `Season_min`, rnk
FROM cte
HAVING rnk = 1) AS M;

-- how many titles were added per month over the years
SELECT MONTH(release_date) AS `month`, COUNT(*)
FROM netflixdata2
GROUP BY 1
ORDER BY 1;

-- what is the most frquent movie length
SELECT Category, Duration, COUNT(*)
FROM category
GROUP BY 1,2
HAVING `Category` = 'movie'
ORDER BY 3 DESC;

-- which genre of movies have the most title with TV-MA or PG-13 ratings
SELECT Genre, RAting, CNT
FROM (WITH CTE AS
(SELECT G.Genre, M.Rating, COUNT(*) AS CNT, 
RANK() OVER(PARTITION BY M.Rating ORDER BY COUNT(*)DESC) AS RNK
FROM genre AS G
JOIN movie_info AS M
ON G.Title = M.Title
GROUP BY 1,2
HAVING M.Rating = 'TV-MA' 
OR M.Rating ='PG-13'
ORDER BY 3 DESC)
SELECT Genre, Rating, CNT, RNK
FROM CTE
WHERE RNK = 1) AS N;

-- What's the year over year growth-rate in netflix content?
SELECT YEAR(Release_date), COUNT(*) AS CNT, 
LAG(COUNT(*)) OVER (ORDER BY YEAR(Release_date)) AS `LAG`,
CAST(((COUNT(*)-LAG(COUNT(*)) OVER (ORDER BY YEAR(Release_date)))/LAG(COUNT(*)) OVER (ORDER BY YEAR(Release_date)))*100 AS DECIMAL(5,2)) AS `YoY_Growth(%)`
FROM netflixdata2
GROUP BY 1
ORDER BY 1;

-- how many TV-shows have 1 season?
SELECT Category,Duration, Season_min, COUNT(*)
FROM category
GROUP BY 1,2,3
HAVING Season_min LIKE '%SEA%' AND Duration = 1;

-- how many TV-shows have 3+ season?
SELECT Category,Duration, Season_min, COUNT(*)
FROM category
GROUP BY 1,2,3
HAVING Season_min LIKE '%SEA%' AND Duration >= 3;

-- Which Country Dominate each genre?
SELECT country,genre,CNT
FROM (WITH CTE AS 
(SELECT C.country, G.genre, COUNT(*) AS CNT, RANK() OVER(PARTITION BY G.genre ORDER BY COUNT(*)DESC) AS RNK
FROM rcountry AS C
JOIN rgenre AS G
ON C.Show_id = G.Show_id
GROUP BY 1,2)
SELECT country, genre, CNT, RNK
FROM CTE
WHERE RNK = 1) AS N;

-- Which director work across the most genres?
SELECT Genre, Director_name, CNT
FROM(WITH CTE AS 
(SELECT G.Genre, D.Director_name, COUNT(*) CNT,RANK() OVER(PARTITION BY G.Genre ORDER BY COUNT(*)DESC) AS RNK
FROM rgenre AS G
JOIN rdirector AS D
ON G.Show_id = D.Show_id
GROUP BY 1,2)
SELECT Genre, Director_name, CNT, RNK
FROM CTE
WHERE RNK = 1) AS M;

-- Percentage of movies above 100min
SELECT COUNT(*) AS CNT, (SELECT COUNT(*)
FROM category
WHERE Duration > 100 AND Season_min ='min') AS CNT1, CAST((((SELECT COUNT(*)
FROM category
WHERE Duration > 100 AND Season_min ='min')/COUNT(*))*100) AS DECIMAL(4,2)) AS `Percentage_of_movies>100min`
FROM category;

SELECT *
FROM netflixdata2;

SELECT *
FROM category;

SELECT *
FROM rcast;

SELECT *
FROM rcountry;

SELECT *
FROM rdirector;

SELECT *
FROM rgenre;

SELECT *
FROM movie_info;