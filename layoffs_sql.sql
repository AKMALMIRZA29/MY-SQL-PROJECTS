SELECT * 
FROM world_layoffs.layoffs;



-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;


-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways



-- 1. Remove Duplicates

# First let's check for duplicates



SELECT *
FROM world_layoffs.layoffs_staging
;

SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`) AS row_num
	FROM 
		world_layoffs.layoffs_staging;



SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;
    
-- let's just look at oda to confirm
SELECT *
FROM world_layoffs.layoffs_staging
WHERE company = 'Oda'
;
-- it looks like these are all legitimate entries and shouldn't be deleted. We need to really look at every single row to be accurate

-- these are our real duplicates 
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;

-- these are the ones we want to delete where the row number is > 1 or 2or greater essentially

-- now you may want to write it like this:
WITH DELETE_CTE AS 
(
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1
)
DELETE
FROM DELETE_CTE
;


WITH DELETE_CTE AS (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, 
    ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM world_layoffs.layoffs_staging
)
DELETE FROM world_layoffs.layoffs_staging
WHERE (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num) IN (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num
	FROM DELETE_CTE
) AND row_num > 1;

-- one solution, which I think is a good one. Is to create a new column and add those row numbers in. Then delete where row numbers are over 2, then delete that column


ALTER TABLE world_layoffs.layoffs_staging ADD row_num INT;


SELECT *
FROM world_layoffs.layoffs_staging
;

CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

INSERT INTO `world_layoffs`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging;

-- now that we have this we can delete rows were row_num is greater than 2

DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2;


SELECT * FROM layoffs_staging2
WHERE ROW_NUM >1;

DELETE
 FROM layoffs_staging2
WHERE ROW_NUM >1;

SELECT * FROM layoffs_staging2;

-- STANDARDISING DATA
 SELECT COMPANY,TRIM(COMPANY)
 FROM layoffs_staging2;

UPDATE layoffs_staging2
SET COMPANY = TRIM(COMPANY);

SELECT DISTINCT INDUSTRY
 FROM layoffs_staging2
 ORDER BY 1;
 
 
SELECT *
 FROM layoffs_staging2
WHERE INDUSTRY LIKE 'cRYPTO%';

-- standardising industry values
UPDATE layoffs_staging2
SET INDUSTRY = 'CRYPTO'
WHERE INDUSTRY LIKE 'CRYPTO%';

-- standardising counry values

SELECT DISTINCT COUNTRY
 FROM layoffs_staging2
 WHERE COUNTRY LIKE 'UNITED STATE%';


SELECT *
 FROM layoffs_staging2;
 
 
 SELECT DISTINCT COUNTRY, TRIM(TRAILING '.' FROM COUNTRY)
 FROM layoffs_staging2
 ORDER BY 1;

 
 UPDATE layoffs_staging2
 SET COUNTRY = TRIM(TRAILING '.' FROM COUNTRY)
 WHERE COUNTRY LIKE 'UNITED STATE%';
 --------------------------------------------
 
 SELECT *
 FROM layoffs_staging2;
 
 -- date format 
 SELECT `date`, 
 str_to_date(`date`,'%m/%d/%Y')
 FROM layoffs_staging2;

update layoffs_staging2
set `date` =  str_to_date(`date`,'%m/%d/%Y');

-- CHANGE DATE TYPE
alter table layoffs_staging2
modify column `date` DATE;

--  ------------------------------------------------------------------

 SELECT *
 FROM layoffs_staging2
 WHERE  total_laid_off IS NULL
 AND percentage_laid_off IS NULL;



SELECT *
FROM layoffs_staging2
WHERE INDUSTRY IS NULL 
OR INDUSTRY = '';

SELECT *
FROM layoffs_staging2
WHERE COMPANY like 'Bally%';

select * from
layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    and t1.location = t2.location
    where (t1.industry is null or t1.industry = '')
    and t2.industry is not null;

-- before imputation set '' as null 

update layoffs_staging2
set industry = null
where industry = '';

-- imputation method populate values from result of t1 to t2
update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    set t1.industry = t2.industry
where (t1.industry is null or t1.industry = '')
    and t2.industry is not null;

select * 
from layoffs_staging2;

-- founding null values 
  SELECT *
 FROM layoffs_staging2
 WHERE  total_laid_off IS NULL
 AND percentage_laid_off IS NULL;
 
 -- deletion of null values
delete 
 FROM layoffs_staging2
 WHERE  total_laid_off IS NULL
 AND percentage_laid_off IS NULL;
 
 select * 
from layoffs_staging2;
 
 alter table layoffs_staging2
 drop column row_num;
 
 ------------------------------------------------- 
 
 -- EDA DATA ANALYSIS
 
  select * 
from layoffs_staging2;
 
 
 -- max total laid off and max percentage laid off
 
  select MAX(total_laid_off),MAX(percentage_laid_off)
from layoffs_staging2;
 
 -- 1 percentage of laid off order by funds raised millions--
   select * 
from layoffs_staging2
WHERE percentage_laid_off = 1
 ORDER BY funds_raised_millions DESC;
 
 -- total layd off by company

 SELECT COMPANY, SUM(total_laid_off) AS TOTAL_LAID_OFF 
 from layoffs_staging2
 GROUP BY COMPANY
 ORDER BY 2 DESC;
 
 SELECT MIN(`date`),MAX(`date`)
 FROM layoffs_staging2;
 
-- total lays off by country
  SELECT COUNTRY, SUM(total_laid_off) AS TOTAL_LAID_OFF 
 from layoffs_staging2
 GROUP BY COUNTRY
 ORDER BY 2 DESC;
 
  select * 
from layoffs_staging2;
 
-- total laid off by year
  SELECT YEAR(`DATE`), SUM(total_laid_off) AS TOTAL_LAID_OFF 
 from layoffs_staging2
 GROUP BY YEAR(`DATE`)
 ORDER BY 2 DESC;
  
 -- totoal sum of laid off group by stage
 
   SELECT STAGE, SUM(total_laid_off) AS TOTAL_LAID_OFF 
 from layoffs_staging2
 GROUP BY STAGE
 ORDER BY 2 DESC;
 
 -- avg percentage laid off
 
    SELECT COMPANY, avg(percentage_laid_off)AS AVG_PERCENTAGE_LAID_OFF 
 from layoffs_staging2
 GROUP BY COMPANY
 ORDER BY 2 DESC;
 
 
 -- time series analysis monthwise 
 
 SELECT substring(`DATE`,1,7) AS MONTH, SUM(total_laid_off)
 FROM layoffs_staging2
 WHERE substring(`DATE`,1,7)IS NOT NULL
 GROUP BY MONTH
 ORDER BY 1 ASC;
 
 SELECT *
  FROM layoffs_staging2;

-- rolling_total-----

WITH Rolling_Total AS
(
SELECT substring(`DATE`,1,7) AS MONTH, SUM(total_laid_off) AS TOTAL_OFF
 FROM layoffs_staging2
 WHERE substring(`DATE`,1,7)IS NOT NULL
 GROUP BY MONTH
 ORDER BY 1 ASC
 )
 SELECT `MONTH`, total_off,SUM(TOTAL_OFF) OVER(ORDER BY `MONTH`) AS rolling_total
 FROM Rolling_Total;
 
 
 -- total laid off by company
  SELECT COMPANY, SUM(total_laid_off) AS TOTAL_LAID_OFF 
 from layoffs_staging2
 GROUP BY COMPANY
 ORDER BY 2 DESC;
 
 
   SELECT  COMPANY,year( `date`), SUM(total_laid_off) AS TOTAL_LAID_OFF 
 from layoffs_staging2
 GROUP BY COMPANY, year( `date`)
 order by 3 desc;
 
 -- ranking  by years and company
 with company_year (company,years,TOTAL_LAID_OFF) as 
 (
  SELECT  COMPANY,year( `date`), SUM(total_laid_off) AS TOTAL_LAID_OFF 
 from layoffs_staging2
 GROUP BY COMPANY, year( `date`)
 order by 3 desc
 ), company_year_rank as (
 select *, dense_rank() over (partition by years order by total_laid_off desc) as ranking
 from company_year
 where years is  not null)
 select * from company_year_rank
 where ranking <= 5;
 
 
 select * from layoffs_staging2;
 -- max funds_raised_millions by company
 select company, max(funds_raised_millions) as max_funds
 from layoffs_staging2
 group by company
 order by max_funds desc
 limit 5;
 
 -- company,ranking by years and country
 
  with country_year (company,country,years,TOTAL_LAID_OFF) as 
 (
  SELECT company,  country,year( `date`), SUM(total_laid_off) AS TOTAL_LAID_OFF
 from layoffs_staging2
 GROUP BY company,country, year( `date`)
 order by 4 desc
 ), country_year_rank as (
 select *, dense_rank() over (partition by years order by total_laid_off desc) as ranking
 from country_year
 where years is  not null)
 select * from country_year_rank
 where ranking <= 5;
 
 
 
 
 
 