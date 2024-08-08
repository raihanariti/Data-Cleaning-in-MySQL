-- Data cleaning

SELECT *
FROM layoffs;

CREATE TABLE  layoffs_satging
LIKE layoffs;

SELECT *
FROM layoffs_satging ;

INSERT layoffs_satging
SELECT *
FROM layoffs;


-- REMOVE DUPLICATES

 SELECT *,
 ROW_NUMBER () OVER(
 PARTITION BY company, industry, total_laid_off, percentage_laid_off, 'date') AS row_num
 FROM layoffs_satging ;
 
 WITH duplicate_cte AS
 (
 SELECT *,
 ROW_NUMBER () OVER(
 PARTITION BY company, location , stage , country, funds_raised_millions , total_laid_off, percentage_laid_off, 'date') AS row_num
 FROM layoffs_satging 
 )

SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_satging 
WHERE COMPANY ='casper' ;



WITH duplicate_cte AS
 (
 SELECT *,
 ROW_NUMBER () OVER(
 PARTITION BY company, location , stage , country, funds_raised_millions , total_laid_off, percentage_laid_off, 'date') AS row_num
 FROM layoffs_satging 
 )

DELETE 
FROM duplicate_cte
WHERE row_num > 1;



CREATE TABLE `layoffs_satging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT *
FROM layoffs_satging2 
WHERE row_num > 1;

INSERT INTO layoffs_satging2
SELECT *,
ROW_NUMBER () OVER(
PARTITION BY company, location , stage , country, funds_raised_millions , total_laid_off, percentage_laid_off, 'date')
AS row_num
FROM layoffs_satging ;


DELETE
FROM layoffs_satging2 
WHERE row_num > 1;

SELECT *
FROM layoffs_satging2 ;

-- standardizing data

SELECT company, TRIM(company)
FROM layoffs_satging2 ;

UPDATE layoffs_satging2
SET company =TRIM(company);

SELECT *
FROM layoffs_satging2 
WHERE INDUSTRY LIKE 'crypto%';

UPDATE layoffs_satging2
SET industry ='crypto'
WHERE industry LIKE 'crypto%';

SELECT DISTINCT industry 
FROM layoffs_satging2 ;

SELECT DISTINCT country , TRIM(TRAILING '.' FROM country )
FROM layoffs_satging2 
ORDER BY 1;

UPDATE layoffs_satging2
SET country =TRIM(TRAILING '.' FROM country )
WHERE country LIKE 'United States%';


SELECT 	date
FROM layoffs_satging2;

SELECT 	`date` ,
STR_TO_DATE(`date` , '%m/%d/%Y')
FROM layoffs_satging2;


UPDATE layoffs_satging2
SET `date` =STR_TO_DATE(`date` , '%m/%d/%Y');

SELECT 	`date` 
FROM layoffs_satging2;

ALTER TABLE layoffs_satging2
MODIFY COLUMN `date` DATE ;

SELECT *
FROM layoffs_satging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

UPDATE layoffs_satging2
SET industry =NULL
WHERE industry ='';

SELECT *
FROM layoffs_satging2
WHERE industry IS NULL
OR industry ='';

SELECT *
FROM layoffs_satging2
WHERE company ='Airbnb';

SELECT t1.industry , t2.industry
FROM layoffs_satging2 t1
JOIN layoffs_satging2 t2
		ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry= '')
AND t2.industry IS NOT NULL ;

UPDATE layoffs_satging2 t1
JOIN layoffs_satging2 t2
		ON t1.company = t2.company
SET t1.industry =t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL ;

SELECT *
FROM layoffs_satging2;

SELECT *
FROM layoffs_satging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_satging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_satging2;


ALTER TABLE layoffs_satging2
DROP COLUMN row_num;