create database layoffs_data;
use layoffs_data;
select * from layoffs;
 create table layoffs2 like layoffs;
 select * from layoffs2;
insert layoffs2 select * from layoffs;
with duplicates_cte as(
select *,row_number() over (partition by  company,location,industry,total_laid_off
,percentage_laid_off,date,stage,country,funds_raised_millions) as row_num
from layoffs2)
select * from duplicates_cte
where row_num>1;
select * from layoffs2
where company ='Casper';

select * from layoffs2
where company ='Cazoo';

with duplicates_cte as(
select *,row_number() over (partition by  company,location,industry,total_laid_off
,percentage_laid_off,date,stage,country,funds_raised_millions) as row_num
from layoffs2)
delete  from duplicates_cte
where row_num>1;

create table layoffs3
(company text ,
location text, 
industry text, 
total_laid_off int ,
percentage_laid_off text ,
date text ,
stage text ,
country text, 
funds_raised_millions int,
row_num int)
select * from layoffs3;

insert layoffs3 select *,row_number() over (partition by  company,location,industry,total_laid_off
,percentage_laid_off,date,stage,country,funds_raised_millions) as row_num
from layoffs2;

delete  from layoffs3
where row_num>1;
select * from layoffs3
where row_num>1;

select company, trim(company) from layoffs3;

update layoffs3
set company=trim(company) ;

select trim(location) from layoffs3
order by 1;
update layoffs3 
set location =trim(location);

select industry from layoffs3
order by 1;
select industry from layoffs3
where industry like 'Crypto%';
update layoffs3
set industry='Crypto'
where industry like 'Crypto%';

select date from layoffs3;

select date ,
str_to_date (date, '%m/%d/%Y') 
from layoffs3;
select * from layoffs3;
update layoffs3
set date =str_to_date (date, '%m/%d/%Y') ;
select * from layoffs3;
alter table layoffs3
modify column date date;

select * from layoffs 
where total_laid_off is null and
percentage_laid_off is null;


delete from layoffs 
where total_laid_off is null and
percentage_laid_off is null;

select * from layoffs3;
SELECT 
    MAX(total_laid_off) AS max_laid_off,
    MAX(percentage_laid_off) AS max_laid_off_per
FROM
    layoffs3;
    
    select company, sum(total_laid_off) from layoffs3
    group by company
    order by sum(total_laid_off) desc;
    
    select industry, sum(total_laid_off) from layoffs3
    group by industry
    order by sum(total_laid_off) desc;
    
    select country ,sum(total_laid_off) 
    from layoffs3
    group by country
    order by sum(total_laid_off) desc;
    
    
  select year(date) as year_ ,sum(total_laid_off) 
    from layoffs3
    group by year_ 
    order by sum(total_laid_off) desc;  
    
     select monthname(date) as monthname_ ,sum(total_laid_off) 
    from layoffs3
    group by monthname_ 
    order by sum(total_laid_off) desc; 
    use layoffs_data;
    
    select * from layoffs3;
    select stage , sum(total_laid_off) as total_laid_off, round(sum(percentage_laid_off),2) as percentage_laid_off from layoffs3
    group	by stage
    order by 2 desc;
    
    select country,max(funds_raised_millions) from layoffs3
    group by country
    order by 2 desc;
    
    select * from layoffs3;
     select country,max(total_laid_off), max(funds_raised_millions) from layoffs3
    group by country
    order by 2 desc;
    
    select company,sum(total_laid_off), max(funds_raised_millions) from layoffs3
    group by company
    order by 2 desc;
    
    
    
