/******************************************************
 *
 * Name:         0103-pg-execute-data-tests
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     01-01-2026
 *     Purpose:  Create a test case for each data test.
 * 
 ******************************************************/
 
--
-- duplicate values
--

-- existing row
select * from "data_raw"."books01"
where "BookID" = 1;

-- make duplicate
insert into "data_raw"."books01"
values
(1,'Configurable stable methodology','Jeffrey Carey', '978-0-03-050997-1', 'Tech', 0)


--
-- null values
--

-- no matching row
select * from "data_raw"."books01"
where "BookID" is null;

-- create row with null id
insert into "data_raw"."books01"
values
(Null,'Configurable stable methodology','Jeffrey Carey', '978-0-03-050997-1', 'Tech', 0)


--
-- accepted values
--

-- matching row
select * from "data_raw"."books01"
where "BookID" = 1;

-- update both rows
update "data_raw"."books01"
set "Genre" = 'Health'
where "BookID" = 1;


--
-- referential test - seeds
--

select * from "data_raw"."loans01" where "LoanID" = 1;

select * from "data_raw"."books01" order by "BookID" desc limit 5;

update 
"data_raw"."loans01" 
set "BookID" = 99
where "LoanID" = 1;


--
-- referential test - mart
--

select * from "data_mart"."fact_loans" where "LoanID" = 1;

update "data_mart"."fact_loans"
set "BookID" = 99
where "LoanID" = 1;


--
-- Custom Test #1 - raw integrity check
--

select 
 *
from 
 data_raw.loans01 as a
where 
  a."BookID" not in
  (
	  select distinct COALESCE("BookID", -1) from data_raw.books01
  )


--
--  How many loans have due dates > 30 days
--

select * 
from data_raw.loans01
where 
"ReturnDate" is null and
CURRENT_DATE - "DueDate"::date > 30


--
--  Custom Test #2 - overdue books
--

select * from "library01"."data_dbt_test__audit"."overdue30days"
