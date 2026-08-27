/******************************************************
 *
 * Name:         0102-pg-incremental-snapshot-tests.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     09-01-2026
 *     Purpose:  Test the snapshots + incremental loading.
 * 
 ******************************************************/
  
--
--  Test snaphots (SCD Type 2)
--

-- show data (raw)
select * from "data_raw"."members01" where "MemberID" = 1;

-- update record
update "data_raw"."members01"
set "JoinDate" = NOW(), "Status" = 'Active'
where "MemberID" = 1;

-- show data (snapshot)
select * from "data_snapshot"."members02" where "MemberID" = 1;


--
--  Test incremental load
--

-- show existing loans table
select * from "data_raw"."loans01" order by "ReturnDate" desc limit 5;

-- update record
update "data_raw"."loans01"
set "ReturnDate" = NOW()
where "LoanID" = 10;

-- show existing loans table - by loan id
select * from "data_raw"."loans01" where "LoanID" = 10;

-- show existing loans table - by member id
select * from "data_raw"."loans01" where "MemberID" = 11;

-- add new record
insert into "data_raw"."loans01"
select
    (select max("LoanID") + 1 from "data_raw"."loans01") as id,     
    24 as "BookID",
	11 as "MemberID",
	NOW() as "BorrowDate",
	NOW() + INTERVAL '30 days' as "DueDate",
	NULL as "ReturnDate"
;

-- show existing loans table
select * from "data_raw"."loans01" where "MemberID" in (6, 11);

-- show existing loans table
select * from "data_stage"."loans02" where "MemberID" in (6, 11);

