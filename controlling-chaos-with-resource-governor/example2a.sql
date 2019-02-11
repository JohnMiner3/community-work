/******************************************************
 *
 * Name:         example-2a.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     11-07-2017
 *     Purpose:  Create a 1 million row table in  
 *               hospital database one.
 * 
 ******************************************************/

--
-- Hospital database
--

-- Which database to use.
USE [master]
GO
 
-- Delete existing database
IF  EXISTS (SELECT name FROM sys.databases WHERE name = N'HOSPITAL1')
DROP DATABASE HOSPITAL1
GO
 
-- Add new database
CREATE DATABASE HOSPITAL1 ON  
 PRIMARY 
  ( NAME = N'HOSPITAL1_PRI_DAT', FILENAME = N'C:\MSSQL\DATA\HOSPITAL1.MDF' , SIZE = 64MB, FILEGROWTH = 64MB) 
 LOG ON 
  ( NAME = N'HOSPITAL1_ALL_LOG', FILENAME = N'C:\MSSQL\LOG\HOSPITAL1.LDF' , SIZE = 32MB, FILEGROWTH = 32MB)
GO

-- Which database to use.
USE [HOSPITAL1]
GO

-- Switch owner to system admin
ALTER AUTHORIZATION ON DATABASE::[HOSPITAL1] TO SA;
GO


--
-- Define 2 schemas
--

USE [HOSPITAL1]
GO
 
-- Delete existing schema.
IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'ACTIVE')
DROP SCHEMA [ACTIVE]
GO
 
-- Add new schema.
CREATE SCHEMA [ACTIVE] AUTHORIZATION [dbo]
GO


-- Delete existing schema.
IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'STAGE')
DROP SCHEMA [STAGE]
GO
 
-- Add new schema.
CREATE SCHEMA [STAGE] AUTHORIZATION [dbo]
GO


--
-- Patient table
--

-- drop existing table
drop table if exists [ACTIVE].[PATIENT_INFO]
go

-- create new table
create table [ACTIVE].[PATIENT_INFO]
(
id int identity(1,1) primary key,
fname varchar(128),
lname varchar(128),
ssn varchar(9)
)



--
-- First name table
--

-- remove temp table
drop table if exists [STAGE].[FIRST_NAME]
go

create table [STAGE].[FIRST_NAME]
(
myid int,
myname varchar(64)
)
go


--
-- Girl names (25)
--

-- http://baby-names.familyeducation.com/popular-names/girls/


-- Create a derived table
INSERT INTO [STAGE].[FIRST_NAME] 
SELECT * FROM 
(
    VALUES
    (1, 'Emily'),
    (2, 'Madison'),
    (3, 'Emma'),
    (4, 'Hannah'),
    (5, 'Olivia'),
    (6, 'Abigail'),
    (7, 'Isabella'),
    (8, 'Ashley'),
    (9, 'Samantha'),
    (10, 'Elizabeth'),
    (11, 'Alexis'),
    (12, 'Sarah'),
    (13, 'Alyssa'),
    (14, 'Grace'),
    (15, 'Sophia'),
    (16, 'Taylor'),
    (17, 'Brianna'),
    (18, 'Lauren'),
    (19, 'Ava'),
    (20, 'Kayla'),
    (21, 'Jessica'),
    (22, 'Natalie'),
    (23, 'Chloe'),
    (24, 'Anna'),
    (25, 'Victoria')
) AS G (MyId, MyValue);
GO


--
-- Boy names (25)
--

-- http://baby-names.familyeducation.com/popular-names/boys/

-- Create a derived table
INSERT INTO [STAGE].[FIRST_NAME] 
SELECT * FROM 
(
    VALUES
    (26, 'Jacob'),
    (27, 'Michael'),
    (28, 'Joshua'),
    (29, 'Matthew'),
    (30, 'Christopher'),
    (31, 'Andrew'),
    (32, 'Daniel'),
    (33, 'Ethan'),
    (34, 'Joseph'),
    (35, 'William'),
    (36, 'Anthony'),
    (37, 'Nicholas'),
    (38, 'David'),
    (39, 'Alexander'),
    (40, 'Ryan'),
    (41, 'Tyler'),
    (42, 'James'),
    (43, 'John'),
    (44, 'Jonathan'),
    (45, 'Brandon'),
    (46, 'Christian'),
    (47, 'Dylan'),
    (48, 'Zachary'),
    (49, 'Noah'),
    (50, 'Samuel')
) AS B (MyId, MyValue);
GO

-- Upper case
update [STAGE].[FIRST_NAME] set myname = upper(myname);

-- Show the data
select * from [STAGE].[FIRST_NAME];
go


--
-- Last name table
--

-- remove temp table
drop table if exists [STAGE].[LAST_NAME]
go

create table [STAGE].[LAST_NAME]
(
myid int,
myname varchar(64)
)
go


--
-- Sur names (50)
--

-- http://names.mongabay.com/most_common_surnames.htm

-- Create a derived table
INSERT INTO [STAGE].[LAST_NAME] 
SELECT * FROM 
(
    VALUES
	(1, 'SMITH'),
	(2, 'JOHNSON'),
	(3, 'WILLIAMS'),
	(4, 'JONES'),
	(5, 'BROWN'),
	(6, 'DAVIS'),
	(7, 'MILLER'),
	(8, 'WILSON'),
	(9, 'MOORE'),
	(10, 'TAYLOR'),
	(11, 'ANDERSON'),
	(12, 'THOMAS'),
	(13, 'JACKSON'),
	(14, 'WHITE'),
	(15, 'HARRIS'),
	(16, 'MARTIN'),
	(17, 'THOMPSON'),
	(18, 'GARCIA'),
	(19, 'MARTINEZ'),
	(20, 'ROBINSON'),
	(21, 'CLARK'),
	(22, 'RODRIGUEZ'),
	(23, 'LEWIS'),
	(24, 'LEE'),
	(25, 'WALKER'),
	(26, 'HALL'),
	(27, 'ALLEN'),
	(28, 'YOUNG'),
	(29, 'HERNANDEZ'),
	(30, 'KING'),
	(31, 'WRIGHT'),
	(32, 'LOPEZ'),
	(33, 'HILL'),
	(34, 'SCOTT'),
	(35, 'GREEN'),
	(36, 'ADAMS'),
	(37, 'BAKER'),
	(38, 'GONZALEZ'),
	(39, 'NELSON'),
	(40, 'CARTER'),
	(41, 'MITCHELL'),
	(42, 'PEREZ'),
	(43, 'ROBERTS'),
	(44, 'TURNER'),
	(45, 'PHILLIPS'),
	(46, 'CAMPBELL'),
	(47, 'PARKER'),
	(48, 'EVANS'),
	(49, 'EDWARDS'),
	(50, 'COLLINS')
) AS S (MyId, MyValue);
GO

-- Show the data
select * from [STAGE].[LAST_NAME];
go


--
--  Create 10K rows
--

truncate table [ACTIVE].[PATIENT_INFO];

-- declare variables
declare @i int;
declare @fname varchar(128);
declare @lname varchar(128);
declare @ssn varchar(9);

-- loop to create data
set @i = 0;
while (@i < 1000000)
begin
    -- create random data
    select @fname = myname from [STAGE].[FIRST_NAME] where myid = cast(rand() * 50 + 1 as int);
    select @lname = myname from [STAGE].[LAST_NAME] where myid = cast(rand() * 50 + 1 as int);
	select @ssn = right('000000000' + cast(cast(rand() * 999999999.0 + 1.0 as bigint) as varchar(9)), 9);

	-- show the data
	/*
	print @fname + ' ' + @lname
	print @ssn;
	*/

	-- increment loop counter
	set @i += 1;

	-- add new record
    insert into [ACTIVE].[PATIENT_INFO] values (@fname, @lname, @ssn)
end
go

-- Show row count
select count(*) from [ACTIVE].[PATIENT_INFO];
go

-- Show the new data
select top 100 * from [ACTIVE].[PATIENT_INFO];
go


