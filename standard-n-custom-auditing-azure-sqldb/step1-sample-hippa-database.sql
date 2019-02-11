/******************************************************
 *
 * Name:         step1-sample-hippa-database.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     05-18-2017
 *     Purpose:  Create a very simple hippa database.
 * 
 ******************************************************/
 
--
-- Remove old database
--

/*

-- Which database to use.
USE [master]
GO

-- Delete existing database
DROP DATABASE IF EXISTS [db4hippa]
GO

*/

--
-- Hippa database (in cloud code)
--

/*

-- Create new database
CREATE DATABASE [db4hippa]
(
MAXSIZE = 2GB,
EDITION = 'STANDARD',
SERVICE_OBJECTIVE = 'S0'
)
GO  

*/

-- Do not display rec cnts
SET NOCOUNT ON


--
-- Create active schema
--

-- Delete existing schema.
DROP SCHEMA IF EXISTS [active]
GO
 
-- Add new schema.
CREATE SCHEMA [active] AUTHORIZATION [dbo]
GO


--
-- Create stage schema
--

-- Delete existing schema.
DROP SCHEMA IF EXISTS [stage]
GO
 
-- Add new schema.
CREATE SCHEMA [stage] AUTHORIZATION [dbo]
GO


--
-- Patient table
--

-- drop existing table
drop table if exists active.patient_info
go

-- create new table
create table active.patient_info
(
patient_id int identity(1,1) constraint pk_patient_id primary key,
first_name varchar(128),
last_name varchar(128),
middle_initial char(1),
birth_date date,
gender_flag char(1),
social_security_num varchar(9)
);
go

-- clear the table
truncate table active.patient_info;
go


--
-- First name table
--

-- remove temp table
drop table if exists stage.first_name
go

-- create temp table
create table stage.first_name
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
INSERT INTO stage.first_name 
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
INSERT INTO stage.first_name 
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
update stage.first_name set myname = upper(myname);
go

-- Show the data
/*
select * from stage.first_name;
*/
go


--
-- Last name table
--

-- remove temp table
drop table if exists stage.last_name
go

-- create temp table
create table stage.last_name
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
INSERT INTO stage.last_name 
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
go

-- Show the data
/*
select * from stage.last_name;
go
*/



--
-- Height by percentile table
--

-- https://www2.census.gov/library/publications/2010/compendia/statab/130ed/tables/11s0205.pdf

-- remove temp table
drop table if exists stage.height_by_percentile
go

-- create temp table
create table stage.height_by_percentile
(
MyId int,
MyValue real,
MalePct real,
FemalePct real
)
go

-- Create a derived table
INSERT INTO stage.height_by_percentile 
SELECT * FROM 
(
    VALUES
    (1, 56, 0, 0.5), 
    (2, 57, 0, 1.1), 
    (3, 58, 0, 3), 
    (4, 59, 0, 6.4), 
    (5, 60, 0.3, 11.4), 
    (6, 61, 0.7, 19.7), 
    (7, 62, 0.9, 31.7), 
    (8, 63, 2.8, 45.9), 
    (9, 64, 5.3, 63.9), 
    (10, 65, 9.3, 77.4), 
    (11, 66, 17.5, 88.9), 
    (12, 67, 29.1, 94.6), 
    (13, 68, 43.5, 97.7), 
    (14, 69, 58.2, 99.4), 
    (15, 70, 71.5, 99.8), 
    (16, 71, 83.1, 99.8), 
    (17, 72, 91, 100), 
    (18, 73, 96.3, 100), 
    (19, 74, 97.7, 100), 
    (20, 75, 98.9, 100), 
    (21, 76, 100, 100)
) AS H (MyId, MyValue, MalePct, FemalePct);
go

-- Show the data
/*
select * from stage.height_by_percentile 
*/
go


--
-- Weight by percentile table
--

-- https://www2.census.gov/library/publications/2010/compendia/statab/130ed/tables/11s0205.pdf

-- remove temp table
drop table if exists stage.weight_by_percentile
go

-- create temp table
create table stage.weight_by_percentile
(
MyId int,
MyValue real,
MalePct real,
FemalePct real
)
go

-- Create a derived table
INSERT INTO stage.weight_by_percentile 
SELECT * FROM 
(
    VALUES
    (1, 100, 0, 0.4), 
    (2, 110, 0, 4), 
    (3, 120, 1.1, 7.9), 
    (4, 130, 2.3, 17.1), 
    (5, 140, 5.6, 27.3), 
    (6, 150, 8.6, 38.7), 
    (7, 160, 13.9, 49.7), 
    (8, 170, 22, 56.9), 
    (9, 180, 33.2, 63.7), 
    (10, 190, 44.5, 70.3), 
    (11, 200, 55.7, 75.3), 
    (12, 210, 64.6, 81.9), 
    (13, 220, 74, 85.9), 
    (14, 230, 78.8, 89.5), 
    (15, 240, 85.6, 91.4), 
    (16, 250, 88, 92.9), 
    (17, 260, 91.3, 96.5), 
    (18, 270, 93.5, 97.2), 
    (19, 280, 94.2, 98.2), 
    (20, 290, 95.8, 98.9), 
    (21, 300, 98.1, 99.4), 
    (22, 320, 99, 99.7), 
    (23, 340, 99.1, 99.8), 
    (24, 360, 99.8, 99.9), 
    (25, 380, 99.8, 100), 
    (26, 400, 99.9, 100), 
    (27, 420, 100, 100), 
    (28, 440, 100, 100)
) AS W (MyId, MyValue, MalePct, FemalePct);
go

-- Show the data
/*
select * from stage.weight_by_percentile 
go
*/

--
-- Top 20 discharge codes
--

-- https://sph.uth.edu/content/uploads/2011/12/Top-2009-ER-diagnoses-by-age-and-payer-source.pdf

-- remove temp table
drop table if exists stage.er_discharge_codes
go

-- create temp table
create table stage.er_discharge_codes
(
MyId int,
MyICD9 int,
MyDesc varchar(128),
MyCnt int,
MyPct real
)
go

-- Create a derived table
INSERT INTO stage.er_discharge_codes
SELECT * FROM 
(
    VALUES
    (1, 780, 'General symptoms', 5151, 9.2), 
    (2, 873, 'Open wound of head', 3059, 9.4), 
    (3, 465, 'Acute upper respiratory infections', 2677, 8.2), 
    (4, 789, 'Symptoms involving abdomen and pelvis', 2177, 6.6), 
    (5, 382, 'Suppurative and unspecified otitis media', 2057, 6.4), 
    (6, 787, 'Symptoms involving digestive system', 1800, 5.6), 
    (7, 493, 'Asthma', 1789, 5.4), 
    (8, 920, 'Contusion of face, scalp, and neck', 1621, 5), 
    (9, 79, 'Viral and chlamydial infection', 1503, 4.6), 
    (10, 786, 'Respiratory symptoms', 1497, 4.6), 
    (11, 959, 'Injury, other and unspecified', 1445, 4.4), 
    (12, 682, 'Cellulitis and abscess', 1319, 4), 
    (13, 487, 'Influenza', 1308, 4), 
    (14, 462, 'Acute pharyngitis', 1123, 3.4), 
    (15, 486, 'Pneumonia, organism unspecified', 1112, 3.4), 
    (16, 813, 'Fracture of radius and ulna', 1099, 3.4), 
    (17, 558, 'Unspecified noninfectious gastroenteritis and colitis', 1066, 3.2), 
    (18, 466, 'Acute bronchitis and bronchiolitis', 1043, 3.2), 
    (19, 784, 'Symptoms involving head and neck', 1023, 3.2), 
    (20, 599, 'Disorders of urethra and urinary tract', 923, 2.8)
) AS ER (MyId, MyICD9, MyDesc, MyCnt, MyPct);
go
    
-- Show the data
/*
select * from stage.er_discharge_codes 
select sum(mypct) from stage.er_discharge_codes 
*/
go


--
--  Create x records
--

-- clear table
truncate table active.patient_info;
go

-- declare variables
declare @cnt int;
declare @recs int;
declare @roll int;

declare @fname varchar(128);
declare @lname varchar(128);
declare @minit char(1);

declare @ssn varchar(9);
declare @gender char(1);
declare @bdate date;


-- loop to create data
set @cnt = 0;
set @recs = 20;
while (@cnt < @recs)
begin

    -- roll a dice (0 to 50)
	set @roll = cast(rand() * 50 + 1 as int)

	-- random gender
    select @gender = case when @roll < 26 then 'F' else 'M' end;

    -- random first name
    select @fname = myname from stage.first_name where myid = @roll;

	-- roll a dice (0 to 50)
	set @roll = cast(rand() * 50 + 1 as int)

	-- random last name
    select @lname = myname from stage.last_name where myid = @roll;

	-- roll a dice (0 to 27)
	set @roll = cast(rand() * 26 + 1 as int)

	-- random middle initial
    select @minit = CHAR(cast(@roll + 64 as int));
	
	-- random ssn
	select @ssn = right('000000000' + cast(cast(rand() * 999999999.0 + 1.0 as bigint) as varchar(9)), 9);

	-- random birth day (age 10 - 75)
	select @bdate = dateadd(d, cast(rand() * 365.25 * 65 as int), dateadd(d, -1 * 365.25 * 75, getdate()));

	-- show the data
	print @fname; 
	print @lname;
	print @ssn;
	print @gender;

	-- increment loop counter
	set @cnt += 1;

	-- add new record
    insert into active.patient_info 
	(
	    first_name, 
		last_name, 
		middle_initial,
		birth_date,
		gender_flag, 
		social_security_num
    )
	values 
	(
	    @fname, 
		@lname, 
		@minit,
		@bdate,
		@gender,
		@ssn
	);
end
go

-- Show the new data
/*
select * from active.patient_info;
go
*/

--
-- Doctor table
--

-- drop existing table
drop table if exists active.doctor_info
go

-- create new table
create table active.doctor_info
(
doctor_id int identity(1,1) constraint pk_doctor_id primary key,
first_name varchar(128),
last_name varchar(128),
infamous_desc varchar(128)
);
go

-- evil doctors
-- http://www.toptenz.net/top-10-evil-doctors.php

-- add data
INSERT INTO active.doctor_info
(
    first_name,
    last_name,
    infamous_desc
)
SELECT * FROM 
(
    VALUES
    ('Jack', 'Kevorkian', 'Euthanasia'), 
    ('Walter', 'Freeman', 'Lobotomies'), 
    ('Harry', 'Holmes', 'Serial Killer'),
    ('Arnfinn', 'Nesset', 'Deadly Injection'),
    ('Carl', 'Clauberg', 'Human Experimentation'),
    ('John', 'Adams', 'Abusing Prescription Drugs'),
    ('Harold', 'Shipman', 'Serial Killer'),
    ('Michael', 'Swango', 'Death By Poison'),	 
    ('Shiro', 'Ishii', 'Biological Warfare'),	 
    ('Josef', 'Mengele', 'Angel of Death')
) AS D (MyFirstName, MyLastName, MyDesc);
go

-- Make upper case
UPDATE 
    active.doctor_info
SET 
    first_name = UPPER(first_name),
    last_name = UPPER(last_name),
    infamous_desc = UPPER(infamous_desc);
 
-- Show the new data
/*
select * from active.doctor_info;
go
*/

--
-- Visit table
--

-- drop existing table
drop table if exists active.visit_info
go

-- create new table
create table active.visit_info
(
visit_id int identity(1,1) constraint pk_visit_id primary key,
visit_date datetime,
patient_weight real,
patient_height real,
patient_temp real,
patient_systolic int,
patient_diastolic int,
diagnosis_icd9 int,
diagnosis_desc varchar(128),
patient_id int,
doctor_id int
);
go


-- Add foreign key 1
ALTER TABLE active.visit_info WITH CHECK 
ADD CONSTRAINT fk_visit_2_patient FOREIGN KEY(patient_id)
REFERENCES active.patient_info (patient_id)
GO
 
ALTER TABLE active.visit_info CHECK CONSTRAINT fk_visit_2_patient
GO


-- Add foreign key 2
ALTER TABLE active.visit_info WITH CHECK 
ADD CONSTRAINT fk_visit_2_doctor FOREIGN KEY(doctor_id)
REFERENCES active.doctor_info (doctor_id)
GO
 
ALTER TABLE active.visit_info CHECK CONSTRAINT fk_visit_2_doctor
GO


-- Show the new data
/*
select * from active.visit_info;
go
*/


--
--  Create x records
--

-- clear table
truncate table active.visit_info;
go

-- declare variables
declare @cnt int;
declare @recs int;
declare @roll int;

declare @weight real;
declare @height real;

declare @temp real;
declare @systolic int;
declare @diastolic int;

declare @icd9 int;
declare @diagnosis varchar(128);

declare @vdate date;

declare @gender char(1);

-- loop to create data
set @cnt = 0;
set @recs = 20;

while (@cnt < @recs)
begin

    -- find the gender
    select @gender = gender_flag from [active].[patient_info] where patient_id = @cnt

    -- roll a dice (0 to 100)
	set @roll = cast(rand() * 100 + 1 as int) 

	-- pick avg. height & weight by gender
	if (@gender = 'F')
	begin
		select top 1 @height = myvalue from [stage].[height_by_percentile] where femalepct <= @roll order by myid desc;
		if (@roll = 100) select @height = 72

		select top 1 @weight = myvalue from [stage].[weight_by_percentile] where femalepct <= @roll order by myid desc;
		if (@roll = 100) select @weight = 380
	end
	else
	begin
		select top 1 @height = myvalue from [stage].[height_by_percentile] where malepct <= @roll order by myid desc;
		if (@roll = 100) select @height = 76

		select top 1 @weight = myvalue from [stage].[weight_by_percentile] where malepct <= @roll order by myid desc;
		if (@roll = 100) select @weight = 420
	end

    -- roll a dice (0 to 2)
	set @roll = cast(rand() * 2 + 1 as int) 
	select @temp = 97 + @roll


	-- roll a dice (0 to 30)
	set @roll = cast(rand() * 60 + 1 as int) 
	select @systolic = 100 + @roll

	-- roll a dice (0 to 30)
	set @roll = cast(rand() * 30 + 1 as int) 
	select @diastolic = 50 + @roll

	-- roll a dice (0 to 20)
	set @roll = cast(rand() * 20 + 1 as int) 
	select @icd9 = [MyICD9] from [stage].[er_discharge_codes] where [MyId] = @roll;
	select @diagnosis = UPPER([MyDesc]) from [stage].[er_discharge_codes] where [MyId] = @roll;

	-- visit within year
	select @vdate = dateadd(d, cast(rand() * 365.25 * -1 as int), getdate());

	-- which evil doctor - roll a dice (0 to 10)
	set @roll = cast(rand() * 10 + 1 as int) 

	-- show the data
	print @height; 
	print @weight;
	print @temp;
	print @systolic;
	print @diastolic;
	print @icd9;
	print @diagnosis;
	print @vdate;
	print @roll;
	print '';

	-- increment loop counter
	set @cnt += 1;

	-- add new record
    insert into active.visit_info
	(
	    [visit_date],
        [patient_weight],
        [patient_height],
        [patient_temp],
        [patient_systolic],
        [patient_diastolic],
        [diagnosis_icd9],
        [diagnosis_desc],
        [patient_id],
        [doctor_id]
    )
	values 
	(
	    @vdate, 
		@weight, 
		@height,
        @temp,
		@systolic,
		@diastolic,
		@icd9,
		@diagnosis,
		@cnt,
		@roll
	);

end;
go

--
--  Remove staging tables
--

/*

drop table [stage].[er_discharge_codes];
go

drop table [stage].[first_name];
go

drop table [stage].[height_by_percentile];
go

drop table [stage].[last_name];
go

drop table [stage].[weight_by_percentile];
go

*/

--
--  Show active tables
--

select * from [active].[doctor_info]
go

select * from [active].[patient_info]
go

select * from[active].[visit_info] 
go


--
--  Show database definition
--


select s.name, o.name, o.type, o.type_desc 
from sys.objects o join sys.schemas s
on o.schema_id = s.schema_id
where o.is_ms_shipped = 0
order by o.type desc


