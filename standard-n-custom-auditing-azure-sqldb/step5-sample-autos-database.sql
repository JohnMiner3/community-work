/******************************************************
 *
 * Name:         step5-sample-autos-database.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     05-18-2017
 *     Purpose:  Create a very simple autos database.
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
DROP DATABASE IF EXISTS [db4autos]
GO

*/

--
-- Hippa database (in cloud code)
--

/*

-- Create new database
CREATE DATABASE [db4autos]
(
MAXSIZE = 2GB,
EDITION = 'STANDARD',
SERVICE_OBJECTIVE = 'S0'
)
GO  

*/



--
-- 2 - Create two schemas
--


-- Delete existing schema
IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'AUDIT')
  DROP SCHEMA [AUDIT]
GO

-- Add schema for audit purposes
CREATE SCHEMA [AUDIT] AUTHORIZATION [dbo]
GO

-- Delete existing schema
IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'ACTIVE')
  DROP SCHEMA [ACTIVE]
GO

-- Add schema for active data
CREATE SCHEMA [ACTIVE] AUTHORIZATION [dbo]
GO



--
-- 3 - Create Continents table & load with data
--


-- Delete existing table
IF OBJECT_ID('[ACTIVE].[CONTINENT]') IS NOT NULL 
  DROP TABLE [ACTIVE].[CONTINENT]
GO

-- Add the table
CREATE TABLE [ACTIVE].[CONTINENT]
(
  CONTINENT_ID CHAR(1) NOT NULL,
  CONTINENT_NAME VARCHAR(25) NOT NULL,
)
GO

-- Add primary key
ALTER TABLE [ACTIVE].[CONTINENT]
  ADD CONSTRAINT PK_CONTINENT_ID PRIMARY KEY ([CONTINENT_ID]);
GO


-- Delete existing procedure
IF OBJECT_ID('[ACTIVE].[USP_LOAD_CONTINENT]') IS NOT NULL 
  DROP PROCEDURE [ACTIVE].[USP_LOAD_CONTINENT]
GO

-- Add the new procedure
CREATE PROCEDURE [ACTIVE].[USP_LOAD_CONTINENT] 
WITH EXECUTE AS CALLER
AS
BEGIN

  -- Do not show messages
  SET NOCOUNT ON;

  -- Insert data from wikipedia
  INSERT INTO [ACTIVE].[CONTINENT] VALUES 
    ('A', 'Africa'),
    ('B', 'Asia'),
    ('C', 'Europe'),
    ('D', 'North America'),
    ('E', 'South America'),
    ('F', 'Oceania'),
    ('G', 'Antarctica');

END;
GO

-- Execute the load
EXECUTE [ACTIVE].[USP_LOAD_CONTINENT];
GO 

-- Show the data
SELECT * FROM [ACTIVE].[CONTINENT]
GO



--
-- 4 - Create Cars by Country table 
--

-- Delete existing table
IF OBJECT_ID('[ACTIVE].[CARS_BY_COUNTRY]') IS NOT NULL 
  DROP TABLE [ACTIVE].[CARS_BY_COUNTRY]
GO

-- Add the table
CREATE TABLE [ACTIVE].[CARS_BY_COUNTRY]
(
  COUNTRY_ID SMALLINT NOT NULL,
  COUNTRY_NAME VARCHAR(25) NOT NULL,
  PERSONAL_VEHICLES INT NOT NULL,
  COMMERCIAL_VEHICLES INT NOT NULL,
  TOTAL_VEHICLES INT NOT NULL,
  CHANGE_PCT FLOAT NOT NULL,
  CONTINENT_ID CHAR(1) NOT NULL  
);
GO

-- Add primary key
ALTER TABLE [ACTIVE].[CARS_BY_COUNTRY]
  ADD CONSTRAINT PK_COUNTRY_ID PRIMARY KEY ([COUNTRY_ID]);
GO


-- Add foreign key 
ALTER TABLE [ACTIVE].[CARS_BY_COUNTRY] WITH CHECK 
  ADD CONSTRAINT [FK_CONTINENT_ID] FOREIGN KEY([CONTINENT_ID])
  REFERENCES [ACTIVE].[CONTINENT] ([CONTINENT_ID])
GO


--
-- 5 - Load Cars by Country table with data
--

-- Delete existing procedure
IF OBJECT_ID('[ACTIVE].[USP_LOAD_CARS_BY_COUNTRY]') IS NOT NULL 
  DROP PROCEDURE [ACTIVE].[USP_LOAD_CARS_BY_COUNTRY]
GO

-- Add the new procedure
CREATE PROCEDURE [ACTIVE].[USP_LOAD_CARS_BY_COUNTRY] 
WITH EXECUTE AS CALLER
AS
BEGIN

  -- Do not show messages
  SET NOCOUNT ON;

  -- Insert Car Data From www.worldometers.info/cars
  INSERT INTO [ACTIVE].[CARS_BY_COUNTRY] VALUES 
  (1,'Argentina',263120,168981,432101,35.1,'E'),
  (2,'Australia',270000,60900,330900,-16.2,'F'),
  (3,'Austria',248059,26873,274932,8.6,'C'),
  (4,'Belgium',881929,36127,918056,-1.2,'C'),
  (5,'Brazil',2092029,519005,2611034,3.3,'E'),
  (6,'Canada',1389536,1182756,2572292,-4.3,'D'),
  (7,'China',5233132,1955576,7188708,25.9,'B'),
  (8,'Czech Rep.',848922,5985,854907,41.3,'C'),
  (9,'Egypt',59462,32111,91573,32.2,'A'),
  (10,'Finland',32417,353,32770,51.4,'C'),
  (11,'France',2723196,446023,3169219,-10.7,'C'),
  (12,'Germany',5398508,421106,5819614,1.1,'C'),
  (13,'Hungary',187633,3190,190823,25.5,'C'),
  (14,'India',1473000,546808,2019808,24.2,'B'),
  (15,'Indonesia',206321,89687,296008,-40.1,'B'),
  (16,'Iran',800000,104500,904500,10.7,'B'),
  (17,'Italy',892502,319092,1211594,16.7,'C'),
  (18,'Japan',9756515,1727718,11484233,6.3,'B'),
  (19,'Malaysia',377952,125021,502973,-10.8,'B'),
  (20,'Mexico',1097619,947899,2045518,22.4,'D'),
  (21,'Netherlands',87332,72122,159454,-11.8,'C'),
  (22,'Poland',632300,82300,714600,14.2,'C'),
  (23,'Portugal',143478,83847,227325,3.7,'C'),
  (24,'Romania',201663,11934,213597,9.6,'C'),
  (25,'Russia',1177918,330440,1508358,11.6,'B'),
  (26,'Serbia',9832,1350,11182,-21.1,'B'),
  (27,'Slovakia',295391,0,295391,35.3,'B'),
  (28,'Slovenia',115000,35320,150320,-15.5,'B'),
  (29,'South Africa',334482,253237,587719,11.9,'A'),
  (30,'South Korea',3489136,350966,3840102,3.8,'B'),
  (31,'Spain',2078639,698796,2777435,0.9,'C'),
  (32,'Sweden',288583,44585,333168,-1.6,'C'),
  (33,'Taiwan',211306,91915,303221,-32.1,'B'),
  (34,'Thailand',298819,895607,1296060,15.2,'B'),
  (35,'Turkey',545682,442098,987780,12.4,'C'),
  (36,'Ukraine',274860,20400,295260,36.8,'C'),
  (37,'United Kingdom',1442085,206303,1648388,-8.6,'C'),
  (38,'United States',4366220,6897766,11263986,-6.0,'D'),
  (39,'Uzbekistan',100000,10000,110000,14.8,'B');
END;
GO

-- Execute the load
EXECUTE [ACTIVE].[USP_LOAD_CARS_BY_COUNTRY];
GO 

-- Show the data
SELECT * FROM [ACTIVE].[CARS_BY_COUNTRY]
GO