/******************************************************
 *
 * Name:         audit-n-prevent-unwanted-actions.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     03-01-2013
 *     Blog:     www.craftydba.com
 *
 *     Purpose:  Update the TSQL sample code that 
 *               goes along with the presentation.
 *
 *     Topics:   
 *       A - Schemas for security partitioning.
 *       B - Table triggers for data auditing.
 *       C - Database triggers for schema auditing.  
 *       D - Table triggers for preventing data changes.
 *       E - Database triggers for preventing schema changes
 *       F - View with check option to prevent data changes.
 *       G - Foreign key relationship to prevent table truncation.
 *       H - Audit logins for review.
 *       I - Login trigger for time constraint.
 *       J - Login trigger for application constraint.
 *       K - Change data capture.
 *
 ******************************************************/



--
-- 1 - Create the autos sample database
--


-- Which database to use.
USE [master]
GO

-- Delete existing databases.
IF EXISTS (SELECT name FROM sys.databases WHERE name = N'AUTOS')
DROP DATABASE [AUTOS]
GO

-- Add new databases.
CREATE DATABASE [AUTOS] ON PRIMARY
( NAME = N'AUTOS_DAT', FILENAME = N'C:\MSSQL\DATA\AUTOS-DAT.MDF' , SIZE = 32MB , MAXSIZE = UNLIMITED, FILEGROWTH = 4MB)
LOG ON
( NAME = N'AUTOS_LOG', FILENAME = N'C:\MSSQL\LOG\AUTOS-LOG.LDF' , SIZE = 4MB , MAXSIZE = UNLIMITED , FILEGROWTH = 1MB)
GO



--
-- 2 - Create two schemas
--


-- Which database to use.
USE [AUTOS]
GO

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


-- Which database to use.
USE [AUTOS]
GO

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


-- Which database to use.
USE [AUTOS]
GO

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


-- Which database to use.
USE [AUTOS]
GO

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



-- 
-- 6 - Granting user access by schema & application role
-- 


-- Which database to use.
USE [master]
GO

-- Delete existing login.
IF  EXISTS (SELECT * FROM sys.server_principals WHERE name = N'AUTO_USER')
  DROP LOGIN [AUTO_USER]
GO

-- Add new login.
CREATE LOGIN [AUTO_USER] WITH PASSWORD=N'SzfX6ThnLeDPwpelMHYdV2MW', DEFAULT_DATABASE=[AUTOS]
GO


-- Make sure we are in autos
USE [AUTOS]
GO

-- Delete existing user.
IF  EXISTS (SELECT * FROM sys.database_principals WHERE name = N'AUTO_USER')
  DROP USER [AUTO_USER]
GO

-- Add new user.
CREATE USER [AUTO_USER] FOR LOGIN [AUTO_USER] WITH DEFAULT_SCHEMA=[ACTIVE]
GO


-- Make sure we are in autos
USE [AUTOS]
GO

-- Delete existing role.
IF EXISTS (SELECT * FROM sys.database_principals WHERE type_desc = 'DATABASE_ROLE' AND name = 'AUTO_ROLE')
  DROP ROLE [AUTO_ROLE]
GO

-- Create application role
CREATE ROLE [AUTO_ROLE] AUTHORIZATION [dbo]
GO


-- Apply permissions to schemas
GRANT ALTER ON SCHEMA::[ACTIVE] TO [AUTO_ROLE]
GO

GRANT CONTROL ON SCHEMA::[ACTIVE] TO [AUTO_ROLE]
GO

GRANT SELECT ON SCHEMA::[ACTIVE] TO [AUTO_ROLE]
GO

-- Ensure role membership is correct
EXEC sp_addrolemember N'AUTO_ROLE', N'AUTO_USER'
GO



-- 
-- 7 - Auditing data changes (table for DML trigger)
-- 


-- Delete existing table
IF OBJECT_ID('[AUDIT].[LOG_TABLE_CHANGES]') IS NOT NULL 
  DROP TABLE [AUDIT].[LOG_TABLE_CHANGES]
GO


-- Add the table
CREATE TABLE [AUDIT].[LOG_TABLE_CHANGES]
(
  [CHG_ID] [numeric](18, 0) IDENTITY(1,1) NOT NULL,
  [CHG_DATE] [datetime] NOT NULL,
  [CHG_TYPE] [varchar](20) NOT NULL,
  [CHG_BY] [nvarchar](256) NOT NULL,
  [APP_NAME] [nvarchar](128) NOT NULL,
  [HOST_NAME] [nvarchar](128) NOT NULL,
  [SCHEMA_NAME] [sysname] NOT NULL,
  [OBJECT_NAME] [sysname] NOT NULL,
  [XML_RECSET] [xml] NULL,
 CONSTRAINT [PK_LTC_CHG_ID] PRIMARY KEY CLUSTERED ([CHG_ID] ASC)
) ON [PRIMARY]
GO

-- Add defaults for key information
ALTER TABLE [AUDIT].[LOG_TABLE_CHANGES] ADD CONSTRAINT [DF_LTC_CHG_DATE] DEFAULT (getdate()) FOR [CHG_DATE];
ALTER TABLE [AUDIT].[LOG_TABLE_CHANGES] ADD CONSTRAINT [DF_LTC_CHG_TYPE] DEFAULT ('') FOR [CHG_TYPE];
ALTER TABLE [AUDIT].[LOG_TABLE_CHANGES] ADD CONSTRAINT [DF_LTC_CHG_BY] DEFAULT (coalesce(suser_sname(),'?')) FOR [CHG_BY];
ALTER TABLE [AUDIT].[LOG_TABLE_CHANGES] ADD CONSTRAINT [DF_LTC_APP_NAME] DEFAULT (coalesce(app_name(),'?')) FOR [APP_NAME];
ALTER TABLE [AUDIT].[LOG_TABLE_CHANGES] ADD CONSTRAINT [DF_LTC_HOST_NAME] DEFAULT (coalesce(host_name(),'?')) FOR [HOST_NAME];
GO



--
--  8 - Make DML trigger to capture changes
--


-- Delete existing trigger
IF OBJECT_ID('[ACTIVE].[TRG_FLUID_DATA]') IS NOT NULL 
  DROP TRIGGER [ACTIVE].[TRG_FLUID_DATA]
GO

-- Add trigger to log all changes
CREATE TRIGGER [ACTIVE].[TRG_FLUID_DATA] ON [ACTIVE].[CARS_BY_COUNTRY]
  FOR INSERT, UPDATE, DELETE AS
BEGIN

  -- Detect inserts
  IF EXISTS (select * from inserted) AND NOT EXISTS (select * from deleted)
  BEGIN
    INSERT [AUDIT].[LOG_TABLE_CHANGES] ([CHG_TYPE], [SCHEMA_NAME], [OBJECT_NAME], [XML_RECSET])
    SELECT 'INSERT', '[ACTIVE]', '[CARS_BY_COUNTRY]', (SELECT * FROM inserted as Record for xml auto, elements , root('RecordSet'), type)
    RETURN;
  END
    
  -- Detect deletes
  IF EXISTS (select * from deleted) AND NOT EXISTS (select * from inserted)
  BEGIN
    INSERT [AUDIT].[LOG_TABLE_CHANGES] ([CHG_TYPE], [SCHEMA_NAME], [OBJECT_NAME], [XML_RECSET])
    SELECT 'DELETE', '[ACTIVE]', '[CARS_BY_COUNTRY]', (SELECT * FROM deleted as Record for xml auto, elements , root('RecordSet'), type)
    RETURN;
  END

  -- Update inserts
  IF EXISTS (select * from inserted) AND EXISTS (select * from deleted)
  BEGIN
    INSERT [AUDIT].[LOG_TABLE_CHANGES] ([CHG_TYPE], [SCHEMA_NAME], [OBJECT_NAME], [XML_RECSET])
    SELECT 'UPDATE', '[ACTIVE]', '[CARS_BY_COUNTRY]', (SELECT * FROM deleted as Record for xml auto, elements , root('RecordSet'), type)
    RETURN;
  END

END;
GO



--
--  9 - Test DML trigger by updating, deleting and inserting data
--

-- Execute an update
UPDATE [ACTIVE].[CARS_BY_COUNTRY]
SET COUNTRY_NAME = 'Czech Republic'
WHERE COUNTRY_ID = 8
GO

-- Remove all data
DELETE FROM [ACTIVE].[CARS_BY_COUNTRY];
GO

-- Execute the load
EXECUTE [ACTIVE].[USP_LOAD_CARS_BY_COUNTRY];
GO 

-- Show the audit trail
SELECT * FROM [AUDIT].[LOG_TABLE_CHANGES]
GO

-- Disable the trigger
ALTER TABLE [ACTIVE].[CARS_BY_COUNTRY] DISABLE TRIGGER [TRG_FLUID_DATA];



-- 
-- 10 - Auditing schema changes (table for DDL trigger)
-- 

-- Delete existing table
IF OBJECT_ID('[AUDIT].[LOG_DATABASE_CHANGES]') IS NOT NULL 
  DROP TABLE [AUDIT].[LOG_DATABASE_CHANGES]
GO


-- Add the table
CREATE TABLE [AUDIT].[LOG_DATABASE_CHANGES]
(
  [CHG_ID] [numeric](18, 0) IDENTITY(1,1) NOT NULL,
  [CHG_DATE] [datetime] NOT NULL,
  [CHG_TYPE] [sysname] NOT NULL,
  [CHG_BY] [nvarchar](256) NOT NULL,
  [APP_NAME] [nvarchar](128) NOT NULL,
  [HOST_NAME] [nvarchar](128) NOT NULL,
  [SCHEMA_NAME] [sysname] NOT NULL,
  [OBJECT_NAME] [sysname] NOT NULL,
  [TSQL] nvarchar(MAX) NULL,
 CONSTRAINT [PK_LDC_CHG_ID] PRIMARY KEY CLUSTERED ([CHG_ID] ASC)
) ON [PRIMARY]
GO

-- Add defaults for key information
ALTER TABLE [AUDIT].[LOG_DATABASE_CHANGES] ADD CONSTRAINT [DF_LDC_CHG_DATE] DEFAULT (getdate()) FOR [CHG_DATE];
ALTER TABLE [AUDIT].[LOG_DATABASE_CHANGES] ADD CONSTRAINT [DF_LDC_CHG_TYPE] DEFAULT ('') FOR [CHG_TYPE];
ALTER TABLE [AUDIT].[LOG_DATABASE_CHANGES] ADD CONSTRAINT [DF_LDC_CHG_BY] DEFAULT (coalesce(suser_sname(),'?')) FOR [CHG_BY];
ALTER TABLE [AUDIT].[LOG_DATABASE_CHANGES] ADD CONSTRAINT [DF_LDC_APP_NAME] DEFAULT (coalesce(app_name(),'?')) FOR [APP_NAME];
ALTER TABLE [AUDIT].[LOG_DATABASE_CHANGES] ADD CONSTRAINT [DF_LDC_HOST_NAME] DEFAULT (coalesce(host_name(),'?')) FOR [HOST_NAME];
GO



--
--  11 - Make DDL trigger to capture schema changes
--


-- Remove trigger if it exists
IF EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[TRG_FLUID_SCHEMA]'))
  DROP TRIGGER [TRG_FLUID_SCHEMA]
GO

-- Add trigger to log all schema changes
CREATE TRIGGER [TRG_FLUID_SCHEMA] ON DATABASE 
FOR 
  DDL_TRIGGER_EVENTS, DDL_FUNCTION_EVENTS, DDL_PROCEDURE_EVENTS, DDL_TABLE_VIEW_EVENTS, DDL_DATABASE_SECURITY_EVENTS 
AS 
BEGIN

    -- Declare local variables
    DECLARE @VAR_DATA XML;
    DECLARE @VAR_SCHEMA sysname;
    DECLARE @VAR_OBJECT sysname; 
    DECLARE @VAR_EVENT_TYPE sysname;

    -- Set local variables
    SET @VAR_DATA = EVENTDATA();
    SET @VAR_EVENT_TYPE = @VAR_DATA.value('(/EVENT_INSTANCE/EventType)[1]', 'sysname');
    SET @VAR_SCHEMA = @VAR_DATA.value('(/EVENT_INSTANCE/SchemaName)[1]', 'sysname');
    SET @VAR_OBJECT = @VAR_DATA.value('(/EVENT_INSTANCE/ObjectName)[1]', 'sysname') 

    IF (( @VAR_EVENT_TYPE <> 'CREATE_STATISTICS') and ( @VAR_OBJECT not like 'syncobj_0x%')) 
    BEGIN
      INSERT [AUDIT].[LOG_DATABASE_CHANGES] ([OBJECT_NAME], [CHG_TYPE], [TSQL], [SCHEMA_NAME]) VALUES 
      ( 
        CONVERT(sysname, @VAR_OBJECT), 
	    @VAR_EVENT_TYPE, 
	    @VAR_DATA.value('(/EVENT_INSTANCE/TSQLCommand)[1]', 'nvarchar(max)'), 
	    CONVERT(sysname, @VAR_SCHEMA)
      );
    END

END;
GO



--
-- 12 - Test DDL trigger by creating, altering and droping a table.
--

-- Delete existing table
IF OBJECT_ID('[ACTIVE].[MAKES]') IS NOT NULL 
  DROP TABLE [ACTIVE].[MAKES]
GO

-- Create new table
CREATE TABLE [ACTIVE].[MAKES]
(
    MAKER_ID INT IDENTITY(1,1),
    MAKER_NM VARCHAR(25),
    START_YR SMALLINT,
    END_YR SMALLINT 
) 
GO

-- User defined constraint
ALTER TABLE [ACTIVE].[MAKES]
ADD CONSTRAINT CHK_START_YR CHECK (ISNULL(START_YR, 0) >= 1903 and ISNULL(START_YR, 0) <= YEAR(GETDATE()) + 1);
GO

-- User defined constraint
ALTER TABLE [ACTIVE].[MAKES]
ADD CONSTRAINT CHK_END_YR CHECK ( (END_YR IS NULL) OR (END_YR >= 1903 AND END_YR <= YEAR(GETDATE()) + 1) );
GO

-- Entity constraint - natural key
ALTER TABLE [ACTIVE].[MAKES]
ADD CONSTRAINT UNQ_MAKER_NM UNIQUE (MAKER_NM); 
GO

-- Entity constraint - surrogate key
ALTER TABLE [ACTIVE].[MAKES]
ADD CONSTRAINT PK_MAKER_ID PRIMARY KEY CLUSTERED (MAKER_ID);
GO

-- Delete existing table
IF OBJECT_ID('[ACTIVE].[MAKES]') IS NOT NULL 
  DROP TABLE [ACTIVE].[MAKES]
GO

-- Show the logged schema changes
SELECT TOP 6 * FROM [AUDIT].[LOG_DATABASE_CHANGES] ORDER BY CHG_ID DESC
GO

-- Disable the trigger
DISABLE TRIGGER [TRG_FLUID_SCHEMA] ON DATABASE;
GO



-- 
-- 13 - Preventing unwanted data changes (static data) - DML trigger
-- 

-- Remove trigger if it exists
IF OBJECT_ID('[ACTIVE].[TRG_STATIC_DATA]') IS NOT NULL 
  DROP TRIGGER [ACTIVE].[TRG_STATIC_DATA]
GO

-- Add trigger to prevent data changes
CREATE TRIGGER [ACTIVE].[TRG_STATIC_DATA] ON [ACTIVE].[CONTINENT]
FOR INSERT, UPDATE, DELETE AS
BEGIN

  -- Detect inserts
  IF EXISTS (select * from inserted) AND NOT EXISTS (select * from deleted)
    BEGIN
        ROLLBACK TRANSACTION;
        RAISERROR ('INSERTS ARE NOT ALLOWED ON TABLE [ACTIVE].[CONTINENT]!', 15, 1);
        RETURN;
    END
    
  -- Detect deletes
  IF EXISTS (select * from deleted) AND NOT EXISTS (select * from inserted)
    BEGIN
        ROLLBACK TRANSACTION;
        RAISERROR ('DELETES ARE NOT ALLOWED ON TABLE [ACTIVE].[CONTINENT]!', 15, 1);
        RETURN;
    END

  -- Detect updates
  IF EXISTS (select * from inserted) AND EXISTS (select * from deleted)
    BEGIN
        ROLLBACK TRANSACTION;
        RAISERROR ('UPDATES ARE NOT ALLOWED ON TABLE [ACTIVE].[CONTINENT]!', 15, 1);
        RETURN;
    END

END;
GO



--
-- 14 - Test DML trigger by inserting, updating or deleting data.
--

-- Inserts will fail
INSERT INTO [ACTIVE].[CONTINENT] VALUES ('M', 'The Moon');
GO

-- Deletes will fail
DELETE FROM [ACTIVE].[CONTINENT] WHERE CONTINENT_ID = 'G';
GO

-- Updates will fail
UPDATE [ACTIVE].[CONTINENT] SET CONTINENT_NAME = 'East Asia' WHERE CONTINENT_ID = 'B';
GO

-- Disable the trigger
DISABLE TRIGGER [ACTIVE].[TRG_STATIC_DATA] ON [ACTIVE].[CONTINENT];
GO



-- 
-- 15 - Preventing unwanted data changes (static data) - View Check Option
-- 

-- Remove view if it exists
IF OBJECT_ID('[ACTIVE].[PREVENT_CONTINENT_CHGS]') IS NOT NULL 
  DROP VIEW [ACTIVE].[PREVENT_CONTINENT_CHGS] 
GO

-- Add view to exclude out of range data (<> where clause)
CREATE VIEW [ACTIVE].[PREVENT_CONTINENT_CHGS] 
AS
    SELECT *
    FROM [ACTIVE].[CONTINENT] AS C
    WHERE C.CONTINENT_ID IN ('A', 'B','C', 'D', 'E', 'F', 'G')
WITH CHECK OPTION    
GO



--
-- 16 - Test view with check option by inserting, deleting and updating data
--

-- Inserts out of range will fail (where clause)
INSERT INTO [ACTIVE].[PREVENT_CONTINENT_CHGS]  VALUES ('M', 'The Moon');
GO

-- Deletes in range will work
DELETE FROM [ACTIVE].[PREVENT_CONTINENT_CHGS] WHERE CONTINENT_ID = 'G';
GO

-- Inserts in range will work
INSERT INTO [ACTIVE].[CONTINENT] VALUES 
    ('G', 'Antarctica');
GO

-- Updates out of range will fail (where clause)
UPDATE [ACTIVE].[PREVENT_CONTINENT_CHGS] SET CONTINENT_ID = 'Z' WHERE CONTINENT_ID = 'B';
GO

-- Remove the view 
DROP VIEW [ACTIVE].[PREVENT_CONTINENT_CHGS] 
GO



-- 
-- 17 - Preventing Truncate Table - Foreign Key Relationship
-- 


-- Get record count
SELECT COUNT(*) AS TOTAL_RECS FROM [ACTIVE].[CARS_BY_COUNTRY];
GO

-- Data goes bye bye
TRUNCATE TABLE [ACTIVE].[CARS_BY_COUNTRY];
GO


-- Get record count
SELECT COUNT(*) AS TOTAL_RECS FROM [ACTIVE].[CARS_BY_COUNTRY];
GO

-- Execute the load
EXECUTE [ACTIVE].[USP_LOAD_CARS_BY_COUNTRY];
GO 


-- Delete existing schema
IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'KEYS')
  DROP SCHEMA [KEYS]
GO

-- Make a schema to hold just keys
CREATE SCHEMA [KEYS] AUTHORIZATION [dbo];
GO


-- Remove table if it exists
IF OBJECT_ID('[KEYS].[CARS_BY_COUNTRY]') IS NOT NULL 
  DROP VIEW [KEYS].[CARS_BY_COUNTRY]
GO

-- Make key table for FK relationship
SELECT COUNTRY_ID INTO [KEYS].[CARS_BY_COUNTRY] FROM [ACTIVE].[CARS_BY_COUNTRY];
GO

-- Add FK to existing table
ALTER TABLE [KEYS].[CARS_BY_COUNTRY] WITH CHECK 
ADD CONSTRAINT [FK_COUNTRY_ID] FOREIGN KEY([COUNTRY_ID])
REFERENCES [ACTIVE].[CARS_BY_COUNTRY] ([COUNTRY_ID])
GO


-- Data sticks around now!
TRUNCATE TABLE [ACTIVE].[CARS_BY_COUNTRY];
GO

-- Remove constraint for next example
ALTER TABLE [KEYS].[CARS_BY_COUNTRY] DROP CONSTRAINT [FK_COUNTRY_ID];
GO



-- 
-- 18 - Preventing Unwanted Schema Changes
-- 

-- Remove trigger if it exists
IF EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[TRG_PROTECT_TABLES]'))
  DROP TRIGGER [TRG_PROTECT_TABLES]
GO

-- Create new trigger to prevent table changes
CREATE TRIGGER [TRG_PROTECT_TABLES] ON DATABASE 
FOR DROP_TABLE, ALTER_TABLE
AS
    -- Declare local variables
    DECLARE @VAR_DATA XML;
    DECLARE @VAR_SCHEMA sysname;
    DECLARE @VAR_OBJECT sysname; 
    DECLARE @VAR_EVENT_TYPE sysname;

    -- Set local variables
    SET @VAR_DATA = EVENTDATA();
    SET @VAR_EVENT_TYPE = @VAR_DATA.value('(/EVENT_INSTANCE/EventType)[1]', 'sysname');
    SET @VAR_SCHEMA = @VAR_DATA.value('(/EVENT_INSTANCE/SchemaName)[1]', 'sysname');
    SET @VAR_OBJECT = @VAR_DATA.value('(/EVENT_INSTANCE/ObjectName)[1]', 'sysname') 


    -- My tables must presist
    IF @VAR_OBJECT IN ('CARS_BY_COUNTRY', 'CONTINENT') AND (@VAR_SCHEMA = 'ACTIVE')
	BEGIN
	    DECLARE @err varchar(100)
        SET @err = 'Table ' + @VAR_OBJECT  + ' is super duper protected and cannot be dropped.'
        RAISERROR (@err, 16, 1) ;
        ROLLBACK;
    END
GO

-- Try this ...
-- We are protected!
DROP TABLE [ACTIVE].[CARS_BY_COUNTRY];
GO

-- Try this, oops!    
-- If we want to protect just active, need to check schema name! 
DROP TABLE [KEYS].[CARS_BY_COUNTRY];
GO

-- Disable the trigger
DISABLE TRIGGER [TRG_PROTECT_TABLES] ON DATABASE;
GO



--
-- 19 - Audit successful logins 
--


-- Select msdb database
USE MSDB;
GO

-- Delete existing table
IF OBJECT_ID('[AUDIT].[LOG_DATABASE_LOGINS]') IS NOT NULL 
  DROP TABLE [AUDIT].[LOG_DATABASE_LOGINS]
GO

-- Delete existing schema.
IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'AUDIT')
DROP SCHEMA [AUDIT]
GO


-- Add new schema.
CREATE SCHEMA [AUDIT] AUTHORIZATION [dbo]
GO

-- Create login audit table
CREATE TABLE [AUDIT].[LOG_DATABASE_LOGINS]
(
	[LoginId] BIGINT IDENTITY(1, 1) NOT NULL,
	[LoginDate] [datetime] NOT NULL,
	[LoginName] [varchar](20) NOT NULL,
	[AppName] [nvarchar](128) NOT NULL,
	[HostName] [nvarchar](128) NOT NULL,
 CONSTRAINT [pk_Ldl_LoginId] PRIMARY KEY CLUSTERED ([LoginId] ASC)
) ON [PRIMARY]
GO

-- Add defaults for key information
ALTER TABLE [AUDIT].[LOG_DATABASE_LOGINS] ADD CONSTRAINT [df_Ldl_LoginDate] DEFAULT (getdate()) FOR [LoginDate];
ALTER TABLE [AUDIT].[LOG_DATABASE_LOGINS] ADD CONSTRAINT [df_Ldl_LoginName] DEFAULT (coalesce(suser_sname(),'?')) FOR [LoginName];
ALTER TABLE [AUDIT].[LOG_DATABASE_LOGINS] ADD CONSTRAINT [df_Ldl_AppName] DEFAULT (coalesce(APP_NAME(),'?')) FOR [AppName];
ALTER TABLE [AUDIT].[LOG_DATABASE_LOGINS] ADD CONSTRAINT [df_Ldl_HostName] DEFAULT (coalesce(HOST_NAME(),'?')) FOR [HostName];
GO


-- Remove existing logon trigger
IF EXISTS (SELECT * FROM master.sys.server_triggers 
   WHERE parent_class_desc = 'SERVER' AND name = N'AUDIT_SERVER_LOGINS')
DROP TRIGGER [AUDIT_SERVER_LOGINS] ON ALL SERVER
GO

-- Create new logon trigger
CREATE TRIGGER AUDIT_SERVER_LOGINS
ON ALL SERVER WITH EXECUTE AS 'SA' FOR LOGON
AS
BEGIN
     -- Add to the audit table
     INSERT INTO [MSDB].[AUDIT].[LOG_DATABASE_LOGINS] (LoginName, LoginDate)
     VALUES (ORIGINAL_LOGIN(), GETDATE());
END;
GO

-- Test auditing
  -- USER=[AUTO_USER] 
  -- PASSWORD=N'SzfX6ThnLeDPwpelMHYdV2MW'

-- Disable the trigger
DISABLE TRIGGER [AUDIT_SERVER_LOGINS] ON ALL SERVER;
GO



--
-- 20 - Prevent user logins during maintenance 
--

-- Remove existing logon trigger
IF EXISTS 
(
  SELECT * FROM master.sys.server_triggers 
  WHERE parent_class_desc = 'SERVER' AND name = N'DISALLOW_USERS'
)
DROP TRIGGER [DISALLOW_USERS] ON ALL SERVER
GO

-- Create new logon trigger
CREATE TRIGGER DISALLOW_USERS
ON ALL SERVER FOR LOGON
AS
BEGIN

    -- Perform maintenance between 11 PM and 7 AM
    IF ( (ORIGINAL_LOGIN() <> 'HQ\jminer') AND (DATEPART(HOUR, GETDATE()) IN (23, 0, 1, 2, 3, 4, 5, 6)) )
        ROLLBACK;

END;
GO

-- Manually change system clock

-- Test disallow users during maintenance
  -- USER=[AUTO_USER] 
  -- PASSWORD=N'SzfX6ThnLeDPwpelMHYdV2MW'

-- Disable the trigger
DISABLE TRIGGER [DISALLOW_USERS] ON ALL SERVER;
GO



--
-- 22 - Stop MS Office Users
--

-- Remove existing logon trigger
IF  EXISTS (SELECT * FROM master.sys.server_triggers 
WHERE parent_class_desc = 'SERVER' AND name = N'DISALLOW_MS_OFFICE')
  DROP TRIGGER [DISALLOW_MS_OFFICE] ON ALL SERVER
GO

-- Create new logon trigger
CREATE TRIGGER DISALLOW_MS_OFFICE
ON ALL SERVER FOR LOGON
AS
BEGIN
    -- Account for case sensitive collations
    IF LOWER(APP_NAME()) LIKE '%microsoft office system%' 
        ROLLBACK;
END;


-- Manually connect via access

-- Test disallow users during maintenance
  -- USER=[AUTO_USER] 
  -- PASSWORD=N'SzfX6ThnLeDPwpelMHYdV2MW'

-- Disable the trigger
DISABLE TRIGGER DISALLOW_MS_OFFICE ON ALL SERVER;
GO



-- 
-- 23 - CHANGE DATA TRACKING - NEW IN 2008!
-- 

-- http://www.simple-talk.com/sql/learn-sql-server/introduction-to-change-data-capture-(cdc)-in-sql-server-2008/


-- Step A - Clean up play database?

-- Use the correct database
USE [AUTOS]
GO

-- Drop database triggers
DROP TRIGGER [TRG_FLUID_SCHEMA] ON DATABASE;
DROP TRIGGER [TRG_PROTECT_TABLES] ON DATABASE;
GO

-- Drop tables
DROP TABLE [KEYS].[CARS_BY_COUNTRY];
DROP TABLE [AUDIT].[LOG_DATABASE_CHANGES];
DROP TABLE [AUDIT].[LOG_TABLE_CHANGES];
GO

-- Truncate tables
DELETE FROM [ACTIVE].[CARS_BY_COUNTRY];
DELETE FROM [ACTIVE].[CONTINENT];
GO

-- Show empty tables
SELECT * FROM [ACTIVE].[CARS_BY_COUNTRY];
SELECT * FROM [ACTIVE].[CONTINENT];
GO



-- Step B - Is CDC enabled?

-- Use the master db
USE master 
GO 

-- Check the information from database system table
SELECT 
  CAST([name] AS NVARCHAR(25)) AS database_nm, 
  database_id, 
  is_cdc_enabled  
FROM sys.databases s
WHERE [name] NOT IN ('master', 'msdb', 'model', 'tempdb')
ORDER BY [name]
GO    

-- Switch owner to system admin
ALTER AUTHORIZATION ON DATABASE::AUTOS TO SA;
GO


-- Step C - Enable CDC for AUTOS database
USE AUTOS
GO

EXEC sys.sp_cdc_enable_db
GO


-- Step C - What tables are tracked by CDC?
USE AUTOS 
GO 

SELECT  
  CAST(s.[name] AS NVARCHAR(25)) AS schema_nm, 
  CAST(t.[name] AS NVARCHAR(25)) AS table_nm, 
  is_tracked_by_cdc  
FROM sys.tables t JOIN sys.schemas s ON t.SCHEMA_ID = s.schema_id
GO 


-- Step D - make sure SQL Agent is running, add tables to CDC

EXEC sys.sp_cdc_enable_table 
@source_schema = N'ACTIVE', 
@source_name   = N'CARS_BY_COUNTRY', 
@role_name     = NULL 
GO

EXEC sys.sp_cdc_enable_table 
@source_schema = N'ACTIVE', 
@source_name   = N'CONTINENT', 
@role_name     = NULL 
GO


-- Step E - Test CDC by executing DML statements

-- (INSERT) --

-- Insert data using sp's above
EXECUTE [ACTIVE].[USP_LOAD_CONTINENT];
EXECUTE [ACTIVE].[USP_LOAD_CARS_BY_COUNTRY];
GO

-- Insert data using stmts above and check cdc tables
SELECT * FROM cdc.ACTIVE_CONTINENT_CT
SELECT * FROM cdc.ACTIVE_CARS_BY_COUNTRY_CT
GO


-- (UPDATE) --

-- Update data in the tables, 1 row each
UPDATE [ACTIVE].[CARS_BY_COUNTRY]
SET COUNTRY_NAME = 'Czech Republic'
WHERE COUNTRY_ID = 8;
GO

UPDATE [ACTIVE].[CONTINENT]
SET CONTINENT_NAME = 'No residents here!'
WHERE CONTINENT_ID = 'G';
GO

-- Check cdc tables
SELECT * FROM cdc.ACTIVE_CONTINENT_CT
SELECT * FROM cdc.ACTIVE_CARS_BY_COUNTRY_CT


-- (DELETE) --

-- Delete data in the tables, 1 row each
DELETE FROM [ACTIVE].[CARS_BY_COUNTRY]
WHERE COUNTRY_ID = 8
GO

DELETE FROM [ACTIVE].[CONTINENT]
WHERE CONTINENT_ID = 'G'
GO

-- Check cdc tables
SELECT * FROM cdc.ACTIVE_CONTINENT_CT
SELECT * FROM cdc.ACTIVE_CARS_BY_COUNTRY_CT
GO


-- STEP F - Add computed column, no change

-- Add the Personal Vehicle calculation  
ALTER TABLE [ACTIVE].[CARS_BY_COUNTRY] 
ADD PERSONAL_PCT AS (
  CASE 
    WHEN TOTAL_VEHICLES <> 0 THEN
      CAST(PERSONAL_VEHICLES AS FLOAT) / CAST(TOTAL_VEHICLES AS FLOAT) * CAST(100 AS FLOAT)
    ELSE
      CAST(0 AS FLOAT)
    END
   ) PERSISTED
GO

-- Check the tracking table
SELECT * FROM cdc.ddl_history
GO


-- STEP G - Add a real column
ALTER TABLE [ACTIVE].[CARS_BY_COUNTRY] 
ADD GOVT_SPONSORED CHAR(1) DEFAULT 'Y';
GO

-- Check the tracking table
SELECT * FROM cdc.ddl_history
GO

-- Try truncate table, does not work
TRUNCATE TABLE [ACTIVE].[CONTINENT]
GO

-- Try drop foreign key
ALTER TABLE [ACTIVE].[CARS_BY_COUNTRY]
DROP CONSTRAINT [FK_CONTINENT_ID] 
GO

-- Try drop table
DROP TABLE [ACTIVE].[CONTINENT]
GO


--
-- NOTE: The cdc.captured_columns nor the _CT have changed for new fields
--   There is no user id so that we know who made the change.
--


-- H - Disabling CDC at the table level
USE AUTOS
GO

-- What tables are involved?
EXEC sys.sp_cdc_help_change_data_capture 
GO

-- Remove tracking from the tables
EXEC sys.sp_cdc_disable_table 
@source_schema = N'ACTIVE', 
@source_name   = N'CARS_BY_COUNTRY',
@capture_instance = N'ACTIVE_CARS_BY_COUNTRY';
GO

EXEC sys.sp_cdc_disable_table 
@source_schema = N'ACTIVE', 
@source_name   = N'CONTINENT', 
@capture_instance    = N'ACTIVE_CONTINENT';
GO


-- I - Disabling CDC at the database level
USE AUTOS
GO 

EXEC sys.sp_cdc_disable_db 
GO 

