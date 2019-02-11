/******************************************************
 *
 * Name:         Step 6 - audit-n-prevent-schema-changes.sql
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
 *       A - Database triggers for recording schema changes
 *       B - Database triggers for preventing schema changes
 *
 ******************************************************/


-- 
-- 1 - Auditing schema changes (table for DDL trigger)
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
--  2 - Make DDL trigger to capture schema changes
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
-- 3 - Test DDL trigger by creating, altering and droping a table.
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

-- Disable the database trigger
DISABLE TRIGGER [TRG_FLUID_SCHEMA] ON DATABASE;
GO


-- 
-- 4 - Preventing Unwanted Schema Changes
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
    IF @VAR_OBJECT IN ('LOG_DATABASE_CHANGES') AND (@VAR_SCHEMA = 'AUDIT')
	BEGIN
	    DECLARE @err varchar(100)
        SET @err = 'Table ' + @VAR_OBJECT  + ' is super duper protected and cannot be dropped.'
        RAISERROR (@err, 16, 1) ;
        ROLLBACK;
    END
GO

-- Try this ...
-- We are protected!
DROP TABLE [AUDIT].[LOG_DATABASE_CHANGES]
GO

-- Disable the database trigger
DISABLE TRIGGER [TRG_PROTECT_TABLES] ON DATABASE;
GO
