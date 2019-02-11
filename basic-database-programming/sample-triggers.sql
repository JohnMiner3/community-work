/******************************************************
 *
 * Name:         sample-triggers.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     06-12-2012
 *     Purpose:  Demonstrate how to create, alter and 
 *               drop triggers.
 * 
 ******************************************************/


--
--  Create sample database 
--

-- Start with master database
USE MASTER;
GO

-- Delete existing databases.
IF EXISTS (SELECT name FROM sys.databases WHERE name = N'AUTOS')
DROP DATABASE [AUTOS]
GO

-- Create a autos database
CREATE DATABASE AUTOS;
GO

-- Use the database
USE [AUTOS]
GO

-- Create a USA schema
CREATE SCHEMA USA AUTHORIZATION dbo;
GO

-- Create a BRANDS table
CREATE TABLE USA.BRANDS
(
MyId INT PRIMARY KEY CLUSTERED,
MyValue VARCHAR(20)
)
GO

-- Load the table with data
INSERT INTO USA.BRANDS (MyId, MyValue) VALUES
(1, 'Continental'),
(2, 'Edsel'),
(3, 'Lincoln'),
(4, 'Mercury'),
(5, 'Ram')
GO

-- Show the data
SELECT * FROM USA.BRANDS
GO


--
-- Prevent data modifications - http://craftydba.com/?p=1923
--

-- Delete the existing trigger.
IF EXISTS (select * from sysobjects where id = object_id('TRG_PREVENT_CHANGES') and type = 'TR')
   DROP TRIGGER [USA].[TRG_PREVENT_CHANGES]
GO

-- Create the new trigger.
CREATE TRIGGER [USA].[TRG_PREVENT_CHANGES] on [USA].[BRANDS]
FOR INSERT, UPDATE, DELETE NOT FOR REPLICATION
AS

BEGIN

    -- declare local variable
    DECLARE @MYMSG VARCHAR(250);

    -- nothing to do?
    IF (@@rowcount = 0) RETURN;

    -- do not count rows
    SET NOCOUNT ON;

    -- deleted data
    IF NOT EXISTS (SELECT * FROM inserted) 
        BEGIN
            SET @MYMSG = 'The read only [USA].[BRANDS] table does not allow records to be deleted!'
            ROLLBACK TRANSACTION;
            RAISERROR (@MyMsg, 15, 1);
            RETURN;
        END

    ELSE 
        BEGIN
        
            -- inserted data
            IF NOT EXISTS (SELECT * FROM deleted)  	        
                SET @MYMSG = 'The read only [USA].[BRANDS] table does not allow new records to be inserted!'
                
            -- updated data
            ELSE
                SET @MYMSG = 'The read only [USA].[BRANDS] table does not allow records to be updated!'

            ROLLBACK TRANSACTION;
            RAISERROR (@MyMsg, 15, 1);
            RETURN;
       END
       	
END
GO

-- Try to delete a record
DELETE FROM USA.BRANDS WHERE MyValue = 'Lincoln';
GO

-- Try to update a record
UPDATE USA.BRANDS SET MyValue = 'Ford' WHERE MyId = 1;
GO

-- Try to insert a new record
INSERT INTO USA.BRANDS (MyId, MyValue) VALUES (6, 'Ford');
GO

-- Disable the trigger
DISABLE TRIGGER [USA].[TRG_PREVENT_CHANGES] on [USA].[BRANDS];


--
-- Auditing data modifications - http://craftydba.com/?p=2060
--

-- Delete existing schema.
IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'ADT')
DROP SCHEMA [ADT]
GO

-- Add new schema.
CREATE SCHEMA [ADT] AUTHORIZATION [dbo]
GO


-- Remove table if it exists
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = 
    OBJECT_ID(N'[ADT].[LOG_DML_CHANGES]') AND type in (N'U'))
DROP TABLE [ADT].[LOG_DML_CHANGES]
GO

CREATE TABLE [ADT].[LOG_DML_CHANGES]
(
	[ChangeId]BIGINT IDENTITY(1,1) NOT NULL,
	[ChangeDate] [datetime] NOT NULL,
	[ChangeType] [varchar](20) NOT NULL,
	[ChangeBy] [nvarchar](256) NOT NULL,
	[AppName] [nvarchar](128) NOT NULL,
	[HostName] [nvarchar](128) NOT NULL,
	[SchemaName] [sysname] NOT NULL,
	[ObjectName] [sysname] NOT NULL,
	[XmlRecSet] [xml] NULL,
 CONSTRAINT [pk_Ltc_ChangeId] PRIMARY KEY CLUSTERED ([ChangeId] ASC)
) 
GO

-- Add defaults for key information
ALTER TABLE [ADT].[LOG_DML_CHANGES] 
    ADD CONSTRAINT [df_Ltc_ChangeDate] DEFAULT (getdate()) FOR [ChangeDate];

ALTER TABLE [ADT].[LOG_DML_CHANGES] 
    ADD CONSTRAINT [df_Ltc_ChangeType] DEFAULT ('') FOR [ChangeType];

ALTER TABLE [ADT].[LOG_DML_CHANGES] 
    ADD CONSTRAINT [df_Ltc_ChangeBy] DEFAULT (coalesce(suser_sname(),'?')) FOR [ChangeBy];

ALTER TABLE [ADT].[LOG_DML_CHANGES] 
    ADD CONSTRAINT [df_Ltc_AppName] DEFAULT (coalesce(APP_NAME(),'?')) FOR [AppName];

ALTER TABLE [ADT].[LOG_DML_CHANGES] 
    ADD CONSTRAINT [df_Ltc_HostName] DEFAULT (coalesce(HOST_NAME(),'?')) FOR [HostName];
GO

-- Should be no data
SELECT * FROM [ADT].[LOG_DML_CHANGES]
GO




--
-- Audit trigger
--

-- Delete the existing trigger.
IF EXISTS (select * from sysobjects where id = object_id('TRG_TRACK_DML_CHGS_BRANDS') and type = 'TR')
   DROP TRIGGER [USA].[TRG_TRACK_DML_CHGS_BRANDS]
GO

-- Create new trigger
CREATE TRIGGER [USA].[TRG_TRACK_DML_CHGS_BRANDS] ON [USA].[BRANDS] 
FOR INSERT, UPDATE, DELETE AS 

  -- Author:   John Miner 
  -- Date:     May 2012
  -- Purpose:  Automated change detection trigger (ins, upd, del).

BEGIN 

  -- Detect inserts
  IF EXISTS (SELECT * FROM inserted) AND NOT EXISTS(SELECT * FROM deleted) 
    BEGIN 
	  INSERT [ADT].[LOG_DML_CHANGES] ([ChangeType], [SchemaName], [ObjectName], [XmlRecSet]) 
      SELECT 'Insert', '[USA]', '[BRANDS]', (SELECT * FROM inserted as Record FOR XML AUTO, elements , root('RecordSet'), type); 
      RETURN; 
    END; 

  -- Detect deletes
  IF EXISTS (SELECT * FROM deleted) AND NOT EXISTS(SELECT * FROM inserted) 
    BEGIN 
      INSERT [ADT].[LOG_DML_CHANGES] ([ChangeType], [SchemaName], [ObjectName], [XmlRecSet]) 
      SELECT 'Delete', '[USA]', '[BRANDS]', (SELECT * FROM deleted as Record FOR XML AUTO, elements , root('RecordSet'), type); 
      RETURN; 
    END; 

  -- Update inserts
  IF EXISTS (SELECT * FROM inserted) AND EXISTS(SELECT * FROM deleted) 
    BEGIN 
      INSERT [ADT].[LOG_DML_CHANGES] ([ChangeType], [SchemaName], [ObjectName], [XmlRecSet]) 
      SELECT 'Update', '[USA]', '[BRANDS]', (SELECT * FROM deleted as Record FOR XML AUTO, elements , root('RecordSet'), type); 
      RETURN; 
    END; 

END;
GO


-- Load the table with data
INSERT INTO USA.BRANDS (MyId, MyValue) VALUES
(6, 'Tesla')
GO