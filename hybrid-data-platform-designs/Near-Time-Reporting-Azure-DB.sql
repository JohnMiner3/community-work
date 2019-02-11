/******************************************************
 *
 * Name:         near-time-reporting-azure-db.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     02-08-2016
 *     Purpose:  Capture changes
 * 
 ******************************************************/

-- Which database to use.
USE [BANKING01]
GO

/*
   1 - Create log schema
*/

-- Which database to use.
USE [BANKING01]
GO

-- Delete existing schema.
IF EXISTS (SELECT * FROM sys.schemas WHERE name = N'LOG')
DROP SCHEMA [LOG]
GO

-- Add new schema.
CREATE SCHEMA [LOG] AUTHORIZATION [dbo]
GO

-- Show the new schema
SELECT * FROM sys.schemas WHERE name = 'LOG';
GO


/*  
	2 - Create audit table
*/

-- Remove table if it exists
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = 
    OBJECT_ID(N'[LOG].[DML_CHANGES]') AND type in (N'U'))
DROP TABLE [LOG].[DML_CHANGES]
GO

CREATE TABLE [LOG].[DML_CHANGES]
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
ALTER TABLE [LOG].[DML_CHANGES] 
    ADD CONSTRAINT [df_Ltc_ChangeDate] DEFAULT (getdate()) FOR [ChangeDate];

ALTER TABLE [LOG].[DML_CHANGES] 
    ADD CONSTRAINT [df_Ltc_ChangeType] DEFAULT ('') FOR [ChangeType];

ALTER TABLE [LOG].[DML_CHANGES] 
    ADD CONSTRAINT [df_Ltc_ChangeBy] DEFAULT (coalesce(suser_sname(),'?')) FOR [ChangeBy];

ALTER TABLE [LOG].[DML_CHANGES] 
    ADD CONSTRAINT [df_Ltc_AppName] DEFAULT (coalesce(APP_NAME(),'?')) FOR [AppName];

ALTER TABLE [LOG].[DML_CHANGES] 
    ADD CONSTRAINT [df_Ltc_HostName] DEFAULT (coalesce(HOST_NAME(),'?')) FOR [HostName];
GO


/*  
	3 - Create triggers for logging actions
*/

-- Local variables
DECLARE @MYSCH AS sysname;
DECLARE @MYOBJ AS sysname;
DECLARE @MYSQL AS VARCHAR(MAX);
DECLARE @VARSCH AS sysname;
DECLARE @VAROBJ AS sysname;
DECLARE @VARFULL AS sysname;
DECLARE @RET AS VARCHAR(2);

-- Allocate cursor, return table names
DECLARE MYTRG CURSOR FAST_FORWARD FOR 
    SELECT s.name AS SchemaName,  o.name AS ObjectName
    FROM sys.objects o (NOLOCK) JOIN  sys.schemas s (NOLOCK) ON o.[schema_id] = s.[schema_id]
    WHERE o.[type] = 'U' AND s.[name] <> 'LOG'
    ORDER BY s.name, o.name;

-- Open cursor    
OPEN MYTRG;

-- Set the cr/lf
SELECT @RET = CHAR(10) --+ CHAR(13);

-- Get the first row    
FETCH NEXT FROM MYTRG INTO @MYSCH, @MYOBJ;

-- While there is data
WHILE (@@FETCH_STATUS = 0) 
BEGIN   

    -- Set up variables
    SELECT @VARSCH = QuoteName(@MYSCH);
    SELECT @VAROBJ = QuoteName(@MYOBJ);
    SELECT @VARFULL = QuoteName(@MYSCH) + '.' + QuoteName(@MYOBJ);

    -- Show table name
    --PRINT 'Making trigger for ' + @VARFULL;

    -- Dynamic SQL to drop object
    SELECT @MYSQL = 'IF  EXISTS (SELECT * FROM sys.triggers WHERE name = ' + CHAR(39) 
        + 'TRG_TRACK_DML_CHGS_' + @MYOBJ + CHAR(39) + ') DROP TRIGGER ' 
        + @VARSCH + '.' + QUOTENAME('TRG_TRACK_DML_CHGS_' + @MYOBJ) + ';';
    EXEC (@MYSQL);
    --PRINT @MYSQL
    
    -- Dynamic SQL to create object
    SELECT @MYSQL = ''
    SELECT @MYSQL = @MYSQL + 'CREATE TRIGGER ' + @VARSCH + '.' + QUOTENAME('TRG_TRACK_DML_CHGS_' + @MYOBJ) + ' ON ' + @VARFULL + ' ' + @RET
    SELECT @MYSQL = @MYSQL + 'FOR INSERT, UPDATE, DELETE AS ' + @RET + @RET
    SELECT @MYSQL = @MYSQL + '  -- Author:  John Miner ' + @RET
    SELECT @MYSQL = @MYSQL + '  -- Date:  May 2012' + @RET
    SELECT @MYSQL = @MYSQL + '  -- Purpose:  Automated change detection trigger (ins, upd, del).' + @RET + @RET

    SELECT @MYSQL = @MYSQL + 'BEGIN ' + @RET + @RET

    SELECT @MYSQL = @MYSQL + '  -- Detect inserts' + @RET
    SELECT @MYSQL = @MYSQL + '  IF EXISTS (SELECT * FROM inserted) AND NOT EXISTS(SELECT * FROM deleted) ' + @RET
    SELECT @MYSQL = @MYSQL + '    BEGIN ' + @RET
    SELECT @MYSQL = @MYSQL + '	  INSERT [LOG].[DML_CHANGES] ([ChangeType], [SchemaName], [ObjectName], [XmlRecSet]) ' + @RET
    SELECT @MYSQL = @MYSQL + '      SELECT ' + CHAR(39) + 'Insert'  + CHAR(39) + ', ' + CHAR(39) + @VARSCH  + CHAR(39) + ', ' 
        + CHAR(39) + @VAROBJ + CHAR(39) 
        + ', (SELECT * FROM inserted as Record FOR XML AUTO, elements , root(' + CHAR(39) + 'RecordSet' + CHAR(39) + '), type); ' + @RET
    SELECT @MYSQL = @MYSQL + '      RETURN; ' + @RET
    SELECT @MYSQL = @MYSQL + '    END; ' + @RET + @RET
    
    SELECT @MYSQL = @MYSQL + '  -- Detect deletes' + @RET
    SELECT @MYSQL = @MYSQL + '  IF EXISTS (SELECT * FROM deleted) AND NOT EXISTS(SELECT * FROM inserted) ' + @RET    
    SELECT @MYSQL = @MYSQL + '    BEGIN ' + @RET
    SELECT @MYSQL = @MYSQL + '      INSERT [LOG].[DML_CHANGES] ([ChangeType], [SchemaName], [ObjectName], [XmlRecSet]) ' + @RET
    SELECT @MYSQL = @MYSQL + '      SELECT ' + CHAR(39) + 'Delete'  + CHAR(39) + ', ' + CHAR(39) + @VARSCH  + CHAR(39) + ', ' 
        + CHAR(39) + @VAROBJ + CHAR(39) 
        + ', (SELECT * FROM deleted as Record FOR XML AUTO, elements , root(' + CHAR(39) + 'RecordSet' + CHAR(39) + '), type); ' + @RET
    SELECT @MYSQL = @MYSQL + '      RETURN; ' + @RET
    SELECT @MYSQL = @MYSQL + '    END; ' + @RET + @RET
        
    SELECT @MYSQL = @MYSQL + '  -- Update inserts' + @RET
    SELECT @MYSQL = @MYSQL + '  IF EXISTS (SELECT * FROM inserted) AND EXISTS(SELECT * FROM deleted) ' + @RET
    SELECT @MYSQL = @MYSQL + '    BEGIN ' + @RET
    SELECT @MYSQL = @MYSQL + '      INSERT [LOG].[DML_CHANGES] ([ChangeType], [SchemaName], [ObjectName], [XmlRecSet]) ' + @RET
    SELECT @MYSQL = @MYSQL + '      SELECT ' + CHAR(39) + 'Update'  + CHAR(39) + ', ' + CHAR(39) + @VARSCH  + CHAR(39) + ', ' 
        + CHAR(39) + @VAROBJ + CHAR(39) 
        + ', (SELECT * FROM deleted as Record FOR XML AUTO, elements , root(' + CHAR(39) + 'RecordSet' + CHAR(39) + '), type); ' + @RET
    SELECT @MYSQL = @MYSQL + '      RETURN; ' + @RET
    SELECT @MYSQL = @MYSQL + '    END; ' + @RET + @RET

    SELECT @MYSQL = @MYSQL + 'END;' 

    -- Execute the SQL stmt
    EXEC (@MYSQL);
    --PRINT @MYSQL
    
    -- Get the next row
    FETCH NEXT FROM MYTRG INTO @MYSCH, @MYOBJ;
    
END;

-- Close the cursor
CLOSE MYTRG; 

-- Release the cursor
DEALLOCATE MYTRG; 
GO


/*  
	4 - Test the triggers
*/

-- First record
SELECT TOP 1 * 
FROM [ACTIVE].[TRANSACTION];

-- Sample update action
UPDATE T
SET T.TRAN_AMOUNT = - T.TRAN_AMOUNT
FROM [ACTIVE].[TRANSACTION] AS T
WHERE TRAN_ID = 1;
GO

-- Last record
SELECT TOP 1 * 
FROM [ACTIVE].[TRANSACTION] AS T
ORDER BY T.TRAN_ID DESC;
GO

-- Removed record
DELETE 
FROM [ACTIVE].[TRANSACTION] 
WHERE TRAN_ID = 8624681;
GO

-- Re-insert deleted record
INSERT INTO [ACTIVE].[TRANSACTION] 
VALUES
(
-999.0000,
'2015-11-17 00:00:00',
25000
);

-- Show the log table
SELECT * FROM [LOG].[DML_CHANGES];
GO



/*
    5 - Turn on ad hoc distributed queries
*/
 
-- Just shows standard options
sp_configure;
GO
 
-- Turn on advance options
sp_configure 'show advanced options', 1;
GO
 
-- Reconfigure the server
reconfigure;
GO
 
-- Turn on ad hoc dist queries
sp_configure 'Ad Hoc Distributed Queries', 1;
GO
 
-- Reconfigure the server
reconfigure;
GO


/*
    6 - Remove existing linked server
*/

-- Table to hold server info
DECLARE @LinkServers TABLE 
(
    SRV_NAME sysname NULL,
    SRV_PROVIDERNAME nvarchar(128) NULL,
    SRV_PRODUCT nvarchar(128) NULL,
    SRV_DATASOURCE nvarchar(4000) NULL,
    SRV_PROVIDERSTRING nvarchar(4000) NULL,
    SRV_LOCATION nvarchar(4000) NULL,
    SRV_CAT sysname NULL
);

-- Are there any existing linked servers
INSERT INTO @LinkServers
    EXEC sp_linkedservers;

-- Show servers
SELECT * FROM @LinkServers;

-- Remove servers / logins
IF EXISTS (SELECT * FROM @LinkServers WHERE SRV_NAME like 'MyAzureDb')
    EXEC sp_dropserver 'MyAzureDb', 'droplogins';
GO


/*
    7 - Create new linked server
*/

-- Make a link to the cloud
EXEC sp_addlinkedserver   
   @server=N'MyAzureDb', 
   @srvproduct=N'Azure SQL Db',
   @provider=N'SQLNCLI', 
   @datasrc=N'ri2016ssug.database.windows.net,1433',
   @catalog='BANKING01';
GO

--Set up login mapping
EXEC sp_addlinkedsrvlogin 
    @rmtsrvname = 'MyAzureDb', 
    @useself = 'FALSE', 
    @locallogin=NULL,
    @rmtuser = 'JMINER',
    @rmtpassword = 'RI2016ssug#1'
GO

-- Test the connection
sp_testlinkedserver MyAzureDb;
GO


/*
    8 - Test linked server
*/

SELECT top 5 * 
FROM [MyAzureDb].[banking01].[Active].[Transaction]
GO


/*
    9 - Create december 2015 data
*/

-- Clear the log table
TRUNCATE TABLE [LOG].[DML_CHANGES];
GO

-- Add data
EXEC [ACTIVE].[USP_MAKE_TRANSACTIONS] 
	    @VAR_GIVEN_MONTH = '12-01-2015', 
		@VAR_VERBOSE_IND = 1;
GO


/*
    10 - How do we parse XML?
*/

-- Show top records xml
SELECT TOP 1 * FROM [LOG].[DML_CHANGES]
GO

-- Convert xml to recordset
SELECT 
    p.value('(./TRAN_ID)[1]', 'INT') AS TRAN_ID,
    p.value('(./TRAN_AMOUNT)[1]', 'MONEY') AS TRAN_AMOUNT,
    p.value('(./TRAN_DATE)[1]', 'DATETIME') AS TRAN_DATE,
    p.value('(./ACCT_ID)[1]', 'INT') AS ACCT_ID
FROM 
    [LOG].[DML_CHANGES]
CROSS APPLY 
    XmlRecSet.nodes('/RecordSet/Record') t(p)
WHERE 
    ChangeType = 'Insert' 
	AND ChangeId = 1;
GO



/*
    11 - Move data to Azure
*/

INSERT INTO [MyAzureDb].[banking01].[Active].[Transaction]
(TRAN_AMOUNT, TRAN_DATE, ACCT_ID)
SELECT 
--    p.value('(./TRAN_ID)[1]', 'INT') AS TRAN_ID,
    p.value('(./TRAN_AMOUNT)[1]', 'MONEY') AS TRAN_AMOUNT,
    p.value('(./TRAN_DATE)[1]', 'DATETIME') AS TRAN_DATE,
    p.value('(./ACCT_ID)[1]', 'INT') AS ACCT_ID
FROM 
    [LOG].[DML_CHANGES]
CROSS APPLY 
    XmlRecSet.nodes('/RecordSet/Record') t(p)
WHERE 
    ChangeType = 'Insert' 
	AND ChangeId = 1;
GO

/*
   Note:  Performance was really bad.
*/