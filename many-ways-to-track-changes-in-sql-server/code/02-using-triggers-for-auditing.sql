/******************************************************
 *
 * Name:         02-using-triggers-for-auditing.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     05-01-2026
 *     Purpose:  Server + Database Audits
 * 
 ******************************************************/
 

-- Which database to use.
USE [master]
GO


-- Add new login. 
CREATE LOGIN [hippa_user] 
WITH PASSWORD = N'SzfX6ThnLeDPwpelMHYdV2MW',
     DEFAULT_DATABASE = [dbs_hippa01],
     CHECK_POLICY = ON; 


-- Which database to use.
USE [dbs_hippa01]
GO

-- Do not display rec cnts
SET NOCOUNT ON 
 
/* 
   1 - Create audit schema 
*/ 
  
-- Delete existing schema 
DROP SCHEMA IF EXISTS [audit] 
GO 
  
-- Add schema for audit purposes 
CREATE SCHEMA [audit] AUTHORIZATION [dbo] 
GO 
  
-- Show database schemas 
SELECT * FROM sys.schemas 
WHERE principal_id < 16384 
GO 


/* 
   2 - Create contained user 
*/ 
  
-- Add new user. 
CREATE USER [hippa_user] 
FOR LOGIN [hippa_user] 
GO 

-- Show the user 
SELECT * FROM sys.database_principals 
WHERE type_desc = 'SQL_USER' 
GO


/* 
   3 - Create database role 
*/ 
  
-- Delete existing role. 
DROP ROLE IF EXISTS [hippa_role] 
GO 
  
-- Create database role 
CREATE ROLE [hippa_role] AUTHORIZATION [dbo] 
GO 


/* 
   4 - Add user 2 role & apply permissions 2 schema 
*/ 
  
-- Apply permissions to schema 
GRANT INSERT ON SCHEMA::[active] TO [hippa_role] 
GRANT UPDATE ON SCHEMA::[active] TO [hippa_role] 
GRANT DELETE ON SCHEMA::[active] TO [hippa_role] 
GRANT SELECT ON SCHEMA::[active] TO [hippa_role] 
GO 
  
-- Add user to role 
EXEC sp_addrolemember N'hippa_role', N'hippa_user' 
GO 



/* 
   5 - Show role membership 
*/ 
  
-- Show role membership 
SELECT 
  DP1.name AS Roles_Name,   
  isnull(DP2.name, 'No members') AS Users_Name   
FROM 
    sys.database_role_members AS RM  
RIGHT OUTER JOIN sys.database_principals AS DP1  
    ON RM.role_principal_id = DP1.principal_id  
LEFT OUTER JOIN sys.database_principals AS DP2  
    ON RM.member_principal_id = DP2.principal_id  
WHERE 
    DP1.name = 'hippa_role' 



/* 
   6A - Audit data changes (table for DML trigger) 
*/ 
  
-- Delete existing table 
DROP TABLE IF EXISTS [audit].[log_table_changes] 
GO 
  
-- Add the table 
CREATE TABLE [audit].[log_table_changes] 
( 
  [chg_id] [numeric](18, 0) IDENTITY(1,1) NOT NULL, 
  [chg_date] [datetime] NOT NULL, 
  [chg_type] [varchar](20) NOT NULL, 
  [chg_by] [nvarchar](256) NOT NULL, 
  [app_name] [nvarchar](128) NOT NULL, 
  [host_name] [nvarchar](128) NOT NULL, 
  [schema_name] [sysname] NOT NULL, 
  [object_name] [sysname] NOT NULL, 
  [xml_recset] [xml] NULL, 
  CONSTRAINT [pk_ltc_chg_id] PRIMARY KEY CLUSTERED ([chg_id] ASC) 
); 
GO 
  
-- Add defaults for key information 
ALTER TABLE [audit].[log_table_changes] 
  ADD CONSTRAINT [DF_LTC_CHG_DATE] DEFAULT (getdate()) FOR [chg_date]; 
  
ALTER TABLE [audit].[log_table_changes] 
  ADD CONSTRAINT [DF_LTC_CHG_TYPE] DEFAULT ('') FOR [chg_type]; 
  
ALTER TABLE [audit].[log_table_changes] 
  ADD CONSTRAINT [DF_LTC_CHG_BY] DEFAULT (coalesce(suser_sname(),'?')) FOR [chg_by]; 
  
ALTER TABLE [audit].[log_table_changes] 
  ADD CONSTRAINT [DF_LTC_APP_NAME] DEFAULT (coalesce(app_name(),'?')) FOR [app_name]; 
  
ALTER TABLE [audit].[log_table_changes] 
  ADD CONSTRAINT [DF_LTC_HOST_NAME] DEFAULT (coalesce(host_name(),'?')) FOR [host_name]; 
GO


/*
   6B - Stored procedure to create triggers
*/


-- Delete existing procedure
DROP PROCEDURE IF EXISTS [audit].[manage_table_triggers]
GO


-- Create new procedure
CREATE PROCEDURE [audit].[manage_table_triggers]
    @target_schema sysname = 'active',
	@command_action varchar(16) = 'create',
	@verbose_flag int = 0
AS
BEGIN
    -- Local variables
	DECLARE @my_schema_name sysname;
	DECLARE @my_table_name sysname;
    DECLARE @var_tsql nvarchar(2048);

    -- List non audit tables
    DECLARE var_cursor CURSOR FOR    
        SELECT s.name as my_schema_name, t.name as my_table_name
        FROM sys.tables t join sys.schemas s on t.schema_id = s.schema_id
        WHERE s.name = @target_schema;

    -- Open cursor
    OPEN var_cursor;

    -- Get first row
    FETCH NEXT FROM var_cursor 
        INTO @my_schema_name,  @my_table_name;

    -- While there is data
    WHILE (@@fetch_status = 0)
    BEGIN

	    -- Show the values
		/*
		PRINT @my_schema_name
		PRINT @my_table_name
		*/

		-- Remove existing trigger
		SET @var_tsql = '';
		SET @var_tsql += '-- Drop trigger' + CHAR(13)
		SET @var_tsql += 'IF OBJECT_ID(''['+ @my_schema_name + '].[trg_ltc_' + @my_table_name + ']'') IS NOT NULL ' + CHAR(13) 
		SET @var_tsql += '    DROP TRIGGER ['+ @my_schema_name + '].[trg_ltc_' + @my_table_name + '];';

		-- Process drop command
		IF (@command_action = 'create') OR (@command_action = 'drop')
		BEGIN

    		-- Verbose messaging
	    	IF (@verbose_flag = 1)
		    BEGIN
		        PRINT @var_tsql;
		        PRINT '';
		    END

            -- Execute dynamic code
			EXEC(@var_tsql);
        END

		-- Create new trigger
		SET @var_tsql = '';
		SET @var_tsql += '-- Create trigger' + CHAR(13)
		SET @var_tsql += 'CREATE TRIGGER ['+ @my_schema_name + '].[trg_ltc_' + @my_table_name + '] '
		SET @var_tsql += 'ON ['+ @my_schema_name + '].[' + @my_table_name + '] ' + CHAR(13)
		SET @var_tsql += 'FOR INSERT, UPDATE, DELETE AS ' + CHAR(13)
		SET @var_tsql += 'BEGIN ' + CHAR(13) + CHAR(13)

		SET @var_tsql += '-- Detect inserts' + CHAR(13)
		SET @var_tsql += 'IF EXISTS (select * from inserted) AND NOT EXISTS (select * from deleted)' + CHAR(13)
		SET @var_tsql += 'BEGIN' + CHAR(13)
		SET @var_tsql += '    INSERT [audit].[log_table_changes] ([chg_type], [schema_name], [object_name], [xml_recset])' + CHAR(13)
		SET @var_tsql += '    SELECT ''INSERT'', ''[' +  @my_schema_name + ']'', ''[' + @my_table_name + ']'',' 
		SET @var_tsql += ' (SELECT * FROM inserted as Record for xml auto, elements , root(''RecordSet''), type)' + CHAR(13)
		SET @var_tsql += '    RETURN;' + CHAR(13)
		SET @var_tsql += 'END' + CHAR(13) + CHAR(13)

		SET @var_tsql += '-- Detect deletes' + CHAR(13)
		SET @var_tsql += 'IF EXISTS (select * from deleted) AND NOT EXISTS (select * from inserted)' + CHAR(13)
		SET @var_tsql += 'BEGIN' + CHAR(13)
		SET @var_tsql += '    INSERT [audit].[log_table_changes] ([chg_type], [schema_name], [object_name], [xml_recset])' + CHAR(13)
		SET @var_tsql += '    SELECT ''DELETE'', ''[' +  @my_schema_name + ']'', ''[' + @my_table_name + ']'',' 
		SET @var_tsql += ' (SELECT * FROM deleted as Record for xml auto, elements , root(''RecordSet''), type)' + CHAR(13)
		SET @var_tsql += '    RETURN;' + CHAR(13)
		SET @var_tsql += 'END' + CHAR(13) + CHAR(13)

		SET @var_tsql += '-- Detect updates' + CHAR(13)
		SET @var_tsql += 'IF EXISTS (select * from inserted) AND EXISTS (select * from deleted)' + CHAR(13)
		SET @var_tsql += 'BEGIN' + CHAR(13)
		SET @var_tsql += '    INSERT [audit].[log_table_changes] ([chg_type], [schema_name], [object_name], [xml_recset])' + CHAR(13)
		SET @var_tsql += '    SELECT ''UPDATE'', ''[' +  @my_schema_name + ']'', ''[' + @my_table_name + ']'',' 
		SET @var_tsql += ' (SELECT * FROM deleted as Record for xml auto, elements , root(''RecordSet''), type)' + CHAR(13)
		SET @var_tsql += '    RETURN;' + CHAR(13)
		SET @var_tsql += 'END' + CHAR(13) + CHAR(13)

		SET @var_tsql += 'END; ' + CHAR(13)
	

		-- Process create command
		IF (@command_action = 'create')
		BEGIN

    		-- Verbose messaging
	    	IF (@verbose_flag = 1)
		    BEGIN
		        PRINT @var_tsql;
		        PRINT '';
		    END

            -- Execute dynamic code
			EXEC(@var_tsql);
        END


		-- Get next row
        FETCH NEXT FROM var_cursor 
        INTO @my_schema_name,  @my_table_name;

	END

    -- Close cursor
    CLOSE var_cursor;

    -- Release memory
    DEALLOCATE var_cursor;

END;
GO



/* 
   7 - Dynamically make DML after triggers to capture changes 
*/ 
  
-- Add triggers to tables 
EXEC [audit].[manage_table_triggers] @target_schema = 'active', @command_action = 'create', @verbose_flag = 1 
GO 
  
-- Show the triggers 
SELECT 
  s.name, 
  tr.name, 
  tr.type_desc, 
  tr.is_instead_of_trigger 
FROM 
  sys.triggers tr 
  join sys.tables t on tr.parent_id = t.object_id 
  join sys.schemas s ON t.schema_id = s.schema_id 
WHERE 
  s.name = 'active' 
GO 



/*
  ~ Start of make changes ~
*/


 -- Show the Campbell family 
select * from active.patient_info where last_name = 'CAMPBELL' 
go 

-- Add a new visit 
insert into active.visit_info values (getdate(), 125, 60, 98.6, 120, 60, 487, 'Influenza', 11, 1); 
go 

-- Update the visit 
update active.visit_info 
set diagnosis_desc = upper(diagnosis_desc), patient_temp = 98.4 
where visit_id = 21 
go 

-- Delete first visit 
delete from active.visit_info where visit_id = 11; 
go 


/*
  ~ End of make changes ~
*/


/*  
    8 - Reverse delete Action  
*/ 
  

-- allow identity insert
SET IDENTITY_INSERT [active].[visit_info] ON;
go

-- Find deleted record 
DECLARE @xml1 XML 
SELECT @xml1 = xml_recset FROM [audit].[log_table_changes] WHERE chg_type = 'DELETE'; 

-- PRINT(cast(@xml1 as varchar(max)))

-- Insert lost record 
WITH cte_Captured_Record 
as 
( 
SELECT 
  Tbl.Col.value('visit_id[1]', 'int') as visit_id, 
  Tbl.Col.value('visit_date[1]', 'datetime') as visit_date, 
  Tbl.Col.value('patient_weight[1]', 'real') as patient_weight, 
  Tbl.Col.value('patient_height[1]', 'real') as patient_height, 
  Tbl.Col.value('patient_temp[1]', 'real') as patient_temp, 
  Tbl.Col.value('patient_systolic[1]', 'int') as patient_systolic, 
  Tbl.Col.value('patient_diastolic[1]', 'int') as patient_diastolic, 
  Tbl.Col.value('diagnosis_icd9[1]', 'int') as diagnosis_icd9, 
  Tbl.Col.value('diagnosis_desc[1]', 'varchar(128)') as diagnosis_desc, 
  Tbl.Col.value('patient_id[1]', 'int') as patient_id, 
  Tbl.Col.value('doctor_id[1]', 'int') as doctor_id 
FROM @xml1.nodes('//Record') Tbl(Col) 
) 
INSERT INTO [active].[visit_info] 
(
	[visit_id]
	,[visit_date]
    ,[patient_weight]
    ,[patient_height]
    ,[patient_temp]
    ,[patient_systolic]
    ,[patient_diastolic]
    ,[diagnosis_icd9]
    ,[diagnosis_desc]
    ,[patient_id]
    ,[doctor_id]
)
SELECT * FROM  cte_Captured_Record; 
GO 

-- dont allow identity insert
SET IDENTITY_INSERT [active].[visit_info] ON;
go


/*  
    9 - Update Action  
*/ 
  
-- Find updated record 
DECLARE @xml2 XML 
SELECT @xml2 = xml_recset FROM [audit].[log_table_changes] WHERE chg_type = 'UPDATE'; 
  
-- Reverse record change 
WITH cte_Captured_Record 
as 
( 
SELECT 
  Tbl.Col.value('visit_id[1]', 'int') as visit_id, 
  Tbl.Col.value('visit_date[1]', 'datetime') as visit_date, 
  Tbl.Col.value('patient_weight[1]', 'real') as patient_weight, 
  Tbl.Col.value('patient_height[1]', 'real') as patient_height, 
  Tbl.Col.value('patient_temp[1]', 'real') as patient_temp, 
  Tbl.Col.value('patient_systolic[1]', 'int') as patient_systolic, 
  Tbl.Col.value('patient_diastolic[1]', 'int') as patient_diastolic, 
  Tbl.Col.value('diagnosis_icd9[1]', 'int') as diagnosis_icd9, 
  Tbl.Col.value('diagnosis_desc[1]', 'varchar(128)') as diagnosis_desc, 
  Tbl.Col.value('patient_id[1]', 'int') as patient_id, 
  Tbl.Col.value('doctor_id[1]', 'int') as doctor_id 
FROM @xml2.nodes('//Record') Tbl(Col) 
) 
UPDATE cur 
SET cur.patient_temp = prv.patient_temp 
FROM [active].[visit_info] as cur JOIN cte_Captured_Record as prv 
ON cur.visit_id = prv.visit_id 
GO 



/*  
    10 - Insert Action  
*/ 
  
-- Find inserted record 
DECLARE @xml3 XML 
SELECT @xml3 = xml_recset FROM [audit].[log_table_changes] WHERE chg_type = 'INSERT'; 
  
-- Remove identified record 
WITH cte_Captured_Record 
as 
( 
    SELECT Tbl.Col.value('visit_id[1]', 'int') as visit_id 
    FROM @xml3.nodes('//Record') Tbl(Col) 
) 
DELETE 
FROM [active].[visit_info] 
WHERE visit_id in (SELECT visit_id FROM cte_Captured_Record) 
GO 


/*  
    11 - Preventing unwanted data changes (static data) 
*/ 
  
-- Remove trigger if it exists 
IF OBJECT_ID('[active].[trg_sd_doctor_info]') IS NOT NULL 
  DROP TRIGGER [active].[trg_sd_doctor_info] 
GO 
  
-- Add trigger to prevent data changes 
CREATE TRIGGER [active].[trg_sd_doctor_info] ON [active].[doctor_info] 
FOR INSERT, UPDATE, DELETE AS 
BEGIN 
  
  -- Detect inserts 
  IF EXISTS (select * from inserted) AND NOT EXISTS (select * from deleted) 
    BEGIN 
        ROLLBACK TRANSACTION; 
        RAISERROR ('inserts are not allowed on table  [active].[doctor_info]!', 15, 1); 
        RETURN; 
    END 
  
  -- Detect deletes 
  IF EXISTS (select * from deleted) AND NOT EXISTS (select * from inserted) 
    BEGIN 
        ROLLBACK TRANSACTION; 
        RAISERROR ('deletes are not allowed on table  [active].[doctor_info]!', 15, 1); 
        RETURN; 
    END 
  
  -- Detect updates 
  IF EXISTS (select * from inserted) AND EXISTS (select * from deleted) 
    BEGIN 
        ROLLBACK TRANSACTION; 
        RAISERROR ('updates are not allowed on table  [active].[doctor_info]!', 15, 1); 
        RETURN; 
    END 
  
END; 
GO 


/*  
    12 - Test static data trigger
*/ 


 -- Disable audit trigger 
DISABLE TRIGGER [active].[trg_ltc_doctor_info] ON [active].[doctor_info]; 
GO 

 
-- Inserts will fail 
INSERT INTO active.doctor_info ( first_name, last_name, infamous_desc ) 
VALUES ('Jackie', 'Kevorkian', 'Euthanasia'); 
GO

 -- Updates will fail 
UPDATE active.doctor_info SET first_name = 'John' WHERE doctor_id = 1; 
GO

-- Deletes will fail
DELETE FROM active.doctor_info WHERE doctor_id = 1; 
GO 

