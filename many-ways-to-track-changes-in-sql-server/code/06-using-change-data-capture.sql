/******************************************************
 *
 * Name:         06-using-change-data-capture.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     05-01-2026
 *     Purpose:  Change Data Capture
 * 
 ******************************************************/
 
/*
  1 - Is change data capture enabled?
*/

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


/*
  2 - House cleaning
*/


-- Switch owner to system admin
ALTER AUTHORIZATION ON DATABASE::[dbs_hippa01] TO SA;
GO

-- Switch owner to system admin
ALTER AUTHORIZATION ON DATABASE::[dbs_hippa02] TO SA;
GO

-- Switch owner to system admin
ALTER AUTHORIZATION ON DATABASE::[dbs_hippa03] TO SA;
GO


/*
  3 - Enable change data capture (database level)
*/

USE [dbs_hippa03]
GO

EXEC sys.sp_cdc_enable_db
GO


/*
  4 - Enable change data capture (table level)
*/


-- What tables are tracked by CDC?
USE [dbs_hippa03]
GO

SELECT  
  CAST(s.[name] AS NVARCHAR(25)) AS schema_nm, 
  CAST(t.[name] AS NVARCHAR(25)) AS table_nm, 
  is_tracked_by_cdc  
FROM sys.tables t JOIN sys.schemas s ON t.SCHEMA_ID = s.schema_id
GO 


-- Make sure SQL Agent is running, add tables to CDC
EXEC sys.sp_cdc_enable_table 
@source_schema = N'active', 
@source_name   = N'doctor_info', 
@role_name     = NULL 
GO

EXEC sys.sp_cdc_enable_table 
@source_schema = N'active', 
@source_name   = N'patient_info', 
@role_name     = NULL 
GO

EXEC sys.sp_cdc_enable_table 
@source_schema = N'active', 
@source_name   = N'visit_info', 
@role_name     = NULL 
GO


-- What tables are involved?
EXEC sys.sp_cdc_help_change_data_capture 
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


-- Check cdc tables
SELECT * FROM cdc.active_doctor_info_CT
GO

SELECT * FROM cdc.active_patient_info_CT
GO

SELECT * FROM cdc.active_visit_info_CT
GO


/*
  5 - Main issue with CDC (schema drift)
*/

-- Add a survival flag
ALTER TABLE active.visit_info
ADD survived_visit_flag BIT NOT NULL DEFAULT 1;
GO

-- Check the tracking table
SELECT * FROM cdc.ddl_history
GO

-- schema change does not show
SELECT * FROM cdc.active_visit_info_CT
GO


/*
  6 - Remove tracking
*/

-- pick da database
USE [dbs_hippa03]
GO

-- What tables are involved?
EXEC sys.sp_cdc_help_change_data_capture 
GO

-- Remove tracking from the tables
EXEC sys.sp_cdc_disable_table 
@source_schema = N'active', 
@source_name   = N'doctor_info',
@capture_instance = N'active_doctor_info';
GO

EXEC sys.sp_cdc_disable_table 
@source_schema = N'active', 
@source_name   = N'patient_info',
@capture_instance = N'active_patient_info';
GO

EXEC sys.sp_cdc_disable_table 
@source_schema = N'active', 
@source_name   = N'visit_info',
@capture_instance = N'active_visit_info';
GO


-- Remove tracking from the database
EXEC sys.sp_cdc_disable_db 
GO 
