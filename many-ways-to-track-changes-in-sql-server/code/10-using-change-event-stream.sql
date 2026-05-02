/******************************************************
 *
 * Name:         10-using-change-event-stream.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     05-01-2026
 *     Purpose:  Change Event Stream
 * 
 ******************************************************/

-- Use correct database
USE [dbs_hippa05];
GO

-- 1 - Create the master key with a password.
IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##DatabaseMasterKey##')
BEGIN
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<your secret pwd>';
END
GO


-- 2 - Create a database scoped credential (event hub policy)
IF NOT EXISTS (
    SELECT * FROM sys.database_scoped_credentials 
    WHERE name = 'EventStreamClass'
)
BEGIN
    CREATE DATABASE SCOPED CREDENTIAL [EventStreamClass]
    WITH IDENTITY = 'RootManageSharedAccessKey', 
    SECRET = '<event hub key>';
END


-- 3 - Enable general preview features
ALTER DATABASE SCOPED CONFIGURATION SET PREVIEW_FEATURES = ON;
GO

-- 4 - Use the dedicated stored procedure to enable streaming
EXEC sys.sp_enable_event_stream;
GO


-- 5 - Setup a streaming group
EXECUTE sys.sp_create_event_stream_group
    @stream_group_name = N'ClassStreamGroup',
    @destination_type = N'AzureEventHubsAmqp',
    @destination_location = N'nsChangeEventStream.servicebus.windows.net/ehchangeeventstream',
    @destination_credential = EventStreamClass;
GO


-- 6 - Add tables to the group
EXEC sys.sp_add_object_to_event_stream_group
    N'ClassStreamGroup',
    N'active.doctor_info';
GO

EXEC sys.sp_add_object_to_event_stream_group
    N'ClassStreamGroup',
    N'active.patient_info';
GO

EXEC sys.sp_add_object_to_event_stream_group
    N'ClassStreamGroup',
    N'active.visit_info';
GO


--
--  Diagnostics
--

-- Is the database enabled
SELECT name, is_event_stream_enabled 
FROM sys.databases 
WHERE is_event_stream_enabled = 1;
GO

-- Group settings
EXEC sys.sp_help_change_feed_settings
GO

-- Show table settings
EXEC sp_help_change_feed_table @source_schema = 'active', @source_name = 'visit_info'
GO

-- Show errors
SELECT * FROM sys.dm_change_feed_errors
GO


/*
  ~ Start of make changes ~
*/


-- Show visit for head wound
select * from active.visit_info where visit_id = 1
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


-- 6 - Drop a streaming group
EXECUTE sys.sp_drop_event_stream_group
    @stream_group_name = N'ClassStreamGroup'
GO

-- Show table settings
EXEC sp_help_change_feed_table @source_schema = 'active', @source_name = 'visit_info'
GO

-- 7 - Use the dedicated stored procedure to disable streaming
EXEC sys.sp_disable_event_stream;
GO

-- Is the database enabled
SELECT name, is_event_stream_enabled 
FROM sys.databases 
WHERE is_event_stream_enabled = 1;
GO