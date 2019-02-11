/******************************************************
 *
 * Name:         enable-deadlock-alerting.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Blog:     www.craftydba.com
 *
 *     Version:  1.1
 *     Date:     08-01-2014
 *     Purpose:  Enable deadlock alerting
 *
 ******************************************************/

--
-- Make the performance alert
--

-- Select the correct database
USE [msdb]
GO

-- Delete existing alert
IF EXISTS(SELECT * FROM msdb.dbo.sysalerts WHERE name = N'Alert For Deadlocks')
    EXEC msdb.dbo.sp_delete_alert @name = N'Alert For Deadlocks';
GO

-- Create new alert
EXEC msdb.dbo.sp_add_alert @name=N'Alert For Deadlocks', 
		@message_id=0, 
		@severity=0, 
		@enabled=1, 
		@delay_between_responses=0, 
		@include_event_description_in=0, 
		@category_name=N'[Basic Alerting]',
		@performance_condition=N'Locks|Number of Deadlocks/sec|_Total|>|0'
GO


--
-- Option trace flags for deadlocks
--

/* 

dbcc traceon(1204, -1)
go

dbcc traceon(1222, -1)
go

*/