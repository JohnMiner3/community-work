/******************************************************
 *
 * Name:         enable-blocking-alert.sql
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
IF EXISTS(SELECT * FROM msdb.dbo.sysalerts WHERE name = N'Alert For Blocking')
    EXEC msdb.dbo.sp_delete_alert @name = N'Alert For Blocking';
GO

-- Create new alert (30 ms times)
EXEC msdb.dbo.sp_add_alert @name=N'Alert For Blocking', 
		@message_id=0, 
		@severity=0, 
		@enabled=1, 
		@delay_between_responses=0, 
		@include_event_description_in=0, 
		@category_name=N'[Basic Alerting]',
		@performance_condition=N'Locks|Lock Wait Time (ms)|_Total|>|30'
GO


--
-- Some free resources from Brent Ozars team
--

-- http://www.brentozar.com/sql/locking-and-blocking-in-sql-server/