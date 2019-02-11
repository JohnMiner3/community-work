/******************************************************
 *
 * Name:         ufn_Parse_Blocking_Report
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Blog:     www.craftydba.com
 *
 *     Version:  1.1
 *     Date:     07-01-2013
 *     Purpose:  Given a alert id, parse the xml into
 *               a result set.
 * 
 ******************************************************/

-- Use the correct database
USE [msdb]
GO

-- Delete existing table value function (tvf)
IF  EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dbo].[ufn_Parse_Blocking_Report]') AND [type] in (N'TF')
	)
DROP FUNCTION [dbo].[ufn_Parse_Blocking_Report] 
GO


-- Create inline table value function (tvf)
CREATE FUNCTION [dbo].[ufn_Parse_Blocking_Report] (@alert_id int)
RETURNS TABLE
AS
RETURN 
(
    --
    -- Blocked processes
    --

    SELECT 
        alert_report.value('(//TextData/blocked-process-report/blocking-process/process/@spid)[1]','varchar(16)') + 'H' as [code],
        alert_report.value('(//TextData/blocked-process-report/blocking-process/process/@spid)[1]','varchar(16)') as [spid],
        alert_report.value('(//TextData/blocked-process-report/blocking-process/process/@sbid)[1]','varchar(16)') as [blocked_by_spid],
        alert_report.value('(//TextData/blocked-process-report/blocking-process/process/@hostname)[1]','varchar(64)') as [client_machine],
        alert_report.value('(//TextData/blocked-process-report/blocking-process/process/@hostpid)[1]','varchar(16)') as [client_process_id], 
        alert_report.value('(//TextData/blocked-process-report/blocking-process/process/@clientapp)[1]','varchar(128)') as [application_name],
        alert_report.value('(//TextData/blocked-process-report/blocking-process/process/@loginname)[1]','varchar(64)') as [login_name], 
        alert_report.value('(//TextData/blocked-process-report/blocking-process/process/@lastbatchstarted)[1]','varchar(32)') as [last_batch],
        alert_report.value('(//TextData/blocked-process-report/blocking-process/process/@priority)[1]','varchar(16)') as [task_priority],
        alert_report.value('(//TextData/blocked-process-report/blocking-process/process/@trancount)[1]','varchar(16)') as [transaction_count],
        alert_report.value('(//TextData/blocked-process-report/blocking-process/process/@isolationlevel)[1]','varchar(32)') as [isolation_level],
        alert_report.value('(//TextData/blocked-process-report/blocking-process/process/@lockMode)[1]','varchar(32)') as [wait_type],
        alert_report.value('(//TextData/blocked-process-report/blocking-process/process/@waitresource)[1]','varchar(128)') as [wait_resource],
        alert_report.value('(//TextData/blocked-process-report/blocking-process/process/@waittime)[1]','varchar(32)') as [wait_time],
        alert_report.value('(//TextData/blocked-process-report/blocking-process/process/@status)[1]','varchar(32)') as [status],
        alert_report.value('(//TextData/blocked-process-report/blocking-process/process/@currentdb)[1]','varchar(16)') as [database_id],
        db_name
        (
            alert_report.value('(//TextData/blocked-process-report/blocking-process/process/@currentdb)[1]','varchar(128)')
        )
        as [database_name],
        alert_report.value('(//TextData/blocked-process-report/blocking-process/process/inputbuf)[1]','varchar(4000)') as [command_text]

    FROM [msdb].[dbo].[tbl_Wmi_Xml_Reports] 
    WHERE alert_id = @alert_id

    UNION ALL

    --
    -- Blocking processes
    --

    SELECT 
        alert_report.value('(//TextData/blocked-process-report/blocking-process/process/@spid)[1]','varchar(16)') + 'T' as [code],
        alert_report.value('(//TextData/blocked-process-report/blocked-process/process/@spid)[1]','varchar(16)') as [spid],
        alert_report.value('(//TextData/blocked-process-report/blocking-process/process/@spid)[1]','varchar(16)') as [blocked_by_spid],
        alert_report.value('(//TextData/blocked-process-report/blocked-process/process/@hostname)[1]','varchar(64)') as [client_machine],
        alert_report.value('(//TextData/blocked-process-report/blocked-process/process/@hostpid)[1]','varchar(16)') as [client_process_id], 
        alert_report.value('(//TextData/blocked-process-report/blocked-process/process/@clientapp)[1]','varchar(128)') as [application_name],
        alert_report.value('(//TextData/blocked-process-report/blocked-process/process/@loginname)[1]','varchar(64)') as [login_name], 
        alert_report.value('(//TextData/blocked-process-report/blocked-process/process/@lastbatchstarted)[1]','varchar(32)') as [last_batch],
        alert_report.value('(//TextData/blocked-process-report/blocked-process/process/@priority)[1]','varchar(16)') as [task_priority],
        alert_report.value('(//TextData/blocked-process-report/blocked-process/process/@trancount)[1]','varchar(16)') as [transaction_count],
        alert_report.value('(//TextData/blocked-process-report/blocked-process/process/@isolationlevel)[1]','varchar(32)') as [isolation_level],
        alert_report.value('(//TextData/blocked-process-report/blocked-process/process/@lockMode)[1]','varchar(32)') as [wait_type],
        alert_report.value('(//TextData/blocked-process-report/blocked-process/process/@waitresource)[1]','varchar(128)') as [wait_resource],
        alert_report.value('(//TextData/blocked-process-report/blocked-process/process/@waittime)[1]','varchar(32)') as [wait_time],
        alert_report.value('(//TextData/blocked-process-report/blocked-process/process/@status)[1]','varchar(32)') as [status],
        alert_report.value('(//TextData/blocked-process-report/blocked-process/process/@currentdb)[1]','varchar(16)') as [database_id],
        db_name
        (
            alert_report.value('(//TextData/blocked-process-report/blocked-process/process/@currentdb)[1]','varchar(128)')
        )
        as [database_name],
        alert_report.value('(//TextData/blocked-process-report/blocked-process/process/inputbuf)[1]','varchar(4000)') as [command_text]

    FROM [msdb].[dbo].[tbl_Wmi_Xml_Reports] 
    WHERE alert_id = @alert_id
);
GO


