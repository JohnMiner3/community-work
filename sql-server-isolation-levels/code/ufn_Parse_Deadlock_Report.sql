/******************************************************
 *
 * Name:         ufn_Parse_Deadlock_Report
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
    WHERE object_id = OBJECT_ID(N'[dbo].[ufn_Parse_Deadlock_Report]') AND [type] in (N'TF')
    )
DROP FUNCTION [dbo].[ufn_Parse_Deadlock_Report]
GO


-- Create inline table value function (tvf)
CREATE FUNCTION [dbo].[ufn_Parse_Deadlock_Report] (@alert_id int)
RETURNS @mtv_deadlock TABLE 
(
    [procid] [varchar](32) NULL,
    [currentdb] [nvarchar](128) NULL,
    [spid] [varchar](16) NULL,
    [procname] [varchar](128) NULL,
    [waittime] [varchar](16) NULL,
    [lockmode] [varchar](16) NULL,
    [trancount] [varchar](16) NULL,
    [clientapp] [varchar](128) NULL,
    [hostname] [varchar](64) NULL,
    [loginname] [varchar](64) NULL,
    [logused] [varchar](16) NULL,
    [priority] [varchar](16) NULL,
    [status] [varchar](32) NULL,
    [ecid] [varchar](16) NULL,
    [lasttranstarted] [varchar](32) NULL,
    [lastbatchstarted] [varchar](32) NULL,
    [lastbatchcompleted] [varchar](32) NULL,
    [isolationlevel] [varchar](64) NULL,
    [transactionname] [varchar](128) NULL,
    [waitresource] [varchar](128) NULL,
    [clientoption1] [varchar](16) NULL,
    [clientoption2] [varchar](16) NULL,
    [frame] [varchar](128) NULL,
    [inputbuf] [varchar](4000) NULL
)
AS 
BEGIN

    --
    -- Use five common table expressions 
    --

    ;
    WITH cte_Process_Details  
    AS
    (  
        SELECT 
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@id)[1]','VARCHAR(128)') AS id,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@taskpriority)[1]','VARCHAR(128)') AS taskpriority,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@logused)[1]','VARCHAR(128)') AS logused,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@waitresource)[1]','VARCHAR(128)') AS waitresource,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@waittime)[1]','VARCHAR(128)') AS waittime,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@ownerId)[1]','VARCHAR(128)') AS ownerId,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@transactionname)[1]','VARCHAR(128)') AS transactionname,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@lasttranstarted)[1]','VARCHAR(128)') AS lasttranstarted,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@XDES)[1]','VARCHAR(128)') AS XDES,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@lockMode)[1]','VARCHAR(128)') AS lockMode,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@schedulerid)[1]','VARCHAR(128)') AS schedulerid,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@kpid)[1]','VARCHAR(128)') AS kpid,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@status)[1]','VARCHAR(128)') AS status,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@spid)[1]','VARCHAR(128)') AS spid,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@sbid)[1]','VARCHAR(128)') AS sbid,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@ecid)[1]','VARCHAR(128)') AS ecid,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@priority)[1]','VARCHAR(128)') AS priority,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@trancount)[1]','VARCHAR(128)') AS trancount,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@lastbatchstarted)[1]','VARCHAR(128)') AS lastbatchstarted,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@lastbatchcompleted)[1]','VARCHAR(128)') AS lastbatchcompleted,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@clientapp)[1]','VARCHAR(128)') AS clientapp,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@hostname)[1]','VARCHAR(128)') AS hostname,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@hostpid)[1]','VARCHAR(128)') AS hostpid,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@loginname)[1]','VARCHAR(128)') AS loginname,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@isolationlevel)[1]','VARCHAR(128)') AS isolationlevel,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@xactid)[1]','VARCHAR(128)') AS xactid,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@currentdb)[1]','VARCHAR(128)') AS currentdb,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@lockTimeout)[1]','VARCHAR(128)') AS lockTimeout,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@clientoption1)[1]','VARCHAR(128)') AS clientoption1,  
           alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@clientoption2)[1]','VARCHAR(128)') AS clientoption2  
        FROM [msdb].[dbo].[tbl_Wmi_Xml_Reports] 
        WHERE alert_id = @alert_id
    ),  

    cte_Frame_Details
    AS
    (
        SELECT 
             alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@id)[1]','VARCHAR(128)') AS id,  
             alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/executionStack/frame/@procname)[1]','VARCHAR(128)') AS procname,  
             alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/executionStack/frame/@line)[1]','VARCHAR(128)') AS line,  
             alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/executionStack/frame/@stmtstart)[1]','VARCHAR(128)') AS stmtstart,  
             alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/executionStack/frame/@sqlhandle)[1]','VARCHAR(128)') AS sqlhandle
        FROM [msdb].[dbo].[tbl_Wmi_Xml_Reports] 
        WHERE alert_id = @alert_id
    ),

    cte_Frame_Values
    AS
    (
        SELECT 
             alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@id)[1]','VARCHAR(128)') AS id,  
             alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/executionStack/frame)[1]','VARCHAR(128)') AS frame  
        FROM [msdb].[dbo].[tbl_Wmi_Xml_Reports] 
        WHERE alert_id = @alert_id
    ),

    cte_Input_Buffer
    AS
    (
        SELECT 
             alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/@id)[1]','VARCHAR(128)') AS id,  
             alert_report.value('(//TextData/deadlock-list/deadlock/process-list/process/inputbuf)[1]','VARCHAR(4000)') AS inputbuf     
        FROM [msdb].[dbo].[tbl_Wmi_Xml_Reports] 
        WHERE alert_id = @alert_id
    ),

    cte_Deadlock_Victim
    AS
    (
        SELECT
             alert_report.value('(//TextData/deadlock-list/deadlock/@victim)[1]','VARCHAR(128)') AS id  
        FROM [msdb].[dbo].[tbl_Wmi_Xml_Reports] 
        WHERE alert_id = @alert_id
    )
    
    INSERT INTO
        @mtv_deadlock
    SELECT DISTINCT 
        isnull(pd.id, '') as [id],  
        isnull(db_name(pd.currentdb), '') as [currentdb],
        isnull(pd.spid, '') as [spid],  
        isnull(fd.procname, '') as [procname],  
        isnull(pd.waittime, '') as [waittime],  
        isnull(pd.lockMode, '') as [lockMode],  
        isnull(pd.trancount, '') as [trancount],  
        isnull(pd.clientapp, '') as [clientapp],  
        isnull(pd.hostname, '') as [hostname],  
        isnull(pd.loginname, '') as [loginname],
        isnull(pd.logused, '') as [logused],
        isnull(pd.priority, '') as [priority],
        isnull(pd.status, '') as [status], 
        isnull(pd.ecid, '') as [ecid], 
        isnull(pd.lasttranstarted, '') as [lasttranstarted],
        isnull(pd.lastbatchstarted, '') as [lastbatchstarted],
        isnull(pd.lastbatchcompleted, '') as [lastbatchcompleted],
        isnull(pd.isolationlevel, '') as [isolationlevel],
        isnull(pd.transactionname, '') as [transactionname],
        isnull(pd.waitresource, '') as [waitresource],
        isnull(pd.clientoption1, '') as [clientoption1],
        isnull(pd.clientoption2, '') as [clientoption2],
        isnull(fv.frame, '') as [frame],  
        isnull(ib.inputbuf, '') as [inputbuf]  
    FROM
        cte_Deadlock_Victim as dv
    JOIN     
        cte_Process_Details as pd on dv.id = pd.id
    LEFT JOIN
        cte_Frame_Details as fd on pd.id=fd.id  
    LEFT JOIN
        cte_Frame_Values as fv on fd.id=fv.id      
    LEFT JOIN
        cte_Input_Buffer as ib on fv.id=ib.id  
    
    -- All done
    RETURN;
END;
    
GO
    
    
