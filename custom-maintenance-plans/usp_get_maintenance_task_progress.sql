 /******************************************************
 *
 * Name:         usp_get_maintenance_progress.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     01-21-2014
 *     Blog:     www.craftydba.com
 *
 *     Purpose:  What is the percentage complete for the task?
 *
 ******************************************************/

/* 
	Choose the database.
*/

USE [msdb]
GO


/*  
	Drop the old stored procedure.
*/

IF OBJECT_ID('[dbo].[usp_get_maintenance_progress]') > 0
DROP PROCEDURE [dbo].[usp_get_maintenance_progress]
GO


/*  
	Create the new stored procedure.
*/

CREATE PROCEDURE [dbo].[usp_get_maintenance_progress]

AS
BEGIN

  SELECT 
    req.session_id,
    req.command AS command_nm,
    CONVERT(NUMERIC(6,2), req.percent_complete) AS percent_complete,
    CONVERT(VARCHAR(20), DATEADD(ms, req.estimated_completion_time, GetDate()), 20) AS estimated_completion_time,
    CONVERT(NUMERIC(10,2), req.total_elapsed_time/1000.0/60.0) AS elapsed_time_min,
    CONVERT(NUMERIC(10,2), req.estimated_completion_time/1000.0/60.0) AS estimated_time_min,
    CONVERT(NUMERIC(10,2), req.estimated_completion_time/1000.0/60.0/60.0) AS elapsed_time_hrs,
    CONVERT(VARCHAR(1024),
    (
        SELECT 
            SUBSTRING(text,req.statement_start_offset/2,
            CASE WHEN req.statement_end_offset = -1 THEN 1000
            ELSE (req.statement_end_offset-req.statement_start_offset)/2 END)
         FROM sys.dm_exec_sql_text(sql_handle))
    ) as command_txt

  FROM 
    sys.dm_exec_requests as req 
  WHERE 
    req.command IN ('RESTORE DATABASE','BACKUP DATABASE', 'RESTORE HEADERON', 'DBCC TABLE CHECK', 'DBCC CHECKCATALO');
END;
GO

