/******************************************************
 *
 * Name:         job_monitor_blocking
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Blog:     www.craftydba.com
 *
 *     Version:  1.1
 *     Date:     07-01-2013
 *     Purpose:  
 *               1 - Enable internal blocking report
 *               2 - Create a job category
 *               3 - Create a new job
 *               4 - Create a new WMI alert
 * 
 ******************************************************/

-- Use correct database
USE [msdb]
GO


/*  
    Must run to enable internal report
*/

BEGIN TRY

    -- Show advance options
    EXEC sp_configure 'show advanced options', 1;

    -- Apply change
    reconfigure;

    -- Debug line
    PRINT '- show advance options';

    -- Make the report every 5 secs
    EXEC sp_configure 'blocked process threshold', 5 ;

    -- Apply change
    reconfigure;

    -- Debug line
    PRINT '- set blocked process threshold';

END TRY

BEGIN CATCH
    PRINT 'Issue occurred when enabling the internal blocking report';
    SELECT
        ERROR_NUMBER() AS ErrorNumber
       ,ERROR_SEVERITY() AS ErrorSeverity
       ,ERROR_STATE() AS ErrorState
       ,ERROR_PROCEDURE() AS ErrorProcedure
       ,ERROR_LINE() AS ErrorLine
       ,ERROR_MESSAGE() AS ErrorMessage;
	RETURN;
END CATCH;
GO



/*  
    Create the job category?
*/

BEGIN TRY

    -- Catch the code
    DECLARE @ReturnCode1 INT = 0;

    -- Create the category
    IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Database Maintenance]' AND category_class=1)
        EXEC @ReturnCode1 = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Database Maintenance]';

    -- Bad return code
    IF (@ReturnCode1 <> 0) 	 
        RAISERROR ('Non-zero error code detected after adding category', 16, 1);

    -- Debug line
    PRINT '- create new job category';

END TRY

BEGIN CATCH
    PRINT 'Issue occurred when creating the job category';
    SELECT
        ERROR_NUMBER() AS ErrorNumber
       ,ERROR_SEVERITY() AS ErrorSeverity
       ,ERROR_STATE() AS ErrorState
       ,ERROR_PROCEDURE() AS ErrorProcedure
       ,ERROR_LINE() AS ErrorLine
       ,ERROR_MESSAGE() AS ErrorMessage;
	RETURN;
END CATCH;
GO



/*  
    Delete existing job and alert
*/

BEGIN TRY

    -- Catch the code
    DECLARE @ReturnCode2 INT = 0;

    -- Job id
    DECLARE @JobId2 BINARY(16);

    -- Delete the existing job
    SELECT @JobId2 = job_id FROM msdb.dbo.sysjobs WHERE ltrim(name) = 'Alerts:  Blocking Report';
    IF (@JobId2 IS NOT NULL)
        EXEC @ReturnCode2 = msdb.dbo.sp_delete_job @job_id=@JobId2;

    -- Bad return code
    IF (@ReturnCode2 <> 0) 	 
        RAISERROR ('Non-zero error code detected after deleting old job', 16, 1);

    -- Debug line
    PRINT '- deleting old job';


    -- Delete the existing alert
    IF EXISTS (SELECT * FROM msdb.dbo.sysalerts WHERE name = N'Alert For Blocking')
        EXEC @ReturnCode2 = msdb.dbo.sp_delete_alert @name=N'Alert For Blocking';

    -- Bad return code
    IF (@ReturnCode2 <> 0) 	 
        RAISERROR ('Non-zero error code detected after deleting old alert', 16, 1);

    -- Debug line
    PRINT '- deleting old alert';

END TRY

BEGIN CATCH
    PRINT 'Issue occurred when deleting the job and alert';
    SELECT
        ERROR_NUMBER() AS ErrorNumber
       ,ERROR_SEVERITY() AS ErrorSeverity
       ,ERROR_STATE() AS ErrorState
       ,ERROR_PROCEDURE() AS ErrorProcedure
       ,ERROR_LINE() AS ErrorLine
       ,ERROR_MESSAGE() AS ErrorMessage;
	RETURN;
END CATCH;
GO



/*  
    Create the job and the alert
*/

BEGIN TRY

    -- Catch the code
    DECLARE @ReturnCode3 INT = 0;

    -- Job id
    DECLARE @JobId3 BINARY(16);
    
    -- Add the job
    EXEC @ReturnCode3 = msdb.dbo.sp_add_job @job_name=N'Alerts:  Blocking Report', 
        @enabled=1, 
        @notify_level_eventlog=0, 
        @notify_level_email=0, 
        @notify_level_netsend=0, 
        @notify_level_page=0, 
        @delete_level=0, 
        @description=N'Create job to store and report blocking information.', 
        @category_name=N'[Database Maintenance]', 
        @owner_login_name=N'sa', @job_id = @JobId3 OUTPUT;

    -- Bad return code
    IF (@ReturnCode3 <> 0) 	 
        RAISERROR ('Non-zero error code detected after adding job', 16, 1);

    -- Debug line
    PRINT '- adding new job';

    -- Add step 1
    EXEC @ReturnCode3 = msdb.dbo.sp_add_jobstep @job_id=@jobId3, 
        @step_name=N'Save xml report', 
        @step_id=1, 
        @cmdexec_success_code=0, 
        @on_success_action=3, 
        @on_success_step_id=0, 
        @on_fail_action=2, 
        @on_fail_step_id=0, 
        @retry_attempts=0, 
        @retry_interval=0, 
        @os_run_priority=0, @subsystem=N'TSQL', 
        @command=N'INSERT INTO [dbo].[tbl_Wmi_Xml_Reports] (alert_type, alert_report) VALUES (''blocking'', N''$(ESCAPE_SQUOTE(WMI(TextData)))'')', 
        @database_name=N'msdb', 
        @flags=0;

    -- Bad return code
    IF (@ReturnCode3 <> 0) 	 
        RAISERROR ('Non-zero error code detected after adding job step 1', 16, 1);

    -- Debug line
    PRINT '- adding new step 1';

    -- Add step 2 - blocking report (cross join) sysprocesses (2 min time limit)
    EXEC @ReturnCode3 = msdb.dbo.sp_add_jobstep @job_id=@jobId3, 
        @step_name=N'Monitor Blocking', 
        @step_id=2, 
        @cmdexec_success_code=0, 
        @on_success_action=1, 
        @on_success_step_id=0, 
        @on_fail_action=2, 
        @on_fail_step_id=0, 
        @retry_attempts=0, 
        @retry_interval=0, 
        @os_run_priority=0, @subsystem=N'TSQL', 
        @command=N'exec [dbo].[usp_monitor_blocking] ''monitor'', ''jminer@sensata.com'', ''prf_default'', 5;', 
        @database_name=N'msdb', 
        @flags=0;

    -- Bad return code
    IF (@ReturnCode3 <> 0) 	 
        RAISERROR ('Non-zero error code detected after adding job step 2', 16, 1);

    -- Debug line
    PRINT '- adding new step 2';

    -- Add starting step
    EXEC @ReturnCode3 = msdb.dbo.sp_update_job @job_id = @JobId3, @start_step_id = 1;

    -- Bad return code
    IF (@ReturnCode3 <> 0) 	 
        RAISERROR ('Non-zero error code detected after adding starting step', 16, 1);

    -- Debug line
    PRINT '- setting start step';

    -- Add job server
    EXEC @ReturnCode3 = msdb.dbo.sp_add_jobserver @job_id = @JobId3, @server_name = N'(local)';

    -- Bad return code
    IF (@ReturnCode3 <> 0) 	 
        RAISERROR ('Non-zero error code detected after adding job server', 16, 1);

    -- Debug line
    PRINT '- adding job server';

    -- Add WMI alert, any blocking > 2.5 secs, delay between alerts = 5 secs
    EXEC @ReturnCode3 = msdb.dbo.sp_add_alert @name=N'Alert For Blocking', 
        @message_id=0, 
        @severity=0, 
        @enabled=1, 
        @delay_between_responses=5, 
        @include_event_description_in=0, 
        @category_name=N'[Uncategorized]', 
        @wmi_namespace=N'\\.\root\Microsoft\SqlServer\ServerEvents\MSSQLSERVER', 
        @wmi_query=N'SELECT * FROM BLOCKED_PROCESS_REPORT Where Duration >= 4999', 
        @job_id=@JobId3;

    -- Bad return code
    IF (@ReturnCode3 <> 0) 	 
        RAISERROR ('Non-zero error code detected after adding the alert', 16, 1);

    -- Debug line
    PRINT '- adding wmi alert';

END TRY

BEGIN CATCH
    PRINT 'Issue occurred when creating the job and alert';
    SELECT
        ERROR_NUMBER() AS ErrorNumber
       ,ERROR_SEVERITY() AS ErrorSeverity
       ,ERROR_STATE() AS ErrorState
       ,ERROR_PROCEDURE() AS ErrorProcedure
       ,ERROR_LINE() AS ErrorLine
       ,ERROR_MESSAGE() AS ErrorMessage;
	RETURN;
END CATCH;
GO

