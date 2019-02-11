/******************************************************
 *
 * Name:         calculate-prime-numbers.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     09-12-2017
 *     Purpose:  Calculate & store prime numbers.
 * 
 ******************************************************/

-- Where jobs are stored
USE msdb ;
GO

-- Add a new job
EXEC dbo.sp_add_job
@job_name = N'Job - Calc Prime Numbers' ;
GO

-- Add a new job step
EXEC sp_add_jobstep
@job_name = N'Job - Calc Prime Numbers',
@step_name = N'Step 1 - Calc 250K of numbers',
@subsystem = N'TSQL',
@command = N'

-- Calculate prime numbers
DECLARE @THE_LOW_VALUE [BIGINT];
DECLARE @THE_HIGH_VALUE [BIGINT];

-- Low & High water marks
SELECT @THE_LOW_VALUE = [MY_VALUE] FROM [DBO].[TBL_CONTROL_CARD];
SELECT @THE_HIGH_VALUE = @THE_LOW_VALUE + 250000 - 1;

-- Start the process
BEGIN TRANSACTION

-- Run the calculation
EXEC SP_STORE_PRIMES @THE_LOW_VALUE, @THE_HIGH_VALUE;

UPDATE [DBO].[TBL_CONTROL_CARD]
SET  [MY_VALUE]  =  @THE_HIGH_VALUE + 1;

-- End the process
COMMIT TRANSACTION;
',
@database_name  = 'MATH',
@retry_attempts = 5,
@retry_interval = 5 ;
GO

-- Add new schedule
EXEC dbo.sp_add_schedule
@schedule_name = N'Schd - Calc Prime Numbers',
@enabled=1, 
@freq_type=4, 
@freq_interval=1, 
@freq_subday_type=4, 
@freq_subday_interval=10, 
@freq_relative_interval=0, 
@freq_recurrence_factor=1, 
@active_start_date=20170101, 
@active_end_date=99991231, 
@active_start_time=0, 
@active_end_time=235959;
GO

-- Attach schedule to job
EXEC sp_attach_schedule
@job_name = N'Job - Calc Prime Numbers',
@schedule_name = N'Schd - Calc Prime Numbers';
GO

-- Add to job server
EXEC dbo.sp_add_jobserver
@job_name = N'Job - Calc Prime Numbers',
@server_name = N'(LOCAL)';
GO

-- Change the notification
EXEC msdb.dbo.sp_update_job 
@job_name='Job - Calc Prime Numbers', 
@notify_level_email=1, 
@notify_email_operator_name=N'Basic Monitoring' 
GO

-- Force start a job
EXEC dbo.sp_start_job N'Job - Calc Prime Numbers';
GO


