/******************************************************
 *
 * Name:         enable-operators-n-alerts.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Blog:     www.craftydba.com
 *
 *     Version:  1.1
 *     Date:     08-01-2014
 *     Purpose:  Enable / add operators and alerts.
 *
 ******************************************************/

-- Select the correct database
USE [msdb]
GO


/*  
	Delete the existing operator
*/

IF EXISTS (SELECT * FROM msdb.dbo.sysoperators WHERE name = N'Basic Monitoring')
BEGIN
    EXEC master.dbo.sp_MSsetalertinfo @failsafeoperator= '';
    EXEC msdb.dbo.sp_delete_operator @name = 'Basic Monitoring';
END
GO
 

/*  
	Create new operator
*/

-- Add operator with email from 8am-to-5pm weekdays
EXEC msdb.dbo.sp_add_operator
    @name = N'Basic Monitoring',
    @enabled = 1,
    @email_address = N'craftydba@outlook.com',
    @weekday_pager_start_time = 000000,
    @weekday_pager_end_time = 235959,
    @saturday_pager_start_time = 000000,
    @saturday_pager_end_time = 235959,
    @sunday_pager_start_time = 000000,
    @sunday_pager_end_time = 235959,
    @pager_days = 126 ;
GO

-- Show the new entries
EXEC msdb.dbo.sp_help_operator
    @operator_name = N'Basic Monitoring' ;
GO


/*
    Create the fail-safe operator
*/

EXEC master.dbo.sp_MSsetalertinfo @failsafeoperator=N'Basic Monitoring';
GO


/*
   Add category for alerts
*/

IF NOT EXISTS (SELECT * FROM msdb.dbo.syscategories where name = '[Basic Alerting]' and category_class = 2)
EXEC msdb.dbo.sp_add_category
    @class=N'ALERT',
    @type=N'NONE',
    @name=N'[Basic Alerting]' ;
GO


/*
   Add alerts for severity errors 10 to 25
*/


-- Loop from 10 to 26
DECLARE @VAR_ACNT INT;
DECLARE @VAR_ANAME VARCHAR(125);
SET @VAR_ACNT = 10;

-- Add severe alerts to system
WHILE (@VAR_ACNT < 26)
BEGIN

    -- Alert name
    SET @VAR_ANAME = 'Alert For Severity ' + STR(@VAR_ACNT, 2, 0);

	-- Delete existing alert
	IF EXISTS(SELECT * FROM msdb.dbo.sysalerts WHERE name = @VAR_ANAME)
	    EXEC msdb.dbo.sp_delete_alert
            @name = @VAR_ANAME;

    -- Recreate new alert
    EXEC msdb.dbo.sp_add_alert @name=@VAR_ANAME,
	    @message_id=0, 
	    @severity=@VAR_ACNT, 
	    @enabled=1, 
	    @delay_between_responses=60, 
	    @include_event_description_in=5, 
	    @category_name=N'[Basic Alerting]';

    -- Attach the operator to the alerts
    EXEC msdb.dbo.sp_add_notification
        @alert_name = @VAR_ANAME,
        @operator_name = N'Basic Monitoring',
        @notification_method = 1 ;
		
    -- User message
    PRINT 'Add: ' + @VAR_ANAME;
    SET @VAR_ACNT = @VAR_ACNT + 1;
END



-- Disable Alert 10
EXEC msdb.dbo.sp_update_alert
    @name = N'Alert For Severity 10',
    @enabled = 0 ;
GO


/*
   Add alerts for errors 823 to 825
*/


-- Loop from 10 to 26
DECLARE @VAR_BCNT INT;
DECLARE @VAR_BNAME VARCHAR(125);
SET @VAR_BCNT = 823;

-- Add severe alerts to system
WHILE (@VAR_BCNT < 826)
BEGIN

    -- Alert name
    SET @VAR_BNAME = 'Alert For Error ' + STR(@VAR_BCNT, 3, 0);

	-- Delete existing alert
	IF EXISTS(SELECT * FROM msdb.dbo.sysalerts WHERE name = @VAR_BNAME)
	BEGIN
	    EXEC msdb.dbo.sp_delete_alert @name = @VAR_BNAME;
	END

    -- Recreate new alert
    EXEC msdb.dbo.sp_add_alert @name=@VAR_BNAME,
	    @message_id=@VAR_BCNT, 
	    @severity=0, 
	    @enabled=1, 
	    @delay_between_responses=60, 
	    @include_event_description_in=5, 
	    @category_name=N'[Basic Alerting]';

    -- Attach the operator to the alerts
    EXEC msdb.dbo.sp_add_notification
        @alert_name = @VAR_BNAME,
        @operator_name = N'Basic Monitoring',
        @notification_method = 1 ;
		
    -- User message
    PRINT 'Add: ' + @VAR_BNAME;
    SET @VAR_BCNT = @VAR_BCNT + 1;
END
GO

-- Show the current alerts
EXEC msdb.dbo.sp_help_alert 
GO
