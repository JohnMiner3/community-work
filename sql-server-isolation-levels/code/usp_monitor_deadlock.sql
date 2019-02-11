/******************************************************
 *
 * Name:         usp_monitor_deadlock
 *     
 * Design Phase:
 *     Authors:  John Miner
 *     Blog:     www.craftydba.com
 *
 *     Version:  1.1
 *     Date:     07-15-2013
 *     Purpose:  The deadlock alert calls the deadlock
 *               job that calls this stored procedure.
 *               The [wmi xml report] table has the deadlock
 *               graph that needs to be parsed into a readable email.
 *
 *     Inputs:
 *         @var_option - 
 *             print = show output to screen.
 *             email = send an email to dba.
 *             monitor = save to table & email.
 *         @var_email_list - email notification list.
 *         @var_profile_name - use named email profile.
 * 
 ******************************************************/


-- Dead lock graph details
-- http://technet.microsoft.com/en-us/library/ms179292(v=sql.90).aspx

-- Still valid in SQL Server 2012
-- http://technet.microsoft.com/en-us/library/ms186385.aspx


/*  
    Which database to use.
*/

USE msdb;
GO


/*  
    Delete the existing table.
*/

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[tbl_Monitor_Deadlocks]') AND type = 'U')
DROP TABLE [tbl_Monitor_Deadlocks];
GO


/*  
    Create table to hold the deadlock info
*/

CREATE TABLE [dbo].[tbl_Monitor_Deadlocks]
(
    [poll_id] [int] IDENTITY(1,1) NOT NULL,
    [poll_date] [datetime] DEFAULT (GETDATE()) NOT NULL,

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


    CONSTRAINT [pk_poll_id_4_monitor_deadlocks] PRIMARY KEY CLUSTERED 
    (
    [poll_id] ASC, [poll_date] ASC
    )
);
GO


/*  
    Delete the existing stored procedure.
*/

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[usp_monitor_deadlocks]') AND type in (N'P', N'PC'))
DROP PROCEDURE [usp_monitor_deadlocks]
GO
 

/*  
    Create the new stored procedure.
*/

CREATE PROC [dbo].[usp_monitor_deadlocks]
    @var_option varchar(256) = 'monitor',
    @var_email_list varchar(256) = 'none',
    @var_profile_name varchar(256) = 'none'
AS  
BEGIN  

    -- Error handling variables
    DECLARE @err_number int;
    DECLARE @err_line int;
    DECLARE @err_message varchar(max);
    DECLARE @err_procedure varchar(max);

    -- ** Error Handling - Start Try **
    BEGIN TRY

        -- No counting of rows
        SET NOCOUNT ON;

   
        --
        -- Grab deadlock info from wmi xml reports table
        --

        -- Latest report id variable
        DECLARE @last_alert int

        -- Grab the id
        select top 1 @last_alert = alert_id 
        from msdb.dbo.tbl_wmi_xml_reports
        where alert_type = 'deadlocks'
        order by alert_id desc;
      
        -- Save the formatted results to a temporary table
        select * into #deadlock_table
        from dbo.ufn_parse_deadlock_report (@last_alert)


        --
        -- Define body of email just like SQL Sentry
        --

        -- Body of email 
        DECLARE @body varchar(max) = '';

        -- Report header
        SELECT @body +=
            N'[Connection]:            ' + CONVERT(varchar(30), SERVERPROPERTY('servername')) + char(10) + 
            N'[Message]:               SQL Server Deadlock Detected' + char(10) +
            N'[Time Stamp (Local)]:    ' + convert(varchar(30), getdate()) + char(10) + 
            N'[Time Stamp (UTC)]:      ' + convert(varchar(30), getutcdate()) + char(10) + 
            N'[Program]:               usp_Monitor_Deadlock()' + char(10) +
            N'[Version]:               1.1' + char(10) + char(10) + 
            replicate ('-', 75) + char(10) + char(10);

        -- Report body
        SELECT @body +=  
            N'[Deadlock Victim Information] ' + char(10) + 
            N'[SPID [ecid]]:                ' + coalesce(td.spid, '') + ' [' + td.ecid + ']' + char(10) + 
            N'[Host]:                       ' + coalesce(td.hostname, '') + char(10) + 
            N'[Application]:                ' + coalesce(td.clientapp, '') + char(10) + 
            N'[Database]:                   ' + coalesce(td.currentdb, '') + char(10) + 
            N'[Login]:                      ' + coalesce(td.loginname, '') + char(10) + 
            N'[Log Used]:                   ' + coalesce(td.logused, '') + char(10) + 
            N'[Deadlock Priority]:          ' + coalesce(td.[priority], '') + char(10) + 
            N'[Wait Time]:                  ' + coalesce(td.waittime, '') + char(10) + 
            N'[Transaction Start Time]:     ' + coalesce(td.lasttranstarted, '') + char(10) + 
            N'[Last Batch Start Time]:      ' + coalesce(td.lastbatchstarted, '') + char(10) + 
            N'[Last Batch Completion Time]: ' + coalesce(td.lastbatchcompleted, '') + char(10) + 
            N'[Lock Mode]:                  ' + coalesce(td.lockmode, '') + char(10) + 
            N'[Status]:                     ' + coalesce(td.status, '') + char(10) + 
            N'[Isolation Level]:            ' + coalesce(td.isolationlevel, '') + char(10) + 
            N'[Client Options 2]:           ' + coalesce(td.clientoption2, '') + char(10) + 
            N'[Client Options 1]:           ' + coalesce(td.clientoption1, '') + char(10) + 
            N'[Is Victim]:                  True' + char(10) + 
            N'[Wait Resource]:              ' + coalesce(td.waitresource, '') + char(10) + 
            N'[SPID]:                       ' + coalesce(td.spid, '') + char(10) + 
            N'[ECID]:                       ' + coalesce(td.ecid, '') + char(10) + 
            N'[Transaction Name]:           ' + coalesce(td.transactionname, '') + char(10) + 
            N'[Database ID]:                ' + CAST(DB_ID(coalesce(td.currentdb, '')) as varchar) + char(10) + 
            N'[Text Data]:                  ' + coalesce(td.inputbuf, '')
        FROM #deadlock_table as td;


        --
        -- Save formatted data to table for reporting
        --

        IF (@var_option = 'monitor')
        BEGIN

            -- Insert data into table
            INSERT INTO msdb.dbo.tbl_Monitor_Deadlocks 
            (
                [procid],
                [currentdb],
                [spid],
                [procname],
                [waittime],
                [lockmode],
                [trancount],
                [clientapp],
                [hostname],
                [loginname],
                [logused],
                [priority],
                [status],
                [ecid],
                [lasttranstarted],
                [lastbatchstarted],
                [lastbatchcompleted],
                [isolationlevel],
                [transactionname],
                [waitresource],
                [clientoption1],
                [clientoption2],
                [frame],
                [inputbuf]
            )
            SELECT *
            FROM #deadlock_table;

            -- Keep only 1 year of data online
            DELETE FROM [msdb].[dbo].[tbl_Monitor_Deadlocks]
            WHERE poll_date < CAST(DATEADD(YY, -1, GETDATE()) AS datetime);

        END;


        --
        -- Print, Email or Monitor?
        --

        -- Just show data w/out emailing
        IF (@var_option = 'print')
        BEGIN
            PRINT @body;
        END

        -- Email report to mail list
        IF ((@var_option = 'email') or (@var_option = 'monitor'))
        BEGIN

            -- Create email strings
            DECLARE @var_body varchar(max) = @body;
            DECLARE @var_subject varchar(30) = @@servername + ': SQL Server Deadlock Detected';
                                    
            -- Send out email (named profile)
            IF (@var_profile_name <> 'none')
            BEGIN
                EXEC msdb.dbo.sp_send_dbmail 
                    @profile_name = @var_profile_name, 
                    @recipients = @var_email_list,
                    @subject = @var_subject,
                    @body = @var_body,
                    @body_format = 'TEXT' ;    
            END

            -- Send out email (default profile)
            ELSE
            BEGIN
                EXEC msdb.dbo.sp_send_dbmail 
                    @recipients = @var_email_list,
                    @subject = @var_subject,
                    @body = @var_body,
                    @body_format = 'TEXT' ;    
            END

    END

    -- ** Error Handling - End Try **
    END TRY

    -- ** Error Handling - Begin Catch **
    BEGIN CATCH
       
      -- Grab variables 
      SELECT 
          @err_number = ERROR_NUMBER(), 
          @err_procedure = ERROR_PROCEDURE(),
          @err_line = ERROR_LINE(), 
          @err_message = ERROR_MESSAGE();

      -- Raise error
      RAISERROR ('An error occurred within a user transaction. 
                  Error Number        : %d
                  Error Message       : %s  
                  Affected Procedure  : %s
                  Affected Line Number: %d'
                  , 16, 1
                  , @err_number, @err_message, @err_procedure, @err_line);       

    -- ** Error Handling - End Catch **    
    END CATCH                
            
END;
GO