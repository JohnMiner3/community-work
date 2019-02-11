/******************************************************
 *
 * Name:         usp_monitor_blocking
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Blog:     www.craftydba.com
 *
 *     Version:  1.1
 *     Date:     07-01-2013
 *     Purpose:  Gather blocking information when
 *               a SQL performance alert executes
 *               the job.
 *
 *     Inputs:
 *         @var_option - 
 *             print = show output to screen.
 *             email = send an email to dba.
 *             monitor = save to table & email.
 *         @var_email_list - email notification list.
 *         @var_profile_name - use named email profile.
 *         @var_monitoring_secs - how often to save/email? 
 * 
 ******************************************************/


/*
    Excellent resources on blocking!
*/

-- Overview 
--   http://support.microsoft.com/kb/224453

-- Blocked processor report
--   http://msdn.microsoft.com/en-us/library/ms191168.aspx

-- Time period for report
--   http://msdn.microsoft.com/en-us/library/ms181150.aspx



/*  
    Which database to use.
*/

USE msdb;
GO


/*  
    Delete the existing table.
*/

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[tbl_Wmi_Xml_Reports]') AND type = 'U')
DROP TABLE [dbo].[tbl_Wmi_Xml_Reports];
GO


/*  
    Create table to contain xml reports
*/

CREATE TABLE [dbo].[tbl_Wmi_Xml_Reports]
(
    [alert_id] int identity(1, 1) not null, 
    [alert_date] [datetime] default (getdate()),
    [alert_type] varchar(32) default 'unknown',
    [alert_report] [xml] null
	
    CONSTRAINT [pk_alert_id_4_wmi_xml_reports] PRIMARY KEY CLUSTERED 
    ( [alert_id] ASC )	
);
GO



/*  
    Which database to use.
*/

USE msdb
GO


/*  
    Delete the existing table.
*/

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[tbl_Monitor_Blocking]') AND type = 'U')
DROP TABLE [dbo].[tbl_Monitor_Blocking]
GO


/*  
    Create table to contain history profile
*/

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[tbl_Monitor_Blocking]') AND type = 'U')
BEGIN
    CREATE TABLE [dbo].[tbl_Monitor_Blocking]
    (
        [poll_id] int identity(1,1) not null, 
        [poll_date] [datetime] default (getdate()),

        [code] [varchar](128) NULL,
        [spid] [int] NULL,
        [blocked_by_spid] [int] NULL,
        [client_machine] [varchar](128) NULL,
        [client_process_id] [varchar](10) NULL,
        [application_name] [varchar](128) NULL,
        [login_name] [varchar](128) NULL,
        [last_batch] [datetime] NULL,
        [wait_type] [varchar](32) NULL,
        [wait_resource] [varchar](256) NULL,
        [wait_time] [datetime] NULL,
        [status] [varchar](30) NULL,
        [database_id] [int] NULL,
        [database_name] [varchar](128) NULL,
        [command_text] [varchar] (max) NULL

        Constraint pk_poll_id_4_blocking Primary Key Clustered (poll_id)
    );

    -- Debug line
    PRINT 'Table [msdb].[dbo].[tbl_Monitor_Blocking] has been created.';    
END
GO



/*  
    Delete the existing stored procedure.
*/

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[usp_monitor_blocking]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_monitor_blocking]
GO
 

/*  
    Create the new stored procedure.
*/

CREATE PROCEDURE [dbo].[usp_monitor_blocking]
    @var_option varchar(256) = 'print',
    @var_email_list varchar(256) = 'none',
    @var_profile_name varchar(256) = 'none',
    @var_monitoring_secs float = 15.0
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
    -- Grab topmost xml blocking report
    --

    -- Declare local variables
    declare @alert_id int;

	-- Find the id
    select top 1 @alert_id = [alert_id]
    from [dbo].[tbl_Wmi_Xml_Reports]
    where [alert_type] = 'blocking'
	order by [alert_id] desc;
	
    --
    -- Create the html blocking report
    --

    -- Body of email 
    declare @body varchar(max) = '';

    -- Report header
    select @body +=
        N' ' + char(10) + 
        N'[Connection]:      ' + CONVERT(sysname, SERVERPROPERTY('servername')) + char(10) + 
        N'[Message]:         SQL Server Blocking Detected' + char(10) +
        N'[Time Stamp]:      ' + convert(varchar(30), getdate(), 121) + char(10) + 
        N'[Program]:         usp_Monitor_Blocking()' + char(10) +
        N'[Version]:         1.1' + char(10) +
        N' ' + char(10);

    -- Report body (blocking items)
    select @body += 
        replicate ('-', 100) + char(10) + char(10) +
        N'[Relationship]:         ' + 
        case 
            when coalesce(code, '') like '%H' then 'head of chain'
            else 'tail of chain'
        end + char(10) + 
        N'[SPID]:                 ' + coalesce(spid, '') + char(10) + 
        N'[Blocked By SPID]:      ' + 
        case coalesce(blocked_by_spid, '')
            when '0' then ''
            else coalesce(blocked_by_spid, '')
        end + char(10) + 
        N'[Client Machine]:       ' + coalesce(client_machine, '')  + char(10) + 
        N'[Client Process Id]:    ' + coalesce(client_process_id, '')  + char(10) + 
        N'[Application]:          ' + coalesce(application_name, '')   + char(10) + 
        N'[Login Name]:           ' + coalesce(login_name, '')  + char(10) + 
        N'[Last Batch]:           ' + coalesce(last_batch, '')  + char(10) + 
        N'[Wait Type]:            ' + coalesce(wait_type, '')  + char(10) + 
        N'[Wait Resource]:        ' + coalesce(wait_resource, '')  + char(10) + 
        N'[Wait Time]:            ' + coalesce(wait_time, '')  + char(10) + 
        N'[Database]:             ' + coalesce(database_name, '')  + char(10) + 
        N'[Command Text]:         ' + char(10) + char(10) + coalesce(command_text, '')  + char(10) + char(10)

    from [msdb].[dbo].[ufn_Parse_Blocking_Report] (@alert_id)
    order by code;
    
    -- Report footer
    select @body +=
        REPLICATE ('-', 100) + char(10);

    --
    -- Just show data w/out emailing
    --
    
    IF (@var_option = 'print')
    BEGIN
        PRINT @body;
		RETURN;
    END


    --
    -- Save data to table, replace if same day
    --
    
    IF (@var_option = 'monitor')
    BEGIN

        -- Keep only 1 year of data online
        DELETE FROM [msdb].[dbo].[tbl_Monitor_Blocking]
        WHERE poll_date < CAST(DATEADD(YY, -1, GETDATE()) AS smalldatetime);
         
        -- Add data for alert into table
        INSERT INTO [msdb].[dbo].[tbl_Monitor_Blocking]
        (
            [code],
            [spid],
            [blocked_by_spid],
            [client_machine],
            [client_process_id],
            [application_name], 
            [login_name],
            [last_batch],
            [wait_type],
            [wait_resource],
            [wait_time], 
            [status],
            [database_id],
            [database_name],
            [command_text]
        )
        select 
            code,
            spid,
            blocked_by_spid,
            client_machine, 
            client_process_id,
            application_name,
            login_name,
            last_batch,
            wait_type,
            wait_resource,
            dateadd(ms, coalesce(wait_time, 0), '19000101'),
            status,
            database_id,
            database_name,
            command_text
        from [dbo].[ufn_Parse_Blocking_Report] (@alert_id);
    END


    --
    -- Email report to mail list
    --
    
    IF ((@var_option = 'email') or (@var_option = 'monitor')) 
    BEGIN
  
        -- Create email strings
        DECLARE @var_body varchar(max);
        DECLARE @var_subject varchar(255);
        
        -- Load string report
        SET @var_body = @body;

        -- Compose the subject
        select @var_subject = 'Blocking Processes Report - ' +  @@SERVERNAME + ' - detected blocking chains exceeding the limit of ' + str(@var_monitoring_secs, 4, 2) + ' seconds.';
        
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


-- Debug line
PRINT 'Procedure [msdb].[dbo].[usp_monitor_blocking] has been created.';    
GO


--
-- Sample code retrieve wait resource
--

/*

SELECT o.name as object_name, i.name as index_name
FROM sys.partitions p 
JOIN sys.objects o ON p.object_id = o.object_id 
JOIN sys.indexes i ON p.object_id = i.object_id 
AND p.index_id = i.index_id 
WHERE p.hobt_id = 72057594047037440

*/


-- Debug line
PRINT '';    
PRINT 'Check out sample code at end of script.';    
GO
