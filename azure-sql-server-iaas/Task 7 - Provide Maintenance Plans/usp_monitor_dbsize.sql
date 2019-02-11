/******************************************************
 *
 * Name:         usp_monitor_dbsize
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Blog:     www.craftydba.com
 *
 *     Version:  1.1
 *     Date:     12-17-2012
 *     Purpose:  To monitor the size of data and log
 *               files for all databases.
 *
 *     Inputs:
 *         @var_option - 
 *             print = show output to screen.
 *             email = send an email 2 list.
 *             monitor = save 2 history table & email list.
 *         @var_min_free_pct - min free space pct.
 *         @var_email_list - email notification list.
 *         @var_profile_name - use named profile (<> default)
 * 
 ******************************************************/

/*  
    Which database to use.
*/

USE msdb
GO


/*  
    Delete the existing table.
*/

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[tbl_Monitor_Dbsize]') AND type = 'U')
DROP TABLE [dbo].[tbl_Monitor_Dbsize]
GO


/*  
    Create table to contain history profile
*/

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[tbl_Monitor_Dbsize]') AND type = 'U')
BEGIN
    CREATE TABLE [dbo].[tbl_Monitor_Dbsize]
    (
        poll_id int identity(1,1) not null, 
        server_name varchar(128), 
        database_name varchar(128), 
        logical_file_name varchar(128), 
        physical_file_name varchar(512), 
        database_status varchar(128), 
        update_option varchar(128), 
        recovery_mode varchar(128), 
        file_size_mb int, 
        free_space_mb int, 
        free_space_pct real,
        poll_date smalldatetime
        Constraint pk_poll_id_4_dbsize Primary Key Clustered (poll_id)
    );

    -- Debug line
    PRINT 'Table [msdb].[dbo].[tbl_Monitor_Dbsize] has been created.';    
END
GO


/*  
    Delete the existing stored procedure.
*/

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[usp_monitor_dbsize]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_monitor_dbsize]
GO
 

/*  
    Create the new stored procedure.
*/

CREATE PROCEDURE [dbo].[usp_monitor_dbsize]
    @var_option varchar(256) = 'print',
    @var_min_free_pct smallint = 15,
    @var_email_list varchar(256) = 'none',
    @var_profile_name varchar(256) = 'none'
AS
BEGIN

    -- Error handling variables
    DECLARE @err_number int;
    DECLARE @err_line int;
    DECLARE @err_message nvarchar(4000);
    DECLARE @err_procedure nvarchar(4000);

    -- ** Error Handling - Start Try **
    BEGIN TRY


    -- No counting of rows
    SET NOCOUNT ON;
    
    -- Create table variable to hold results
    DECLARE @database_sizing TABLE
    (
        server_name varchar(128), 
        database_name varchar(128), 
        logical_file_name varchar(128), 
        physical_file_name varchar(512), 
        database_status varchar(128), 
        update_option varchar(128), 
        recovery_mode varchar(128), 
        file_size_mb int, 
        free_space_mb int, 
        free_space_pct as convert(real, case when file_size_mb = 0 then 0.0 else free_space_mb * 1.0 / file_size_mb * 100.0 end) ,  
        poll_date smalldatetime
    );

 
    -- Declare variable to hold tsql
    DECLARE @tsql_database_info varchar(max);

    -- Create tsql for each database
    SELECT @tsql_database_info = 'Use [' + '?' + '] 
        SELECT 
            @@SERVERNAME as server_name, 
            ' + '''' + '?' + '''' + ' as database_name, 
            f.name as logical_file_name, 
            f.filename as physical_file_name, 
            CONVERT(varchar(128), DatabasePropertyEx(''?'',''Status'')) as database_status, 
            CONVERT(varchar(128), DatabasePropertyEx(''?'',''Updateability'')) as update_option, 
            CONVERT(varchar(128), DatabasePropertyEx(''?'',''Recovery'')) as recovery_mode, 
            CAST(f.size/128.0 AS int) AS file_size_mb, 
            CAST(f.size/128.0 - CAST(FILEPROPERTY(f.name, ' + '''' +  'SpaceUsed' + '''' + ' ) AS int)/128.0 AS int) as free_space_mb,
            CAST(CONVERT(CHAR(10), GETDATE(), 101) AS smalldatetime) as poll_date 
        FROM dbo.sysfiles as f';

    -- Fill the table with data
    INSERT INTO @database_sizing
    (
        server_name,
        database_name,
        logical_file_name,
        physical_file_name,
        database_status,
        update_option,
        recovery_mode,
        file_size_mb,
        free_space_mb,
        poll_date
    )
    EXEC sp_MSForEachDB @tsql_database_info;


    --
    -- Just show data w/out saving or emailing
    --
    
    IF (@var_option = 'print')
    BEGIN
        SELECT 
            *, 
            case 
                when ([free_space_pct] < @var_min_free_pct * 1.0) 
                then 'Y'
                else 'N'
            end as need_space
        FROM @database_sizing
    END


    --
    -- Save data to table, replace if same day
    --
    
    IF (@var_option = 'monitor')
    BEGIN

        -- Keep only 1 year of data online
        DELETE FROM [msdb].[dbo].[tbl_Monitor_Dbsize]
        WHERE poll_date < CAST(DATEADD(YY, -1, GETDATE()) AS smalldatetime);
    
        -- Remove prior run data for today
        DELETE FROM [msdb].[dbo].[tbl_Monitor_Dbsize]
        WHERE poll_date = CAST(CONVERT(CHAR(10), GETDATE(), 101) AS smalldatetime);
        
        -- Add data for today into table
        INSERT INTO [msdb].[dbo].[tbl_Monitor_Dbsize]
        (
            server_name,
            database_name,
            logical_file_name,
            physical_file_name,
            database_status,
            update_option,
            recovery_mode,
            file_size_mb,
            free_space_mb,
            free_space_pct,
            poll_date
        )
        SELECT * FROM  @database_sizing;
    END
    

    --
    -- Email report to mail list
    --
    
    IF ((@var_option = 'email') or (@var_option = 'monitor'))
    BEGIN

        -- Must pass email address, raise error
        IF (@var_email_list = 'none')
            RAISERROR ('An address needs to be passed if email option is selected.', 16, 1);       
        
        -- Number of databases with low space
        DECLARE @var_count smallint;
        
        -- Any databases with free space less than min
        SELECT @var_count = count (distinct [database_name])
        FROM @database_sizing
        WHERE [free_space_pct] < @var_min_free_pct
      
        -- Low Space Email Message
        IF (@var_count > 0)
        BEGIN        
        
            -- Create email strings
            DECLARE @var_body varchar(max);
            DECLARE @var_subject varchar(128);
            
            -- Load string with html (summary)
            SET @var_body =
            N'<H1>Summary Report - Database Files - ' +  @@SERVERNAME + '</H1>' +
            N'<table border="1">' +
            N'<tr><th>Database Name</th>' +
            N'<th>Files Needing Space</th></tr>' +
            CAST ( ( SELECT td = [database_name], '',
                        td = count([database_name]), ''
                  FROM @database_sizing
                  WHERE [free_space_pct] < @var_min_free_pct
                  GROUP BY [database_name]
                  ORDER BY [database_name]                  
                  FOR XML PATH('tr'), TYPE 
            ) AS NVARCHAR(MAX) ) +
            N'</table>' ;

            -- Load string with html (detail)
            SET @var_body = @var_body +
            N'&nbsp;&nbsp;&nbsp;&nbsp;' +
            N'<H1>Detailed Report - Database Files - ' +  @@SERVERNAME + '</H1>' +
            N'<table border="1">' +
            N'<tr><th>Database Name</th>' +
            N'<th>File Name</th>' +
            N'<th>File Size (mb)</th>' +
            N'<th>Free Space (mb)</th>' +
            N'<th>Free Space (pct)</th></tr>' +
            CAST ( ( SELECT 
                        td = [database_name], '',
                        td = [physical_file_name], '',
                        td = [file_size_mb], '',
                        td = [free_space_mb], '',
                        td = str([free_space_pct], 5, 2), ''
                  FROM @database_sizing
                  WHERE [free_space_pct] < @var_min_free_pct
                  ORDER BY [database_name], [physical_file_name]                  
                  FOR XML PATH('tr'), TYPE 
            ) AS NVARCHAR(MAX) ) +
            N'</table>' ;

            -- Compose the subject
            select @var_subject = 'Database Files Report - ' +  @@SERVERNAME + ' - detected ' 
                + replace(str(@var_count, 5, 0), ' ', '') + ' database exceeding limit of ' + replace(str(@var_min_free_pct, 5, 0), ' ', '') + ' % free space.';
            
            -- Send out email (named profile)
            IF (@var_profile_name <> 'none')
            BEGIN
                EXEC msdb.dbo.sp_send_dbmail 
                    @profile_name = @var_profile_name, 
                    @recipients = @var_email_list,
                    @subject = @var_subject,
                    @body = @var_body,
                    @body_format = 'HTML';    
            END

            -- Send out email (default profile)
            ELSE
            BEGIN
                EXEC msdb.dbo.sp_send_dbmail 
                    @recipients = @var_email_list,
                    @subject = @var_subject,
                    @body = @var_body,
                    @body_format = 'HTML';    
            END            
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
            
END