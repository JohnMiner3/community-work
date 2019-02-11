/******************************************************
 *
 * Name:         usp_monitor_vlfs
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Blog:     www.craftydba.com
 *
 *     Version:  1.1
 *     Date:     12-17-2012
 *     Purpose:  To monitor the size of virtual
 *               log files for all on-line databases.
 *
 *     Inputs:
 *         @var_option - 
 *             print = show output to screen.
 *             email = send an email to dba.
 *             monitor = save to table & email
 *         @var_max_vlfs - max # of allowed vlfs.
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

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[tbl_Monitor_Vlfs]') AND type = 'U')
DROP TABLE [dbo].[tbl_Monitor_Vlfs]
GO


/*  
    Create table to contain history profile
*/

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[tbl_Monitor_Vlfs]') AND type = 'U')
BEGIN
    CREATE TABLE [dbo].[tbl_Monitor_Vlfs]
    (
        [poll_id] int identity(1,1) not null, 
        [db_id] smallint, 
        [db_name] varchar(128),
        [avg_vlf_size_kb] real,
        [num_of_vlfs] smallint,
        [poll_date] smalldatetime
        Constraint pk_poll_id_4_vlfs Primary Key Clustered (poll_id)
    );

    -- Debug line
    PRINT 'Table [msdb].[dbo].[tbl_Monitor_Vlfs] has been created.';    
END
GO


/*  
    Delete the existing stored procedure.
*/

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[usp_monitor_vlfs]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_monitor_vlfs]
GO
 

/*  
    Create the new stored procedure.
*/

CREATE PROCEDURE [dbo].[usp_monitor_vlfs]
    @var_option varchar(256) = 'print',
    @var_max_vlfs smallint = 50,
    @var_email_list varchar(256) = 'none',
    @var_profile_name varchar(256) = 'none'
AS
BEGIN

    -- Error handling variables
    DECLARE @err_number int;
    DECLARE @err_line int;
    DECLARE @err_message varchar(2048);
    DECLARE @err_procedure varchar(2048);

    -- ** Error Handling - Start Try **
    BEGIN TRY


    -- No counting of rows
    SET NOCOUNT ON;
    
    -- Declare variables
    DECLARE @var_query varchar(256);
    DECLARE @var_dbid int;
    DECLARE @var_dbname varchar(128);
    DECLARE @var_sqlver varchar(64);
    DECLARE @var_count smallint;
    DECLARE @var_avgsize real;


    -- Get list of on-line databases
    DECLARE var_vlfs_cursor CURSOR FOR
        SELECT database_id, name 
        FROM master.sys.databases 
        WHERE state_desc = 'ONLINE';
        
    -- Create table variable to hold results
    DECLARE @log_results TABLE
    (
        [db_id] smallint, 
        [db_name] varchar(128),
        [avg_vlf_size_kb] real,
        [num_of_vlfs] smallint
    );

    -- Get sql server version
    SELECT @var_sqlver = SUBSTRING(LTRIM(CAST(SERVERPROPERTY('productversion') AS varchar(20))), 1, 2);

    -- Correct table variable for SQL 2012
    IF (@var_sqlver = '11') OR (@var_sqlver = '12') 
    BEGIN
        DECLARE @log_info12 TABLE 
        (
            [recovery_unit_id] int,
            [file_id] tinyint,
            [file_size] bigint,
            [start_offset] bigint,
            [vfile_seq_no] int,
            [status] tinyint,
            [parity] tinyint,
            [create_lsn] numeric(25,0)
        )
    END

    -- Correct table variable for SQL 2005, 2008, 2008 R2
    ELSE
    BEGIN
        DECLARE @log_info05 TABLE 
        (
            [file_id] tinyint,
            [file_size] bigint,
            [start_offset] bigint,
            [vfile_seq_no] int,
            [status] tinyint,
            [parity] tinyint,
            [create_lsn] numeric(25,0)
        )
    END

    -- Open cursor
    OPEN var_vlfs_cursor;

    -- Get first row
    FETCH NEXT FROM var_vlfs_cursor 
        INTO @var_dbid, @var_dbname;

    -- While there is data
    WHILE (@@fetch_status = 0)
    BEGIN

        -- Create dynamic sql
        SET @var_query = 'DBCC LOGINFO (' + '''' + @var_dbname + ''') ';

        -- Get results from DBCC specific to sql server version
        IF (@var_sqlver = '11') OR (@var_sqlver = '12') 
            INSERT INTO @log_info12 EXEC (@var_query);
        ELSE
            INSERT INTO @log_info05 EXEC (@var_query);

        -- Find out count
        SET @var_count = @@rowcount;

        -- Get average size of vlf
        IF (@var_sqlver = '11') OR (@var_sqlver = '12') 
            SELECT @var_avgsize = AVG([file_size])/1024.0 FROM @log_info12
        ELSE
            SELECT @var_avgsize = AVG([file_size])/1024.0 FROM @log_info05

        -- Save count to results table
        INSERT @log_results VALUES(@var_dbid, @var_dbname, @var_avgsize, @var_count);

        -- Clear table variable - specific to sql server version
        IF (@var_sqlver = '11') OR (@var_sqlver = '12') 
            DELETE FROM @log_info12;
        ELSE
            DELETE FROM @log_info05;

        -- Grab the next record
        FETCH NEXT FROM var_vlfs_cursor INTO @var_dbid, @var_dbname;

    END

    -- Close cursor
    CLOSE var_vlfs_cursor;

    -- Release memory
    DEALLOCATE var_vlfs_cursor;


    --
    -- Just show data w/out emailing
    --
    
    IF (@var_option = 'print')
    BEGIN
        SELECT 
            *, 
            case 
                when ([num_of_vlfs] >= @var_max_vlfs) 
                then 'Y'
                else 'N'
            end as reduce_vlfs
        FROM @log_results
    END


    --
    -- Save data to table, replace if same day
    --
    
    IF (@var_option = 'monitor')
    BEGIN

        -- Keep only 1 year of data online
        DELETE FROM [msdb].[dbo].[tbl_Monitor_Vlfs]
        WHERE poll_date < CAST(DATEADD(YY, -1, GETDATE()) AS smalldatetime);
    
        -- Remove prior run data for today
        DELETE FROM [msdb].[dbo].[tbl_Monitor_Vlfs]
        WHERE poll_date = CAST(convert(char(10), GETDATE(), 101) AS smalldatetime);
        
        -- Add data for today into table
        INSERT INTO [msdb].[dbo].[tbl_Monitor_Vlfs]
        (
              db_id, 
            db_name,
            avg_vlf_size_kb,
            num_of_vlfs,
            poll_date
        )
        SELECT 
            *, 
            CAST(convert(char(10), GETDATE(), 101) AS smalldatetime)
        FROM @log_results;
    END


    --
    -- Email report to mail list
    --
    
    IF ((@var_option = 'email') or (@var_option = 'monitor'))
    BEGIN

        -- Any databases with VLF's > Max parameter?
        SELECT @var_count = count (distinct [db_name])
        FROM @log_results
        WHERE [num_of_vlfs] >= @var_max_vlfs
  
        -- Reject Email Message
        IF (@var_count > 0)
        BEGIN        
    
            -- Create email strings
            DECLARE @var_body varchar(2048);
            DECLARE @var_subject varchar(128);
        
            -- Load string with html
            SET @var_body =
            N'<H1>Virtual Log File Report - ' +  @@SERVERNAME + '</H1>' +
            N'<table border="1">' +
            N'<tr><th>Database Name</th>' +
            N'<th>Number Of Vlfs</th></tr>' +
            CAST ( ( SELECT td = [db_name], '',
                        td = [num_of_vlfs], ''
                  FROM @log_results
                  WHERE [num_of_vlfs] >= @var_max_vlfs   
                  ORDER BY [db_name]                  
                  FOR XML PATH('tr'), TYPE 
            ) AS NVARCHAR(MAX) ) +
            N'</table>' ;

            -- Compose the subject
            select @var_subject = 'Virtual Log File Report - ' +  @@SERVERNAME + ' - detected ' 
                + replace(str(@var_count, 5, 0), ' ', '') + ' database exceeding limit of ' + replace(str(@var_max_vlfs, 5, 0), ' ', '') + '.';
        
            -- Send out email (named profile)
            IF (@var_profile_name <> 'none')
            BEGIN
                EXEC msdb.dbo.sp_send_dbmail 
                    @profile_name = @var_profile_name, 
                    @recipients = @var_email_list,
                    @subject = @var_subject,
                    @body = @var_body,
                    @body_format = 'HTML' ;    
            END

            -- Send out email (default profile)
            ELSE
            BEGIN
                EXEC msdb.dbo.sp_send_dbmail 
                    @recipients = @var_email_list,
                    @subject = @var_subject,
                    @body = @var_body,
                    @body_format = 'HTML' ;    
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