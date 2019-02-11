/******************************************************
 *
 * Name:         usp_get_backup_chain.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     01-21-2014
 *     Blog:     www.craftydba.com
 *
 *     Purpose:  What is the backup chain for a database?
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

IF OBJECT_ID('[dbo].[usp_get_backup_chain]') > 0
DROP PROCEDURE [dbo].[usp_get_backup_chain]
GO


/*  
	Create the new stored procedure.
*/

CREATE PROCEDURE [dbo].[usp_get_backup_chain]
    @NAME SYSNAME = '*'
AS
BEGIN

    -- Declare variables
    DECLARE @VAR_TSQL VARCHAR(2048);

	-- Dynamic T-SQL
	SET @VAR_TSQL =
     'SELECT
          s.server_name,
          s.database_name,
          s.name as software_name,
          CASE s.[type]
              WHEN ''D'' THEN ''Database''
              WHEN ''I'' THEN ''Differential database''
              WHEN ''L'' THEN ''Log''
              WHEN ''F'' THEN ''File or filegroup''
              WHEN ''G'' THEN ''Differential file''
              WHEN ''P'' THEN ''Partial''
              WHEN ''Q'' THEN ''Differential partial''
              ELSE ''none''
          END AS backup_type,
          s.backup_start_date,
          s.backup_finish_date,
		  f.physical_device_name
      FROM 
          msdb.dbo.backupset AS s 
	  JOIN
		  msdb.dbo.backupmediafamily as f 
	  ON  
	      s.media_set_id = f.media_set_id
		  ';

    -- All databases?
	IF @NAME <> '*'
        SET @VAR_TSQL = @VAR_TSQL + ' WHERE s.database_name = ' + CHAR(39) + @NAME + CHAR(39);

    -- Show in desc order
    SET @VAR_TSQL = @VAR_TSQL + 'ORDER BY s.backup_start_date DESC ';

	-- Run the command
	EXECUTE(@VAR_TSQL);
END;

GO


