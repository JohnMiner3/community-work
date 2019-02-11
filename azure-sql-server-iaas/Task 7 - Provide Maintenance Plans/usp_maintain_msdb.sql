/******************************************************
 *
 * Name:         usp_maintain_msdb.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     04-26-2011
 *     Purpose:  Remove old data from msdb.
 * 
 ******************************************************/

/*  
	Which database to use.
*/

USE msdb
GO


/*  
	Delete the existing stored procedure.
*/

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[usp_maintain_msdb]') AND type in (N'P', N'PC'))
DROP PROCEDURE [dbo].[usp_maintain_msdb]
GO
 


/*  
	Create the new stored procedure.
*/

CREATE PROCEDURE [dbo].[usp_maintain_msdb]
  @VAR_DAYS INT = 31
AS
BEGIN

  -- DECLARE VARIABLES
  DECLARE @VAR_OLDEST DATETIME;

  -- GET OLDEST DAY WE WANT TO KEEP
  SELECT @VAR_OLDEST =  GETDATE() - @VAR_DAYS;

  -- RECORD OUTPUT TO WINDOW
  PRINT 'MAINTAIN MSDB:';

  -- CLEAR BACKUP SETS
  EXEC sp_delete_backuphistory @oldest_date = @VAR_OLDEST;

  -- RECORD OUTPUT TO WINDOW
  PRINT ' ';
  PRINT '    CLEAR BACKUP SETS';

  -- CLEAR JOB HISTORY
  EXEC sp_purge_jobhistory @oldest_date = @VAR_OLDEST;

  -- RECORD OUTPUT TO WINDOW
  PRINT ' ';
  PRINT '    CLEAR JOB HISTORY'

  -- CLEAR JOB HISTORY
  EXEC sp_maintplan_delete_log @oldest_time = @VAR_OLDEST;

  -- RECORD OUTPUT TO WINDOW
  PRINT ' ';
  PRINT '    CLEAR MAINT PLAN HISTORY';

  -- CLEAR OUT MAIL ITEMS
  EXECUTE sysmail_delete_mailitems_sp @sent_before = @VAR_OLDEST;

  -- RECORD OUTPUT TO WINDOW
  PRINT ' ';
  PRINT '    CLEAR MAIL ITEMS HISTORY';

  -- CLEAR OUT MAIL LOG
  EXECUTE sysmail_delete_log_sp @logged_before = @VAR_OLDEST;

  -- RECORD OUTPUT TO WINDOW
  PRINT ' ';
  PRINT '    CLEAR MAIL LOG HISTORY';

END

GO