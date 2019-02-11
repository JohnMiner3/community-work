/******************************************************
 *
 * Name:         exploring-ola-hallengren.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     01-21-2014
 *     Blog:     www.craftydba.com
 *
 *     Purpose:  Explore the free scripts supplied by
 *               Ola Hallengren.
 *
 ******************************************************/

-- http://ola.hallengren.com/


/*
	Cleanup the house
*/


-- Remove previous backup directories
DECLARE @cmd varchar(128) = 'C:\mssql\clean.cmd';
EXEC master..xp_cmdshell @cmd
GO

-- Remove old ola hallengren logs
TRUNCATE TABLE msdb.dbo.CommandLog;
GO

-- Remove backup history
DECLARE @today1 datetime = getdate();
EXEC msdb.dbo.sp_delete_backuphistory @oldest_date = @today1;
GO

-- Remove job history
DECLARE @today2 datetime = getdate();
EXEC msdb.dbo.sp_purge_jobhistory @oldest_date = @today2;
GO

-- Remove one record
DELETE FROM [AUTOS].[ACTIVE].[MAKES] WHERE [MAKER_NM] = 'Saturn';
GO


/*
	Example 1 - system vs user databases
*/

--
-- System databases
--

SELECT *
FROM sys.databases
WHERE [name] IN ('master','model','msdb','tempdb')
ORDER BY [name];
GO


--
-- User databases
--

SELECT *
FROM sys.databases
WHERE [name] NOT IN ('master','model','msdb','tempdb')
ORDER BY [name];
GO


/*
	Example 2 - basic backup commands
*/

--
-- Full system backup (1 x day)
--

-- Keep two weeks of backups
DECLARE @var_days1 INT = 24 * 14;

-- Full backup of system databases
EXECUTE msdb.dbo.DatabaseBackup
    @Databases = 'SYSTEM_DATABASES',
    @Directory = 'C:\MSSQL\BACKUP',
    @BackupType = 'FULL',
    @Verify = 'Y',
    @Compress = 'Y',
    @CheckSum = 'Y',
	@LogToTable = 'Y',
    @CleanupTime = @var_days1;


--
-- Log system backup (1 x hour)
--

-- Keep one week of backups
DECLARE @var_days2 INT = 24 * 7;

-- Full backup of system databases
EXECUTE msdb.dbo.DatabaseBackup
    @Databases = 'SYSTEM_DATABASES',
    @Directory = 'C:\MSSQL\BACKUP',
    @BackupType = 'LOG',
    @Verify = 'Y',
    @Compress = 'Y',
    @CheckSum = 'Y',
	@LogToTable = 'Y',
    @CleanupTime = @var_days2;


--
-- Full user backup (1 x week on sunday)
--

-- Anything running?
EXECUTE msdb.dbo.usp_get_maintenance_progress;
GO

-- Keep two weeks of backups
DECLARE @var_days3 INT = 24 * 14;

-- Full backup of user databases
EXECUTE msdb.dbo.DatabaseBackup
    @Databases = 'USER_DATABASES',
    @Directory = 'C:\MSSQL\BACKUP',
    @BackupType = 'FULL',
    @Verify = 'Y',
    @Compress = 'Y',
    @CheckSum = 'Y',
	@LogToTable = 'Y',
    @CleanupTime = @var_days3;


--
-- Diff user backup (1 x day except sunday)
--

-- Keep two weeks of backups
DECLARE @var_days4 INT = 24 * 14;

-- Differential backup of user databases
EXECUTE msdb.dbo.DatabaseBackup
    @Databases = 'USER_DATABASES',
    @Directory = 'C:\MSSQL\BACKUP',
    @BackupType = 'DIFF',
    @Verify = 'Y',
    @Compress = 'Y',
    @CheckSum = 'Y',
	@LogToTable = 'Y',
    @CleanupTime = @var_days4;


--
-- Setup for Point In Time (PIT) restore
--

/*

DELETE FROM [BIG_JONS_BBQ_DW].[STAGE].[COMMON_FIRST_NAMES];
DELETE FROM [BIG_JONS_BBQ_DW].[STAGE].[COMMON_LAST_NAMES];
GO

*/

--
-- Log user backup (1 x hour)
--

-- Keep one week of backups
DECLARE @var_days5 INT = 24 * 7;

-- Log backup of user databases
EXECUTE msdb.dbo.DatabaseBackup
    @Databases = 'USER_DATABASES',
    @Directory = 'C:\MSSQL\BACKUP',
    @BackupType = 'LOG',
    @Verify = 'Y',
    @Compress = 'Y',
    @CheckSum = 'Y',
	@LogToTable = 'Y',
    @CleanupTime = @var_days5;


/*
	Example 3 - advanced backup commands
*/

--
-- Copy only with single file, multiple files, and i/o options
--

-- Show time & i/o
SET STATISTICS TIME ON
SET STATISTICS IO ON
GO

-- Remove clean buffers & clear plan cache
CHECKPOINT 
DBCC DROPCLEANBUFFERS 
DBCC FREEPROCCACHE
GO

-- Run the backup
EXECUTE msdb.dbo.DatabaseBackup
    @Databases = 'BIG_JONS_BBQ_DW',
    @Directory = 'C:\MSSQL\SPECIAL1\',
    @BackupType = 'FULL',
	@CopyOnly = 'Y',
    @Verify = 'Y',
    @Compress = 'Y',
    @CheckSum = 'Y',
	@LogToTable = 'Y';

/*
	SQL Server Execution Times:
	CPU time = 1154 ms,  elapsed time = 10303 ms.
*/


-- Run the backup
EXECUTE msdb.dbo.DatabaseBackup
    @Databases = 'BIG_JONS_BBQ_DW',
    @Directory = 'C:\MSSQL\SPECIAL2\',
    @BackupType = 'FULL',
	@CopyOnly = 'Y',
    @Verify = 'Y',
    @Compress = 'Y',
    @CheckSum = 'Y',
	@LogToTable = 'Y',
    @NumberOfFiles = 4;

/*
	SQL Server Execution Times:
	CPU time = 685 ms,  elapsed time = 8939 ms.
*/


 -- Run the backup
EXECUTE msdb.dbo.DatabaseBackup
    @Databases = 'BIG_JONS_BBQ_DW',
    @Directory = 'C:\MSSQL\SPECIAL3\',
    @BackupType = 'FULL',
	@CopyOnly = 'Y',
    @Verify = 'Y',
    @Compress = 'Y',
    @CheckSum = 'Y',
	@LogToTable = 'Y',
	@BufferCount = 12,
    @MaxTransferSize = 4194304,
	@BLOCKSIZE=4096,
    @NumberOfFiles = 6;

/*
	SQL Server Execution Times:
	CPU time = 531 ms,  elapsed time = 6419 ms.
*/

-- Do not show time & i/o
SET STATISTICS TIME OFF
SET STATISTICS IO OFF
GO

-- Read only file groups = homework


/*
	Example 4 - database integrity
*/

--
-- Check system databases (1 x day)
--

EXECUTE msdb.dbo.DatabaseIntegrityCheck
    @Databases = 'SYSTEM_DATABASES',
    @CheckCommands = 'CHECKDB',
    @LogToTable = 'Y';


--
-- Check small user databases (1 x week)
--

EXECUTE msdb.dbo.DatabaseIntegrityCheck
    @Databases = 'USER_DATABASES, -%BBQ%, -%BIG%',
    @CheckCommands = 'CHECKDB',
    @LogToTable = 'Y';


--
-- Check large user database - skip index (1 x week)
--

-- Tablock = instead of internal database snapshot
EXECUTE msdb.dbo.DatabaseIntegrityCheck
    @Databases = '%BBQ%, %BIG%',
    @CheckCommands = 'CHECKDB',
    @NoIndex = 'Y',
	@TabLock = 'Y',
	@LockTimeOut = 60000,
    @LogToTable = 'Y';


/*
	Example 5 - optimize indexes
*/

--
-- Small user databases - Reorganize or Rebuild indexes - (1 x week)
--

EXECUTE msdb.dbo.IndexOptimize
    @Databases = 'USER_DATABASES, -%BBQ%, -%BIG%',
    @FragmentationLow = NULL,
    @FragmentationMedium = 'INDEX_REORGANIZE,INDEX_REBUILD_ONLINE,INDEX_REBUILD_OFFLINE',
    @FragmentationHigh = 'INDEX_REBUILD_ONLINE,INDEX_REBUILD_OFFLINE',
    @FragmentationLevel1 = 10,
    @FragmentationLevel2 = 30,
    @UpdateStatistics = 'ALL',
    @OnlyModifiedStatistics = 'Y',
	@LogToTable = 'Y';


--
-- Large user databases - just update stats
--

EXECUTE msdb.dbo.IndexOptimize
    @Databases = '%BBQ%, %BIG%',
    @FragmentationLow = NULL,
    @FragmentationMedium = NULL,
    @FragmentationHigh = NULL,
    @UpdateStatistics = 'ALL';


--
-- System Databases - Reorganize or Rebuild indexes - (1 x week)
--

EXECUTE msdb.dbo.IndexOptimize
    @Databases = 'SYSTEM_DATABASES',
    @FragmentationLow = NULL,
    @FragmentationMedium = 'INDEX_REORGANIZE,INDEX_REBUILD_ONLINE,INDEX_REBUILD_OFFLINE',
    @FragmentationHigh = 'INDEX_REBUILD_ONLINE,INDEX_REBUILD_OFFLINE',
    @FragmentationLevel1 = 10,
    @FragmentationLevel2 = 30,
    @UpdateStatistics = 'ALL',
    @OnlyModifiedStatistics = 'Y',
	@LogToTable = 'Y';


/*
	Example 6 - restoring a database
*/

-- Get backup chain
EXECUTE msdb.dbo.usp_get_backup_chain 
    @name = 'BIG_JONS_BBQ_DW';
GO

--
--  Verify & list
--

-- Verify/list the full backup
DECLARE @file1 nvarchar(1024) =
    'C:\MSSQL\BACKUP\LATG1292\BIG_JONS_BBQ_DW\FULL\LATG1292_BIG_JONS_BBQ_DW_FULL_20140122_123457.bak'
RESTORE VERIFYONLY FROM DISK = @file1;
RESTORE FILELISTONLY FROM DISK = @file1;
GO


-- Verify the diff backup
DECLARE @file2 nvarchar(1024) =
    'C:\MSSQL\BACKUP\LATG1292\BIG_JONS_BBQ_DW\DIFF\LATG1292_BIG_JONS_BBQ_DW_DIFF_20140122_123514.bak'
RESTORE VERIFYONLY FROM DISK = @file2;
RESTORE FILELISTONLY FROM DISK = @file2;
GO


--
--  Restore missing staging data
--

-- Restore full backup
DECLARE @file3 nvarchar(1024) =
    'C:\MSSQL\BACKUP\LATG1292\BIG_JONS_BBQ_DW\FULL\LATG1292_BIG_JONS_BBQ_DW_FULL_20140122_123457.bak'
RESTORE DATABASE [BIG_JONS_BBQ_DW]
   FROM DISK = @file3
   WITH REPLACE,
   NORECOVERY;
GO

-- Apply diff backup
DECLARE @file4 nvarchar(1024) =
    'C:\MSSQL\BACKUP\LATG1292\BIG_JONS_BBQ_DW\DIFF\LATG1292_BIG_JONS_BBQ_DW_DIFF_20140122_123514.bak'
RESTORE DATABASE [BIG_JONS_BBQ_DW]
   FROM DISK = @file4
   WITH RECOVERY;
GO



--
--	Tail backup
--

-- Show the backup chain
EXEC msdb.[dbo].[usp_get_backup_chain] @name = 'AUTOS';
GO

-- Add one record
INSERT INTO [AUTOS].[ACTIVE].[MAKES]
VALUES('Saturn', 1990, 2010);
GO

-- Bring database off-line
ALTER DATABASE [AUTOS] SET OFFLINE
GO

-- Remove all data files
/*
DECLARE @cmd varchar(128) = 'C:\mssql\damage.cmd';
EXEC master..xp_cmdshell @cmd
GO
*/

-- Bring database off-line
ALTER DATABASE [AUTOS] SET ONLINE
GO

--
--  Two options for tail backup
--

-- Online database
BACKUP LOG [AUTOS] 
    TO DISK = 'C:\mssql\backup\autos_log_tail.trn' 
	WITH NO_TRUNCATE;

-- Off-line database
BACKUP LOG [AUTOS] 
    TO DISK = 'C:\mssql\backup\autos_log_tail.trn' 
	WITH CONTINUE_AFTER_ERROR;


-- Restore full backup
DECLARE @file5 nvarchar(1024) =
    'C:\MSSQL\BACKUP\LATG1292\AUTOS\FULL\LATG1292_AUTOS_FULL_20140122_075518.bak';
RESTORE DATABASE [AUTOS]
   FROM DISK = @file5
   WITH REPLACE,
   NORECOVERY;
GO

-- Apply diff backup
DECLARE @file6 nvarchar(1024) =
    'C:\MSSQL\BACKUP\LATG1292\AUTOS\DIFF\LATG1292_AUTOS_DIFF_20140122_075600.bak';
RESTORE DATABASE [AUTOS]
   FROM DISK = @file6
   WITH NORECOVERY;
GO

-- Apply log backup
DECLARE @file7 nvarchar(1024) =
    'C:\MSSQL\BACKUP\LATG1292\AUTOS\LOG\LATG1292_AUTOS_LOG_20140122_075617.trn';
RESTORE DATABASE [AUTOS]
   FROM DISK = @file7
   WITH NORECOVERY;
GO

-- Apply tail backup
DECLARE @file8 nvarchar(1024) =
    'C:\mssql\backup\autos_log_tail.bak';
RESTORE DATABASE [AUTOS]
   FROM DISK = @file8
   WITH RECOVERY;
GO
