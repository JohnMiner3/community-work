/******************************************************
 *
 * Name:         setup-sharding-example.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     02-07-2016
 *     Purpose:  Split database by year.
 * 
 ******************************************************/


/*
    BANKING - 2014 DATA
*/

-- Which database to use.
USE [master]
GO
 
-- Delete existing database
IF  EXISTS (SELECT name FROM sys.databases WHERE name = N'BANKING_2014')
DROP DATABASE BANKING_2014
GO

-- Get file list
RESTORE FILELISTONLY FROM DISK = 'C:\ZULU\BANKING01-FULL.BAK'
   WITH FILE=1;
GO

-- Restore JAN 2014 to NOV 2015
RESTORE DATABASE BANKING_2014
   FROM DISK = 'C:\ZULU\BANKING01-FULL.BAK'
   WITH RECOVERY,
   MOVE 'BANKING_PRI_DAT' TO 'C:\MSSQL\DATA\BANKING-2014.MDF', 
   MOVE 'BANKING_ALL_LOG' TO 'C:\MSSQL\LOG\BANKING-2014.LDF'
GO

-- Remove 2015 transactions
USE [BANKING_2014]
GO

-- Get record count - 2015 transactions
SELECT COUNT(*) AS TOTAL_RECS 
FROM [ACTIVE].[TRANSACTION]
WHERE YEAR(TRAN_DATE) = 2015
GO

-- Remove 2015 transactions
BEGIN TRAN;
DELETE
FROM [ACTIVE].[TRANSACTION]
WHERE YEAR(TRAN_DATE) = 2015;
COMMIT TRAN;
GO

-- Make sure it is owned by [sa]
ALTER AUTHORIZATION ON DATABASE::[BANKING_2014] TO SA;
GO



/*
    BANKING - 2014 DATA
*/

-- Which database to use.
USE [master]
GO
 
-- Delete existing database
IF  EXISTS (SELECT name FROM sys.databases WHERE name = N'BANKING_2015')
DROP DATABASE BANKING_2015
GO

-- Get file list
RESTORE FILELISTONLY FROM DISK = 'C:\ZULU\BANKING01-FULL.BAK'
   WITH FILE=1;
GO

-- Restore JAN 2014 to NOV 2015
RESTORE DATABASE BANKING_2015
   FROM DISK = 'C:\ZULU\BANKING01-FULL.BAK'
   WITH NORECOVERY,
   MOVE 'BANKING_PRI_DAT' TO 'C:\MSSQL\DATA\BANKING-2015.MDF', 
   MOVE 'BANKING_ALL_LOG' TO 'C:\MSSQL\LOG\BANKING-2015.LDF'
GO


-- Get file list
RESTORE FILELISTONLY FROM DISK = 'C:\ZULU\BANKING01-LOG.TRN'
   WITH FILE=1;
GO

-- Restore DEC 2015
RESTORE LOG BANKING_2015
   FROM DISK = 'C:\ZULU\BANKING01-LOG.TRN'
   WITH RECOVERY;
GO


-- Remove 2014 transactions
USE [BANKING_2015]
GO

-- Get record count - 2015 transactions
SELECT COUNT(*) AS TOTAL_RECS 
FROM [ACTIVE].[TRANSACTION]
WHERE YEAR(TRAN_DATE) = 2014
GO

-- Remove 2014 transactions
BEGIN TRAN;
DELETE
FROM [ACTIVE].[TRANSACTION]
WHERE YEAR(TRAN_DATE) = 2014;
COMMIT TRAN;
GO

-- Make sure it is owned by [sa]
ALTER AUTHORIZATION ON DATABASE::[BANKING_2015] TO SA;
GO


/*
    SHRINK LOG 4 BANKING 2014 DATA
*/

-- Which database to use.
USE [master]
GO
 
-- Full backup of the database
BACKUP DATABASE [BANKING_2014] 
    TO DISK = 'C:\MSSQL\BACKUP\SHARD2014-FULL.BAK' WITH FORMAT;
GO

-- Transaction log backup of the database
BACKUP LOG [BANKING_2014] 
    TO DISK = 'C:\MSSQL\BACKUP\SHARD2014-LOG.TRN' WITH FORMAT;
GO

-- Look for log pointer
DBCC LOGINFO
GO

-- Try shrinking the log
DBCC SHRINKFILE (BANKING_ALL_LOG, 32)
GO


/*
    Move log pointer
*/

-- Create temp table and move pointer
DECLARE @VAR_CNT INT;
SELECT @VAR_CNT = 1;

CREATE TABLE [dbo].[TMP_LOGPTR] (MYVAL INT);
INSERT INTO [dbo].[TMP_LOGPTR] VALUES (@VAR_CNT);

WHILE (@VAR_CNT  < 5000)
BEGIN
    UPDATE [dbo].[TMP_LOGPTR] SET MYVAL = MYVAL + 1;
    SET @VAR_CNT = @VAR_CNT + 1
END

DROP TABLE [dbo].[TMP_LOGPTR]
GO


/*
    SHRINK LOG 4 BANKING 2015 DATA
*/

-- Which database to use.
USE [master]
GO
 
-- Full backup of the database
BACKUP DATABASE [BANKING_2015] 
    TO DISK = 'C:\MSSQL\BACKUP\SHARD2015-FULL.BAK' WITH FORMAT;
GO

-- Transaction log backup of the database
BACKUP LOG [BANKING_2015] 
    TO DISK = 'C:\MSSQL\BACKUP\SHARD2015-LOG.TRN' WITH FORMAT;
GO

-- Look for log pointer
DBCC LOGINFO
GO

-- Try shrinking the log
DBCC SHRINKFILE (BANKING_ALL_LOG, 32)
GO