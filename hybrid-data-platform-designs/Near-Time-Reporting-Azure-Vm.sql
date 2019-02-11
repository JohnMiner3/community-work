/******************************************************
 *
 * Name:         near-time-reporting-azure-vm.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     02-08-2016
 *     Purpose:  Use warm standby for near real-time reporting.
 * 
 ******************************************************/

 -- Which database to use.
USE [master]
GO
 
-- Delete existing database
IF  EXISTS (SELECT name FROM sys.databases WHERE name = N'BANKING01')
DROP DATABASE BANKING01
GO

-- Get file list
RESTORE FILELISTONLY FROM DISK = 'C:\ZULU\banking01-initial.BAK'
   WITH FILE=1;
GO

-- Validate backup
RESTORE VERIFYONLY FROM DISK = 'C:\ZULU\banking01-initial.BAK'
   WITH FILE=1;
GO

-- Restore JAN 2014 to NOV 2015
RESTORE DATABASE BANKING01
   FROM DISK = 'C:\ZULU\banking01-initial.BAK'
   WITH STANDBY = 'C:\MSSQL\LOG\undo-list.LDF'
GO

-- Get file list
RESTORE FILELISTONLY FROM DISK = 'C:\ZULU\BANKING01-LOG.TRN'
   WITH FILE=1;
GO

-- Restore DEC 2015
RESTORE LOG BANKING01
   FROM DISK = 'C:\ZULU\BANKING01-LOG.TRN'
   WITH STANDBY = 'C:\MSSQL\LOG\undo-list.LDF'
GO