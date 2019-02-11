/******************************************************
 *
 * Name:         enable-dedicated-admin-connection.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Blog:     www.craftydba.com
 *
 *     Version:  1.1
 *     Date:     12-17-2012
 *     Purpose:  Add operators and alerts.
 *
 ******************************************************/

-- http://www.mssqltips.com/sqlservertip/1801/enable-sql-server-2008-dedicated-administrator-connection/


-- Select the correct database
USE [master]
GO

-- To allow advanced options to be changed.
EXEC sp_configure 'show advanced options', 1;
GO

RECONFIGURE WITH OVERRIDE;
GO

-- 0 = Allow Local Connection, 1 = Allow Remote Connections
sp_configure 'remote admin connections', 1 
GO

RECONFIGURE WITH OVERRIDE;
GO