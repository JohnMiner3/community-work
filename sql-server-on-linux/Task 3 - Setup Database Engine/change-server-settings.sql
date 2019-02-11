/******************************************************
 *
 * Name:         change-server-settings.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Blog:     www.craftydba.com
 *
 *     Version:  1.1
 *     Date:     08-01-2014
 *     Purpose:  Each setting has a optimal value for 
 *               a particular server and workload.
 *
 ******************************************************/



-- Select the correct database
USE [msdb]
GO


-- To allow advanced options to be changed.
EXEC sp_configure 'show advanced options', 1;
GO

RECONFIGURE WITH OVERRIDE;
GO


-- Set min memory to 1 GB
EXEC sp_configure 'min server memory (MB)', 1024;
GO

RECONFIGURE WITH OVERRIDE;
GO


-- Set max memory to 4 GB
EXEC sp_configure 'max server memory (MB)', 4096;
GO

RECONFIGURE WITH OVERRIDE;
GO


-- User connections 0 - 32K, but each connection has overhead
EXEC sp_configure 'user connections', 1024;
GO

RECONFIGURE WITH OVERRIDE;
GO


-- The amount of time before a remote query times out (5 minutes)
EXEC sp_configure 'remote query timeout', 300;
GO

RECONFIGURE WITH OVERRIDE;
GO


-- Keep backups around for 30 days
EXEC sp_configure 'media retention', 30;
GO

RECONFIGURE WITH OVERRIDE;
GO


-- Use compressed backups (>= 2008)
EXEC sp_configure 'backup compression default', 1;
GO

RECONFIGURE WITH OVERRIDE;
GO


-- Reduces query plan cache for single use stmts
EXEC sp_configure 'optimize for ad hoc workloads', 1;
GO

RECONFIGURE WITH OVERRIDE;
GO



-- Cost threshold (n seconds) 
EXEC sp_configure 'cost threshold for parallelism', 60;
GO

-- Force them to take effect
RECONFIGURE;
GO


-- Max Dop (run as single plan for sharepoint) 
EXEC sp_configure 'max degree of parallelism', 1;
GO

-- Force them to take effect
RECONFIGURE;
GO


/*
    Optional settings
*/

-- Allow openrowset and opendatasource to connect to remote ole db

/*
EXEC sp_configure 'Ad Hoc Distributed Queries', 1;
GO

RECONFIGURE WITH OVERRIDE;
GO
*/


-- Some that depends on MTU (max transmission unit)

/*
EXEC sp_configure 'network packet size (B)', 4096;
GO

RECONFIGURE WITH OVERRIDE;
GO
*/

-- Adjust for SSIS installations, depends on NETWORK diagram
-- http://www.sqlsoldier.com/wp/sqlserver/networkpacketsizetofiddlewithornottofiddlewith
