/******************************************************
 *
 * Name:         example-2b.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     11-07-2017
 *     Purpose:  Create a duplicate of
 *               hospital database one.
 * 
 ******************************************************/

--
-- Take database offline
--

-- Which database to use.
USE [master]
GO
 
-- Kick off users, roll back current work
ALTER DATABASE [HOSPITAL1] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
GO

-- Set the database to off-line
ALTER DATABASE [HOSPITAL1] SET OFFLINE
GO


--
-- Enable the cmd shell
--

-- To allow advanced options to be changed.
EXEC sp_configure 'show advanced options', 1
GO
 
-- To update the currently configured value for advanced options.
RECONFIGURE
GO
 
-- To enable the feature.
EXEC sp_configure 'xp_cmdshell', 1
GO
 
-- To update the currently configured value for this feature.
RECONFIGURE
GO


--
-- Duplicate data & log
--

-- duplicate data file 
EXEC xp_cmdshell 'COPY C:\MSSQL\DATA\HOSPITAL1.mdf C:\MSSQL\DATA\HOSPITAL2.mdf';
GO
 
-- duplicate log file 
EXEC xp_cmdshell 'COPY C:\MSSQL\LOG\HOSPITAL1.ldf C:\MSSQL\LOG\HOSPITAL2.ldf';
GO


--
-- Bring database online
--

-- Bring database on-line
ALTER DATABASE [HOSPITAL1] SET ONLINE
GO

-- Add users
ALTER DATABASE [HOSPITAL1] SET MULTI_USER
GO


--
-- Attach using create database
--

-- Option 1 Attach & create a new database using existing files
CREATE DATABASE [HOSPITAL2] ON
(FILENAME = 'C:\MSSQL\DATA\HOSPITAL2.mdf'),
(FILENAME = 'C:\MSSQL\LOG\HOSPITAL2.ldf')
FOR ATTACH ;

-- Option 2 - Attach & create a new database using just data file
CREATE DATABASE [HOSPITAL2] ON
(FILENAME = 'C:\MSSQL\DATA\HOSPITAL2.mdf')
FOR ATTACH_REBUILD_LOG;

-- Option 3 - Attach database
EXEC sp_attach_db @dbname = N'HOSPITAL2',
    @filename1 = N'C:\MSSQL\DATA\HOSPITAL2.mdf',
    @filename2 = N'C:\MSSQL\LOG\HOSPITAL2.ldf';


--
-- Disable the cmd shell
--

-- To allow advanced options to be changed.
EXEC sp_configure 'show advanced options', 1
GO
 
-- To update the currently configured value for advanced options.
RECONFIGURE
GO
 
-- To enable the feature.
EXEC sp_configure 'xp_cmdshell', 0
GO
 
-- To update the currently configured value for this feature.
RECONFIGURE
GO