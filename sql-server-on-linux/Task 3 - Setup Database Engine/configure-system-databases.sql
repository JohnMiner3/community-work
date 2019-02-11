/******************************************************
 *
 * Name:         configure-system-databases.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     08-01-2014
 *     Purpose:  Make changes to system databases. 
 * 
 ******************************************************/

-- Master catalog
use master;



/*
    [TEMPDB]
*/

-- Modify data file
ALTER DATABASE [TEMPDB] 
MODIFY FILE
(NAME = 'tempdev', SIZE = 1024MB, MAXSIZE = UNLIMITED , FILEGROWTH = 128MB)
GO


-- Modify log file 
ALTER DATABASE [TEMPDB] 
MODIFY FILE
(NAME = 'templog', SIZE = 256MB, MAXSIZE = UNLIMITED , FILEGROWTH = 128MB)
GO


-- Add data file #2
ALTER DATABASE [TEMPDB] 
ADD FILE
(
    NAME= 'tempdev2',
    FILENAME = '/var/opt/mssql/data/tempdev2.NDF',
    SIZE = 1024MB , MAXSIZE = UNLIMITED , FILEGROWTH = 128MB
)
GO


-- Add data file #3
ALTER DATABASE [TEMPDB] 
ADD FILE
(
    NAME= 'tempdev3',
    FILENAME = '/var/opt/mssql/data/tempdev3.NDF',
    SIZE = 1024MB , MAXSIZE = UNLIMITED , FILEGROWTH = 128MB
)
GO


-- Add data file #4
ALTER DATABASE [TEMPDB] 
ADD FILE
(
    NAME= 'tempdev4',
    FILENAME = '/var/opt/mssql/data/tempdev4.NDF',
    SIZE = 1024MB , MAXSIZE = UNLIMITED , FILEGROWTH = 128MB
)
GO


/*
    [MODEL]
*/


-- Modify data file
ALTER DATABASE [MODEL] 
MODIFY FILE
(NAME = 'modeldev', SIZE = 128MB, MAXSIZE = UNLIMITED , FILEGROWTH = 32MB)
GO


-- Modify log file 
ALTER DATABASE [MODEL] 
MODIFY FILE
(NAME = 'modellog', SIZE = 32MB, MAXSIZE = UNLIMITED , FILEGROWTH = 32MB)
GO

-- Change recovery model to full
ALTER DATABASE [MODEL] SET RECOVERY FULL;
GO


/*
    [MSDB]
*/


-- Modify data file
ALTER DATABASE [MSDB] 
MODIFY FILE
(NAME = 'msdbdata', SIZE = 128MB, MAXSIZE = UNLIMITED , FILEGROWTH = 32MB)
GO


-- Modify log file 
ALTER DATABASE [MSDB] 
MODIFY FILE
(NAME = 'msdblog', SIZE = 32MB, MAXSIZE = UNLIMITED , FILEGROWTH = 32MB)
GO

-- Change recovery model to full
ALTER DATABASE [MSDB] SET RECOVERY FULL;
GO


/*
    [MASTER]
*/


-- Modify data file
ALTER DATABASE [MASTER] 
MODIFY FILE
(NAME = 'master', SIZE = 128MB, MAXSIZE = UNLIMITED , FILEGROWTH = 32MB)
GO


-- Modify log file 
ALTER DATABASE [MASTER] 
MODIFY FILE
(NAME = 'mastlog', SIZE = 32MB, MAXSIZE = UNLIMITED , FILEGROWTH = 32MB)
GO

-- Change recovery model to simple
ALTER DATABASE [MASTER] SET RECOVERY SIMPLE;
GO