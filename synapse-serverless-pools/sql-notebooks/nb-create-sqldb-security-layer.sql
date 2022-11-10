--
-- Name:         nb-create-sqldb-security-layer
--

--
-- Design Phase:
--     Author:   John Miner
--     Date:     09-15-2022
--     Purpose:  Give rights to sql database and single named credential to dilbert.  
--               The sqladminacct already has rights.
--

--
-- SQL Database Notes:  
--         Must use database scoped credential with external data source.
--         The SAS key (defined at container level) must have both read and list privledges.
--         

--
-- 1 - create a login
--

-- Which database?
USE master;
GO

-- Drop login
DROP LOGIN [Dogbert]

-- Create login
CREATE LOGIN [Dogbert] WITH PASSWORD = 'sQer9wEBVGZjQWjd', DEFAULT_DATABASE = mssqltips;
GO

-- Show logins
SELECT * FROM sys.sql_logins
GO


--
-- 2 - create a user
--

-- Which database?
USE mssqltips;
GO

-- What are the principles
SELECT * FROM sys.database_principals where (type='S' or type = 'U')
GO

-- Drop user
DROP USER [Dogbert];
GO

-- Create user
CREATE USER [Dogbert] FROM LOGIN [Dogbert];
GO

-- Give read rights
ALTER ROLE db_datareader ADD MEMBER [Dogbert];
GO


-- Give rights to credential
GRANT CONTROL ON DATABASE SCOPED CREDENTIAL :: [LakeCredential] TO [Dogbert];
GO


--
-- 3 - show user permissions
--

-- Show users
SELECT * FROM sys.database_principals;
GO

-- Execute as user
EXECUTE AS USER = 'Dogbert'
GO

-- Show permissions
SELECT * FROM fn_my_permissions(null, 'database'); 
GO


