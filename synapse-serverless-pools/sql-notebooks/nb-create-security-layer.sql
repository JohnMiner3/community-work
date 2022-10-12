--
-- Name:         nb-create-security-layer
--

--
-- Design Phase:
--     Author:   John Miner
--     Date:     09-15-2022
--     Purpose:  First try at serverless SQL pools
--

-- Notes:  Must use database scoped credential with external data source.
--         The SAS key must have both read and list privledges.


--
-- 1 - create a login
--

-- Which database?
USE master;
GO

-- Drop login
-- DROP LOGIN [Dogbert]

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

SELECT * FROM sys.database_principals where (type='S' or type = 'U')


-- Drop user
--DROP USER [Dogbert];
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





