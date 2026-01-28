
--
--  Make library database (master)
--

CREATE DATABASE [library] ( EDITION = 'GeneralPurpose', SERVICE_OBJECTIVE = 'GP_S_Gen5_4' ) ;
GO

-- 
--  Make server login (master)
--

CREATE LOGIN [svcacct02] WITH PASSWORD = 'QkbGZrBSR37u' ;


--
--  Create user for login (library)
--

CREATE USER [svcacct02] FOR LOGIN svcacct02;
GO


--
--  Give user owner access (library)
--

ALTER ROLE db_owner ADD MEMBER svcacct02;
GO

