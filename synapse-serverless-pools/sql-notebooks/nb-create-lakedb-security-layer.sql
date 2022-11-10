--
-- Name:         nb-create-lakedb-security-layer
--

--
-- Design Phase:
--     Author:   John Miner
--     Date:     09-15-2022
--     Purpose:  Give rights to lake database to dilbert.  
--               The sqladminacct already has rights.
--               Create server credential using file path for each table.
--
     


--
-- 1 - give rights to dilbert -- lake database
--

-- Which database?
USE saleslt;
GO

-- Create user
CREATE USER [Dogbert] FROM LOGIN [Dogbert];
GO

-- Give read rights
ALTER ROLE db_datareader ADD MEMBER [Dogbert];
GO


--
-- 2 - show user permissions
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


--
-- 3 - load control table
--

-- need sql database
use mssqltips;
GO

-- seperate from data
CREATE SCHEMA [control_card]
GO

-- Drop if required
DROP EXTERNAL FILE FORMAT [CsvFile] 
GO

-- Format for CSV files
CREATE EXTERNAL FILE FORMAT [CsvFile] 
WITH 
( 
	FORMAT_TYPE = DELIMITEDTEXT ,
	FORMAT_OPTIONS 
	(
		FIELD_TERMINATOR = ',',
		USE_TYPE_DEFAULT = FALSE,
        FIRST_ROW = 2
	)
)
GO


-- Drop if required
DROP EXTERNAL TABLE [control_card].[access_control]
GO 

-- Create table 
CREATE EXTERNAL TABLE [control_card].[access_control]
(
	[TableName] [nvarchar](128),
	[FileName] [nvarchar](128)
)
WITH 
(
	LOCATION = 'synapse/access-control-file.csv',
	DATA_SOURCE = [LakeDataSource],
    FILE_FORMAT = [CsvFile]
)
GO

-- Show data
SELECT * FROM [control_card].[access_control];
GO


--
-- 5 - Create view to execute commands
--

-- drop existing
DROP VIEW IF EXISTS [control_card].[sqlstmts_2_exec];
GO

-- create new
CREATE VIEW [control_card].[sqlstmts_2_exec]
AS
SELECT 

-- Artifical row number
ROW_NUMBER() OVER(ORDER BY TableName ASC) AS rid,

-- Fields from file
  *,

-- Drop Credential
' 
  USE master;
  DECLARE @ID INT;
  SELECT @ID = credential_id 
  FROM sys.credentials 
  WHERE name = ''https://sa4adls2030.dfs.core.windows.net/sc4adls2030/synapse/parquet-files/' + FileName + '/*.parquet'';
  IF (@ID IS NOT NULL)
    DROP CREDENTIAL [https://sa4adls2030.dfs.core.windows.net/sc4adls2030/synapse/parquet-files/' + FileName + '/*.parquet];
' AS drop_credential,

-- Create credential
' 
  USE master;
  CREATE CREDENTIAL [https://sa4adls2030.dfs.core.windows.net/sc4adls2030/synapse/parquet-files/' + FileName + '/*.parquet]
  WITH IDENTITY=''SHARED ACCESS SIGNATURE'', 
  SECRET = ''sp=racwdlmeop&st=2022-11-10T01:22:13Z&se=2022-12-31T09:22:13Z&sv=2021-06-08&sr=c&sig=IbIGDyvIQqXnjLv%2FfVyvdkpQH1C09ZDPfKWrmGY7%2BHI%3D'';
' AS create_credential,

-- give rights to credential
'
  USE master;
  GRANT CONTROL ON CREDENTIAL :: [https://sa4adls2030.dfs.core.windows.net/sc4adls2030/synapse/parquet-files/' + FileName + '/*.parquet] 
  TO [Dogbert];
' AS grant_access

-- External table
FROM [control_card].[access_control];
GO

-- Show data
SELECT * FROM [control_card].[sqlstmts_2_exec];
GO


--
-- 6 - Use while loop to exec cmds
--

-- count statements
DECLARE @N INT = (SELECT COUNT(*) FROM [control_card].[sqlstmts_2_exec]);

-- set counter
DECLARE @I INT = 1;

-- while there is work ...
WHILE @I <= @N
BEGIN

    -- drop credential
    DECLARE @sql_code1 NVARCHAR(4000) = (SELECT drop_credential FROM [control_card].[sqlstmts_2_exec] WHERE rid = @I);
    EXEC sp_executesql @sql_code1;

    -- create credential
    DECLARE @sql_code2 NVARCHAR(4000) = (SELECT create_credential FROM [control_card].[sqlstmts_2_exec] WHERE rid = @I);
    EXEC sp_executesql @sql_code2;

    -- grant access
    DECLARE @sql_code3 NVARCHAR(4000) = (SELECT grant_access FROM [control_card].[sqlstmts_2_exec] WHERE rid = @I);
    EXEC sp_executesql @sql_code3;

    -- increment counter
    SET @i +=1;
END;
GO
