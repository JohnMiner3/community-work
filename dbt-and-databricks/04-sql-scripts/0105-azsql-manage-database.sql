/******************************************************
 *
 * Name:         0105-azsql-manage-database.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     09-01-2026
 *     Purpose:   
 *               Drop tables + views + schemas from database
 * 
 ******************************************************/


--
--  Drop user defined tables
--

DECLARE @sql VARCHAR(MAX) = '';
SELECT @sql = @sql + 'DROP TABLE ' + QUOTENAME(SCHEMA_NAME(schema_id)) + '.' + QUOTENAME(t.name) + ';' + CHAR(13) + CHAR(10)
FROM sys.tables t where t.is_ms_shipped = 0;
EXEC(@sql);
GO


--
--  Drop user defined views
--

DECLARE @sql VARCHAR(MAX) = '';
SELECT @sql = @sql + 'DROP VIEW ' + QUOTENAME(SCHEMA_NAME(schema_id)) + '.' + QUOTENAME(v.name) + ';' + CHAR(13) + CHAR(10)
FROM sys.views v where v.is_ms_shipped = 0;
EXEC(@sql);
GO


--
--  Drop user defined schemas
--

DECLARE @sql VARCHAR(MAX) = '';
SELECT @sql = @sql + 'DROP SCHEMA ' + QUOTENAME(s.name) + ';' + CHAR(13) + CHAR(10)
FROM sys.schemas AS s 
WHERE (s.schema_id >= 5 and s.schema_id < 16384) and s.name not in ('queryinsights')
EXEC(@sql);
GO

