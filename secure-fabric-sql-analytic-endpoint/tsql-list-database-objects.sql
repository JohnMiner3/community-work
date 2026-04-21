/******************************************************
 *
 * Name:         03-tsql-list-database-objects
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     01-01-2026
 *     Purpose:  show schema, name, object type and row count.
 * 
 ******************************************************/

-- Declare variable
DECLARE @SqlStatement NVARCHAR(MAX) = '';

-- Recursively create stmt
SELECT @SqlStatement = @SqlStatement + 
    'SELECT ''' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) + ''' AS [TableName], 
    COUNT(*) AS [RowCount] FROM ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) + ' UNION ALL '
FROM sys.tables AS t
INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
WHERE t.is_ms_shipped = 0;

-- Remove the last ' UNION ALL ', add order bny
SET @SqlStatement = LEFT(@SqlStatement, LEN(@SqlStatement) - 10) + ' ORDER BY 1';

-- Debug sql stmt?
-- PRINT  @SqlStatement;

-- Run sql stmt
EXEC sp_executesql @SqlStatement;

