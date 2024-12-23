/******************************************************
 *
 * Name:         export-customers-bcp.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     03-25-2015
 *     Purpose:  Dump some customers from the sample
 *               Big John's BBQ database.
 * 
 ******************************************************/
 
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

-- BCP - Export whole table, csv format, trusted security, character format
DECLARE @bcp_cmd1 VARCHAR(1000);
DECLARE @exe_path1 VARCHAR(200) = 
    ' cd C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\110\Tools\Binn & ';
SET @bcp_cmd1 = @exe_path1 + 
    ' BCP.EXE BIG_JONS_BBQ_DW.FACT.VW_CUSTOMERS out ' +
    ' C:\TEMP\CUSTOMERS.TXT -T -c -t \t -r \n ';
PRINT @bcp_cmd1;
EXEC master..xp_cmdshell @bcp_cmd1;
GO