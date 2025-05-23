/******************************************************
 *
 * Name:         grab-customers-from-bbq-dw.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     02-07-2016
 *     Purpose:  Get customers from datawarehouse.
 * 
 ******************************************************/


-- BCP - Export query, pipe delimited format, trusted security, character format
DECLARE @bcp_cmd4 VARCHAR(1000);
DECLARE @exe_path4 VARCHAR(200) = ' cd C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\110\Tools\Binn & ';
SET @bcp_cmd4 =  @exe_path4 + ' BCP.EXE ' 
+ '"SELECT TOP 25000 CUS_ID, UPPER(CUS_LNAME) AS CUST_LNAME, UPPER(CUS_FNAME) AS CUST_FNAME,'
+ ' CUS_PHONE, UPPER(CUS_ADDRESS) AS CUST_ADDRESS, UPPER(CUS_CITY) AS CUST_CITY, UPPER(CUS_STATE) AS CUST_STATE,' 
+ ' UPPER(CUS_ZIP) AS CUST_ZIP FROM [BIG_JONS_BBQ_DW].[FACT].[CUSTOMERS] " queryout ' 
+ ' "C:\HSS\CUSTOMERS.TXT" -T -c -q -t0x7c -r\n';
PRINT @bcp_cmd4;
EXEC master..xp_cmdshell @bcp_cmd4;
GO




