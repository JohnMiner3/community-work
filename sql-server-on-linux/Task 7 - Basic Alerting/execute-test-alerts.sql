/******************************************************
 *
 * Name:         usp_test_alerts
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Blog:     www.craftydba.com
 *
 *     Version:  1.1
 *     Date:     12-17-2012
 *     Purpose:  Create a stored procedure to test
 *               alerts by error number.
 * 
 ******************************************************/

/*  
    Which database to use.
*/

USE msdb
GO


/*  
    Create the stored procedure
*/

-- Drop existing stored procedure
IF OBJECT_ID(N'[dbo].[usp_test_alerts]') > 0
DROP PROCEDURE [dbo].usp_test_alerts
GO

-- Add new stored procedure
CREATE PROC dbo.usp_test_alerts @VAR_SEVERITY INT = 0
AS
BEGIN
    DECLARE @VAR_MSG VARCHAR(125);    
    SELECT @VAR_MSG = 'Testing alerting on ' + @@SERVERNAME + 
        ' severity ' + STR(@VAR_SEVERITY, 2, 0) + '.';
    RAISERROR (@VAR_MSG, @VAR_SEVERITY, 1) WITH LOG;
END
GO


/*  
    Call the stored procedure
*/

/*

-- Loop from 11 to 25
DECLARE @VAR_ACNT INT;
SET @VAR_ACNT = 11;

-- Add severe alerts to system
WHILE (@VAR_ACNT < 25)
BEGIN
    EXEC msdb.dbo.usp_test_alerts @VAR_SEVERITY = @VAR_ACNT;
    PRINT 'Test Alert # ' + str(@VAR_ACNT, 2, 0);
    SET @VAR_ACNT = @VAR_ACNT + 1;
END

*/

EXEC msdb.dbo.usp_test_alerts @VAR_SEVERITY = 16;