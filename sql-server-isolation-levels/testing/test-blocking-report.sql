/******************************************************
 *
 * Name:         test-blocking-report.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Blog:     www.craftydba.com
 *
 *     Version:  1.1
 *     Date:     11-07-2013
 *     Purpose:  Create blocking with two threads.
 * 
 ******************************************************/

-- Select the database
USE [AdventureWorks2012]
GO


--
-- Show the record of interest
--

-- Data set 3
SELECT * 
FROM [AdventureWorks2012].[Person].[Person]
WHERE BusinessEntityID in (21, 7);
GO


--
-- Update the Person
--

-- Remove existing
IF  OBJECT_ID(N'dbo.usp_Blocking_Test_Thread1', 'P') IS NOT NULL
DROP PROCEDURE dbo.usp_Blocking_Test_Thread1
GO
 
-- Create new
CREATE PROCEDURE dbo.usp_Blocking_Test_Thread1 (@BusinessEntityId Int)
AS
BEGIN

    -- Start a transaction
    BEGIN TRAN

	    -- Third resource
	    UPDATE P
            SET Title = 'Ms.'
            FROM [AdventureWorks2012].[Person].[Person] AS P 
            WHERE BusinessEntityID = @BusinessEntityId;

	    -- Wait for 2 minutes
            WAITFOR DELAY '00:02:00';

 	    -- Never commit
	    IF (1 = 0) COMMIT TRAN;

	    -- Roll back the transaction
	    ROLLBACK TRAN;
	
END
GO


--
-- Select the Person 
--

-- Remove existing
IF OBJECT_ID(N'dbo.usp_Blocking_Test_Thread2', 'P') IS NOT NULL
DROP PROCEDURE dbo.usp_Blocking_Test_Thread2
GO
 
-- Create new
CREATE PROCEDURE dbo.usp_Blocking_Test_Thread2 (@BusinessEntityId Int)
AS
BEGIN

    -- Start a transaction
    BEGIN TRAN

        -- Third resource
	SELECT * FROM [AdventureWorks2012].[Person].[Person] 
        WHERE BusinessEntityID = @BusinessEntityId;

    -- Commit the transaction
    COMMIT TRAN;
	
END
GO


--
-- A example of blocking that finishes 
--

/*

-- Window 1 (spid x)
EXEC usp_Blocking_Test_Thread1 21;
GO

-- Window 2 (spid y)
EXEC usp_Blocking_Test_Thread2 21;
GO

*/