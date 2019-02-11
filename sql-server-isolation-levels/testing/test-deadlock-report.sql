/******************************************************
 *
 * Name:         test-deadlock-report.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Blog:     www.craftydba.com
 *
 *     Version:  1.1
 *     Date:     11-07-2013
 *     Purpose:  Create a deadlock with two threads.
 * 
 ******************************************************/

-- Select the database
USE [AdventureWorks2012]
GO


--
-- Show the record of interest
--

-- Data set 1
SELECT * 
FROM [AdventureWorks2012].[Person].[EmailAddress]
WHERE BusinessEntityID = 7;
GO

-- Data set 2
SELECT * 
FROM [AdventureWorks2012].[Person].[Person]
WHERE BusinessEntityID = 7;
GO


--
-- Update Email then Person
--

-- Remove existing
IF  OBJECT_ID(N'dbo.usp_Deadlock_Test_Thread1', 'P') IS NOT NULL
DROP PROCEDURE dbo.usp_Deadlock_Test_Thread1
GO
 
-- Create new
CREATE PROCEDURE dbo.usp_Deadlock_Test_Thread1
AS
BEGIN

    -- Start a transaction
    BEGIN TRAN

	    -- First resource
	    UPDATE [AdventureWorks2012].[Person].[EmailAddress]
		SET EmailAddress = 'dylan-007@adventure-works.com'
		WHERE BusinessEntityID = 7;

		-- Wait for 2 minutes
        WAITFOR DELAY '00:02:00';

	    -- Second resource
	    UPDATE [AdventureWorks2012].[Person].[Person]
		SET Title = 'Mr.'
		WHERE BusinessEntityID = 7;

		-- Never commit
		IF (1 = 0) COMMIT TRAN;

	-- Roll back the transaction
	ROLLBACK TRAN;
	
END
GO


--
-- Update Person then Email
--

-- Remove existing
IF  OBJECT_ID(N'dbo.usp_Deadlock_Test_Thread2', 'P') IS NOT NULL
DROP PROCEDURE dbo.usp_Deadlock_Test_Thread2
GO
 
-- Create new
CREATE PROCEDURE dbo.usp_Deadlock_Test_Thread2
AS
BEGIN

    -- Start a transaction
    BEGIN TRAN

	    -- Second resource
	    UPDATE [AdventureWorks2012].[Person].[Person]
		SET Title = 'Mr.'
		WHERE BusinessEntityID = 7;

		-- Wait for 2 minutes
        WAITFOR DELAY '00:02:00';

	    -- First resource
	    UPDATE [AdventureWorks2012].[Person].[EmailAddress]
		SET EmailAddress = 'dylan-007@adventure-works.com'
		WHERE BusinessEntityID = 7;

		-- Never commit
		IF (1 = 0) COMMIT TRAN;

	-- Roll back the transaction
	ROLLBACK TRAN;
	
END
GO


--
-- Blocking then deadlock
--

/*

-- Window 1 (spid x)
EXEC usp_Deadlock_Test_Thread1;
GO

-- Window 2 (spid y)
EXEC usp_Deadlock_Test_Thread2;
GO

*/