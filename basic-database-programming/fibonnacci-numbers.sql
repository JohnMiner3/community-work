/******************************************************
 *
 * Name:         fibonnacci-numbers.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     06-20-2014
 *     Purpose:  Demonstrate error handling.
 * 
 ******************************************************/

-- 
-- Snippet 0 - add table to hold fib(n)
--

-- Use the correct database
USE MATH
GO

-- Remove the existing table
IF OBJECT_ID('[MATH].[dbo].[TBL_FIBONACCI]') IS NOT NULL
    DROP TABLE [MATH].[dbo].[TBL_FIBONACCI];

-- Create a new table
CREATE TABLE [TBL_FIBONACCI]
(
    [MY_VALUE] [bigint] NOT NULL,
    [MY_NUMBER] [bigint] NOT NULL,
    [MY_TIME] [datetime] NOT NULL,
    CONSTRAINT [PK_TBL_FIBONACCI] PRIMARY KEY CLUSTERED 
    ([MY_NUMBER] ASC)
);
GO


-- Use the correct database
USE MATH
GO

-- Show the data
SELECT * FROM [TBL_FIBONACCI]
GO


-- Ignore arithmetic warnings
/*
SET ARITHABORT OFF;
SET ARITHIGNORE ON;
SET ANSI_WARNINGS OFF;
*/

-- Allow arithmetic warnings
SET ARITHABORT ON;
SET ARITHIGNORE OFF;
SET ANSI_WARNINGS ON;


-- 
-- Snippet 1 - IF @@ERROR construct
--

-- Clear the table
TRUNCATE TABLE [TBL_FIBONACCI]
GO

-- Declare variables
DECLARE @FN1 BIGINT = 0;
DECLARE @FN2 BIGINT = 1;
DECLARE @FN3 BIGINT = 0;

DECLARE @ERR INT = 0;
DECLARE @CNT INT = 1;

-- Insert first two values
INSERT INTO [TBL_FIBONACCI] VALUES 
    (@FN1, 1, GETDATE()),
    (@FN2, 2, GETDATE());

-- Simple error handling
SET @ERR = @@ERROR;
IF (@ERR <> 0)
    PRINT 'CALCULATION:  DID NOT EXPECT THAT FOR THE FIRST TWO FIBS!';

-- Calculate the first 100 fibonacci numbers
WHILE (@CNT <= 100 AND @ERR = 0)
BEGIN
    -- Calculate next value
    SET @FN3 = @FN2 + @FN1;

    -- Simple error handling
    SET @ERR = @@ERROR;
    IF (@ERR <> 0)
    BEGIN
        PRINT 'CALCULATION:  DID NOT EXPECT THAT FOR FIB(' + LTRIM(STR(@CNT+2)) + ')';
	BREAK;
    END;

    -- Insert Fn two values
    INSERT INTO [TBL_FIBONACCI] VALUES 
    (@FN3, @CNT + 2, GETDATE());

    -- Simple error handling
    SET @ERR = @@ERROR;
    IF (@ERR <> 0)
    BEGIN
        PRINT 'INSERT RECORD:  DID NOT EXPECT THAT FOR FIB(' + LTRIM(STR(@CNT+2)) + ')';
	BREAK;
    END;

    -- Move to next number in sequence
    SET @FN1 = @FN2;
    SET @FN2 = @FN3;

    -- Increment the counter
    SET @CNT += 1;

END;
GO



--
-- Snippet 2 - Raise a custom error.
--

-- Format to 5 characters, take first 4 from source
DECLARE @VAR_STR NVARCHAR(50) = N'<<%5.4s>>';
RAISERROR (@VAR_STR, 17, 1, N'12.34565789');
GO



-- 
-- Snippet 3 - TRY / CATCH example
--

-- Clear the table
TRUNCATE TABLE [TBL_FIBONACCI];
GO

-- Start code block
BEGIN TRY

  -- Declare variables
  DECLARE @FN1 BIGINT = 0;
  DECLARE @FN2 BIGINT = 1;
  DECLARE @FN3 BIGINT = 0;

  DECLARE @ERR INT = 0;
  DECLARE @CNT INT = 1;

  -- Insert first two values
  INSERT INTO [TBL_FIBONACCI] VALUES 
    (@FN1, 1, GETDATE()),
    (@FN2, 2, GETDATE());

  -- Calculate the first 100 fibonacci numbers
  WHILE (@CNT <= 100 AND @ERR = 0)
  BEGIN
  
    -- Calculate next value
    SET @FN3 = @FN2 + @FN1;

    -- Insert Fn two values
    INSERT INTO [TBL_FIBONACCI] VALUES 
    (@FN3, @CNT + 2, GETDATE());

    -- Move to next number in sequence
    SET @FN1 = @FN2;
    SET @FN2 = @FN3;

    -- Increment the counter
    SET @CNT += 1;

  END

END TRY

-- Error Handler
BEGIN CATCH
    PRINT 'CALCULATION:  DID NOT EXPECT THAT FOR FIB(' + LTRIM(STR(@CNT+2)) + ')';
	PRINT ERROR_NUMBER();
    PRINT ERROR_MESSAGE();
END CATCH
GO



--
-- Snippet 4 - Raise a custom error.
--

THROW 50000, N'12.34565789', 12
GO
