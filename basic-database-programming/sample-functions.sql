/******************************************************
 *
 * Name:         sample-functions.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     06-12-2012
 *     Purpose:  Demonstrate how to create, alter and 
 *               drop a function.
 * 
 ******************************************************/


--
-- Scalar valued function - http://craftydba.com/?p=3716
--

-- Use the correct database
USE [AdventureWorks2012]
GO

-- Delete existing scalar valued function
IF  EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dbo].[ufnGetStock]') AND [type] in (N'FN')
	)
DROP FUNCTION [dbo].[ufnGetStock]
GO


-- Create stub scalar valued function
CREATE FUNCTION [dbo].[ufnGetStock] (@ProductID [int])
RETURNS [int] 
AS 
BEGIN
    RETURN 1;
END
GO

-- Alter sclar valued function
ALTER FUNCTION [dbo].[ufnGetStock] (@ProductID [int])
RETURNS [int] 
AS 
BEGIN
    -- Declare local variable
    DECLARE @ret int;
    
    -- Only look at inventory in the misc storage
    SELECT @ret = SUM(p.[Quantity]) 
    FROM [Production].[ProductInventory] p 
    WHERE 
        p.[ProductID] = @ProductID 
        AND p.[LocationID] = '6'; 
    
	-- Return the result
    IF (@ret IS NULL) SET @ret = 0;
    RETURN @ret;

END
GO


-- Example use of the function
SELECT 
    ProductId, 
    [name] as ProductName,
    [dbo].[ufnGetStock](ProductId) as StockLevel
FROM 
    [Production].[Product]
WHERE
    [name] like '%seat%';



--
-- In-line table valued functions - http://craftydba.com/?p=3733
--

-- Use the correct database
USE [AdventureWorks2012]
GO

-- Delete existing inline tvf
IF  EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dbo].[ufnCustomersInRegion]') AND [type] in (N'IF')
	)
DROP FUNCTION [dbo].[ufnCustomersInRegion]
GO


-- Create stub inline tvf
CREATE FUNCTION [dbo].[ufnCustomersInRegion] (@Region varchar(50))
RETURNS TABLE
AS
RETURN 
  ( SELECT 'Stub Store' as Store, 'Stub City' as City  );
GO


-- Alter inline tvf
ALTER FUNCTION [dbo].[ufnCustomersInRegion] (@Region varchar(50))
RETURNS TABLE
AS
RETURN 
(
    SELECT DISTINCT 
        s.Name AS 'Store', 
	    a.City AS 'City'
    FROM 
	    Sales.Store AS s
        INNER JOIN Person.BusinessEntityAddress AS bea 
            ON bea.BusinessEntityID = s.BusinessEntityID 
        INNER JOIN Person.Address AS a 
            ON a.AddressID = bea.AddressID
        INNER JOIN Person.StateProvince AS sp 
            ON sp.StateProvinceID = a.StateProvinceID
    WHERE sp.Name = @Region
);
GO


-- Example use of the function
SELECT *
FROM [dbo].[ufnCustomersInRegion] ('Washington')
ORDER BY City;
GO


--
-- Multi table valued functions - http://craftydba.com/?p=3754
--

-- Use the correct database
USE [AdventureWorks2012]

-- Delete existing table value function
IF  EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dbo].[ufnColor2RgbValues]') AND [type] in (N'TF')
	)
DROP FUNCTION [dbo].[ufnColor2RgbValues]
GO

-- Create new table value function
CREATE FUNCTION [dbo].[ufnColor2RgbValues] (@Name nvarchar(15))
RETURNS @RgbValues TABLE 
(
    [Name] nvarchar(15) NOT NULL, 
    [Red] int NULL, 
    [Green] int NULL, 
    [Blue] int NULL,
    [HexCode] varbinary(3)
)
AS 
BEGIN

    -- Local variable
	DECLARE @Var_Name nvarchar(15);
	SET @Var_Name = LOWER(@Name);

	-- Convert name to RGB values
	IF @Var_Name = 'black'  
	    INSERT INTO @RgbValues VALUES (@Var_Name, 0, 0, 0, 0x000000);
	ELSE IF @Var_Name = 'blue'  
 	    INSERT INTO @RgbValues VALUES (@Var_Name, 0, 0, 255, 0x0000FF);
	ELSE IF @Var_Name = 'grey'  
	    INSERT INTO @RgbValues VALUES (@Var_Name, 128, 128, 128, 0x808080);
	ELSE IF @Var_Name = 'red'  
	    INSERT INTO @RgbValues VALUES (@Var_Name, 255, 0, 0, 0xFF0000);
	ELSE IF @Var_Name = 'silver'  
	    INSERT INTO @RgbValues VALUES (@Var_Name, 224, 224, 224, 0xE0E0E0);
	ELSE IF @Var_Name = 'white'  
	    INSERT INTO @RgbValues VALUES (@Var_Name, 255, 255, 255, 0xFFFFFF);
	ELSE IF @Var_Name = 'yellow'  
	    INSERT INTO @RgbValues VALUES (@Var_Name, 255, 255, 0, 0xFFFF00);
	ELSE IF @Var_Name = 'silver/black'  
	    INSERT INTO @RgbValues VALUES (@Var_Name, 64, 64, 64, 0x404040);
	ELSE IF @Var_Name = 'multi'  
	    INSERT INTO @RgbValues VALUES (@Var_Name, 0, 0, 0, 0x000000);

        -- Nothing left to do
	RETURN;
END
GO


-- Example use of the function
SELECT * FROM [dbo].[ufnColor2RgbValues] ('Yellow')
GO