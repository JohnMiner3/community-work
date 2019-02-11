/******************************************************
 *
 * Name:         grimm.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     06-12-2012
 *     Purpose:  A program to create the brothers
 *               grimm fairy tale database.  This
 *               sample database will be used to
 *               demonstrate full text indexing.
 * 
 ******************************************************/


/*  
	Create a database to hold the fairy tales
*/


-- Which database to use.
USE [master]
GO

-- Delete existing database
IF  EXISTS (SELECT name FROM sys.databases WHERE name = N'GRIMM')
DROP DATABASE GRIMM
GO

-- Add new database
CREATE DATABASE GRIMM ON  
 PRIMARY 
  ( NAME = N'GRIMM_DATA', FILENAME = N'C:\MSSQL\DATA\GRIMM.MDF' , SIZE = 256MB, FILEGROWTH = 32MB) 
 LOG ON 
  ( NAME = N'GRIMM_LOG', FILENAME = N'C:\MSSQL\LOG\GRIMM.LDF' , SIZE = 64MB, FILEGROWTH = 4MB)
GO



/*  
	Create a table to hold the fairy tales
*/

-- Which database to use.
USE [GRIMM]
GO

-- Delete existing table
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DBO].[TBL_FAIRY_TALES]') AND type in (N'U'))
DROP TABLE [DBO].[TBL_FAIRY_TALES]
GO

-- Add new table
CREATE TABLE [DBO].[TBL_FAIRY_TALES] (
	[MY_ID] [SMALLINT] IDENTITY(1, 1) NOT NULL,
	[MY_TITLE] [VARCHAR] (75) NOT NULL,
	[MY_TALE] [TEXT] NULL
) 
GO



/*  
	Insert just the titles (derived table)
*/

-- Which database to use.
USE [GRIMM]
GO

-- Add 62 titles
INSERT INTO TBL_FAIRY_TALES (MY_TITLE)
SELECT T.MY_TITLE FROM 
(
  VALUES
    ('THE GOLDEN BIRD'),
    ('HANS IN LUCK'),
    ('JORINDA AND JORINDEL'),
    ('THE TRAVELLING MUSICIANS'),
    ('OLD SULTAN'),
    ('THE STRAW, THE COAL, AND THE BEAN'),
    ('BRIAR ROSE'),
    ('THE DOG AND THE SPARROW'),
    ('THE TWELVE DANCING PRINCESSES'),
    ('THE FISHERMAN AND HIS WIFE'),
    ('THE WILLOW-WREN AND THE BEAR'),
    ('THE FROG-PRINCE'),
    ('CAT AND MOUSE IN PARTNERSHIP'),
    ('THE GOOSE-GIRL'),
    ('THE ADVENTURES OF CHANTICLEER AND PARTLET'),
    ('RAPUNZEL'),
    ('FUNDEVOGEL'),
    ('THE VALIANT LITTLE TAILOR'),
    ('HANSEL AND GRETEL'),
    ('THE MOUSE, THE BIRD, AND THE SAUSAGE'),
    ('MOTHER HOLLE'),
    ('LITTLE RED-CAP [LITTLE RED RIDING HOOD]'),
    ('THE ROBBER BRIDEGROOM'),
    ('TOM THUMB'),
    ('RUMPELSTILTSKIN'),
    ('CLEVER GRETEL'),
    ('THE OLD MAN AND HIS GRANDSON'),
    ('THE LITTLE PEASANT'),
    ('FREDERICK AND CATHERINE'),
    ('SWEETHEART ROLAND'),
    ('SNOWDROP'),
    ('THE PINK'),
    ('CLEVER ELSIE'),
    ('THE MISER IN THE BUSH'),
    ('ASHPUTTEL'),
    ('THE WHITE SNAKE'),
    ('THE WOLF AND THE SEVEN LITTLE KIDS'),
    ('THE QUEEN BEE'),
    ('THE ELVES AND THE SHOEMAKER'),
    ('THE JUNIPER-TREE'),
    ('THE TURNIP'),
    ('CLEVER HANS'),
    ('THE THREE LANGUAGES'),
    ('THE FOX AND THE CAT'),
    ('THE FOUR CLEVER BROTHERS'),
    ('LILY AND THE LION'),
    ('THE FOX AND THE HORSE'),
    ('THE BLUE LIGHT'),
    ('THE RAVEN'),
    ('THE GOLDEN GOOSE'),
    ('THE WATER OF LIFE'),
    ('THE TWELVE HUNTSMEN'),
    ('THE KING OF THE GOLDEN MOUNTAIN'),
    ('DOCTOR KNOWALL'),
    ('THE SEVEN RAVENS'),
    ('THE WEDDING OF MRS FOX'),
    ('THE SALAD'),
    ('THE STORY OF THE YOUTH WHO WENT FORTH TO LEARN WHAT FEAR WAS'),
    ('KING GRISLY-BEARD'),
    ('IRON HANS'),
    ('CAT-SKIN'),
    ('SNOW-WHITE AND ROSE-RED')
) AS T (MY_TITLE);
GO

-- Show table with tales
SELECT *
FROM [GRIMM].[dbo].[TBL_FAIRY_TALES]



/*  
	Update tales with text file (blob)
*/


-- delete work space
IF EXISTS (SELECT * FROM tempdb.sys.objects T WHERE T.TYPE = 'U' AND T.name LIKE '#TMP_TALE%')
DROP TABLE #TMP_TALE
GO

-- do not count rows
SET NOCOUNT ON;

-- local variables
DECLARE @VAR_CNT SMALLINT;
DECLARE @VAR_MAX SMALLINT;
DECLARE @VAR_FILE VARCHAR(128);
DECLARE @VAR_STMT VARCHAR(2048);

-- setup counters    
SELECT @VAR_CNT = 1;
SELECT @VAR_MAX = 62;

-- create work space
CREATE TABLE #TMP_TALE (MY_TALE VARCHAR(MAX));

-- load the tales
WHILE (@VAR_CNT <= @VAR_MAX)
BEGIN

    -- clear work space
    DELETE FROM #TMP_TALE;
    
    -- make file name
    SELECT @VAR_FILE = 'C:\MSSQL\RAW\GRIMM-' + REPLACE(STR(@VAR_CNT, 2, 0), ' ', '0') + '.TXT';
    
    -- load file into work space
    SELECT @VAR_STMT = 'BULK INSERT #TMP_TALE FROM ''' + @VAR_FILE + ''' WITH (ROWTERMINATOR = ''\0'', CODEPAGE = ''RAW'');';
    PRINT @VAR_STMT;
    EXEC (@VAR_STMT);
    
    -- update fairy tale table
    UPDATE [GRIMM].[dbo].[TBL_FAIRY_TALES]
    SET MY_TALE = (SELECT MY_TALE FROM #TMP_TALE)
    WHERE MY_ID = @VAR_CNT;

    -- increment counter
    SELECT @VAR_CNT = @VAR_CNT + 1;
        
END;
GO

-- count rows
SET NOCOUNT OFF;

-- delete work space
IF EXISTS (SELECT * FROM tempdb.sys.objects T WHERE T.TYPE = 'U' AND T.name LIKE '#TMP_TALE%')
DROP TABLE #TMP_TALE
GO



/*  
	Looking for wolf's 
*/

-- # full table or clustered index scan #
 
-- Show time & i/o
SET STATISTICS TIME ON
SET STATISTICS IO ON
GO

-- Remove clean buffers & clear plan cache
CHECKPOINT
DBCC DROPCLEANBUFFERS
DBCC FREEPROCCACHE
GO

-- Select everything with word 'wolf'
SELECT * FROM [dbo].[TBL_FAIRY_TALES] T WHERE T.MY_TALE LIKE '%wolf%'
GO

-- Select everything with word 'wolf'
SELECT * FROM [dbo].[TBL_FAIRY_TALES] T WHERE PATINDEX('%wolf%', T.MY_TALE) > 0
GO

-- Hide time & i/o
SET STATISTICS TIME OFF
SET STATISTICS IO OFF
GO


/*  
	Add primary key and re-test
*/

-- Delete existing primary key
IF  EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dbo].[TBL_FAIRY_TALES]') AND name = N'PK_FAIRY_TALES_ID')
ALTER TABLE [dbo].[TBL_FAIRY_TALES] DROP CONSTRAINT [PK_FAIRY_TALES_ID]
GO

-- Add index on id (primary key - clustered)
ALTER TABLE [dbo].[TBL_FAIRY_TALES]
ADD CONSTRAINT PK_FAIRY_TALES_ID PRIMARY KEY(MY_ID)
GO 

-- Try to add index to text data type?
CREATE NONCLUSTERED INDEX IDX_FAIRY_TALES_TXT
    ON [dbo].[TBL_FAIRY_TALES] (MY_TALE)
GO


/*  
	Manually create full index using SSMS
	
	0 - right click on table, fti wizard
	1 - select index = pk_fairy_tales_id
	2 - select table column = my_tale
	3 - select change tracking = manually
	4 - create new catalog = ftc_fairy_tales
	5 - create new schedule - sch_fairy_tales
	  daily @ 9:00 am
	  incremental update
	 
	Check under storage for fti properties
	 
*/


-- # clustered index seek / full text match #

-- Exact match word 'wolf' - (7) results
SELECT MY_ID, MY_TITLE
FROM [dbo].[TBL_FAIRY_TALES] T
WHERE CONTAINS(T.MY_TALE,  'wolf')
GO


-- Exact match word 'wolf' as a word or prefix & ranking value - (7) results
SELECT MY_ID, MY_TITLE, [RANK] FROM [dbo].[TBL_FAIRY_TALES] AS T
JOIN CONTAINSTABLE(TBL_FAIRY_TALES, MY_TALE, ' "wolf*" ') FTS
ON T.MY_ID = FTS.[KEY]
ORDER BY [RANK] DESC
GO


-- Fuzzy match on a phrase - (32) results
SELECT MY_ID, MY_TITLE, [RANK] FROM [dbo].[TBL_FAIRY_TALES] AS T
JOIN FREETEXTTABLE(TBL_FAIRY_TALES, MY_TALE, 'big bad wolf') FTS
ON T.MY_ID = FTS.[KEY]
ORDER BY [RANK] DESC
GO


/*  
	Where FTI can go wrong
*/


-- Looking for words with herd such as [shepherd] - (7) results
SELECT * FROM [dbo].[TBL_FAIRY_TALES] T WHERE T.MY_TALE LIKE '%herd%'
GO


-- Exact match word 'herd' - (1) results
SELECT MY_ID, MY_TITLE
FROM [dbo].[TBL_FAIRY_TALES] T
WHERE CONTAINS(T.MY_TALE,  '*herd')
GO

-- Exact match word 'herd' - (1) results
SELECT MY_ID, MY_TITLE
FROM [dbo].[TBL_FAIRY_TALES] T
WHERE FREETEXT(T.MY_TALE,  'herd*')
GO


-- Exact match word 'shepherd' - (7) results
SELECT MY_ID, MY_TITLE, [RANK] FROM [dbo].[TBL_FAIRY_TALES] AS T
JOIN CONTAINSTABLE(TBL_FAIRY_TALES, MY_TALE, ' "shepherd" ') FTS
ON T.MY_ID = FTS.[KEY]
ORDER BY [RANK] DESC
GO

UPDATE [dbo].[TBL_FAIRY_TALES]
SET my_TALE = NULL
WHERE my_id = 22

SELECT * FROM [dbo].[TBL_FAIRY_TALES] WHERE my_id = 22