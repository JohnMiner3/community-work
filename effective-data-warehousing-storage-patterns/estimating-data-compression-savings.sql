/******************************************************
 *
 * Name:         estimating-data-compression-savings.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     03-01-2013
 *     Blog:     www.craftydba.com
 *
 *     Purpose:  How much space can I save with row
 *               versus page compression?
 *
 ******************************************************/


-- Use the correct database
USE [BIG_JONS_BBQ_DW]
GO

-- Sample call to Paul Neilson's routine (estimate)
EXEC dbo.db_compression_estimate
GO

-- What is the data/index page count?
sp_spaceused '[DIM].[DIM_DATE]'

/*
TABLE, ROWS, RESERVED, DATA, INDEX, FREE
DIM_DATE, 1095, 184 KB, 72 KB, 64 KB, 48 KB
*/


-- Try Row Compression (-27 pct)
ALTER TABLE [DIM].[DIM_DATE]
REBUILD WITH (DATA_COMPRESSION = ROW);

-- What is the data/index page count?
sp_spaceused '[DIM].[DIM_DATE]'


/*
TABLE, ROWS, RESERVED, DATA, INDEX, FREE
DIM_DATE, 1095, 112 KB, 48 KB, 64 KB, 0 KB
*/


-- Try Row Compression (-45 pct)
ALTER TABLE [DIM].[DIM_DATE]
REBUILD WITH (DATA_COMPRESSION = PAGE);

-- What is the data/index page count?
sp_spaceused '[DIM].[DIM_DATE]'


/*
TABLE, ROWS, RESERVED, DATA, INDEX, FREE
DIM_DATE, 1095, 96 KB, 32 KB, 64 KB, 0 KB
*/


-- Remove the compression
ALTER TABLE [DIM].[DIM_DATE]
REBUILD WITH (DATA_COMPRESSION = NONE);

-- What is the data/index page count?
sp_spaceused '[DIM].[DIM_DATE]'