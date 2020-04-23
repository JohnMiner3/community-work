-- Truncate the rpt table
TRUNCATE TABLE [rpt].[SalesByYearMonthRegion]
GO

-- Add data to the rpt table
INSERT INTO 
    [rpt].[SalesByYearMonthRegion]
SELECT 
	[CalendarYear] as RptYear,
	[Month] as RptMonth,
	[Region] as RptRegion,
	[Model] as ModelNo,
	SUM([Quantity]) as TotalQty,
	SUM([Amount]) as TotalAmt
FROM 
	[dbo].[vDMPrep]
GROUP BY
	[CalendarYear],
    [Month],
    [Region],
    [Model]
ORDER BY
   [CalendarYear],
   [Month],
   [Region]
 GO 

-- Show the table data
SELECT
	*
FROM
	[rpt].[SalesByYearMonthRegion]
ORDER BY
	RptYear,
	RptMonth,
	RptRegion,
	ModelNo