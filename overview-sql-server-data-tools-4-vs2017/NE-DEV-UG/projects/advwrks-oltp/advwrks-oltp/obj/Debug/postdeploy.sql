/*
Post-Deployment Script Template							
--------------------------------------------------------------------------------------
 This file contains SQL statements that will be appended to the build script.		
 Use SQLCMD syntax to include a file in the post-deployment script.			
 Example:      :r .\myfile.sql								
 Use SQLCMD syntax to reference a variable in the post-deployment script.		
 Example:      :setvar TableName MyTable							
               SELECT * FROM [$(TableName)]					
--------------------------------------------------------------------------------------


*/

-- Update just one table
 INSERT INTO [AdventureWorks].[dbo].[AWBuildVersion]
 VALUES('12.0.1800','2014-02-20 04:26:00.000', '2014-07-08 00:00:00.000');
GO
