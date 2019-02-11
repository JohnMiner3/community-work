/******************************************************
 *
 * Name:         example-1.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     11-07-2017
 *     Purpose:  Enable & Disable governor.
 * 
 ******************************************************/

-- State of resource governor
SELECT * 
FROM sys.resource_governor_configuration
GO

-- Enable resource governor
ALTER RESOURCE GOVERNOR RECONFIGURE; 
GO 

-- Disable resource governor
ALTER RESOURCE GOVERNOR DISABLE;  
GO 


