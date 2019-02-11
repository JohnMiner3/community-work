/******************************************************
 *
 * Name:         example-6.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     08-03-2017
 *     Purpose:  Allocating resources by database names.
 * 
 ******************************************************/
 
USE MASTER
GO

--
-- Pool 1
--

CREATE RESOURCE POOL Hospital1Pool
WITH
(
    MIN_CPU_PERCENT = 0,             -- how much must be assigned to this pool
    MAX_CPU_PERCENT = 100,           -- how much would be assigned if possible
    CAP_CPU_PERCENT = 100,           -- cannot-be-exceeded maximum, useful for predictable billing
    
	AFFINITY SCHEDULER = AUTO,

    MIN_MEMORY_PERCENT = 0,          -- memory allocated to this pool that cannot be shared
    MAX_MEMORY_PERCENT = 100,        -- percentage total server memory which is allowed to be used

    MIN_IOPS_PER_VOLUME = 0,         -- minimum number of I/O operations per second per disk volume
    MAX_IOPS_PER_VOLUME = 2147483647 --maximum.  Note, this is the max allowed value
);

-- Update governor
ALTER RESOURCE GOVERNOR RECONFIGURE;
GO

-- 
-- Workgroup 1
--

CREATE WORKLOAD GROUP Hospital1Group
WITH (
     IMPORTANCE = MEDIUM,                   -- relative importance compared to other workgroups
	 REQUEST_MAX_MEMORY_GRANT_PERCENT = 50, -- how much memory a single process can request
	 REQUEST_MAX_CPU_TIME_SEC = 0,          -- how long a single request can take 
	 MAX_DOP = 0,                           -- max degree of parallelism allowed
	 GROUP_MAX_REQUESTS = 0                 --number of simultaneous events allowed, 0 means unlimited
)
USING Hospital1Pool
;

-- Update governor
ALTER RESOURCE GOVERNOR RECONFIGURE;
GO


--
-- Pool 2
--

CREATE RESOURCE POOL Hospital2Pool
WITH
(
    MIN_CPU_PERCENT = 0,             -- how much must be assigned to this pool
    MAX_CPU_PERCENT = 100,           -- how much would be assigned if possible
    CAP_CPU_PERCENT = 100,           -- cannot-be-exceeded maximum, useful for predictable billing
    
	AFFINITY SCHEDULER = AUTO,

    MIN_MEMORY_PERCENT = 0,          -- memory allocated to this pool that cannot be shared
    MAX_MEMORY_PERCENT = 100,        -- percentage total server memory which is allowed to be used

    MIN_IOPS_PER_VOLUME = 0,         -- minimum number of I/O operations per second per disk volume
    MAX_IOPS_PER_VOLUME = 2147483647 --maximum.  Note, this is the max allowed value
);

-- Update governor
ALTER RESOURCE GOVERNOR RECONFIGURE;
GO


-- 
-- Workgroup 2
--

CREATE WORKLOAD GROUP Hospital2Group
WITH (
     IMPORTANCE = MEDIUM,                   -- relative importance compared to other workgroups
	 REQUEST_MAX_MEMORY_GRANT_PERCENT = 50, -- how much memory a single process can request
	 REQUEST_MAX_CPU_TIME_SEC = 0,          -- how long a single request can take 
	 MAX_DOP = 0,                           -- max degree of parallelism allowed
	 GROUP_MAX_REQUESTS = 0                 --number of simultaneous events allowed, 0 means unlimited
)
USING Hospital2Pool
;

-- Update governor
ALTER RESOURCE GOVERNOR RECONFIGURE;
GO


--
-- Pool 3
--

CREATE RESOURCE POOL Admins2Pool
WITH
(
    MIN_CPU_PERCENT = 0,             -- how much must be assigned to this pool
    MAX_CPU_PERCENT = 100,           -- how much would be assigned if possible
    CAP_CPU_PERCENT = 100,           -- cannot-be-exceeded maximum, useful for predictable billing
    
	AFFINITY SCHEDULER = AUTO,

    MIN_MEMORY_PERCENT = 0,          -- memory allocated to this pool that cannot be shared
    MAX_MEMORY_PERCENT = 100,        -- percentage total server memory which is allowed to be used

    MIN_IOPS_PER_VOLUME = 0,         -- minimum number of I/O operations per second per disk volume
    MAX_IOPS_PER_VOLUME = 2147483647 --maximum.  Note, this is the max allowed value
);

-- Update governor
ALTER RESOURCE GOVERNOR RECONFIGURE;
GO


-- 
-- Workgroup 3
--

CREATE WORKLOAD GROUP Admins2Group
WITH (
     IMPORTANCE = MEDIUM,                   -- relative importance compared to other workgroups
	 REQUEST_MAX_MEMORY_GRANT_PERCENT = 50, -- how much memory a single process can request
	 REQUEST_MAX_CPU_TIME_SEC = 0,          -- how long a single request can take 
	 MAX_DOP = 0,                           -- max degree of parallelism allowed
	 GROUP_MAX_REQUESTS = 0                 --number of simultaneous events allowed, 0 means unlimited
)
USING Admins2Pool
;

-- Update governor
ALTER RESOURCE GOVERNOR RECONFIGURE;
GO


-- 
-- Create reference table
--

-- Drop existing table
DROP TABLE IF EXISTS [DBO].[DBMS_NAME_2_WORKLOAD_GROUP]
GO

-- Create new table
CREATE TABLE [DBO].[DBMS_NAME_2_WORKLOAD_GROUP]
(
id int identity(1,1) primary key,
dbms_name varchar(128) not null,
wrkgrp_name varchar(128) not null
)
GO

-- database 1 to work load group 1
INSERT INTO [DBO].[DBMS_NAME_2_WORKLOAD_GROUP]
VALUES ('HOSPITAL1', 'Hospital1Group');
GO

-- database 2 to work load group 2
INSERT INTO [DBO].[DBMS_NAME_2_WORKLOAD_GROUP]
VALUES ('HOSPITAL2', 'Hospital2Group');
GO

-- Show the data
SELECT * FROM [DBO].[DBMS_NAME_2_WORKLOAD_GROUP]
GO


-- 
-- Classifier function
--

-- Remove existing function
DROP FUNCTION IF EXISTS Udf_Dbms_Classifier
GO

CREATE FUNCTION Udf_Dbms_Classifier()
RETURNS SYSNAME
WITH SCHEMABINDING
AS
BEGIN

   -- Declare variables
   DECLARE @group_name sysname
   DECLARE @dbms_name sysname

   -- Default group
   SELECT @group_name = 'default'

   -- Admin group
   IF IS_SRVROLEMEMBER ('sysadmin') = 1 OR 
      IS_SRVROLEMEMBER('serveradmin') = 1 OR 
	  IS_SRVROLEMEMBER('securityadmin') = 1 OR 
	  IS_SRVROLEMEMBER('processadmin') = 1 OR 
	  IS_SRVROLEMEMBER('diskadmin') = 1
       BEGIN
           SELECT @group_name = 'Admins2Group';
       END
   -- 
   ELSE
       BEGIN
	       -- Session db name might not be given
           SELECT @dbms_name = ORIGINAL_DB_NAME();

		   -- Find login default database
           IF (@dbms_name = '') 
           BEGIN
               SELECT @dbms_name = CONVERT(nvarchar, LOGINPROPERTY(SUSER_NAME(), 'DefaultDatabase'));
           END

		   -- Use database 2 group reference table
           SELECT @group_name = wrkgrp_name
           FROM [DBO].[DBMS_NAME_2_WORKLOAD_GROUP]
           WHERE dbms_name = @dbms_name
       END  

   -- Return work group name 
   RETURN @group_name
END
GO


-- Apply the classifier
ALTER RESOURCE GOVERNOR with (CLASSIFIER_FUNCTION = [dbo].[Udf_Dbms_Classifier]);
GO

-- Update governor
ALTER RESOURCE GOVERNOR RECONFIGURE;
GO


-- 
-- Sample load - make sure logins have default db
--

/*

SELECT 
   [lname] AS LAST_NAME,
   count(*) AS TOTAL_RECS
FROM [HOSPITAL1].[ACTIVE].[PATIENT_INFO]
GROUP BY [lname]

*/

-- 
-- Unbind Classifier function
--

/*

-- Remove function
ALTER RESOURCE GOVERNOR with (CLASSIFIER_FUNCTION = NULL);
GO

-- Update governor
ALTER RESOURCE GOVERNOR RECONFIGURE;
GO

*/


-- 
-- Remove R.G. objects
--

/*

-- Classifier function
DROP FUNCTION IF EXISTS Udf_Dbms_Classifier
GO

-- Delete existing workload group.
IF  EXISTS (SELECT * FROM sys.dm_resource_governor_workload_groups WHERE name = N'Hospital2Group')
DROP WORKLOAD GROUP Hospital2Group
GO

-- Delete existing workload group.
IF  EXISTS (SELECT * FROM sys.dm_resource_governor_workload_groups WHERE name = N'Hospital1Group')
DROP WORKLOAD GROUP Hospital1Group
GO

-- Delete existing workload group.
IF  EXISTS (SELECT * FROM sys.dm_resource_governor_workload_groups WHERE name = N'Admins2Group')
DROP WORKLOAD GROUP Admins2Group
GO

-- Delete existing pool
IF  EXISTS (SELECT * FROM sys.dm_resource_governor_resource_pools WHERE name = N'Hospital2Pool')
DROP RESOURCE POOL Hospital2Pool
GO

-- Delete existing pool
IF  EXISTS (SELECT * FROM sys.dm_resource_governor_resource_pools WHERE name = N'Hospital1Pool')
DROP RESOURCE POOL Hospital1Pool
GO

-- Delete existing pool
IF  EXISTS (SELECT * FROM sys.dm_resource_governor_resource_pools WHERE name = N'Admins2Pool')
DROP RESOURCE POOL Admins2Pool
GO

*/