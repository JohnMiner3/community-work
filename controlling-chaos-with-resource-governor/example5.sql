/******************************************************
 *
 * Name:         example-5.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     08-03-2017
 *     Purpose:  Putting it all together.
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
-- Classifier function
--

CREATE FUNCTION Udf_Dbms_Classifier()
RETURNS SYSNAME
WITH SCHEMABINDING
AS
BEGIN
RETURN 
  CASE 
    WHEN SUSER_NAME() = 'Hospital1' THEN 'Hospital1Group'
    WHEN SUSER_NAME() = 'Hospital2' THEN  'Hospital2Group'
    ELSE 'default'
  END
END
GO

-- Apply the classifier
ALTER RESOURCE GOVERNOR with (CLASSIFIER_FUNCTION = [dbo].[Udf_Dbms_Classifier]);
GO

-- Update governor
ALTER RESOURCE GOVERNOR RECONFIGURE;
GO


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

-- Delete existing pool
IF  EXISTS (SELECT * FROM sys.dm_resource_governor_resource_pools WHERE name = N'Hospital2Pool')
DROP RESOURCE POOL Hospital2Pool
GO

-- Delete existing pool
IF  EXISTS (SELECT * FROM sys.dm_resource_governor_resource_pools WHERE name = N'Hospital1Pool')
DROP RESOURCE POOL Hospital1Pool
GO

*/