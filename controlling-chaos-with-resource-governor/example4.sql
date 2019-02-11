/******************************************************
 *
 * Name:         example-4.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     08-03-2017
 *     Purpose:  Various DMV's.
 * 
 ******************************************************/

-- https://docs.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-resource-governor-configuration-transact-sql

USE master;  
go  

-- Which group am I in? 
SELECT group_id
   FROM sys.dm_exec_sessions
   WHERE session_id = @@SPID;


--
-- SQL Server Engine
--

-- Configuration
SELECT * FROM  sys.resource_governor_configuration;  

-- Resource pools
SELECT * FROM sys.dm_resource_governor_resource_pools;

-- Work load groups
SELECT * FROM sys.dm_resource_governor_workload_groups;

-- Disk I/O
SELECT * FROM sys.dm_resource_governor_resource_pool_volumes;

-- Numa Affinity
SELECT * FROM sys.dm_resource_governor_external_resource_pool_affinity;


--
-- (R & Python)
--

-- External Pools 
SELECT * FROM sys.resource_governor_external_resource_pools

-- External affinity
SELECT * FROM sys.dm_resource_governor_external_resource_pool_affinity;



--
-- Resource Groups - Info & Stats
--

-- Get the stored metadata.  
SELECT   
object_schema_name(classifier_function_id) AS 'Classifier UDF schema in metadata',   
object_name(classifier_function_id) AS 'Classifier UDF name in metadata'  
FROM  sys.resource_governor_configuration;  
go 


-- Get statistics on pools
SELECT 
   name,
   [start] = statistics_start_time,
   cpu = total_cpu_usage_ms,
   memgrant_timeouts = total_memgrant_timeout_count,
   out_of_mem = out_of_memory_count,     
   mem_waiters = memgrant_waiter_count
FROM 
   sys.dm_resource_governor_resource_pools
WHERE
   pool_id > 1;
   

-- Get statistics on workgroups
SELECT 
   name,
   [start] = statistics_start_time,
   waiters = queued_request_count, -- or total_queued_request_count
   [cpu_violations] = total_cpu_limit_violation_count,
   subopt_plans = total_suboptimal_plan_generation_count,
   reduced_mem = total_reduced_memgrant_count
FROM
   sys.dm_resource_governor_workload_groups
WHERE
   group_id > 1;