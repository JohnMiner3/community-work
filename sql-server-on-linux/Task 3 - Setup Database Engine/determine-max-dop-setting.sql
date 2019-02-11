/******************************************************
 *
 * Name:         determine-max-dop-setting.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     08-01-2014
 *     Purpose:  Determine the maxdop setting. 
 * 
 ******************************************************/

-- Select the correct database
USE [master]
GO

-- Figure out MAX DOP value
select
    cpu_count, 
	hyperthread_ratio,
    case
        when cpu_count / hyperthread_ratio > 8 then 8
        else cpu_count / hyperthread_ratio
    end as optimal_maxdop_setting
from 
    sys.dm_os_sys_info;
