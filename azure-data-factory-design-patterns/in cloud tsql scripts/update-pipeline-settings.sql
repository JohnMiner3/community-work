/******************************************************
 *
 * Name:         update-pipeline-settings.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     04-22-2020
 *     Purpose:  Update the Adf Pipeline Settings table 
 *               for a incremental load for the 
 *               Fact Internet Sales table.
 * 
 ******************************************************/

-- Set all runs to disabled 
update c
set active_flg = 'N'
from [ctrl].[AdfPipelineSettings] c
go

-- Id 8 should be our row
select * 
from [ctrl].[AdfPipelineSettings] 
where id = 8
go

-- Update row with required info
update c
set load_type = 'I',
    active_flg = 'Y',
	post_tsql_txt = '[stage].[UpsertFactInternetSales]',
	water_mark_txt = '20140129'
from [ctrl].[AdfPipelineSettings] c 
where id = 8
go
