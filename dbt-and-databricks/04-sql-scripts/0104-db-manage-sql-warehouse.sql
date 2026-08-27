/******************************************************
 *
 * Name:         0104-db-manage-sql-warehouse.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     09-01-2026
 *     Purpose:  Clear + count snapshot tables.
 *               Tear down schemas in unity catalog.
 * 
 ******************************************************/
 
--
--  pick my catalog
--

use catalog uc_sql_server_central;


--
--  clear snapshot tables
--

truncate table data_snapshot.album02;
truncate table data_snapshot.artist02;
truncate table data_snapshot.employee02;
truncate table data_snapshot.genre02;
truncate table data_snapshot.mediatype02;
truncate table data_snapshot.playlist02;
truncate table data_snapshot.playlisttrack02;
truncate table data_snapshot.track02;


--
--  grab row counts - mart schema
--

select 'dimcustomers' as names, count(*) as total from data_mart.dimcustomers
union
select 'dimdate' as names, count(*) as total from data_mart.dimdate
union
select 'dimemployees' as names, count(*) as total from data_mart.dimemployees
union
select 'dimlists' as names, count(*) as total from data_mart.dimlists
union
select 'dimproducts' as names, count(*) as total from data_mart.dimproducts
union
select 'factsales' as names, count(*) as total from data_mart.factsales;


--
--  grab row counts - raw schema
--

select 'album01' as names, count(*) as total from data_raw.album01
union
select 'artist01' as names, count(*) as total from data_raw.artist01
union
select 'employee01' as names, count(*) as total from data_raw.employee01
union
select 'genre01' as names, count(*) as total from data_raw.genre01
union
select 'mediatype01' as names, count(*) as total from data_raw.mediatype01
union
select 'playlist01' as names, count(*) as total from data_raw.playlist01
union
select 'playlisttrack01' as names, count(*) as total from data_raw.playlisttrack01
union
select 'track01' as names, count(*) as total from data_raw.track01
union
select 'customer01' as names, count(*) as total from data_raw.customer01
union
select 'invoice01' as names, count(*) as total from data_raw.invoice01
union
select 'invoiceline01' as names, count(*) as total from data_raw.invoiceline01;


--
--  grab row counts - snapshot schema
--

select 'album02' as names, count(*) as total from data_snapshot.album02
union
select 'artist02' as names, count(*) as total from data_snapshot.artist02
union
select 'employee02' as names, count(*) as total from data_snapshot.employee02
union
select 'genre02' as names, count(*) as total from data_snapshot.genre02
union
select 'mediatype02' as names, count(*) as total from data_snapshot.mediatype02
union
select 'playlist02' as names, count(*) as total from data_snapshot.playlist02
union
select 'playlisttrack02' as names, count(*) as total from data_snapshot.playlisttrack02
union
select 'track02' as names, count(*) as total from data_snapshot.track02;


--
--  grab row counts - stage schema
--

select 'customer02' as names, count(*) as total from data_stage.customer02
union
select 'dates02' as names, count(*) as total from data_stage.dates02
union
select 'invoice02' as names, count(*) as total from data_stage.invoice02
union
select 'invoiceline02' as names, count(*) as total from data_stage.invoiceline02;


--
--  drop schemas
--

drop schema uc_sql_server_central.data_mart cascade;

drop schema uc_sql_server_central.data_raw cascade;

drop schema uc_sql_server_central.data_snapshot cascade;

drop schema uc_sql_server_central.data_stage cascade;

