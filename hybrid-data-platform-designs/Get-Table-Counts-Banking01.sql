/******************************************************
 *
 * Name:         get-table-counts-banking01.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     02-07-2016
 *     Purpose:  Get counts by table or date/type.
 * 
 ******************************************************/

-- Use correct database
use [banking01]
go

-- Information at table level
select 'accounts' as my_type, count(*) as my_total from active.accounts
union all
select 'customers' as my_type, count(*) as my_total from active.customer
union all
select 'transactions' as my_type, count(*) as my_total from active.[transaction]
go

-- Details by month & acct type
select 
    YEAR(T.TRAN_DATE) * 100 + MONTH(T.TRAN_DATE) AS MY_DATE_KEY,
    A.ACCT_TYPE AS MY_ACCT_TYPE,
	COUNT(*) AS MY_TOTAL_TRANS
from 
    [ACTIVE].[ACCOUNTS] A
join 
    [ACTIVE].[TRANSACTION] T
on 
    A.ACCT_ID = T.ACCT_ID
group by
    YEAR(T.TRAN_DATE) * 100 + MONTH(T.TRAN_DATE),
    A.ACCT_TYPE 
order by
    YEAR(T.TRAN_DATE) * 100 + MONTH(T.TRAN_DATE),
    A.ACCT_TYPE 
go
