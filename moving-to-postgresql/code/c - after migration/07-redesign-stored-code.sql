/******************************************************
 *
 * Name:     07-redesign-stored-code.sql
 *   
 * Design Phase:
 *   Author:  John Miner
 *   Date:    01-01-2024
 *   Purpose: Rewrite T-SQL to pgSQL.
 * 
 ******************************************************/

--
-- stored code - title - view
--

-- drop existing
drop view if exists dbo.titleview;

-- create new
create view dbo.titleview
as
select 
  t.title, 
  ta.au_ord, 
  a.au_lname, 
  t.price, 
  t.ytd_sales, 
  t.pub_id
from 
  dbo.authors as a
join 
  dbo.titleauthor as ta
on 
  a.au_id = ta.au_id
join 
  dbo.titles as t
on
  ta.title_id = t.title_id;

-- test view

/*
  select * from dbo.titleview limit 5;
*/



--
-- stored code - byroyalty - use function instead of store procedure
--


-- delete old function
drop function if exists dbo.byroyalty;
 
-- create new function
create function dbo.byroyalty ( var_percentage int = 10 )
returns table ( id varchar(11) ) 
as $func01$
begin
  return query
  select ta.au_id 
  from dbo.titleauthor as ta
  where ta.royaltyper = var_percentage;
end 
$func01$ language plpgsql;

-- test function

/*
  select * from dbo.byroyalty(30);
*/



--
-- stored code - reptq1 - use function instead of store procedure
--

-- delete old function
drop function if exists dbo.reptq1;
 
-- create new function
create function dbo.reptq1()
returns table ( pub_id char(4), avg_price numeric ) 
as $func02$
begin
  return query
  select
	case 
	  when grouping(t.pub_id) = 1 then 'ALL' 
	  else t.pub_id 
	end as pub_id,
	avg(price) as avg_price
  from 
    dbo.titles as t
  where 
    price is not null
  group by 
    rollup(t.pub_id)
  order by 
    t.pub_id;
end 
$func02$ language plpgsql;

-- test function

/*
  select * from dbo.reptq1();
*/



--
-- stored code - reptq2 - use function instead of store procedure
--

-- delete old function
drop function if exists dbo.reptq2;
 
-- create new function
create function dbo.reptq2()
returns table ( "type" char(12), pub_id char(4), avg_ytd_sales numeric ) 
as $func03$
begin
  return query
  select
	case when grouping(t."type") = 1 then 'ALL' else t."type" end as "type",
	case when grouping(t.pub_id) = 1 then 'ALL' else t.pub_id end as pub_id,
	avg(t.ytd_sales) as avg_ytd_sales
  from 
    dbo.titles as t
  where 
    t.pub_id is NOT NULL
  group 
    by rollup(t.pub_id, t."type");
end 
$func03$ language plpgsql;

-- test function

/*
  select * from dbo.reptq2();
*/



--
-- stored code - reptq3 - stored procedure
--

-- delete old function
drop function if exists dbo.reptq3;
 
-- create new function
create function dbo.reptq3(var_lo_limit numeric, var_hi_limit numeric, var_type char(12))
returns table ( "type" char(12), pub_id char(4), cnt bigint ) 
as $func04$
begin
  return query
  select
	case when grouping(t."type") = 1 then 'ALL' else t."type" end as "type",
	case when grouping(t.pub_id) = 1 then 'ALL' else t.pub_id end as pub_id,
	count(t.title_id) as cnt
  from 
    dbo.titles as t
  where 
    t.price > var_lo_limit and 
	t.price < var_hi_limit and 
	(t."type" = var_type or t."type" ILIKE '%cook%')
  group by 
    rollup(t.pub_id, t."type");
end 
$func04$ language plpgsql;

-- test function

/*
  select * from dbo.reptq3(9.99, 19.99, 'psychology');
  select * from dbo.reptq3(2.25, 9.99, 'business');
*/


--
-- stored code - trigger on employee
--


-- delete old trigger
drop trigger if exists employee_trg on dbo.employee;

-- delete old function
drop function if exists dbo.employee_ins_upd;
 
 
-- create new function
create function dbo.employee_ins_upd() 
returns trigger 
as $emp_trg$
declare
   r record;
begin

  -- get record
  select 
    j.min_lvl,
    j.max_lvl,
    e.job_lvl,
    e.job_id
  into 
    r
  from   
    dbo.employee as e
  join
    dbo.jobs as j
  on
    e.job_id = j.job_id
  where
    e.job_id = new.job_id and 
	e.emp_id = new.emp_id;

  
  -- job id 1 requirements
  if (r.job_id = 1) and (r.job_lvl <> 10) then
     raise exception 'Job id 1 expects the default level of 10.';
     rollback;
	 return null;
  end if;
  
  -- job id 1 requirements
  if (r.job_lvl < r.min_lvl) or (r.job_lvl > r.max_lvl) then
     raise exception 'The level for job_id % should be between % and %.', r.job_id, r.min_lvl, r.max_lvl;
     rollback;
	 return null;
  end if;
  
  -- don't do anything
  return null;
  
end;
$emp_trg$ LANGUAGE plpgsql;
 
-- create new trigger
create trigger employee_trg after insert or update on dbo.employee
  for each row execute function dbo.employee_ins_upd();


-- test trigger - job id 1 must be lvl 10
/*
  insert into "dbo"."employee" values ('VAM19779F', 'Valerie', 'A', 'Miner', 1, 9, '9952', to_timestamp('11/11/89', 'MM/DD/YY') );
*/

-- test trigger - job id 2 must have lvl 200-250
/*
  insert into "dbo"."employee" values ('VAM19689M', 'John', 'F', 'Miner', 2, 9, '9952', to_timestamp('11/11/89', 'MM/DD/YY') );
*/

-- show info
/*
delete from dbo.employee where emp_id in ('VAM19689M','VAM19779F');
select * from dbo.jobs;
select * from dbo.employee;
*/