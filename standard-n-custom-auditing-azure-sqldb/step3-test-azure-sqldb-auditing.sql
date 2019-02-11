/******************************************************
 *
 * Name:         step3-test-azure-sqldb-auditing.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     11-17-2017
 *     Purpose:  Perform CRUD actions.
 * 
 ******************************************************/
 
--
-- Test Azure Database Auditing
--


/*
  Audit - Selects
*/

-- patient info
select * from active.patient_info where last_name = 'MILLER'
go

-- visit info
select * from active.visit_info where patient_id = 15
go


/*
  Audit - Insert
*/

-- visit info
insert into active.visit_info 
values
(getdate(), 125, 60, 98.6, 120, 60, 487, 'Influenza', 15, 1);
go


/*
  Audit - Update
*/

-- visit info
update 
    active.visit_info 
set 
    diagnosis_desc = upper(diagnosis_desc),
    patient_temp = 98.4
where
    visit_id = 21
go


/*
  Audit - Delete
*/

-- visit info
delete
from active.visit_info 
where visit_id = 15;
go


/*
  Audit - Truncate
*/

truncate table [active].[visit_info]
go
