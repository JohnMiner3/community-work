/******************************************************
 *
 * Name:         04-using-sql-server-audit.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     05-01-2026
 *     Purpose:  Server + Database Audits
 * 
 ******************************************************/

/*
  1 - Server level audit
*/

-- Which database to use
USE master;
GO

-- Create the Server Audit
CREATE SERVER AUDIT [HippaServerAudit]
TO FILE ( FILEPATH = 'F:\audits\' ); 
GO

-- Enable the Server Audit
ALTER SERVER AUDIT [HippaServerAudit]
WITH (STATE = ON);
GO

-- Create server specification
CREATE SERVER AUDIT SPECIFICATION [HippaServerAuditSpec]
FOR SERVER AUDIT [HippaServerAudit]
ADD (FAILED_LOGIN_GROUP),
ADD (SERVER_ROLE_MEMBER_CHANGE_GROUP)
WITH (STATE = ON);
GO

-- Try a bad login ...

-- Show the details
SELECT * FROM sys.fn_get_audit_file('f:\audits\*', DEFAULT, DEFAULT);



/*
  2 - Database level audit
*/

USE [dbs_hippa02];
GO

-- Create the Database Audit Specification
CREATE DATABASE AUDIT SPECIFICATION [Audit_All_Operations_On_Schema]
FOR SERVER AUDIT [HippaServerAudit]
ADD ( SELECT ON SCHEMA::[active] by [dbo]),
ADD ( INSERT ON SCHEMA::[active] by [dbo]),
ADD ( UPDATE ON SCHEMA::[active] by [dbo]),
ADD ( DELETE ON SCHEMA::[active] by [dbo])
WITH ( STATE = ON );
GO


/*
  ~ Start of make changes ~
*/


 -- Show the Campbell family 
select * from active.patient_info where last_name = 'CAMPBELL' 
go 

-- Add a new visit 
insert into active.visit_info values (getdate(), 125, 60, 98.6, 120, 60, 487, 'Influenza', 11, 1); 
go 

-- Update the visit 
update active.visit_info 
set diagnosis_desc = upper(diagnosis_desc), patient_temp = 98.4 
where visit_id = 21 
go 

-- Delete first visit 
delete from active.visit_info where visit_id = 11; 
go 


/*
  ~ End of make changes ~
*/

-- Show the details
SELECT * FROM sys.fn_get_audit_file('f:\audits\*', DEFAULT, DEFAULT);



/*
  3 - Disable / remove - database level audit
*/


-- Disable database audit
ALTER DATABASE AUDIT SPECIFICATION [Audit_All_Operations_On_Schema] WITH (STATE = OFF);
go

-- Drop database audit
DROP DATABASE AUDIT SPECIFICATION [Audit_All_Operations_On_Schema];
go


/*
  4 - Disable / remove - server level audit
*/


-- Use correct db
use [master]
go

-- Disable server audit
ALTER SERVER AUDIT [HippaServerAudit]
WITH (STATE = OFF);
GO

-- Disable server audit
ALTER SERVER AUDIT SPECIFICATION [HippaServerAuditSpec] WITH (STATE = OFF);
go

-- Remove spect
DROP SERVER AUDIT SPECIFICATION [HippaServerAuditSpec];
go

-- Remove audit
DROP SERVER AUDIT [HippaServerAudit]
GO
