/******************************************************
 *
 * Name:         08-using-change-table-tracking.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     05-01-2026
 *     Purpose:  Change Table Tracking
 * 
 ******************************************************/
 
 
--
--  1 - Enable change tracking at database level
--

-- switch database
USE [master];
GO

-- turn on tracking
ALTER DATABASE dbs_hippa04  
SET CHANGE_TRACKING = ON  
(CHANGE_RETENTION = 7 DAYS, AUTO_CLEANUP = ON);
GO


--
--  2 - Enable change tracking at table level
--

-- switch database
USE dbs_hippa04;
GO

-- alter table 1
ALTER TABLE active.doctor_info  
ENABLE CHANGE_TRACKING  
WITH (TRACK_COLUMNS_UPDATED = ON);
GO

-- alter table 2
ALTER TABLE active.patient_info
ENABLE CHANGE_TRACKING  
WITH (TRACK_COLUMNS_UPDATED = ON);
GO

-- alter table 3
ALTER TABLE active.visit_info
ENABLE CHANGE_TRACKING  
WITH (TRACK_COLUMNS_UPDATED = ON);
GO


--
-- 3 - Validate that change table tracking is on
--

-- databases
select * from sys.change_tracking_databases

-- tables
SELECT 
    s.name as schema_nm, 
    t.name as table_nm, 
    c.is_track_columns_updated_on as ct_enabled
FROM 
    sys.schemas as s 
JOIN 
    sys.tables as t
ON 
    s.schema_id = t.schema_id
JOIN 
    sys.change_tracking_tables AS c
ON 
    t.object_id = c.object_id;
GO


--
--  4 - Create + populate custom table
--

-- drop existing
DROP TABLE IF EXISTS dbo.TrackTableChanges;
GO

-- create table
CREATE TABLE dbo.TrackTableChanges
(
    id int IDENTITY(1, 1) NOT NULL,
    table_nm varchar(128),
    version_no bigint,
    event_dte datetime2(6)
);
GO

-- add data
INSERT INTO dbo.TrackTableChanges
(
    table_nm,
    version_no,
    event_dte
)
SELECT 
    'active.doctor_info',
    CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('active.doctor_info')),
    GETDATE()
UNION
SELECT 
    'active.patient_info',
    CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('active.patient_info')),
    GETDATE()
UNION
SELECT 
    'active.visit_info',
    CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('active.visit_info')),
    GETDATE()
GO

-- show versions
SELECT * FROM dbo.TrackTableChanges
GO


--
--  5 - Get most recent data for visits (s.p.)
--

CREATE PROCEDURE active.get_visit_changes
AS
BEGIN

    -- Ignore counts
    SET NOCOUNT ON

    -- Local variable
    DECLARE @CT_OLD BIGINT;
    DECLARE @CT_NEW BIGINT;


    -- Saved tracking version (old)
    SELECT TOP 1 @CT_OLD = [version_no]
    FROM [dbo].[TrackTableChanges]
    WHERE [table_nm] = 'active.visit_info'
    ORDER BY [event_dte] DESC;


    -- Current tracking version (new)
    SELECT @CT_NEW = CHANGE_TRACKING_CURRENT_VERSION();

    -- Grab table changes
    SELECT
        CASE WHEN C.SYS_CHANGE_OPERATION = 'D' THEN
            C.visit_id
        ELSE 
            D.visit_id
        END AS visit_id,

        D.[visit_date],
        D.[patient_weight],
        D.[patient_height],
        D.[patient_temp],
        D.[patient_systolic],
        D.[patient_diastolic],
        D.[diagnosis_icd9],
        D.[diagnosis_desc],
        D.[patient_id],
        D.[doctor_id],

        C.SYS_CHANGE_OPERATION, 
        C.SYS_CHANGE_VERSION
    FROM 
        active.visit_info AS D
    RIGHT OUTER JOIN
        CHANGETABLE(CHANGES active.visit_info, @CT_OLD, FORCESEEK) AS C
    ON
        D.[visit_id] = C.[visit_id]
    WHERE 
        C.SYS_CHANGE_VERSION <= @CT_NEW;

    -- Update local tracking table?
    IF (@@ROWCOUNT > 0)
    BEGIN
        INSERT INTO [dbo].[TrackTableChanges]
        (
            table_nm,
            version_no,
            event_dte
        )
        VALUES
        (
            'active.visit_info',
            @CT_NEW,
	    GETDATE()
        );
    END

END;



/*
  ~ Start of make changes ~
*/


-- Show visit for head wound
select * from active.visit_info where visit_id = 1
go 

-- any new changes?
exec active.get_visit_changes
go


-- Add a new visit 
insert into active.visit_info values (getdate(), 125, 60, 98.6, 120, 60, 487, 'Influenza', 11, 1); 
go 

-- any new changes?
exec active.get_visit_changes
go


-- Update the visit 
update active.visit_info 
set diagnosis_desc = upper(diagnosis_desc), patient_temp = 98.4 
where visit_id = 21 
go 

-- any new changes?
exec active.get_visit_changes
go


-- Delete first visit 
delete from active.visit_info where visit_id = 11; 
go 

-- any new changes?
exec active.get_visit_changes
go


/*
  ~ End of make changes ~
*/



--
--  6 - Disable change tracking at table level
--

-- switch database
USE dbs_hippa04;
GO

-- alter table 1
ALTER TABLE active.doctor_info  
DISABLE CHANGE_TRACKING  
GO

-- alter table 2
ALTER TABLE active.patient_info
DISABLE CHANGE_TRACKING  
GO

-- alter table 3
ALTER TABLE active.visit_info
DISABLE CHANGE_TRACKING  
GO


--
--  7 - Disable change tracking at database level
--

-- switch database
USE [master];
GO

-- turn off tracking
ALTER DATABASE dbs_hippa04  
SET CHANGE_TRACKING = OFF;
GO
