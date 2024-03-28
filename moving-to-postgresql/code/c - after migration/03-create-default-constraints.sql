/******************************************************
 *
 * Name:         03-create-default-constraints.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     01-01-2024
 *     Purpose:  Add constraints to tables.
 * 
 ******************************************************/


BEGIN;

CREATE SEQUENCE "dbo"."jobs_job_id_seq" INCREMENT BY 1 START WITH 1 OWNED BY "dbo"."jobs"."job_id";
ALTER TABLE "dbo"."authors" ALTER COLUMN "phone" SET DEFAULT 'UNKNOWN';
ALTER TABLE "dbo"."employee" ALTER COLUMN "job_id" SET DEFAULT 1;
ALTER TABLE "dbo"."employee" ALTER COLUMN "job_lvl" SET DEFAULT 10;
ALTER TABLE "dbo"."employee" ALTER COLUMN "pub_id" SET DEFAULT '9952';
ALTER TABLE "dbo"."jobs" ALTER COLUMN "job_desc" SET DEFAULT 'New Position - title not formalized yet';
ALTER TABLE "dbo"."jobs" ALTER COLUMN "job_id" SET DEFAULT nextval('"dbo"."jobs_job_id_seq"');
ALTER TABLE "dbo"."publishers" ALTER COLUMN "country" SET DEFAULT 'USA';
ALTER TABLE "dbo"."titles" ALTER COLUMN "type" SET DEFAULT 'UNDECIDED';
ALTER TABLE "dbo"."employee" ALTER COLUMN "hire_date" SET DEFAULT CURRENT_DATE;
ALTER TABLE "dbo"."titles" ALTER COLUMN "pubdate" SET DEFAULT CURRENT_DATE;

COMMIT;
