\set ON_ERROR_STOP
BEGIN;
ALTER TABLE "public"."authors" ADD CHECK ((au_id] like '[0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9][0-9][0-9'));
ALTER TABLE "public"."authors" ADD CHECK ((zip] like '[0-9][0-9][0-9][0-9][0-9'));
ALTER TABLE "public"."employee" ADD CONSTRAINT "ck_emp_id" CHECK (((emp_id] like '[a-z][a-z][a-z][1-9][0-9][0-9][0-9][0-9][fm' OR emp_id] like '[a-z]-[a-z][1-9][0-9][0-9][0-9][0-9][fm')));
ALTER TABLE "public"."jobs" ADD CHECK ((max_lvl<=(250)));
ALTER TABLE "public"."jobs" ADD CHECK ((min_lvl>=(10)));
ALTER TABLE "public"."publishers" ADD CHECK (((pub_id='1756' OR (pub_id='1622' OR (pub_id='0877' OR (pub_id='0736' OR (pub_id='1389' OR pub_id] like '99[0-9][0-9')))))));
ALTER TABLE "public"."employee" ALTER COLUMN "hire_date" SET DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE "public"."titles" ALTER COLUMN "pubdate" SET DEFAULT CURRENT_TIMESTAMP;
COMMIT;
