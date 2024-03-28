/******************************************************
 *
 * Name:         06-apply-check-constraints.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     01-01-2024
 *     Purpose:  Apply check constraints.
 * 
 ******************************************************/
 

BEGIN;

ALTER TABLE "dbo"."jobs" ADD CHECK ("max_lvl" <= 250);
ALTER TABLE "dbo"."jobs" ADD CHECK ("min_lvl" >= 10);
ALTER TABLE "dbo"."authors" ADD CHECK (REGEXP_LIKE("zip", '[0-9][0-9][0-9][0-9][0-9]'));
ALTER TABLE "dbo"."authors" ADD CHECK (REGEXP_LIKE("au_id", '[0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9][0-9][0-9]'));

ALTER TABLE "dbo"."publishers" ADD CHECK 
(
  "pub_id" IN ('1756', '1622', '0877', '0736', '1389') OR 
  REGEXP_LIKE("pub_id", '99[0-9][0-9]') 
);

ALTER TABLE "dbo"."employee" ADD CONSTRAINT "ck_emp_id" CHECK 
(
 REGEXP_LIKE("emp_id", '[A-Z][A-Z][A-Z][1-9][0-9][0-9][0-9][0-9][FM]') OR 
 REGEXP_LIKE("emp_id", '[A-Z]-[A-Z][1-9][0-9][0-9][0-9][0-9][FM]')
);

COMMIT;
