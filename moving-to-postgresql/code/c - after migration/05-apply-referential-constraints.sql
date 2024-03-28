/******************************************************
 *
 * Name:         05-apply-referential-constraints.sql
 *     
 * Design Phase:
 *     Author:   John Miner
 *     Date:     01-01-2024
 *     Purpose:  Add constraints to tables.
 * 
 ******************************************************/


BEGIN;

ALTER TABLE "dbo"."authors" ADD CONSTRAINT "upkcl_auidind" PRIMARY KEY ("au_id");
ALTER TABLE "dbo"."employee" ADD CONSTRAINT "pk_emp_id" PRIMARY KEY ("emp_id");
ALTER TABLE "dbo"."jobs" ADD PRIMARY KEY ("job_id");
ALTER TABLE "dbo"."pub_info" ADD CONSTRAINT "upkcl_pubinfo" PRIMARY KEY ("pub_id");
ALTER TABLE "dbo"."publishers" ADD CONSTRAINT "upkcl_pubind" PRIMARY KEY ("pub_id");
ALTER TABLE "dbo"."sales" ADD CONSTRAINT "upkcl_sales" PRIMARY KEY ("stor_id","ord_num","title_id");
ALTER TABLE "dbo"."stores" ADD CONSTRAINT "upk_storeid" PRIMARY KEY ("stor_id");
ALTER TABLE "dbo"."titleauthor" ADD CONSTRAINT "upkcl_taind" PRIMARY KEY ("au_id","title_id");
ALTER TABLE "dbo"."titles" ADD CONSTRAINT "upkcl_titleidind" PRIMARY KEY ("title_id");
ALTER TABLE "dbo"."discounts" ADD FOREIGN KEY ("stor_id") REFERENCES "dbo"."stores" ( "stor_id");
ALTER TABLE "dbo"."employee" ADD FOREIGN KEY ("job_id") REFERENCES "dbo"."jobs" ( "job_id");
ALTER TABLE "dbo"."employee" ADD FOREIGN KEY ("pub_id") REFERENCES "dbo"."publishers" ( "pub_id");
ALTER TABLE "dbo"."pub_info" ADD FOREIGN KEY ("pub_id") REFERENCES "dbo"."publishers" ( "pub_id");
ALTER TABLE "dbo"."roysched" ADD FOREIGN KEY ("title_id") REFERENCES "dbo"."titles" ( "title_id");
ALTER TABLE "dbo"."sales" ADD FOREIGN KEY ("stor_id") REFERENCES "dbo"."stores" ( "stor_id");
ALTER TABLE "dbo"."sales" ADD FOREIGN KEY ("title_id") REFERENCES "dbo"."titles" ( "title_id");
ALTER TABLE "dbo"."titleauthor" ADD FOREIGN KEY ("au_id") REFERENCES "dbo"."authors" ( "au_id");
ALTER TABLE "dbo"."titleauthor" ADD FOREIGN KEY ("title_id") REFERENCES "dbo"."titles" ( "title_id");
ALTER TABLE "dbo"."titles" ADD FOREIGN KEY ("pub_id") REFERENCES "dbo"."publishers" ( "pub_id");

COMMIT;
